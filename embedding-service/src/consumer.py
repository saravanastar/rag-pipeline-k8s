"""
Batching Kafka consumer for chunk.ready events.

Batching design:
  Collect up to `batch_size` messages OR wait up to `timeout_seconds` —
  whichever comes first — then embed and insert as a single batch.
  This amortises model inference overhead: a single SentenceTransformer.encode()
  call on 32 texts is ~4x faster than 32 individual calls due to batched
  matrix operations.

Offset commit strategy (same reasoning as chunker):
  Offsets are committed only AFTER a successful Milvus insert. If the pod dies
  between insert and commit, the batch is re-consumed and re-embedded. Milvus
  upserts on chunk_id (VARCHAR primary key) make this idempotent.
"""
import json
import logging
import signal
import threading
import time
from typing import Iterator

from confluent_kafka import Consumer, KafkaError, KafkaException, Message

logger = logging.getLogger(__name__)


class ChunkReadyConsumer:
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        group_id: str,
        poll_timeout: float = 1.0,
        max_poll_interval_ms: int = 300_000,
    ) -> None:
        self._topic = topic
        self._poll_timeout = poll_timeout
        self._shutdown = threading.Event()

        self._consumer = Consumer({
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "max.poll.interval.ms": max_poll_interval_ms,
            "heartbeat.interval.ms": 3_000,
            "session.timeout.ms": 30_000,
        })
        self._consumer.subscribe([topic])

        signal.signal(signal.SIGTERM, self._handle_sigterm)
        signal.signal(signal.SIGINT, self._handle_sigterm)

    def _handle_sigterm(self, signum, frame) -> None:
        logger.info("shutdown signal — draining current batch and exiting")
        self._shutdown.set()

    def batches(
        self,
        batch_size: int,
        timeout_seconds: float,
    ) -> Iterator[list[tuple[Message, dict]]]:
        """
        Yield batches of (raw_message, parsed_payload) pairs.

        A batch is emitted when either:
          - batch_size messages have been accumulated, or
          - timeout_seconds elapsed since the first message in the batch.

        Yields an empty list only on shutdown — callers should break on that.
        """
        batch: list[tuple[Message, dict]] = []
        batch_start: float | None = None

        while not self._shutdown.is_set():
            msg = self._consumer.poll(timeout=self._poll_timeout)

            if msg is None:
                # Flush partial batch on timeout so chunks don't sit buffered
                # indefinitely when Kafka lag drops below batch_size.
                if batch and batch_start and (time.monotonic() - batch_start) >= timeout_seconds:
                    yield batch
                    batch = []
                    batch_start = None
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            try:
                payload = json.loads(msg.value().decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                logger.error("malformed message at offset %d: %s", msg.offset(), e)
                self._consumer.commit(message=msg, asynchronous=False)
                continue

            if batch_start is None:
                batch_start = time.monotonic()
            batch.append((msg, payload))

            if len(batch) >= batch_size:
                yield batch
                batch = []
                batch_start = None

        # Yield the final partial batch before shutdown.
        if batch:
            yield batch

    def commit_batch(self, messages: list[Message]) -> None:
        """Commit the offset of the last message in the batch (covers all earlier offsets)."""
        if messages:
            self._consumer.commit(message=messages[-1], asynchronous=False)

    def close(self) -> None:
        self._consumer.close()
        logger.info("embedding consumer closed")

    @property
    def raw_consumer(self):
        return self._consumer
