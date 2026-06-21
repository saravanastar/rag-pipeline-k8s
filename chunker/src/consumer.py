"""
Kafka consumer with graceful SIGTERM handling.

Design notes:
- Manual offset commit (enable.auto.commit=false): we commit only AFTER all
  chunks for a page have been successfully produced to chunk.ready. If the
  chunker crashes mid-page, the page.crawled event will be re-consumed and
  re-chunked on restart. Duplicate chunk.ready events are idempotent at the
  embedding service because Milvus upserts on chunk_id.
- SIGTERM sets a shutdown flag; the poll loop drains and exits cleanly. This
  prevents Kubernetes from force-killing the pod mid-batch (the default 30s
  grace period is enough for a single page).
"""
import json
import logging
import signal
import threading
from typing import Iterator

from confluent_kafka import Consumer, KafkaError, KafkaException, Message

logger = logging.getLogger(__name__)


class PageCrawledConsumer:
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
            # Manual commit: offset advances only after all chunks for a page are produced.
            "enable.auto.commit": False,
            "max.poll.interval.ms": max_poll_interval_ms,
            # Heartbeat more frequently than the poll interval to avoid spurious rebalances.
            "heartbeat.interval.ms": 3_000,
            "session.timeout.ms": 30_000,
        })
        self._consumer.subscribe([topic])

        # Register SIGTERM handler so Kubernetes pod shutdown is graceful.
        signal.signal(signal.SIGTERM, self._handle_sigterm)
        signal.signal(signal.SIGINT, self._handle_sigterm)

    def _handle_sigterm(self, signum, frame) -> None:
        logger.info("shutdown signal received — draining and exiting")
        self._shutdown.set()

    def messages(self) -> Iterator[tuple[Message, dict]]:
        """
        Yield (raw_message, parsed_payload) pairs until shutdown is signalled.
        Caller must call commit(message) after successfully processing each message.
        """
        while not self._shutdown.is_set():
            msg = self._consumer.poll(timeout=self._poll_timeout)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.debug("reached end of partition %d", msg.partition())
                    continue
                raise KafkaException(msg.error())

            try:
                payload = json.loads(msg.value().decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                logger.error("failed to decode message at offset %d: %s", msg.offset(), e)
                # Commit and skip malformed messages rather than blocking the consumer.
                self._consumer.commit(message=msg, asynchronous=False)
                continue

            yield msg, payload

    def commit(self, message: Message) -> None:
        self._consumer.commit(message=message, asynchronous=False)

    def close(self) -> None:
        self._consumer.close()
        logger.info("consumer closed")
