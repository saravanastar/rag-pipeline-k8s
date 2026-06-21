import json
import logging
from confluent_kafka import Producer, KafkaException

logger = logging.getLogger(__name__)


class ChunkReadyProducer:
    def __init__(self, bootstrap_servers: str, topic: str, acks: str = "all") -> None:
        self._topic = topic
        self._producer = Producer({
            "bootstrap.servers": bootstrap_servers,
            "acks": acks,
            "retries": 3,
            "retry.backoff.ms": 500,
        })

    def emit(self, event: dict) -> None:
        """Publish a chunk.ready event. Keyed by chunk_id for even partition distribution."""
        payload = json.dumps(event, ensure_ascii=False).encode("utf-8")
        key = event["chunk_id"].encode("utf-8")

        self._producer.produce(
            topic=self._topic,
            key=key,
            value=payload,
            on_delivery=self._on_delivery,
        )
        self._producer.poll(0)

    def emit_batch(self, events: list[dict]) -> None:
        for event in events:
            self.emit(event)

    def flush(self, timeout: float = 30.0) -> None:
        remaining = self._producer.flush(timeout=timeout)
        if remaining:
            raise KafkaException(f"{remaining} chunk.ready messages not delivered after flush")

    @staticmethod
    def _on_delivery(err, msg) -> None:
        if err:
            logger.error("delivery failed for chunk %s: %s", msg.key(), err)
        else:
            logger.debug("chunk delivered: %s [%d] @ %d", msg.topic(), msg.partition(), msg.offset())
