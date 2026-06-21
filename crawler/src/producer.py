import json
import logging
from confluent_kafka import Producer, KafkaException

logger = logging.getLogger(__name__)


class PageCrawledProducer:
    def __init__(self, bootstrap_servers: str, topic: str, acks: str = "all") -> None:
        self._topic = topic
        self._producer = Producer({
            "bootstrap.servers": bootstrap_servers,
            "acks": acks,
            # Retry transient broker errors up to 3 times before raising.
            "retries": 3,
            "retry.backoff.ms": 500,
        })

    def emit(self, event: dict) -> None:
        """Publish a page.crawled event. Blocks until delivery confirmation."""
        payload = json.dumps(event, ensure_ascii=False).encode("utf-8")
        # url is the natural key — routes all events for the same URL to the
        # same partition, so consumers see them in order if needed.
        key = event["url"].encode("utf-8")

        self._producer.produce(
            topic=self._topic,
            key=key,
            value=payload,
            on_delivery=self._on_delivery,
        )
        # poll(0) serves delivery callbacks without blocking.
        # flush() at the end of the run guarantees all messages are sent.
        self._producer.poll(0)

    def flush(self) -> None:
        remaining = self._producer.flush(timeout=30)
        if remaining:
            raise KafkaException(f"{remaining} messages not delivered after flush")

    @staticmethod
    def _on_delivery(err, msg) -> None:
        if err:
            logger.error("delivery failed for %s: %s", msg.key(), err)
        else:
            logger.debug("delivered to %s [%d] @ %d", msg.topic(), msg.partition(), msg.offset())
