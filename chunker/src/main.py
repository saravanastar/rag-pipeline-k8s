import logging
import sys
import uuid
from datetime import datetime, timezone

from config import Config
from chunker import chunk_text, token_count
from consumer import PageCrawledConsumer
from metrics import (
    chunks_per_page,
    chunks_produced,
    chunk_size_tokens,
    kafka_consumer_lag,
    pages_consumed,
    pages_errored,
    start_metrics_server,
)
from producer import ChunkReadyProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


def process_page(event: dict, cfg: Config, producer: ChunkReadyProducer) -> int:
    """
    Chunk a single page.crawled event and emit chunk.ready events.
    Returns the number of chunks produced.
    """
    url          = event.get("url", "")
    text         = event.get("text", "")
    content_hash = event.get("content_hash", "")

    if not text:
        logger.warning("empty text in page.crawled event for %s — skipping", url)
        return 0

    raw_chunks = chunk_text(text, cfg.chunk_size_tokens, cfg.chunk_overlap_tokens)
    total      = len(raw_chunks)
    now        = int(datetime.now(timezone.utc).timestamp())

    chunk_events: list[dict] = []
    for idx, chunk in enumerate(raw_chunks):
        chunk_events.append({
            "chunk_id":     str(uuid.uuid4()),
            "url":          url,
            "title":        event.get("title", ""),
            "chunk_index":  idx,
            "total_chunks": total,
            "text":         chunk,
            "content_hash": content_hash,
            "created_at":   now,
        })
        chunk_size_tokens.observe(token_count(chunk))

    producer.emit_batch(chunk_events)
    # flush() after each page ensures all chunk.ready events are delivered
    # before we commit the page.crawled offset. If flush() raises, the
    # consumer will NOT commit and the page will be retried.
    producer.flush()

    chunks_produced.inc(total)
    chunks_per_page.observe(total)
    return total


def main() -> None:
    cfg = Config.from_env()
    logger.info(
        "starting chunker — topic_in=%s topic_out=%s chunk_size=%d overlap=%d",
        cfg.kafka_topic_in, cfg.kafka_topic_out, cfg.chunk_size_tokens, cfg.chunk_overlap_tokens,
    )

    start_metrics_server(cfg.prometheus_port)

    consumer = PageCrawledConsumer(
        bootstrap_servers  = cfg.kafka_bootstrap_servers,
        topic              = cfg.kafka_topic_in,
        group_id           = cfg.kafka_consumer_group,
        poll_timeout       = cfg.poll_timeout_seconds,
        max_poll_interval_ms = cfg.max_poll_interval_ms,
    )
    producer = ChunkReadyProducer(
        bootstrap_servers = cfg.kafka_bootstrap_servers,
        topic             = cfg.kafka_topic_out,
        acks              = cfg.kafka_acks,
    )

    try:
        for msg, event in consumer.messages():
            url = event.get("url", "<unknown>")
            logger.info("processing page: %s", url)
            pages_consumed.inc()

            try:
                n_chunks = process_page(event, cfg, producer)
                consumer.commit(msg)
                logger.info("chunked %s → %d chunks", url, n_chunks)
            except Exception as e:
                logger.error("failed to process %s: %s", url, e, exc_info=True)
                pages_errored.labels(reason=type(e).__name__).inc()
                # Do NOT commit: let Kafka redeliver this message after a restart.
                # This makes the chunker at-least-once, not at-most-once.

            # Update lag gauge. confluent-kafka exposes this via consumer_group_lag
            # but a simpler approximation: just report assignment watermarks.
            # A dedicated lag exporter (kminion, kafka-lag-exporter) is more accurate
            # for the KEDA trigger; this metric is for the Grafana panel only.
            try:
                partitions = consumer._consumer.assignment()
                total_lag = sum(
                    max(0, hi - consumer._consumer.position([p])[0].offset)
                    for p in partitions
                    for lo, hi in [consumer._consumer.get_watermark_offsets(p)]
                )
                kafka_consumer_lag.set(total_lag)
            except Exception:
                pass  # lag is best-effort; never let it crash the main loop

    finally:
        consumer.close()
        logger.info("chunker shut down cleanly")


if __name__ == "__main__":
    main()
