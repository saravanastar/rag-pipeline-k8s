"""
Dual-mode entrypoint for the embedding service:

  Thread 1 (main):   Kafka consumer loop — batch consume → embed → Milvus insert → commit
  Thread 2 (daemon): uvicorn serving FastAPI on :8000 (POST /embed, GET /health)
  Thread 3 (daemon): Prometheus metrics server on :9090

Both threads share the same embedder module (with its internal threading.Lock)
and the same MilvusClient instance.

Startup order matters:
  1. Load the model (slow, ~10s) before starting the API server so /health
     returns 200 only when inference is actually ready.
  2. Connect to Milvus and verify the collection exists.
  3. Start the API server (now /health returns 200, KEDA can scale in).
  4. Start the consumer loop.

On SIGTERM: the consumer.batches() iterator drains the current batch, commits,
and returns. uvicorn receives its own SIGTERM from Kubernetes and shuts down
independently. terminationGracePeriodSeconds=90 in the Deployment covers both.
"""
import logging
import sys
import threading
import time
from collections import deque

import uvicorn

import api
import embedder
import metrics
from config import Config
from consumer import ChunkReadyConsumer
from milvus_client import MilvusClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


def _run_api(cfg: Config) -> None:
    uvicorn.run(
        "api:app",
        host="0.0.0.0",
        port=cfg.api_port,
        log_level="warning",
        # Workers=1: multiple workers would each load their own model copy.
        # The Kafka consumer is in the main thread, not uvicorn, so 1 worker is fine.
        workers=1,
    )


def _update_throughput(recent_timestamps: deque, window_seconds: float = 60.0) -> None:
    """Remove stale timestamps and update the throughput gauge."""
    cutoff = time.monotonic() - window_seconds
    while recent_timestamps and recent_timestamps[0] < cutoff:
        recent_timestamps.popleft()
    metrics.throughput.set(len(recent_timestamps) / window_seconds)


def run_consumer_loop(cfg: Config, milvus: MilvusClient) -> None:
    consumer = ChunkReadyConsumer(
        bootstrap_servers    = cfg.kafka_bootstrap_servers,
        topic                = cfg.kafka_topic_in,
        group_id             = cfg.kafka_consumer_group,
        poll_timeout         = cfg.poll_timeout_seconds,
        max_poll_interval_ms = cfg.max_poll_interval_ms,
    )

    # Rolling window of per-chunk completion timestamps for throughput gauge.
    recent_timestamps: deque = deque()

    try:
        for batch in consumer.batches(cfg.embed_batch_size, cfg.batch_timeout_seconds):
            if not batch:
                continue

            msgs, events = zip(*batch)
            texts      = [e["text"]         for e in events]
            chunk_ids  = [e["chunk_id"]      for e in events]
            urls       = [e.get("url", "")   for e in events]
            titles     = [e.get("title", "") for e in events]

            metrics.batch_size_hist.observe(len(texts))

            try:
                vectors = embedder.embed(texts)
            except Exception as e:
                logger.error("embed failed for batch of %d: %s", len(texts), e)
                metrics.embed_errors.labels(reason="embed_error").inc()
                # Do NOT commit — redeliver on restart.
                continue

            rows = [
                {
                    "chunk_id":     chunk_ids[i],
                    "url":          urls[i],
                    "title":        titles[i],
                    "chunk_index":  events[i].get("chunk_index", 0),
                    "total_chunks": events[i].get("total_chunks", 1),
                    "text":         texts[i],
                    "content_hash": events[i].get("content_hash", ""),
                    "created_at":   events[i].get("created_at", int(time.time())),
                    "embedding":    vectors[i],
                }
                for i in range(len(texts))
            ]

            try:
                milvus.insert(rows)
            except Exception as e:
                logger.error("milvus insert failed for batch of %d: %s", len(rows), e)
                metrics.embed_errors.labels(reason="milvus_error").inc()
                continue

            consumer.commit_batch(list(msgs))
            metrics.chunks_embedded.inc(len(rows))

            now = time.monotonic()
            recent_timestamps.extend([now] * len(rows))
            _update_throughput(recent_timestamps)

            # Update consumer lag gauge (best-effort).
            try:
                partitions = consumer.raw_consumer.assignment()
                lag = sum(
                    max(0, hi - consumer.raw_consumer.position([p])[0].offset)
                    for p in partitions
                    for lo, hi in [consumer.raw_consumer.get_watermark_offsets(p)]
                )
                metrics.kafka_consumer_lag.set(lag)
            except Exception:
                pass

            logger.info("embedded + inserted batch of %d chunks", len(rows))

    finally:
        consumer.close()


def main() -> None:
    cfg = Config.from_env()
    logger.info("starting embedding service — model=%s collection=%s", cfg.model_name, cfg.milvus_collection)

    # 1. Load model (slow path — must complete before /health returns 200).
    embedder.load_model(cfg.model_name)

    # 2. Connect to Milvus.
    milvus = MilvusClient(host=cfg.milvus_host, port=cfg.milvus_port, collection_name=cfg.milvus_collection)
    api.set_milvus_client(milvus)

    # 3. Start Prometheus metrics server.
    metrics.start_metrics_server(cfg.prometheus_port)

    # 4. Start FastAPI in a daemon thread (dies when main thread exits).
    api_thread = threading.Thread(target=_run_api, args=(cfg,), daemon=True, name="api")
    api_thread.start()
    logger.info("API server started on port %d", cfg.api_port)

    # 5. Run consumer loop in the main thread (owns SIGTERM handler).
    try:
        run_consumer_loop(cfg, milvus)
    except Exception:
        logger.exception("consumer loop crashed")
        sys.exit(1)

    logger.info("embedding service shut down cleanly")


if __name__ == "__main__":
    main()
