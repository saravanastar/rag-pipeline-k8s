from prometheus_client import Counter, Gauge, Histogram, start_http_server


chunks_embedded = Counter(
    "embedding_chunks_embedded_total",
    "Total chunks embedded and inserted into Milvus",
)
embed_errors = Counter(
    "embedding_errors_total",
    "Embedding or Milvus insert errors",
    ["reason"],
)
kafka_consumer_lag = Gauge(
    "embedding_kafka_consumer_lag",
    "Current consumer lag on the chunk.ready topic",
)
# throughput_chunks_per_sec is a Gauge updated by main.py from a rolling window.
throughput = Gauge(
    "embedding_throughput_chunks_per_sec",
    "Rolling 60s embedding throughput",
)
inference_latency = Histogram(
    "embedding_inference_latency_seconds",
    "Model inference time per batch",
    buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0],
)
milvus_insert_latency = Histogram(
    "embedding_milvus_insert_latency_seconds",
    "Milvus insert time per batch",
    buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5],
)
batch_size_hist = Histogram(
    "embedding_batch_size",
    "Number of chunks per embedding batch",
    buckets=[1, 4, 8, 16, 32, 64],
)
api_embed_latency = Histogram(
    "embedding_api_latency_seconds",
    "POST /embed end-to-end latency (used by query-api)",
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1],
)


def start_metrics_server(port: int) -> None:
    start_http_server(port)
