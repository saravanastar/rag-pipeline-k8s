from prometheus_client import Counter, Gauge, Histogram, start_http_server


pages_consumed = Counter(
    "chunker_pages_consumed_total",
    "Total page.crawled events consumed",
)
chunks_produced = Counter(
    "chunker_chunks_produced_total",
    "Total chunk.ready events published",
)
pages_errored = Counter(
    "chunker_pages_errored_total",
    "Pages that failed to chunk or produce",
    ["reason"],
)
kafka_consumer_lag = Gauge(
    "chunker_kafka_consumer_lag",
    "Current consumer lag on the page.crawled topic (mirrors KEDA trigger signal)",
)
chunk_size_tokens = Histogram(
    "chunker_chunk_size_tokens",
    "Distribution of actual chunk sizes in tokens",
    buckets=[64, 128, 256, 384, 512, 640],
)
chunks_per_page = Histogram(
    "chunker_chunks_per_page",
    "Number of chunks produced per page",
    buckets=[1, 2, 5, 10, 20, 50],
)


def start_metrics_server(port: int) -> None:
    start_http_server(port)
