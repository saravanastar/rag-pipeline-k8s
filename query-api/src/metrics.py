from prometheus_client import Counter, Histogram, start_http_server


requests_total = Counter(
    "query_api_requests_total",
    "Total query requests",
    ["status"],          # "success" | "error"
)
query_latency = Histogram(
    "query_api_latency_seconds",
    "End-to-end query latency (embed + search)",
    buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0],
)
embed_call_latency = Histogram(
    "query_api_embed_call_latency_seconds",
    "Time spent calling the embedding service",
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25],
)
milvus_search_latency = Histogram(
    "query_api_milvus_search_latency_seconds",
    "Time spent on the Milvus ANN search",
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25],
)


def start_metrics_server(port: int) -> None:
    start_http_server(port)
