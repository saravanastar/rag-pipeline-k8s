from prometheus_client import Counter, Gauge, start_http_server


pages_crawled = Counter(
    "crawler_pages_crawled_total",
    "Total pages fetched from the sitemap",
)
pages_emitted = Counter(
    "crawler_pages_emitted_total",
    "Pages emitted as page.crawled events (new or content-changed)",
)
pages_skipped = Counter(
    "crawler_pages_skipped_total",
    "Pages skipped because content hash matched the stored value",
)
pages_errored = Counter(
    "crawler_pages_errored_total",
    "Pages that failed to fetch or parse",
    ["reason"],
)
sitemap_urls_found = Gauge(
    "crawler_sitemap_urls_found",
    "Number of URLs found in the sitemap matching the filter",
)


def start_metrics_server(port: int) -> None:
    start_http_server(port)
