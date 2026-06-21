import os
from dataclasses import dataclass, field


@dataclass
class Config:
    target_sitemap_url: str       = "https://kubernetes.io/sitemap.xml"
    sitemap_url_filter: str       = "/docs/concepts/"
    kafka_bootstrap_servers: str  = "kafka:9092"
    kafka_topic_out: str          = "page.crawled"
    redis_url: str                = "redis://redis:6379"
    crawl_delay_seconds: float    = 1.0
    prometheus_port: int          = 9090
    # User-agent sent with every HTTP request. Be honest about what we are.
    user_agent: str               = "rag-pipeline-crawler/1.0 (+https://github.com/your-org/rag-pipeline-k8s)"
    # Hard cap on pages per run — protects against sitemap explosions in local dev.
    max_pages: int                = 500
    # HTTP request timeout in seconds.
    request_timeout: int          = 15
    # Kafka producer acks: "all" for durability, "1" for speed, "0" for fire-and-forget.
    kafka_acks: str               = "all"

    @classmethod
    def from_env(cls) -> "Config":
        return cls(
            target_sitemap_url      = os.environ.get("TARGET_SITEMAP_URL",       cls.target_sitemap_url),
            sitemap_url_filter      = os.environ.get("SITEMAP_URL_FILTER",       cls.sitemap_url_filter),
            kafka_bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS",  cls.kafka_bootstrap_servers),
            kafka_topic_out         = os.environ.get("KAFKA_TOPIC_OUT",          cls.kafka_topic_out),
            redis_url               = os.environ.get("REDIS_URL",                cls.redis_url),
            crawl_delay_seconds     = float(os.environ.get("CRAWL_DELAY_SECONDS", cls.crawl_delay_seconds)),
            prometheus_port         = int(os.environ.get("PROMETHEUS_PORT",       cls.prometheus_port)),
            user_agent              = os.environ.get("USER_AGENT",               cls.user_agent),
            max_pages               = int(os.environ.get("MAX_PAGES",             cls.max_pages)),
            request_timeout         = int(os.environ.get("REQUEST_TIMEOUT",       cls.request_timeout)),
            kafka_acks              = os.environ.get("KAFKA_ACKS",               cls.kafka_acks),
        )
