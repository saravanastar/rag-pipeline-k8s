import os
from dataclasses import dataclass


@dataclass
class Config:
    kafka_bootstrap_servers: str = "kafka:9092"
    kafka_topic_in: str          = "page.crawled"
    kafka_topic_out: str         = "chunk.ready"
    kafka_consumer_group: str    = "chunker-group"
    # chunk_size_tokens: target window size in tokens.
    # 512 matches the MiniLM-L6-v2 max sequence length exactly; no truncation
    # happens in the embedding service when chunks are this size or smaller.
    chunk_size_tokens: int       = 512
    # chunk_overlap_tokens: how many tokens the next chunk re-uses from the end
    # of the previous chunk. 64 tokens (~3-4 sentences) is enough to preserve
    # context across the boundary without doubling storage.
    chunk_overlap_tokens: int    = 64
    prometheus_port: int         = 9090
    # poll_timeout_seconds: how long Consumer.poll() blocks waiting for a message.
    # Short timeout keeps the shutdown-signal check loop responsive.
    poll_timeout_seconds: float  = 1.0
    # kafka_acks: "all" for durability. chunk.ready events drive model inference;
    # losing one silently would leave an un-embedded chunk with no retry path.
    kafka_acks: str              = "all"
    # max_poll_interval_ms: how long between polls before the broker considers
    # the consumer dead and triggers a rebalance. Set high because chunking
    # a large page + producing N chunks can take several seconds.
    max_poll_interval_ms: int    = 300_000

    @classmethod
    def from_env(cls) -> "Config":
        return cls(
            kafka_bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", cls.kafka_bootstrap_servers),
            kafka_topic_in          = os.environ.get("KAFKA_TOPIC_IN",          cls.kafka_topic_in),
            kafka_topic_out         = os.environ.get("KAFKA_TOPIC_OUT",         cls.kafka_topic_out),
            kafka_consumer_group    = os.environ.get("KAFKA_CONSUMER_GROUP",    cls.kafka_consumer_group),
            chunk_size_tokens       = int(os.environ.get("CHUNK_SIZE_TOKENS",   cls.chunk_size_tokens)),
            chunk_overlap_tokens    = int(os.environ.get("CHUNK_OVERLAP_TOKENS", cls.chunk_overlap_tokens)),
            prometheus_port         = int(os.environ.get("PROMETHEUS_PORT",     cls.prometheus_port)),
            poll_timeout_seconds    = float(os.environ.get("POLL_TIMEOUT_SECONDS", cls.poll_timeout_seconds)),
            kafka_acks              = os.environ.get("KAFKA_ACKS",              cls.kafka_acks),
            max_poll_interval_ms    = int(os.environ.get("MAX_POLL_INTERVAL_MS", cls.max_poll_interval_ms)),
        )
