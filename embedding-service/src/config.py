import os
from dataclasses import dataclass


@dataclass
class Config:
    kafka_bootstrap_servers: str = "kafka:9092"
    kafka_topic_in: str          = "chunk.ready"
    kafka_consumer_group: str    = "embedding-group"
    milvus_host: str             = "milvus"
    milvus_port: int             = 19530
    milvus_collection: str       = "rag_chunks"
    # model_name: must be a sentence-transformers model identifier.
    # all-MiniLM-L6-v2 produces 384-dim embeddings, runs on CPU in ~5ms/chunk.
    # Changing this requires re-creating the Milvus collection (different dim).
    model_name: str              = "all-MiniLM-L6-v2"
    # embed_batch_size: number of chunks to accumulate before a single model
    # inference call. Larger batches amortize model overhead but increase
    # Kafka commit latency. 32 is a practical sweet spot for CPU inference.
    embed_batch_size: int        = 32
    # batch_timeout_seconds: max time to wait before flushing a partial batch.
    # Prevents chunks from sitting in the buffer indefinitely when lag is low.
    batch_timeout_seconds: float = 2.0
    prometheus_port: int         = 9090
    api_port: int                = 8000
    kafka_acks: str              = "all"
    max_poll_interval_ms: int    = 300_000
    poll_timeout_seconds: float  = 1.0

    @classmethod
    def from_env(cls) -> "Config":
        return cls(
            kafka_bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS",  cls.kafka_bootstrap_servers),
            kafka_topic_in          = os.environ.get("KAFKA_TOPIC_IN",           cls.kafka_topic_in),
            kafka_consumer_group    = os.environ.get("KAFKA_CONSUMER_GROUP",     cls.kafka_consumer_group),
            milvus_host             = os.environ.get("MILVUS_HOST",              cls.milvus_host),
            milvus_port             = int(os.environ.get("MILVUS_PORT",          cls.milvus_port)),
            milvus_collection       = os.environ.get("MILVUS_COLLECTION",        cls.milvus_collection),
            model_name              = os.environ.get("MODEL_NAME",               cls.model_name),
            embed_batch_size        = int(os.environ.get("EMBED_BATCH_SIZE",     cls.embed_batch_size)),
            batch_timeout_seconds   = float(os.environ.get("BATCH_TIMEOUT_SECONDS", cls.batch_timeout_seconds)),
            prometheus_port         = int(os.environ.get("PROMETHEUS_PORT",      cls.prometheus_port)),
            api_port                = int(os.environ.get("API_PORT",             cls.api_port)),
            kafka_acks              = os.environ.get("KAFKA_ACKS",               cls.kafka_acks),
            max_poll_interval_ms    = int(os.environ.get("MAX_POLL_INTERVAL_MS", cls.max_poll_interval_ms)),
            poll_timeout_seconds    = float(os.environ.get("POLL_TIMEOUT_SECONDS", cls.poll_timeout_seconds)),
        )
