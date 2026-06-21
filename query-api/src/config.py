import os
from dataclasses import dataclass


@dataclass
class Config:
    embedding_service_url: str = "http://embedding-service:8000"
    milvus_host: str           = "milvus"
    milvus_port: int           = 19530
    milvus_collection: str     = "rag_chunks"
    default_top_k: int         = 5
    # max_top_k: hard ceiling on k to prevent accidental full-table scans via the API.
    max_top_k: int             = 50
    api_port: int              = 8000
    prometheus_port: int       = 9090
    # embed_timeout_seconds: the embedding service does CPU inference; 5s is generous.
    embed_timeout_seconds: float = 5.0
    # milvus_nprobe: number of IVF cells to search. Higher = better recall, slower query.
    # 16 gives >95% recall for IVF_FLAT with nlist=128 at <500K vectors.
    milvus_nprobe: int         = 16

    @classmethod
    def from_env(cls) -> "Config":
        return cls(
            embedding_service_url  = os.environ.get("EMBEDDING_SERVICE_URL",  cls.embedding_service_url),
            milvus_host            = os.environ.get("MILVUS_HOST",            cls.milvus_host),
            milvus_port            = int(os.environ.get("MILVUS_PORT",        cls.milvus_port)),
            milvus_collection      = os.environ.get("MILVUS_COLLECTION",      cls.milvus_collection),
            default_top_k          = int(os.environ.get("DEFAULT_TOP_K",      cls.default_top_k)),
            max_top_k              = int(os.environ.get("MAX_TOP_K",          cls.max_top_k)),
            api_port               = int(os.environ.get("API_PORT",           cls.api_port)),
            prometheus_port        = int(os.environ.get("PROMETHEUS_PORT",    cls.prometheus_port)),
            embed_timeout_seconds  = float(os.environ.get("EMBED_TIMEOUT_SECONDS", cls.embed_timeout_seconds)),
            milvus_nprobe          = int(os.environ.get("MILVUS_NPROBE",     cls.milvus_nprobe)),
        )
