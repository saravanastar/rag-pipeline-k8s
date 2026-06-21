import logging
import sys

import uvicorn

import api
import metrics
from config import Config
from embedding_client import EmbeddingServiceClient
from milvus_client import MilvusSearchClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


def main() -> None:
    cfg = Config.from_env()
    logger.info(
        "starting query-api — embed_svc=%s milvus=%s:%d collection=%s",
        cfg.embedding_service_url, cfg.milvus_host, cfg.milvus_port, cfg.milvus_collection,
    )

    metrics.start_metrics_server(cfg.prometheus_port)

    embed_client = EmbeddingServiceClient(
        base_url=cfg.embedding_service_url,
        timeout=cfg.embed_timeout_seconds,
    )

    try:
        milvus_client = MilvusSearchClient(
            host=cfg.milvus_host,
            port=cfg.milvus_port,
            collection_name=cfg.milvus_collection,
            nprobe=cfg.milvus_nprobe,
        )
    except RuntimeError as e:
        # Collection missing = milvus-init job hasn't run. Surface clearly.
        logger.error("%s", e)
        sys.exit(1)

    api.set_clients(embed_client, milvus_client)
    logger.info("clients ready — starting API on port %d", cfg.api_port)

    uvicorn.run(
        "api:app",
        host="0.0.0.0",
        port=cfg.api_port,
        log_level="warning",
        # workers > 1 would fork and duplicate the Milvus connection.
        # Horizontal scale is handled by the Deployment replicaCount instead.
        workers=1,
    )


if __name__ == "__main__":
    main()
