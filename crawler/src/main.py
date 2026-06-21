import logging
import sys

from config import Config
from crawler import run_crawl
from metrics import start_metrics_server
from producer import PageCrawledProducer
from redis_store import SeenPagesStore

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


def main() -> None:
    cfg = Config.from_env()
    logger.info("starting crawler — sitemap=%s filter=%s", cfg.target_sitemap_url, cfg.sitemap_url_filter)

    start_metrics_server(cfg.prometheus_port)

    store = SeenPagesStore(cfg.redis_url)
    if not store.ping():
        logger.error("cannot connect to Redis at %s — aborting", cfg.redis_url)
        sys.exit(1)
    logger.info("redis connected")

    producer = PageCrawledProducer(
        bootstrap_servers=cfg.kafka_bootstrap_servers,
        topic=cfg.kafka_topic_out,
        acks=cfg.kafka_acks,
    )
    logger.info("kafka producer ready — topic=%s", cfg.kafka_topic_out)

    try:
        run_crawl(cfg, store, producer)
    except Exception:
        logger.exception("crawl failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
