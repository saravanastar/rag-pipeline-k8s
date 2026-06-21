import logging
import redis

logger = logging.getLogger(__name__)

# Redis hash key that maps URL → SHA-256 of its last-seen content.
_HASH_KEY = "seen-pages"


class SeenPagesStore:
    def __init__(self, redis_url: str) -> None:
        self._client = redis.from_url(redis_url, decode_responses=True)

    def ping(self) -> bool:
        try:
            return self._client.ping()
        except redis.RedisError as e:
            logger.error("redis ping failed: %s", e)
            return False

    def get_hash(self, url: str) -> str | None:
        """Return the stored content hash for url, or None if unseen."""
        return self._client.hget(_HASH_KEY, url)

    def set_hash(self, url: str, content_hash: str) -> None:
        """Store the content hash for url."""
        self._client.hset(_HASH_KEY, url, content_hash)

    def is_changed(self, url: str, content_hash: str) -> bool:
        """Return True if url is new or its stored hash differs from content_hash."""
        stored = self.get_hash(url)
        return stored is None or stored != content_hash
