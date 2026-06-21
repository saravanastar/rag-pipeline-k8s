import logging
import time

from pymilvus import Collection, connections, utility

import metrics
from milvus_schema import COLLECTION_NAME, INDEX_PARAMS, SCHEMA

logger = logging.getLogger(__name__)


class MilvusClient:
    def __init__(self, host: str, port: int, collection_name: str = COLLECTION_NAME) -> None:
        self._collection_name = collection_name
        self._collection: Collection | None = None

        logger.info("connecting to Milvus at %s:%d", host, port)
        connections.connect(host=host, port=port)

        self._ensure_collection()

    def _ensure_collection(self) -> None:
        if not utility.has_collection(self._collection_name):
            # Fallback: create the collection if the Helm init job hasn't run yet.
            # The init job is the preferred path; this is a safety net for local dev.
            logger.warning(
                "collection '%s' not found — creating now (should have been created by milvus-init job)",
                self._collection_name,
            )
            col = Collection(name=self._collection_name, schema=SCHEMA)
            col.create_index(field_name="embedding", index_params=INDEX_PARAMS)

        self._collection = Collection(self._collection_name)
        self._collection.load()
        logger.info("collection '%s' loaded", self._collection_name)

    def insert(self, rows: list[dict]) -> None:
        """
        Insert a batch of chunk rows into Milvus.

        Each dict must have keys matching SCHEMA fields. Raises on failure —
        the caller (consumer loop) will NOT commit Kafka offsets on exception.
        """
        if not rows:
            return

        # Milvus Python SDK expects column-oriented data: a list of lists,
        # one per field, in the same order as the schema field definitions.
        data = [
            [r["chunk_id"]      for r in rows],
            [r["url"]           for r in rows],
            [r["title"]         for r in rows],
            [r["chunk_index"]   for r in rows],
            [r["total_chunks"]  for r in rows],
            [r["text"]          for r in rows],
            [r["content_hash"]  for r in rows],
            [r["created_at"]    for r in rows],
            [r["embedding"]     for r in rows],
        ]

        with metrics.milvus_insert_latency.time():
            self._collection.insert(data)
            # flush() makes the insert durable and immediately searchable.
            # Without flush, inserts may sit in a buffer and be invisible to queries.
            self._collection.flush()

        logger.debug("inserted %d chunks into Milvus", len(rows))

    def search(
        self,
        query_vector: list[float],
        top_k: int = 5,
        output_fields: list[str] | None = None,
    ) -> list[dict]:
        """Vector similarity search. Returns top_k results sorted by score desc."""
        if output_fields is None:
            output_fields = ["chunk_id", "url", "title", "chunk_index", "text", "score"]

        results = self._collection.search(
            data=[query_vector],
            anns_field="embedding",
            param={"metric_type": "COSINE", "params": {"nprobe": 16}},
            limit=top_k,
            output_fields=[f for f in output_fields if f != "score"],
        )

        hits = []
        for hit in results[0]:
            row = {field: hit.entity.get(field) for field in output_fields if field != "score"}
            row["score"] = hit.score
            hits.append(row)
        return hits

    def ping(self) -> bool:
        try:
            return utility.has_collection(self._collection_name)
        except Exception:
            return False
