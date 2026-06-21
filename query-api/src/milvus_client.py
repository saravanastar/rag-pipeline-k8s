"""
Search-only Milvus client for the query-api.

Intentionally separate from the embedding-service's MilvusClient:
  - No insert or schema management — query-api is read-only.
  - nprobe is configurable per-request (useful for latency/recall tuning in tests).
  - output_fields are fixed to what the API response needs.
"""
import logging

from pymilvus import Collection, connections, utility

import metrics

logger = logging.getLogger(__name__)

_OUTPUT_FIELDS = ["chunk_id", "url", "title", "chunk_index", "total_chunks", "text"]


class MilvusSearchClient:
    def __init__(self, host: str, port: int, collection_name: str, nprobe: int = 16) -> None:
        self._collection_name = collection_name
        self._nprobe = nprobe

        logger.info("connecting to Milvus at %s:%d", host, port)
        connections.connect(alias="query", host=host, port=port)

        if not utility.has_collection(collection_name, using="query"):
            raise RuntimeError(
                f"Milvus collection '{collection_name}' does not exist. "
                "Run the milvus-init Helm hook before starting the query-api."
            )

        self._collection = Collection(collection_name, using="query")
        self._collection.load()
        logger.info("collection '%s' loaded for search", collection_name)

    def search(
        self,
        query_vector: list[float],
        top_k: int = 5,
    ) -> list[dict]:
        """
        ANN search over the rag_chunks collection.

        Returns up to top_k results sorted by cosine similarity (descending).
        Each result dict has: chunk_id, url, title, chunk_index, total_chunks, text, score.
        """
        with metrics.milvus_search_latency.time():
            results = self._collection.search(
                data=[query_vector],
                anns_field="embedding",
                param={
                    "metric_type": "COSINE",
                    # nprobe: number of IVF clusters to search.
                    # Higher = better recall at the cost of latency.
                    # 16 / 128 clusters ≈ 12.5% of the index probed — good recall for this scale.
                    "params": {"nprobe": self._nprobe},
                },
                limit=top_k,
                output_fields=_OUTPUT_FIELDS,
            )

        hits = []
        for hit in results[0]:
            row = {field: hit.entity.get(field) for field in _OUTPUT_FIELDS}
            # score is cosine similarity in [0, 1] after L2-normalised embeddings.
            row["score"] = round(float(hit.score), 4)
            hits.append(row)

        return hits

    def ping(self) -> bool:
        try:
            return utility.has_collection(self._collection_name, using="query")
        except Exception:
            return False
