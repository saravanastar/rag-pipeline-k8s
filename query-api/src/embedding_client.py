"""
HTTP client for the embedding service's POST /embed endpoint.

Why a dedicated HTTP call rather than importing embedder.py directly?
  The query-api and the embedding-service are separate Deployments that scale
  independently. The query-api does not run a model — it delegates embedding
  to the embedding-service. This keeps the query-api image small (~50 MB vs
  ~2 GB) and lets KEDA scale the embedding-service based on chunk.ready lag
  without coupling it to query traffic.

  In a higher-throughput system you'd replace this with a gRPC client or a
  connection-pooled async HTTP client (httpx.AsyncClient). The interface here
  (embed_query) is the seam for that future change.
"""
import logging

import requests

import metrics

logger = logging.getLogger(__name__)


class EmbeddingServiceClient:
    def __init__(self, base_url: str, timeout: float = 5.0) -> None:
        self._url = base_url.rstrip("/") + "/embed"
        self._health_url = base_url.rstrip("/") + "/health"
        self._session = requests.Session()
        self._timeout = timeout

    def embed_query(self, text: str) -> list[float]:
        """
        Embed a single query string.
        Returns a float vector. Raises on HTTP or connection errors.
        """
        with metrics.embed_call_latency.time():
            resp = self._session.post(
                self._url,
                json={"texts": [text]},
                timeout=self._timeout,
            )
            resp.raise_for_status()

        data = resp.json()
        embeddings = data.get("embeddings", [])
        if not embeddings:
            raise ValueError("embedding service returned empty embeddings list")
        return embeddings[0]

    def ping(self) -> bool:
        try:
            resp = self._session.get(self._health_url, timeout=2.0)
            return resp.status_code == 200
        except requests.RequestException:
            return False
