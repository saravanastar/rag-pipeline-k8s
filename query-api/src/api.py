import logging
import time
from typing import Annotated

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field, field_validator

import metrics

logger = logging.getLogger(__name__)

app = FastAPI(
    title="RAG Pipeline Query API",
    description="Embeds a text query and returns the top-k most similar chunks from Milvus.",
    version="1.0.0",
)

# Injected at startup by main.py once clients are ready.
_embed_client = None
_milvus_client = None


def set_clients(embed_client, milvus_client) -> None:
    global _embed_client, _milvus_client
    _embed_client = embed_client
    _milvus_client = milvus_client


# ── Request / response models ─────────────────────────────────────────────────

class QueryRequest(BaseModel):
    text: str = Field(..., min_length=1, max_length=2048, description="Natural language query")
    top_k: int = Field(default=5, ge=1, le=50, description="Number of results to return")

    @field_validator("text")
    @classmethod
    def text_not_blank(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("text must not be blank")
        return v.strip()


class ChunkResult(BaseModel):
    chunk_id: str
    url: str
    title: str
    chunk_index: int
    total_chunks: int
    text: str
    score: float


class QueryResponse(BaseModel):
    results: list[ChunkResult]
    query_latency_ms: float
    result_count: int


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    """
    Liveness and readiness probe target.
    Returns 200 only when both the embedding service and Milvus are reachable.
    """
    issues = []
    if _embed_client is None or not _embed_client.ping():
        issues.append("embedding service unreachable")
    if _milvus_client is None or not _milvus_client.ping():
        issues.append("milvus unreachable")

    if issues:
        raise HTTPException(status_code=503, detail=", ".join(issues))
    return {"status": "ok"}


@app.post("/query", response_model=QueryResponse)
def query(req: QueryRequest):
    """
    Embed the query text and return the top_k most similar document chunks.

    Steps:
      1. POST req.text to embedding-service /embed → 384-dim vector
      2. ANN search in Milvus rag_chunks collection → top_k hits
      3. Return hits with cosine similarity scores
    """
    t0 = time.perf_counter()

    try:
        query_vector = _embed_client.embed_query(req.text)
    except Exception as e:
        logger.error("embed_query failed: %s", e)
        metrics.requests_total.labels(status="error").inc()
        raise HTTPException(status_code=502, detail=f"embedding service error: {e}")

    try:
        hits = _milvus_client.search(query_vector, top_k=req.top_k)
    except Exception as e:
        logger.error("milvus search failed: %s", e)
        metrics.requests_total.labels(status="error").inc()
        raise HTTPException(status_code=502, detail=f"milvus search error: {e}")

    elapsed_ms = (time.perf_counter() - t0) * 1000
    metrics.query_latency.observe(elapsed_ms / 1000)
    metrics.requests_total.labels(status="success").inc()

    logger.info(
        "query='%.60s' top_k=%d hits=%d latency=%.1fms",
        req.text, req.top_k, len(hits), elapsed_ms,
    )

    return QueryResponse(
        results=[ChunkResult(**h) for h in hits],
        query_latency_ms=round(elapsed_ms, 2),
        result_count=len(hits),
    )
