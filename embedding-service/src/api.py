"""
FastAPI application — two roles:

1. POST /embed — synchronous embedding for the Query API.
   Calls the same embed() function as the Kafka consumer loop.
   Protected by the threading.Lock inside embedder.py.

2. GET /health — liveness/readiness probe target.
   Returns 200 only when the model is loaded and Milvus is reachable.
"""
import logging
import time

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, field_validator

import embedder
import metrics

logger = logging.getLogger(__name__)

app = FastAPI(title="embedding-service", version="1.0.0")

# Injected at startup by main.py after the model and Milvus client are ready.
_milvus_client = None


def set_milvus_client(client) -> None:
    global _milvus_client
    _milvus_client = client


# ── Request / response models ─────────────────────────────────────────────────

class EmbedRequest(BaseModel):
    texts: list[str]

    @field_validator("texts")
    @classmethod
    def texts_not_empty(cls, v: list[str]) -> list[str]:
        if not v:
            raise ValueError("texts must be a non-empty list")
        if len(v) > 512:
            raise ValueError("texts list too large (max 512 per request)")
        return v


class EmbedResponse(BaseModel):
    embeddings: list[list[float]]
    model: str
    dim: int


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    if embedder._model is None:
        raise HTTPException(status_code=503, detail="model not loaded")
    if _milvus_client is None or not _milvus_client.ping():
        raise HTTPException(status_code=503, detail="milvus not reachable")
    return {"status": "ok"}


@app.post("/embed", response_model=EmbedResponse)
def embed_texts(req: EmbedRequest):
    t0 = time.perf_counter()
    try:
        vectors = embedder.embed(req.texts)
    except Exception as e:
        logger.error("embed failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))

    elapsed = time.perf_counter() - t0
    metrics.api_embed_latency.observe(elapsed)

    return EmbedResponse(
        embeddings=vectors,
        model=embedder._model.get_sentence_embedding_dimension and embedder._model._modules.get("0", None) and "all-MiniLM-L6-v2" or "unknown",
        dim=len(vectors[0]) if vectors else 0,
    )
