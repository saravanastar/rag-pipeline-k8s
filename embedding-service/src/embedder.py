"""
Embedding interface — the Triton swap point.

The public API of this module is a single function:

    embed(texts: list[str]) -> list[list[float]]

Everything in this file is an implementation detail. To replace this service
with NVIDIA Triton (Project 2 in the series), swap the body of embed() for a
Triton gRPC client call. The Kafka consumer, Milvus write path, FastAPI
endpoint, and KEDA ScaledObject are unchanged.

Current implementation: sentence-transformers/all-MiniLM-L6-v2 running on CPU.
  - 384-dimensional dense embeddings
  - ~5ms per chunk on a modern CPU core
  - Model is loaded once at import time; subsequent calls are inference-only.

Threading: SentenceTransformer.encode() releases the GIL during numpy ops but
is NOT safe for concurrent calls from multiple Python threads. Access is
serialised via _LOCK. The FastAPI thread and the Kafka consumer thread both
acquire _LOCK before calling embed(), so they are effectively interleaved.
This is acceptable for CPU inference; with Triton the lock disappears entirely
because gRPC calls are inherently concurrent.
"""
import logging
import threading

from sentence_transformers import SentenceTransformer

import metrics

logger = logging.getLogger(__name__)

_model: SentenceTransformer | None = None
_LOCK = threading.Lock()


def load_model(model_name: str) -> None:
    """Load the model into the module-level singleton. Call once at startup."""
    global _model
    logger.info("loading embedding model: %s", model_name)
    _model = SentenceTransformer(model_name)
    logger.info("model loaded — embedding dim: %d", _model.get_sentence_embedding_dimension())


# ── Public interface ─────────────────────────────────────────────────────────
# This function signature is the contract. Do not change it.
# To swap for Triton: replace the body; keep the signature.

def embed(texts: list[str]) -> list[list[float]]:
    """
    Embed a list of text strings.

    Args:
        texts: non-empty list of strings to embed.

    Returns:
        List of float vectors, one per input string, in the same order.

    Raises:
        RuntimeError: if the model has not been loaded via load_model().
    """
    if _model is None:
        raise RuntimeError("embed() called before load_model()")
    if not texts:
        return []

    with _LOCK:
        with metrics.inference_latency.time():
            vectors = _model.encode(
                texts,
                batch_size=len(texts),
                show_progress_bar=False,
                convert_to_numpy=True,
                normalize_embeddings=True,  # cosine similarity = dot product after L2 norm
            )

    return [v.tolist() for v in vectors]
