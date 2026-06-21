"""
Embedding interface — wraps any sentence-transformers model.

Model is configured via the MODEL_NAME env var (set in values.yaml under
embeddingService.config.modelName). EMBEDDING_DIM must match the model output
and is set in the same values block.

Threading: SentenceTransformer.encode() is not safe for concurrent calls from
multiple Python threads. Access is serialised via _LOCK so the FastAPI thread
and the Kafka consumer thread are interleaved safely.
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


def embed(texts: list[str]) -> list[list[float]]:
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
