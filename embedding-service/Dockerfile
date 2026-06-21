# ── Stage 1: dependency builder ───────────────────────────────────────────────
FROM python:3.11-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends gcc g++ && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY requirements.txt .
RUN pip install --upgrade pip --quiet \
 && pip wheel --no-cache-dir --wheel-dir /wheels -r requirements.txt

# ── Stage 2: model downloader ─────────────────────────────────────────────────
# Separate stage so the model is cached in its own layer. Rebuilds caused by
# code changes don't re-download the model (~90 MB).
FROM python:3.11-slim AS model-downloader

COPY --from=builder /wheels /wheels
RUN pip install --no-cache-dir --no-index --find-links=/wheels /wheels/*.whl

ARG MODEL_NAME=all-MiniLM-L6-v2
ENV SENTENCE_TRANSFORMERS_HOME=/app/models

RUN python -c "
from sentence_transformers import SentenceTransformer
import os
model_name = os.environ.get('MODEL_NAME', '${MODEL_NAME}')
print(f'downloading model: {model_name}')
SentenceTransformer(model_name)
print('model download complete')
"

# ── Stage 3: runtime ──────────────────────────────────────────────────────────
FROM python:3.11-slim

WORKDIR /app

COPY --from=builder /wheels /wheels
RUN pip install --no-cache-dir --no-index --find-links=/wheels /wheels/*.whl \
 && rm -rf /wheels

# Bake the pre-downloaded model into the image. Cold starts don't hit HuggingFace Hub.
# To use a different model: set MODEL_NAME build arg and rebuild.
COPY --from=model-downloader /app/models /app/models
ENV SENTENCE_TRANSFORMERS_HOME=/app/models

COPY src/ .

# Port 8000: FastAPI (POST /embed, GET /health)
# Port 9090: Prometheus metrics
EXPOSE 8000 9090

RUN useradd --uid 1000 --no-create-home embedder
USER embedder

CMD ["python", "main.py"]
