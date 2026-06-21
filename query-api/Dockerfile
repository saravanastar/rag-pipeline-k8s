# ── Stage 1: dependency builder ───────────────────────────────────────────────
# No compiler needed: pymilvus has a pure-Python fallback, fastapi/uvicorn
# are pure Python. gcc stage kept for forward compatibility.
FROM python:3.11-slim AS builder

WORKDIR /build
COPY requirements.txt .
RUN pip install --upgrade pip --quiet \
 && pip wheel --no-cache-dir --wheel-dir /wheels -r requirements.txt

# ── Stage 2: runtime ──────────────────────────────────────────────────────────
FROM python:3.11-slim

WORKDIR /app

COPY --from=builder /wheels /wheels
RUN pip install --no-cache-dir --no-index --find-links=/wheels /wheels/*.whl \
 && rm -rf /wheels

COPY src/ .

# Port 8000: FastAPI (POST /query, GET /health)
# Port 9090: Prometheus metrics
EXPOSE 8000 9090

RUN useradd --uid 1000 --no-create-home queryapi
USER queryapi

CMD ["python", "main.py"]
