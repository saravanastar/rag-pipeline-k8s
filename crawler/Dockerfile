# ── Stage 1: dependency builder ───────────────────────────────────────────────
# Build wheels in a separate stage so the runtime image doesn't need gcc/build-essential.
FROM python:3.11-slim AS builder

WORKDIR /build
COPY requirements.txt .

RUN pip install --upgrade pip --quiet \
 && pip wheel --no-cache-dir --wheel-dir /wheels -r requirements.txt

# ── Stage 2: runtime image ────────────────────────────────────────────────────
FROM python:3.11-slim

# lxml needs libxml2 at runtime (linked against it, not just build-time).
RUN apt-get update && apt-get install -y --no-install-recommends libxml2 \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install pre-built wheels from the builder stage — no compiler needed.
COPY --from=builder /wheels /wheels
RUN pip install --no-cache-dir --no-index --find-links=/wheels /wheels/*.whl \
 && rm -rf /wheels

COPY src/ .

# Run as non-root. UID 1000 is conventional; must not be 0 in production.
RUN useradd --uid 1000 --no-create-home crawler
USER crawler

# Prometheus metrics port. Exposed here for documentation; actual binding
# is controlled by PROMETHEUS_PORT env var in config.py.
EXPOSE 9090

CMD ["python", "main.py"]
