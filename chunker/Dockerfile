# ── Stage 1: dependency builder ───────────────────────────────────────────────
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

# tiktoken downloads its BPE data file on first use. Pre-cache it during
# image build so the pod doesn't make outbound HTTPS calls at startup.
# The file is ~1.7MB and is stored in the tiktoken cache dir.
RUN python -c "import tiktoken; tiktoken.get_encoding('cl100k_base')"

COPY src/ .

RUN useradd --uid 1000 --no-create-home chunker
USER chunker

EXPOSE 9090

CMD ["python", "main.py"]
