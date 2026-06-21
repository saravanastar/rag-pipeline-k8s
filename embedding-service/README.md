# Embedding Service

**Kind:** Kubernetes Deployment  
**Image size:** ~2.1 GB (includes all-MiniLM-L6-v2 model weights)  
**Scaling:** KEDA ScaledObject — 0→8 replicas on `chunk.ready` Kafka consumer lag  
**Consumes:** `chunk.ready` (Kafka consumer loop)  
**Exposes:** `POST /embed` (FastAPI, used by Query API)  
**Writes to:** Milvus `rag_chunks` collection

---

## What it does

The embedding service is the centerpiece of the pipeline. It runs two concurrent roles:

1. **Kafka consumer loop** (main thread) — batches `chunk.ready` events (up to 32 chunks OR 2s timeout), runs batch inference, inserts vectors into Milvus, commits offsets
2. **FastAPI server** (daemon thread) — `POST /embed` for synchronous on-demand embedding from the Query API

Both paths call the same `embed()` function in `embedder.py`.

---

## ◀ Triton swap point

```python
# embedding-service/src/embedder.py

def embed(texts: list[str]) -> list[list[float]]:
    """
    This function is the contract.
    Current: SentenceTransformer.encode()
    Project 2: Triton gRPC client call
    """
```

**To replace this service with NVIDIA Triton (Project 2), change only this function body.** The Kafka consumer, batch accumulation, Milvus write path, FastAPI endpoint, KEDA ScaledObject, Deployment manifest, and all metrics are unchanged. See the full docstring in `embedder.py` for the threading implications.

---

## Model: all-MiniLM-L6-v2

- **384-dimensional** dense embeddings (L2-normalised → cosine similarity = dot product)
- **~22M parameters**, ~5ms per chunk on CPU
- **Pre-downloaded** into the Docker image at build time (3-stage Dockerfile: builder → model-downloader → runtime) — pods don't hit HuggingFace Hub at startup

---

## Batching design

```
consume chunk.ready messages →
    accumulate up to EMBED_BATCH_SIZE=32 chunks
    OR flush after BATCH_TIMEOUT_SECONDS=2.0 if batch not full
→ embed batch (one SentenceTransformer.encode() call)
→ insert batch into Milvus
→ commit batch offset (last message covers all earlier offsets)
```

Batch inference on 32 texts is ~4× faster than 32 individual calls due to matrix parallelism. The 2s timeout ensures chunks don't buffer indefinitely when topic lag is low (e.g. after a small crawl run).

---

## Delivery guarantee

Same pattern as the chunker: embed → Milvus insert → Kafka commit. If the pod dies between insert and commit, the batch is redelivered and re-embedded. Milvus upserts on `chunk_id` (VARCHAR primary key) make this idempotent.

---

## API

```
POST /embed
{"texts": ["chunk text 1", "chunk text 2"]}
→ {"embeddings": [[0.12, -0.34, ...], ...], "dim": 384}

GET /health    → 200 OK only when model loaded AND Milvus reachable
GET /metrics   → Prometheus text (port 9090)
```

`/health` is the target for `startupProbe`, `livenessProbe`, and `readinessProbe` in the Deployment manifest. The startup probe gives the pod up to 90s (18 × 5s) to load the model before liveness takes over.

---

## Milvus collection schema

Defined authoritatively in `src/milvus_schema.py` — not created ad hoc. The Helm `post-install` hook (`milvus-init-job.yaml`) applies the schema on first deploy and on upgrades.

| Field | Type | Description |
|---|---|---|
| `chunk_id` | VARCHAR(64), PK | UUID v4 |
| `url` | VARCHAR(2048) | Source page URL |
| `title` | VARCHAR(512) | Page title |
| `chunk_index` | INT64 | Position within the page |
| `total_chunks` | INT64 | Total chunks for this page |
| `text` | VARCHAR(4096) | Raw chunk text |
| `content_hash` | VARCHAR(64) | SHA-256 of source page |
| `created_at` | INT64 | Unix timestamp |
| `embedding` | FLOAT_VECTOR(384) | L2-normalised dense vector |

**Index:** IVF_FLAT, COSINE metric, `nlist=128`. Good recall at <500K vectors. Switch to HNSW at larger scale.

---

## KEDA ScaledObject

```yaml
# k8s/keda/embedding-scaledobject.yaml
minReplicaCount: 0      # scale to zero between crawl windows
maxReplicaCount: 8
lagThreshold: "10"      # higher than chunker (5) — embedding is the bottleneck
cooldownPeriod: 180     # longer than chunker (120) — model load ~10s, avoid thrash
```

`chunk.ready` has 6 partitions, so at most 6 replicas actively consume. Replicas 7–8 are available for burst headroom if partitions are rebalanced.

---

## Source files

| File | Role |
|---|---|
| `src/embedder.py` | ◀ Triton swap point — `load_model()` + `embed()` with threading.Lock |
| `src/milvus_schema.py` | Collection schema v1 (authoritative definition) |
| `src/milvus_client.py` | `MilvusClient` — batch insert + ANN search |
| `src/consumer.py` | `ChunkReadyConsumer` — batch iterator (size OR timeout), SIGTERM-safe |
| `src/api.py` | FastAPI `POST /embed` + `GET /health` |
| `src/main.py` | Dual-thread startup; model loaded before API starts so /health=200 means ready |
| `src/config.py` | Typed Config from env vars |
| `src/metrics.py` | Throughput, inference latency, Milvus insert latency, batch size histograms |

---

## Configuration (env vars)

| Var | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Kafka broker |
| `KAFKA_TOPIC_IN` | `chunk.ready` | Input topic |
| `KAFKA_CONSUMER_GROUP` | `embedding-group` | Consumer group ID |
| `MILVUS_HOST` | `milvus` | Milvus host |
| `MILVUS_PORT` | `19530` | Milvus gRPC port |
| `MILVUS_COLLECTION` | `rag_chunks` | Target collection |
| `MODEL_NAME` | `all-MiniLM-L6-v2` | sentence-transformers model |
| `EMBED_BATCH_SIZE` | `32` | Max chunks per inference call |
| `BATCH_TIMEOUT_SECONDS` | `2.0` | Flush partial batches after this delay |
| `API_PORT` | `8000` | FastAPI port |
| `PROMETHEUS_PORT` | `9090` | Metrics port |
