# Embedding Service

**Kind:** Kubernetes Deployment  
**Scaling:** KEDA ScaledObject on `chunk.ready` Kafka consumer lag  
**Consumes:** `chunk.ready`  
**Writes to:** Milvus (`rag_chunks` collection)  
**Exposes:** FastAPI endpoint for on-demand embedding (used by Query API)

---

## What it does

The embedding service is the centerpiece of the pipeline. It:

1. Consumes `chunk.ready` events from Kafka
2. Batches chunks and runs inference with `sentence-transformers/all-MiniLM-L6-v2`
3. Inserts the resulting vectors + metadata into Milvus
4. Exposes `POST /embed` for synchronous embedding requests from the Query API

---

## Model: all-MiniLM-L6-v2

- 384-dimensional dense embeddings
- ~22M parameters, fast CPU inference (~5ms per chunk on modern hardware)
- Well-understood quality/speed tradeoff for a general-purpose English corpus
- Chosen for demo portability — no GPU required to run this project end-to-end

---

## Triton swap point

**The embedding call is isolated behind a single interface:**

```python
# embedding-service/src/embedder.py
def embed(texts: list[str]) -> list[list[float]]:
    ...
```

To replace this service with NVIDIA Triton (Project 2 in this series), the only change is the implementation of this function — swap `SentenceTransformer.encode()` for a Triton gRPC client call. The Kafka consumer, Milvus write path, and FastAPI endpoint are unchanged. This is intentional.

---

## API

```
POST /embed
Content-Type: application/json

{"texts": ["chunk text 1", "chunk text 2"]}

→ {"embeddings": [[0.12, -0.34, ...], ...]}
```

```
GET /health → 200 OK
GET /metrics → Prometheus text format
```

---

## KEDA scaling

See `k8s/keda/embedding-scaledobject.yaml`. Scales 0→8 replicas based on lag on `chunk.ready`. `lagThreshold: 10` — the embedding service is the bottleneck; allow more lag before scaling to avoid thrashing.

---

## Milvus collection

Collection schema is defined in `embedding-service/src/milvus_schema.py` — not created ad hoc at runtime. See `helm/rag-pipeline/charts/milvus-init/` for the init job that applies the schema on first deploy.

Collection: `rag_chunks`

| Field | Type | Description |
|---|---|---|
| `chunk_id` | VARCHAR(64) | Primary key (UUID) |
| `url` | VARCHAR(2048) | Source page URL |
| `chunk_index` | INT64 | Position within the page |
| `text` | VARCHAR(4096) | Raw chunk text |
| `content_hash` | VARCHAR(64) | SHA-256 of source page |
| `embedding` | FLOAT_VECTOR(384) | The dense vector |
| `created_at` | INT64 | Unix timestamp |

Index: IVF_FLAT with `nlist=128` (suitable for <500K vectors; switch to HNSW at scale).

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
| `EMBED_BATCH_SIZE` | `32` | Chunks per inference batch |
| `MODEL_NAME` | `all-MiniLM-L6-v2` | sentence-transformers model |
| `PROMETHEUS_PORT` | `9090` | Metrics port |

---

## Metrics

- `embedding_chunks_embedded_total` — total chunks embedded and inserted
- `embedding_throughput_chunks_per_sec` — rolling 60s throughput
- `embedding_inference_latency_seconds` — histogram of model inference time
- `embedding_milvus_insert_latency_seconds` — histogram of Milvus insert time
- `embedding_kafka_consumer_lag` — current lag on `chunk.ready`
