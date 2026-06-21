# Query API

**Kind:** Kubernetes Deployment (2 replicas)  
**Image size:** ~60 MB (no model — smallest image in the stack)  
**Exposes:** `POST /query` via nginx Ingress at `rag.local/api/query`  
**Depends on:** Embedding Service (`POST /embed`), Milvus (ANN search)

---

## What it does

Takes a natural-language query, embeds it by calling the embedding service's `POST /embed` endpoint, performs a cosine similarity search against the Milvus `rag_chunks` collection, and returns the top-k matching chunks with scores.

---

## Why HTTP delegation to the embedding service?

The query-api does **not** load or run the embedding model. Instead it calls `embedding-service:8000/embed` over HTTP.

This is an intentional architecture decision with three benefits:
1. **Lean image** — no torch, no sentence-transformers, no model weights. The query-api image is ~60 MB vs ~2.1 GB.
2. **Independent scaling** — query traffic and embedding throughput (Kafka consumer) scale independently. A spike in queries doesn't force KEDA to scale embedding replicas, and vice versa.
3. **Single model deployment** — the model lives in one place. Updating it (e.g. swapping to Triton in Project 2) only requires changing the embedding-service, not the query-api.

The tradeoff is one extra network hop per query (+2–5ms). Given the total latency target of <50ms, this is acceptable.

---

## Request / Response

```
POST /api/query
Content-Type: application/json

{
  "text": "how does a Kubernetes pod restart policy work?",
  "top_k": 5
}
```

```json
{
  "results": [
    {
      "chunk_id":     "550e8400-...",
      "url":          "https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/",
      "title":        "Pod Lifecycle",
      "chunk_index":  3,
      "total_chunks": 8,
      "text":         "The restartPolicy field...",
      "score":        0.9134
    }
  ],
  "query_latency_ms": 38.2,
  "result_count": 5
}
```

`score` is cosine similarity in `[0, 1]`. Both query and stored vectors are L2-normalised at embed time, so cosine similarity equals their dot product.

---

## Latency budget

| Step | Target p99 |
|---|---|
| Call `POST /embed` (1 text, 384-dim) | <10ms |
| Milvus ANN search (IVF_FLAT, nprobe=16, top-5) | <20ms |
| FastAPI serialisation | <5ms |
| **Total end-to-end** | **<50ms** |

The `query_api_latency_seconds` histogram in Prometheus tracks this. A panel in the Grafana dashboard breaks it into embed call vs. Milvus search vs. total.

---

## Source files

| File | Role |
|---|---|
| `src/config.py` | `Config` dataclass; `milvus_nprobe` configurable per env |
| `src/embedding_client.py` | `EmbeddingServiceClient` — `requests.Session`, `embed_query(text)` |
| `src/milvus_client.py` | `MilvusSearchClient` — search-only, separate pymilvus alias `"query"` |
| `src/api.py` | FastAPI `POST /query` + `GET /health`; pydantic request/response models |
| `src/main.py` | Exits on startup if collection missing (milvus-init job required) |
| `src/metrics.py` | Request counter, e2e latency, embed call latency, Milvus search latency |

---

## Health check design

`GET /health` probes **both** upstream dependencies:
```python
if not embed_client.ping():    → 503
if not milvus_client.ping():   → 503
→ 200 OK
```

This means Kubernetes will not route traffic to a query-api pod whose upstreams are down. Without this, the pod would pass readiness while returning 502 for every query.

---

## Ingress

External path: `rag.local/api/*` → rewrite to `/*` → query-api `:8000`

The Ingress rewrite rule (`nginx.ingress.kubernetes.io/rewrite-target: /$2`) means external callers use `/api/query` while the FastAPI router internally handles `/query`. This is a common production pattern for path-prefix Ingress routing.

For local access: add `127.0.0.1 rag.local` to `/etc/hosts`, then access via port 8080 (kind maps host 8080 → cluster 80).

---

## Configuration (env vars)

| Var | Default | Description |
|---|---|---|
| `EMBEDDING_SERVICE_URL` | `http://embedding-service:8000` | Embedding service base URL |
| `MILVUS_HOST` | `milvus` | Milvus host |
| `MILVUS_PORT` | `19530` | Milvus gRPC port |
| `MILVUS_COLLECTION` | `rag_chunks` | Collection to search |
| `DEFAULT_TOP_K` | `5` | Default results count |
| `MAX_TOP_K` | `50` | Hard ceiling (prevents accidental full-table scans) |
| `MILVUS_NPROBE` | `16` | IVF cells to probe (16/128 ≈ 12.5%; good recall at <500K vectors) |
| `EMBED_TIMEOUT_SECONDS` | `5.0` | HTTP timeout for /embed call |
| `API_PORT` | `8000` | FastAPI port |
| `PROMETHEUS_PORT` | `9090` | Metrics port |
