# Query API

**Kind:** Kubernetes Deployment  
**Exposes:** FastAPI HTTP endpoint, via Kubernetes Ingress  
**Depends on:** Embedding Service (for query embedding), Milvus (for vector search)

---

## What it does

Takes a natural-language query, embeds it by calling the embedding service's `POST /embed` endpoint, performs a similarity search against the `rag_chunks` Milvus collection, and returns the top-k matching chunks with relevance scores.

---

## API

```
POST /query
Content-Type: application/json

{
  "text": "how does a Kubernetes pod restart policy work?",
  "top_k": 5
}

→ {
  "results": [
    {
      "chunk_id": "uuid...",
      "url": "https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/",
      "chunk_index": 3,
      "text": "The restartPolicy field...",
      "score": 0.91
    },
    ...
  ],
  "query_latency_ms": 42
}
```

```
GET /health → 200 OK
GET /metrics → Prometheus text format
```

---

## Latency budget

| Step | Target |
|---|---|
| Embed query (call to embedding-service) | <10ms (384-dim, single text) |
| Milvus ANN search (top-5, IVF_FLAT) | <20ms |
| Total p99 | <50ms |

---

## Configuration (env vars)

| Var | Default | Description |
|---|---|---|
| `EMBEDDING_SERVICE_URL` | `http://embedding-service:8000` | Embedding service base URL |
| `MILVUS_HOST` | `milvus` | Milvus host |
| `MILVUS_PORT` | `19530` | Milvus gRPC port |
| `MILVUS_COLLECTION` | `rag_chunks` | Collection to search |
| `DEFAULT_TOP_K` | `5` | Default number of results |
| `PROMETHEUS_PORT` | `9090` | Metrics port |

---

## Ingress

Exposed at `/query` via the cluster Ingress. See `k8s/ingress/query-api-ingress.yaml`.

For Istio: replace with a `VirtualService` + `DestinationRule` (not scaffolded here — plain Ingress is sufficient for the demo).

---

## Metrics

- `query_api_requests_total` — total requests (labeled by status)
- `query_api_latency_p99_seconds` — p99 end-to-end query latency
- `query_api_milvus_search_latency_seconds` — histogram of Milvus search time
- `query_api_embedding_call_latency_seconds` — histogram of embedding service call time
