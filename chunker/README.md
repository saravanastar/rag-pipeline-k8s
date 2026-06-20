# Chunker

**Kind:** Kubernetes Deployment  
**Scaling:** KEDA ScaledObject on `page.crawled` Kafka consumer lag  
**Consumes:** `page.crawled`  
**Emits:** `chunk.ready`

---

## What it does

The chunker consumes `page.crawled` events and splits each page's text into overlapping fixed-size chunks, then publishes each chunk as a `chunk.ready` event.

---

## Chunking strategy: fixed-size with overlap

Chunks are 512 tokens with a 64-token overlap between consecutive chunks.

**Why fixed-size over semantic chunking?**

Semantic chunking (splitting on paragraph or sentence boundaries) produces variable-length chunks, which complicates batching in the embedding service and makes p99 embedding latency less predictable. Fixed-size chunks are simpler, deterministic, and work well for a general-purpose docs corpus where paragraph lengths vary widely. The 64-token overlap ensures that a sentence spanning a chunk boundary appears in full in at least one chunk, preventing retrieval misses at boundaries.

This is a deliberate "simple and justified" choice. For a future iteration, semantic chunking with a sentence-transformer boundary detector would improve retrieval quality for long multi-paragraph documents.

---

## chunk.ready event schema

```json
{
  "chunk_id": "uuid-v4",
  "url": "https://kubernetes.io/docs/concepts/workloads/pods/",
  "chunk_index": 2,
  "total_chunks": 7,
  "text": "...",
  "content_hash": "abc123...",
  "created_at": "2024-01-15T10:30:05Z"
}
```

---

## KEDA scaling

See `k8s/keda/chunker-scaledobject.yaml`. Scales 0→4 replicas based on lag on the `page.crawled` topic. `lagThreshold: 5` — one new replica per 5 unprocessed pages.

---

## Configuration (env vars)

| Var | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Kafka broker |
| `KAFKA_TOPIC_IN` | `page.crawled` | Input topic |
| `KAFKA_TOPIC_OUT` | `chunk.ready` | Output topic |
| `KAFKA_CONSUMER_GROUP` | `chunker-group` | Consumer group ID |
| `CHUNK_SIZE_TOKENS` | `512` | Chunk size in tokens |
| `CHUNK_OVERLAP_TOKENS` | `64` | Overlap between chunks |
| `PROMETHEUS_PORT` | `9090` | Metrics port |

---

## Metrics

- `chunker_chunks_produced_total` — total chunks published to `chunk.ready`
- `chunker_kafka_consumer_lag` — current lag on `page.crawled` (mirrors KEDA signal)
- `chunker_chunk_size_tokens_histogram` — distribution of actual chunk sizes
