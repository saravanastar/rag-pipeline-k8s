# Chunker

**Kind:** Kubernetes Deployment  
**Image size:** ~180 MB  
**Scaling:** KEDA ScaledObject — 0→4 replicas on `page.crawled` Kafka consumer lag  
**Consumes:** `page.crawled`  
**Emits:** `chunk.ready`

---

## What it does

Consumes `page.crawled` events, splits each page's text into overlapping fixed-size token windows, and publishes one `chunk.ready` event per chunk.

---

## Chunking strategy: fixed-size with overlap

**Chunk size:** 512 tokens  
**Overlap:** 64 tokens  
**Tokenizer:** tiktoken `cl100k_base`

**Why fixed-size over semantic chunking?**  
Semantic chunking (splitting on paragraph or sentence boundaries) produces variable-length chunks, which complicates batching in the embedding service and makes p99 inference latency less predictable. Fixed-size chunks are deterministic, simple to reason about, and work well for a general-purpose English docs corpus where paragraph lengths vary widely.

**Why tiktoken and not the sentence-transformers tokenizer?**  
all-MiniLM-L6-v2 uses WordPiece — a different tokenizer than cl100k_base. In practice their token counts are within ~10% for English text, which is close enough for a 512-token window. Using tiktoken keeps the chunker image small (~180 MB vs ~2 GB) because it avoids importing torch and sentence-transformers.

**Why 64-token overlap?**  
Ensures that a sentence spanning a chunk boundary appears in full in at least one chunk, preventing retrieval misses at boundaries. 64 tokens ≈ 3–4 sentences — enough context, not so much that storage doubles.

**Tiktoken BPE data is pre-cached in the Docker image** — no outbound HTTPS calls at pod startup.

---

## `chunk.ready` event schema

```json
{
  "chunk_id":     "550e8400-e29b-41d4-a716-446655440000",
  "url":          "https://kubernetes.io/docs/concepts/workloads/pods/",
  "title":        "Pods",
  "chunk_index":  2,
  "total_chunks": 7,
  "text":         "...512-token window of page content...",
  "content_hash": "abc123...",
  "created_at":   1705315805
}
```

`chunk_id` is a UUID v4 used as the Kafka message key and the Milvus primary key. Duplicate delivery (at-least-once) is idempotent at the embedding service because Milvus upserts on primary key.

---

## Source files

| File | Role |
|---|---|
| `src/config.py` | `Config` dataclass with chunk size, overlap, poll timeout |
| `src/chunker.py` | `chunk_text()` — tiktoken token windows; `token_count()` for metrics |
| `src/consumer.py` | Manual-commit Kafka consumer; SIGTERM → graceful drain |
| `src/producer.py` | `ChunkReadyProducer` — keyed by `chunk_id` |
| `src/main.py` | `flush()` before `commit()` — offset advances only after all chunks delivered |
| `src/metrics.py` | Counters, histograms, consumer lag gauge |

---

## Delivery guarantee: `flush()` before `commit()`

```
for each page.crawled message:
    chunks = chunk_text(page.text)
    emit all chunk.ready events          # producer.emit_batch()
    flush to Kafka                       # producer.flush() — blocks until delivered
    commit the page.crawled offset       # consumer.commit(msg)
```

If the pod dies between `flush()` and `commit()`, the `page.crawled` event is re-consumed and the page is re-chunked. Duplicate `chunk.ready` events are safe because the embedding service upserts by `chunk_id` in Milvus. This gives **at-least-once** page processing with **idempotent** chunk delivery.

---

## KEDA ScaledObject

```yaml
# k8s/keda/chunker-scaledobject.yaml
minReplicaCount: 0      # scale to zero between crawl windows
maxReplicaCount: 4
lagThreshold: "5"       # 1 new replica per 5 unprocessed page.crawled messages
cooldownPeriod: 120     # seconds before scaling in — lets current batch drain
```

`page.crawled` has 3 partitions, so max effective parallelism is 3 replicas. The 4th replica can absorb a partition if one replica is slow to start.

---

## Deployment design decisions

**`terminationGracePeriodSeconds: 60`** — the SIGTERM handler in `consumer.py` sets a flag and lets the current batch finish (chunk one page, produce N events, flush, commit). 60s is generous for any realistic page.

**`maxUnavailable: 0` in rolling update** — prevents a gap in consumer coverage during deployment. KEDA would otherwise see rising lag during a zero-replica window.

**`checksum/config` annotation** — forces pod restart when chunk size or overlap changes in `values.yaml`, so re-chunking with new parameters takes effect immediately.

---

## Configuration (env vars)

| Var | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Kafka broker |
| `KAFKA_TOPIC_IN` | `page.crawled` | Input topic |
| `KAFKA_TOPIC_OUT` | `chunk.ready` | Output topic |
| `KAFKA_CONSUMER_GROUP` | `chunker-group` | Consumer group ID |
| `CHUNK_SIZE_TOKENS` | `512` | Token window size |
| `CHUNK_OVERLAP_TOKENS` | `64` | Overlap between consecutive windows |
| `PROMETHEUS_PORT` | `9090` | Metrics port |
