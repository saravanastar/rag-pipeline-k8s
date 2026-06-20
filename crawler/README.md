# Crawler

**Kind:** Kubernetes CronJob  
**Schedule:** Configurable (default: every 6 hours)  
**Emits:** `page.crawled` events to Kafka

---

## What it does

The crawler fetches pages from a target documentation site (Kubernetes docs, `concepts/` subtree), computes a SHA-256 hash of each page's text content, and compares it against a Redis hash store. It only emits a `page.crawled` Kafka event for pages that are **new** or **changed**.

This incremental-diff behavior is intentional: the embedding pipeline is expensive (model inference + Milvus write), and re-embedding unchanged pages wastes compute. On a docs site that updates a handful of pages per day, this reduces downstream work by ~95% compared to a full-recrawl-every-run approach.

---

## Incremental diff logic

```
for each URL in sitemap:
    fetch page → extract text
    hash = SHA-256(text)
    stored_hash = Redis.HGET("seen-pages", url)

    if stored_hash is None or stored_hash != hash:
        emit page.crawled event
        Redis.HSET("seen-pages", url, hash)
    else:
        skip (no downstream work triggered)
```

Redis key: `seen-pages` (hash map, url → sha256)

---

## page.crawled event schema

```json
{
  "url": "https://kubernetes.io/docs/concepts/workloads/pods/",
  "title": "Pods",
  "text": "...",
  "crawled_at": "2024-01-15T10:30:00Z",
  "content_hash": "abc123..."
}
```

---

## Rate limiting

- 1 request/second to the target site
- Respects `robots.txt`
- User-agent: `rag-pipeline-crawler/1.0`

---

## Configuration (env vars)

| Var | Default | Description |
|---|---|---|
| `TARGET_SITEMAP_URL` | `https://kubernetes.io/sitemap.xml` | Sitemap to crawl |
| `SITEMAP_URL_FILTER` | `/docs/concepts/` | Only crawl URLs matching this prefix |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Kafka broker |
| `KAFKA_TOPIC_OUT` | `page.crawled` | Output topic |
| `REDIS_URL` | `redis://redis:6379` | Redis for seen-pages store |
| `CRAWL_DELAY_SECONDS` | `1` | Per-request delay |
| `PROMETHEUS_PORT` | `9090` | Metrics port |

---

## Metrics

- `crawler_pages_crawled_total` — total pages fetched from sitemap
- `crawler_pages_emitted_total` — pages emitted as events (new or changed)
- `crawler_pages_skipped_total` — pages skipped (hash matched, no change)

---

## Future scope (not built here)

- Recursive crawl (currently relies on sitemap only)
- Multiple target sites
- Politeness / crawl budget per domain
