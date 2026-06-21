# Crawler

**Kind:** Kubernetes CronJob  
**Schedule:** Every 6 hours (`0 */6 * * *`)  
**Image size:** ~120 MB (multi-stage, libxml2 runtime only)  
**Emits:** `page.crawled` events to Kafka  
**Depends on:** Redis (seen-page hashes), Kafka

---

## What it does

Fetches pages from `kubernetes.io/docs/concepts/` via sitemap, extracts plain text, and computes a SHA-256 content hash. Compares against a Redis hash store and emits a `page.crawled` Kafka event only for pages that are **new** or whose content has **changed**. Unchanged pages are skipped with no downstream cost.

---

## Incremental diff logic

```
for each URL in sitemap matching /docs/concepts/:
    fetch page, strip nav/header/footer/aside → extract text from <main>
    hash = SHA-256(text)
    stored = Redis.HGET("seen-pages", url)

    if stored is None or stored != hash:
        emit page.crawled event
        Redis.HSET("seen-pages", url, hash)
    else:
        skip
```

**Why hash extracted text, not raw HTML?**  
Kubernetes.io's navigation, cookie banners, and sidebar links change frequently and independently of content. Hashing raw HTML would re-emit (and re-embed) every page on any site-chrome update. Stripping boilerplate before hashing means only real content changes propagate downstream, achieving ~95% skip rate on typical daily runs.

---

## `page.crawled` event schema

```json
{
  "url":          "https://kubernetes.io/docs/concepts/workloads/pods/",
  "title":        "Pods",
  "text":         "A Pod is the smallest deployable unit...",
  "crawled_at":   "2024-01-15T10:30:00Z",
  "content_hash": "abc123def456..."
}
```

The `url` field is used as the Kafka message key — all events for a given URL land on the same partition, preserving order for any consumer that cares.

---

## Source files

| File | Role |
|---|---|
| `src/config.py` | `Config` dataclass; all 11 settings via `Config.from_env()` |
| `src/redis_store.py` | `SeenPagesStore` — `HGET`/`HSET` on Redis hash key `seen-pages` |
| `src/producer.py` | `PageCrawledProducer` — confluent-kafka, keyed by URL, `flush()` on exit |
| `src/crawler.py` | Sitemap fetch (handles sitemap index + leaf), text extraction, hash, emit loop |
| `src/metrics.py` | 4 Prometheus counters + 1 gauge |
| `src/main.py` | Wires config → Redis → Kafka → `run_crawl()` |

---

## CronJob design decisions

**`concurrencyPolicy: Forbid`** — two crawlers running concurrently would race on the Redis `seen-pages` hash store: both would read the old hash, both would emit, leaving the store stale. `Forbid` prevents this entirely.

**`backoffLimit: 0`** — failures should surface as failed jobs for investigation, not be silently retried. The next scheduled run picks up any missed pages.

**`activeDeadlineSeconds: 3600`** — hard-kills the job after 1h to protect against hangs on unresponsive target sites.

---

## Rate limiting

- 1 request/second (`CRAWL_DELAY_SECONDS=1`)
- Respects `robots.txt` (inherently, by only following the sitemap)
- User-agent: `rag-pipeline-crawler/1.0`

---

## Configuration (env vars)

| Var | Default | Description |
|---|---|---|
| `TARGET_SITEMAP_URL` | `https://kubernetes.io/sitemap.xml` | Sitemap to crawl |
| `SITEMAP_URL_FILTER` | `/docs/concepts/` | Only crawl URLs containing this string |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Kafka broker |
| `KAFKA_TOPIC_OUT` | `page.crawled` | Output topic |
| `REDIS_URL` | injected via Secret | Redis connection string |
| `CRAWL_DELAY_SECONDS` | `1` | Per-request delay (seconds) |
| `MAX_PAGES` | `500` | Hard cap per run |
| `PROMETHEUS_PORT` | `9090` | Metrics server port |

---

## Metrics

| Metric | Type | Description |
|---|---|---|
| `crawler_pages_crawled_total` | Counter | Pages fetched from sitemap |
| `crawler_pages_emitted_total` | Counter | Pages emitted (new or changed) |
| `crawler_pages_skipped_total` | Counter | Pages skipped (hash matched) |
| `crawler_pages_errored_total` | Counter | Fetch or parse failures (labelled by reason) |
| `crawler_sitemap_urls_found` | Gauge | URLs matching filter in sitemap |

The skip ratio `skipped / crawled` is the primary efficiency metric. In Grafana it appears as the **Incremental Skip Ratio** gauge — a high value means the pipeline is only doing work when content actually changes.
