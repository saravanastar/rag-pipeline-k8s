import hashlib
import logging
import time
from datetime import datetime, timezone
from xml.etree import ElementTree

import requests
from bs4 import BeautifulSoup

from config import Config
from metrics import pages_crawled, pages_emitted, pages_errored, pages_skipped, sitemap_urls_found
from producer import PageCrawledProducer
from redis_store import SeenPagesStore

logger = logging.getLogger(__name__)

# XML namespace used in standard sitemap files.
_SITEMAP_NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}


def _make_session(user_agent: str, timeout: int) -> requests.Session:
    s = requests.Session()
    s.headers["User-Agent"] = user_agent
    s.request = lambda method, url, **kw: requests.Session.request(  # type: ignore[method-assign]
        s, method, url, timeout=timeout, **kw
    )
    return s


def fetch_sitemap_urls(session: requests.Session, sitemap_url: str, url_filter: str) -> list[str]:
    """
    Fetch sitemap_url and return all <loc> URLs that contain url_filter.
    Handles both sitemap index files (pointing to child sitemaps) and
    plain sitemaps. Recurses one level deep for sitemap indexes.
    """
    logger.info("fetching sitemap: %s", sitemap_url)
    resp = session.get(sitemap_url)
    resp.raise_for_status()
    root = ElementTree.fromstring(resp.content)

    # Sitemap index — each <sitemap><loc> points to a child sitemap.
    child_sitemaps = root.findall("sm:sitemap/sm:loc", _SITEMAP_NS)
    if child_sitemaps:
        urls: list[str] = []
        for child in child_sitemaps:
            child_url = child.text or ""
            # Only recurse into child sitemaps likely to contain our filter path.
            # kubernetes.io splits sitemap by section; avoid fetching all of them.
            if url_filter.split("/")[1] in child_url or "docs" in child_url:
                urls.extend(fetch_sitemap_urls(session, child_url, url_filter))
        return urls

    # Plain sitemap — each <url><loc> is a page.
    return [
        loc.text
        for loc in root.findall("sm:url/sm:loc", _SITEMAP_NS)
        if loc.text and url_filter in loc.text
    ]


def extract_text(html: str, url: str) -> tuple[str, str]:
    """
    Return (title, body_text) extracted from raw HTML.
    Strips nav, header, footer, sidebar to reduce hash churn from site-chrome changes.
    Only content inside <main> or <article> is used; falls back to <body>.
    """
    soup = BeautifulSoup(html, "html.parser")

    # Remove boilerplate elements that change independently of content.
    for tag in soup.find_all(["nav", "header", "footer", "aside", "script", "style"]):
        tag.decompose()

    title_tag = soup.find("title")
    title = title_tag.get_text(strip=True) if title_tag else url

    content_root = soup.find("main") or soup.find("article") or soup.find("body")
    if not content_root:
        return title, ""

    # Collapse runs of whitespace to a single space — normalises minor formatting
    # changes that don't reflect content changes.
    text = " ".join(content_root.get_text(separator=" ").split())
    return title, text


def content_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def run_crawl(cfg: Config, store: SeenPagesStore, producer: PageCrawledProducer) -> None:
    session = _make_session(cfg.user_agent, cfg.request_timeout)

    urls = fetch_sitemap_urls(session, cfg.target_sitemap_url, cfg.sitemap_url_filter)
    urls = urls[: cfg.max_pages]
    sitemap_urls_found.set(len(urls))
    logger.info("found %d URLs matching filter '%s'", len(urls), cfg.sitemap_url_filter)

    emitted = skipped = errored = 0

    for i, url in enumerate(urls):
        if i > 0:
            time.sleep(cfg.crawl_delay_seconds)

        try:
            resp = session.get(url)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.warning("fetch failed for %s: %s", url, e)
            pages_errored.labels(reason="fetch_error").inc()
            errored += 1
            continue

        pages_crawled.inc()

        try:
            title, text = extract_text(resp.text, url)
        except Exception as e:
            logger.warning("parse failed for %s: %s", url, e)
            pages_errored.labels(reason="parse_error").inc()
            errored += 1
            continue

        if not text:
            logger.debug("empty content for %s — skipping", url)
            pages_skipped.inc()
            skipped += 1
            continue

        chash = content_hash(text)

        if not store.is_changed(url, chash):
            logger.debug("unchanged: %s", url)
            pages_skipped.inc()
            skipped += 1
            continue

        event = {
            "url": url,
            "title": title,
            "text": text,
            "crawled_at": datetime.now(timezone.utc).isoformat(),
            "content_hash": chash,
        }
        producer.emit(event)
        store.set_hash(url, chash)

        pages_emitted.inc()
        emitted += 1
        logger.info("[%d/%d] emitted: %s", i + 1, len(urls), url)

    producer.flush()
    logger.info(
        "crawl complete — emitted=%d skipped=%d errored=%d total=%d",
        emitted, skipped, errored, len(urls),
    )
