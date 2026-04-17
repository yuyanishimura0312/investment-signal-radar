#!/usr/bin/env python3
"""
Enhanced PR TIMES press release collector.

Collects press releases via three channels:
  1. DuckDuckGo site:prtimes.jp search (reuses Frontier Detector pattern)
  2. PR TIMES category RSS feeds
  3. Full article body extraction via BeautifulSoup

Classifies each release into categories: funding, partnership,
product_launch, hiring, other.

Rate limiting: 2 seconds between HTTP requests.
"""

import hashlib
import json
import logging
import re
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
from urllib.parse import urlparse

import feedparser
import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# ================================================================
# Constants
# ================================================================

# PR TIMES RSS feeds by category (business/tech/science/IT)
PRTIMES_RSS_FEEDS = [
    ("https://prtimes.jp/index.rdf", "prtimes_main"),
    ("https://prtimes.jp/categoryindex.rdf?categoryid=1", "prtimes_business"),
    ("https://prtimes.jp/categoryindex.rdf?categoryid=9", "prtimes_technology"),
    ("https://prtimes.jp/categoryindex.rdf?categoryid=40", "prtimes_it"),
]

# Keywords for classifying press release categories
FUNDING_KEYWORDS = [
    "資金調達", "シリーズA", "シリーズB", "シリーズC", "シリーズD",
    "プレシリーズ", "シードラウンド", "エクイティ調達",
    "リード投資", "第三者割当増資", "調達額", "億円を調達",
    "series a", "series b", "series c", "seed round",
    "raised", "funding round", "venture capital",
]

PARTNERSHIP_KEYWORDS = [
    "業務提携", "資本提携", "パートナーシップ", "協業",
    "共同開発", "MOU", "提携", "アライアンス",
    "partnership", "collaboration", "joint venture",
]

PRODUCT_LAUNCH_KEYWORDS = [
    "新サービス", "リリース", "ローンチ", "提供開始",
    "サービス開始", "新製品", "新機能", "β版", "正式版",
    "launch", "release", "new product", "new service",
]

HIRING_KEYWORDS = [
    "採用", "人材募集", "CTO就任", "CEO就任", "経営陣",
    "組織強化", "hiring", "recruit",
]

# Polite delay between requests (seconds)
REQUEST_DELAY = 2.0

# HTTP request timeout (seconds)
REQUEST_TIMEOUT = 15

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "InvestmentSignalRadar/1.0 (research bot)"
)


# ================================================================
# Data structures
# ================================================================

@dataclass
class PressRelease:
    """A single press release from any source."""
    title: str
    source_url: str
    source: str = "prtimes"  # 'prtimes', 'bridge', 'other'
    body_text: str = ""
    summary: str = ""
    published_at: str = ""
    company_name: str = ""
    category: str = "other"
    is_funding_related: bool = False
    extracted_data: dict = field(default_factory=dict)
    confidence_score: float = 0.5


# ================================================================
# Classification
# ================================================================

def classify_press_release(title: str, body: str) -> tuple[str, bool]:
    """Classify a press release into a category.

    Returns (category, is_funding_related).
    Checks funding first since it's the highest-priority signal.
    """
    text = (title + " " + body).lower()

    # Check funding first (most important for investment radar)
    for kw in FUNDING_KEYWORDS:
        if kw.lower() in text:
            return "funding", True

    for kw in PARTNERSHIP_KEYWORDS:
        if kw.lower() in text:
            return "partnership", False

    for kw in PRODUCT_LAUNCH_KEYWORDS:
        if kw.lower() in text:
            return "product_launch", False

    for kw in HIRING_KEYWORDS:
        if kw.lower() in text:
            return "hiring", False

    return "other", False


# ================================================================
# DuckDuckGo search (reuses Frontier Detector pattern)
# ================================================================

def search_prtimes_ddg(
    query: str,
    max_results: int = 20,
) -> list[PressRelease]:
    """Search prtimes.jp via DuckDuckGo site: filter.

    This mirrors the approach used in frontier-detector/agents/prtimes.py.
    """
    try:
        from duckduckgo_search import DDGS
    except ImportError:
        logger.warning(
            "duckduckgo-search not installed. "
            "Run: pip install duckduckgo-search"
        )
        return []

    results = []
    search_query = f"site:prtimes.jp {query}"

    try:
        with DDGS() as ddgs:
            for r in ddgs.text(search_query, max_results=max_results):
                url = r.get("href", r.get("link", ""))
                if not url or "prtimes.jp" not in url:
                    continue

                title = r.get("title", "")
                snippet = r.get("body", r.get("snippet", ""))
                category, is_funding = classify_press_release(title, snippet)

                results.append(PressRelease(
                    title=title,
                    source_url=url,
                    source="prtimes",
                    summary=snippet,
                    category=category,
                    is_funding_related=is_funding,
                    confidence_score=0.6,
                ))
    except Exception as e:
        logger.error(f"DuckDuckGo search failed for '{query}': {e}")

    logger.info(f"DDG search '{query}': found {len(results)} results")
    return results


# ================================================================
# RSS collection
# ================================================================

def fetch_prtimes_rss_feeds() -> list[PressRelease]:
    """Fetch press releases from multiple PR TIMES RSS category feeds."""
    all_releases = []
    seen_urls: set[str] = set()

    for feed_url, feed_name in PRTIMES_RSS_FEEDS:
        logger.info(f"Fetching RSS: {feed_name} ({feed_url})")
        try:
            feed = feedparser.parse(feed_url)
        except Exception as e:
            logger.error(f"RSS parse failed for {feed_name}: {e}")
            continue

        for entry in feed.entries:
            url = entry.get("link", "")
            if not url or url in seen_urls:
                continue
            seen_urls.add(url)

            title = entry.get("title", "")
            summary = entry.get("summary", entry.get("description", ""))
            # Strip HTML tags from summary
            summary = re.sub(r"<[^>]+>", "", summary).strip()[:2000]
            published = entry.get("published", "")

            category, is_funding = classify_press_release(title, summary)

            all_releases.append(PressRelease(
                title=title,
                source_url=url,
                source="prtimes",
                summary=summary,
                published_at=published,
                category=category,
                is_funding_related=is_funding,
                confidence_score=0.7,
            ))

        time.sleep(REQUEST_DELAY)

    logger.info(f"RSS feeds: collected {len(all_releases)} releases")
    return all_releases


# ================================================================
# Full article body extraction
# ================================================================

def extract_article_body(url: str) -> Optional[str]:
    """Fetch the full article body from a PR TIMES article page.

    Uses BeautifulSoup to extract the main content area.
    Returns None on failure.
    """
    try:
        parsed = urlparse(url)
        if parsed.scheme not in ("http", "https"):
            return None

        resp = requests.get(
            url,
            headers={"User-Agent": USER_AGENT},
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        resp.encoding = resp.apparent_encoding or "utf-8"

        soup = BeautifulSoup(resp.text, "lxml")

        # PR TIMES article body is typically in <div class="rich-text">
        # or <div id="press-release-body">
        body_div = (
            soup.find("div", class_="rich-text")
            or soup.find("div", {"id": "press-release-body"})
            or soup.find("article")
        )
        if body_div:
            text = body_div.get_text(separator="\n", strip=True)
            return text[:10000]  # Cap body length

        # Fallback: grab all paragraph text
        paragraphs = soup.find_all("p")
        if paragraphs:
            text = "\n".join(p.get_text(strip=True) for p in paragraphs)
            return text[:10000]

        return None

    except Exception as e:
        logger.debug(f"Body extraction failed for {url}: {e}")
        return None


def extract_company_from_prtimes(url: str, soup: Optional[BeautifulSoup] = None) -> str:
    """Try to extract the company name from a PR TIMES page.

    PR TIMES pages usually have the company name in the page header.
    """
    if soup is None:
        try:
            resp = requests.get(
                url,
                headers={"User-Agent": USER_AGENT},
                timeout=REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "lxml")
        except Exception:
            return ""

    # PR TIMES company name is often in <a class="company-name"> or similar
    company_el = (
        soup.find("a", class_="company-name")
        or soup.find("div", class_="company-name")
        or soup.find("span", class_="company-name")
    )
    if company_el:
        return company_el.get_text(strip=True)
    return ""


# ================================================================
# Combined collection
# ================================================================

def collect_all(
    search_queries: Optional[list[str]] = None,
    fetch_bodies: bool = False,
) -> list[PressRelease]:
    """Collect press releases from all channels.

    Args:
        search_queries: Optional list of DuckDuckGo search terms.
                       If None, only RSS feeds are used.
        fetch_bodies: If True, fetch full article bodies (slower).

    Returns list of deduplicated PressRelease objects.
    """
    all_releases: list[PressRelease] = []
    seen_urls: set[str] = set()

    def _add(releases: list[PressRelease]) -> None:
        for pr in releases:
            if pr.source_url not in seen_urls:
                seen_urls.add(pr.source_url)
                all_releases.append(pr)

    # 1. RSS feeds
    _add(fetch_prtimes_rss_feeds())

    # 2. DuckDuckGo searches (if queries provided)
    if search_queries:
        for query in search_queries:
            _add(search_prtimes_ddg(query))
            time.sleep(REQUEST_DELAY)

    # 3. Optionally fetch full article bodies
    if fetch_bodies:
        for pr in all_releases:
            if not pr.body_text and "prtimes.jp" in pr.source_url:
                body = extract_article_body(pr.source_url)
                if body:
                    pr.body_text = body
                    # Re-classify with full body text
                    pr.category, pr.is_funding_related = classify_press_release(
                        pr.title, body
                    )
                time.sleep(REQUEST_DELAY)

    logger.info(
        f"Total collected: {len(all_releases)} "
        f"(funding: {sum(1 for p in all_releases if p.is_funding_related)})"
    )
    return all_releases


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    releases = collect_all(search_queries=["スタートアップ 資金調達"])
    for r in releases[:10]:
        print(f"[{r.category}] {r.title[:60]} | {r.source_url}")
