#!/usr/bin/env python3
"""
RSS feed collector for investment news.
Fetches press releases from PR TIMES, The Bridge, etc.
and filters for funding-related articles.
"""

import feedparser
import re
import time
import logging
from datetime import datetime
from typing import Optional
from dataclasses import dataclass, field
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

# Strong keywords: high confidence that article is about startup funding
FUNDING_KEYWORDS_STRONG = [
    "資金調達", "シリーズA", "シリーズB", "シリーズC", "シリーズD",
    "プレシリーズ", "シードラウンド", "エクイティ調達",
    "リード投資", "第三者割当増資", "調達額",
    "series a", "series b", "series c", "seed round",
    "raised", "funding round",
]

# Weak keywords: need 2+ matches to qualify
FUNDING_KEYWORDS_WEAK = [
    "出資", "増資", "VC", "ベンチャーキャピタル",
    "億円調達", "万円調達", "資本参加", "投資契約",
    "ファンド", "venture", "capital",
]


@dataclass
class FeedArticle:
    """A single article from an RSS feed."""
    title: str
    url: str
    summary: str
    published: str
    source_name: str
    is_funding: bool = False


def is_funding_related(title: str, summary: str) -> bool:
    """Check if an article is likely about startup funding."""
    text = (title + " " + summary).lower()

    # Any strong keyword is enough
    for kw in FUNDING_KEYWORDS_STRONG:
        if kw.lower() in text:
            return True

    # Need 2+ weak keyword matches
    weak_matches = sum(1 for kw in FUNDING_KEYWORDS_WEAK if kw.lower() in text)
    return weak_matches >= 2


def fetch_prtimes_rss() -> list[FeedArticle]:
    """Fetch and filter PR TIMES RSS feed for funding articles."""
    url = "https://prtimes.jp/index.rdf"
    return _fetch_rss(url, "PR TIMES")


def fetch_thebridge_rss() -> list[FeedArticle]:
    """Fetch and filter The Bridge RSS feed for funding articles."""
    url = "https://thebridge.jp/feed"
    return _fetch_rss(url, "The Bridge")


def _fetch_rss(url: str, source_name: str) -> list[FeedArticle]:
    """Generic RSS fetcher with funding keyword filter."""
    logger.info(f"Fetching RSS: {source_name} ({url})")

    try:
        feed = feedparser.parse(url)
    except Exception as e:
        logger.error(f"Failed to parse RSS {url}: {e}")
        return []

    articles = []
    for entry in feed.entries:
        title = entry.get("title", "")
        link = entry.get("link", "")
        summary = entry.get("summary", entry.get("description", ""))
        published = entry.get("published", "")

        # Clean HTML from summary
        summary = re.sub(r"<[^>]+>", "", summary).strip()
        # Truncate long summaries
        if len(summary) > 2000:
            summary = summary[:2000]

        if not title or not link:
            continue

        # Validate URL scheme (security: only http/https)
        try:
            parsed = urlparse(link)
            if parsed.scheme not in ("http", "https"):
                continue
        except Exception:
            continue

        article = FeedArticle(
            title=title,
            url=link,
            summary=summary,
            published=published,
            source_name=source_name,
            is_funding=is_funding_related(title, summary),
        )
        articles.append(article)

    funding_articles = [a for a in articles if a.is_funding]
    logger.info(
        f"{source_name}: {len(articles)} total, "
        f"{len(funding_articles)} funding-related"
    )
    return funding_articles


def fetch_all_sources() -> list[FeedArticle]:
    """Fetch funding articles from all configured RSS sources."""
    all_articles = []

    all_articles.extend(fetch_prtimes_rss())
    time.sleep(2)  # Polite interval between sources
    all_articles.extend(fetch_thebridge_rss())

    logger.info(f"Total funding articles collected: {len(all_articles)}")
    return all_articles
