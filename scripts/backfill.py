#!/usr/bin/env python3
"""
Backfill historical funding data from PR TIMES search.
Fetches past press releases about funding and processes them through the pipeline.

Usage:
    python3 scripts/backfill.py [--pages 5] [--delay 5]
"""

import sys
import time
import argparse
import logging
import re
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from extractor.claude_extractor import extract_investment_info
from db.models import get_conn, insert_investment, source_exists

logger = logging.getLogger(__name__)

# PR TIMES search RSS for funding keywords
SEARCH_QUERIES = [
    "資金調達",
    "シリーズA",
    "シリーズB",
    "第三者割当増資",
]


def fetch_prtimes_search(query: str, page: int = 1) -> list[dict]:
    """Fetch PR TIMES search results as articles."""
    import feedparser

    # PR TIMES provides search results as RSS
    url = f"https://prtimes.jp/index.rdf?keyword={query}"
    logger.info(f"Fetching PR TIMES search: '{query}' (page {page})")

    try:
        feed = feedparser.parse(url)
    except Exception as e:
        logger.error(f"Failed to fetch: {e}")
        return []

    articles = []
    for entry in feed.entries:
        title = entry.get("title", "")
        link = entry.get("link", "")
        summary = entry.get("summary", entry.get("description", ""))
        summary = re.sub(r"<[^>]+>", "", summary).strip()
        if len(summary) > 2000:
            summary = summary[:2000]
        published = entry.get("published", "")

        if title and link:
            articles.append({
                "title": title,
                "url": link,
                "summary": summary,
                "published": published,
            })

    logger.info(f"  Found {len(articles)} articles")
    return articles


def process_backfill_article(conn, article: dict) -> bool:
    """Process a single backfill article."""
    if source_exists(conn, article["url"]):
        return False

    data = extract_investment_info(
        title=article["title"],
        text=article["summary"],
    )

    if data is None:
        return False

    investors = data.get("investors", [])
    if not isinstance(investors, list):
        investors = []

    amount_jpy = data.get("amount_jpy")
    if amount_jpy is not None:
        try:
            amount_jpy = int(amount_jpy)
        except (ValueError, TypeError):
            amount_jpy = None

    investment_id = insert_investment(
        conn=conn,
        company_name=data.get("company_name", "Unknown"),
        investors=investors,
        amount_jpy=amount_jpy,
        amount_raw=data.get("amount_raw", ""),
        round_type=data.get("round_type", "unknown"),
        announced_date=data.get("announced_date", ""),
        source_url=article["url"],
        source_title=article["title"],
        sector=data.get("sector", ""),
        pestle_category=data.get("pestle_category", ""),
        confidence=data.get("confidence", "medium"),
        description=data.get("company_description", ""),
        source_id=1,
    )

    if investment_id:
        logger.info(
            f"  Stored: {data.get('company_name', '?')} | "
            f"{data.get('round_type', '?')} | "
            f"{data.get('amount_raw', '?')}"
        )
        return True
    return False


def run_backfill(delay: float = 5.0):
    """Run backfill for all search queries."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    conn = get_conn()
    total_stored = 0
    total_skipped = 0

    for query in SEARCH_QUERIES:
        articles = fetch_prtimes_search(query)
        time.sleep(delay)

        for article in articles:
            try:
                if process_backfill_article(conn, article):
                    total_stored += 1
                else:
                    total_skipped += 1
            except Exception as e:
                logger.error(f"  Error: {e}")
            conn.commit()
            time.sleep(1)  # Polite interval between API calls

    from db.models import get_stats
    stats = get_stats(conn)
    conn.close()

    logger.info(f"=== Backfill complete ===")
    logger.info(f"  Stored: {total_stored}")
    logger.info(f"  Skipped: {total_skipped}")
    logger.info(f"  DB totals: {stats}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--delay", type=float, default=5.0,
                        help="Delay between requests (seconds)")
    args = parser.parse_args()
    run_backfill(delay=args.delay)
