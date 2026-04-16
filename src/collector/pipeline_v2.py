#!/usr/bin/env python3
"""
Main collection pipeline for v2 schema.

Flow: RSS feeds -> keyword filter -> Claude extraction -> v2 DB insert
Writes to funding_rounds + events + organization graph (Organization-centric).
"""

import sys
import logging
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from collector.rss import fetch_all_sources, FeedArticle  # noqa: E402
from extractor.claude_extractor import extract_investment_info, estimate_cost  # noqa: E402
from db.models_v2 import (  # noqa: E402
    get_conn, insert_funding_round, round_exists, get_stats,
)

logger = logging.getLogger(__name__)


# Map RSS source name to data_source name
RSS_SOURCE_MAP = {
    "PR TIMES": "pr_times_rss",
    "The Bridge": "the_bridge_rss",
}


def process_article(conn, article: FeedArticle) -> bool:
    """
    Process a single article: extract funding info and store in v2 DB.
    Returns True if a new funding round was stored.
    """
    # Deduplicate by URL
    if round_exists(conn, article.url):
        logger.debug(f"Skip (duplicate): {article.title[:60]}")
        return False

    # Extract structured info via Claude API
    data = extract_investment_info(title=article.title, text=article.summary)
    if data is None:
        logger.debug(f"Skip (not funding): {article.title[:60]}")
        return False

    # Parse investors
    investors = data.get("investors") or []
    if not isinstance(investors, list):
        investors = []

    # Parse amount
    amount_jpy = data.get("amount_jpy")
    if amount_jpy is not None:
        try:
            amount_jpy = int(amount_jpy)
        except (ValueError, TypeError):
            amount_jpy = None

    # Determine data source name for provenance
    ds_name = RSS_SOURCE_MAP.get(article.source_name, "claude_extracted")

    round_id = insert_funding_round(
        conn=conn,
        company_name=data.get("company_name") or "Unknown",
        investors=investors,
        amount_jpy=amount_jpy,
        amount_raw=data.get("amount_raw") or "",
        round_type=data.get("round_type") or "unknown",
        announced_date=data.get("announced_date") or "",
        source_url=article.url,
        source_title=article.title,
        sector=data.get("sector") or "",
        pestle_category=data.get("pestle_category") or "",
        confidence=data.get("confidence") or "medium",
        description=data.get("company_description") or "",
        data_source_name=ds_name,
    )

    if round_id:
        logger.info(
            f"Stored: {data.get('company_name', '?')} | "
            f"{data.get('round_type', '?')} | "
            f"{data.get('amount_raw', '?')} | "
            f"investors: {len(investors)}"
        )
        return True
    return False


def run_pipeline():
    """Execute the full collection pipeline against the v2 DB."""
    start_time = datetime.now()
    logger.info(f"=== v2 Pipeline started at {start_time.isoformat()} ===")

    articles = fetch_all_sources()
    logger.info(f"Fetched {len(articles)} funding-related articles")

    if not articles:
        logger.info("No new articles found. Pipeline complete.")
        return {"collected": 0, "stored": 0, "skipped": 0}

    cost = estimate_cost(len(articles))
    logger.info(
        f"Estimated cost: {cost['estimated_cost_jpy']} JPY "
        f"({cost['article_count']} articles)"
    )

    conn = get_conn()
    stored = 0
    skipped = 0
    errors = 0

    try:
        for i, article in enumerate(articles, 1):
            logger.info(f"[{i}/{len(articles)}] Processing: {article.title[:80]}")
            try:
                if process_article(conn, article):
                    stored += 1
                else:
                    skipped += 1
            except Exception as e:
                logger.error(f"Error processing article: {e}")
                errors += 1
            conn.commit()
    finally:
        stats = get_stats(conn)
        conn.close()

    elapsed = (datetime.now() - start_time).total_seconds()
    result = {
        "collected": len(articles),
        "stored": stored,
        "skipped": skipped,
        "errors": errors,
        "elapsed_seconds": round(elapsed, 1),
        "db_stats": stats,
    }

    logger.info("=== v2 Pipeline complete ===")
    logger.info(f"  Collected: {len(articles)}")
    logger.info(f"  New funding rounds stored: {stored}")
    logger.info(f"  Skipped (duplicate/not-funding): {skipped}")
    logger.info(f"  Errors: {errors}")
    logger.info(f"  Elapsed: {elapsed:.1f}s")
    logger.info(f"  DB totals: {stats}")
    return result


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    run_pipeline()
