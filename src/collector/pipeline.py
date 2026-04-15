#!/usr/bin/env python3
"""
Main collection pipeline.
Orchestrates RSS collection -> Claude extraction -> DB storage.
"""

import sys
import logging
from pathlib import Path
from datetime import datetime

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from collector.rss import fetch_all_sources, FeedArticle
from extractor.claude_extractor import extract_investment_info, estimate_cost
from db.models import get_conn, insert_investment, source_exists, url_hash, get_stats

logger = logging.getLogger(__name__)


def process_article(conn, article: FeedArticle) -> bool:
    """
    Process a single article: extract info and store in DB.
    Returns True if a new investment was stored.
    """
    # Check for duplicate
    if source_exists(conn, article.url):
        logger.debug(f"Skip (duplicate): {article.title[:60]}")
        return False

    # Extract structured info via Claude API
    text = f"{article.summary}"
    data = extract_investment_info(title=article.title, text=text)

    if data is None:
        logger.debug(f"Skip (not funding): {article.title[:60]}")
        return False

    # Parse investors list
    investors = data.get("investors", [])
    if not isinstance(investors, list):
        investors = []

    # Parse amount
    amount_jpy = data.get("amount_jpy")
    if amount_jpy is not None:
        try:
            amount_jpy = int(amount_jpy)
        except (ValueError, TypeError):
            amount_jpy = None

    # Map source name to source_id
    source_id = 1  # Default to PR TIMES
    if article.source_name == "The Bridge":
        source_id = 2

    # Store in database
    investment_id = insert_investment(
        conn=conn,
        company_name=data.get("company_name", "Unknown"),
        investors=investors,
        amount_jpy=amount_jpy,
        amount_raw=data.get("amount_raw", ""),
        round_type=data.get("round_type", "unknown"),
        announced_date=data.get("announced_date", ""),
        source_url=article.url,
        source_title=article.title,
        sector=data.get("sector", ""),
        pestle_category=data.get("pestle_category", ""),
        confidence=data.get("confidence", "medium"),
        description=data.get("company_description", ""),
        source_id=source_id,
    )

    if investment_id:
        logger.info(
            f"Stored: {data.get('company_name', '?')} | "
            f"{data.get('round_type', '?')} | "
            f"{data.get('amount_raw', '?')} | "
            f"investors: {len(investors)}"
        )
        return True

    return False


def run_pipeline():
    """Execute the full collection pipeline."""
    start_time = datetime.now()
    logger.info(f"=== Pipeline started at {start_time.isoformat()} ===")

    # 1. Fetch RSS articles
    articles = fetch_all_sources()
    logger.info(f"Fetched {len(articles)} funding-related articles")

    if not articles:
        logger.info("No new articles found. Pipeline complete.")
        return {"collected": 0, "stored": 0, "skipped": 0}

    # 2. Estimate costs
    cost = estimate_cost(len(articles))
    logger.info(
        f"Estimated cost: {cost['estimated_cost_jpy']} JPY "
        f"({cost['article_count']} articles)"
    )

    # 3. Process each article
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
        # Get final stats
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

    logger.info(f"=== Pipeline complete ===")
    logger.info(f"  Collected: {len(articles)}")
    logger.info(f"  New investments stored: {stored}")
    logger.info(f"  Skipped (duplicate/not-funding): {skipped}")
    logger.info(f"  Errors: {errors}")
    logger.info(f"  Elapsed: {elapsed:.1f}s")
    logger.info(f"  DB totals: {stats}")

    return result
