#!/usr/bin/env python3
"""
Press release collection pipeline.

Orchestrates collection from all press release sources:
  - PR TIMES (RSS + DuckDuckGo search)
  - Frontier Detector import (existing signals)

For funding-related PRs, optionally extracts structured data via Claude.
Inserts results into the press_releases table and links to organizations
and funding_rounds where applicable.
"""

import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from collector.prtimes_collector import collect_all, PressRelease  # noqa: E402
from db.models_v2 import (  # noqa: E402
    get_conn,
    insert_press_release,
    get_press_release_stats,
    import_frontier_detector_signals,
    find_organization_by_name,
    find_or_create_company,
    get_data_source_id,
)

logger = logging.getLogger(__name__)

# Default search queries for investment-related press releases
DEFAULT_SEARCH_QUERIES = [
    "スタートアップ 資金調達",
    "シリーズA 調達",
    "VC 投資 スタートアップ",
]

# Path to Frontier Detector database
FRONTIER_DB_PATH = Path.home() / "projects" / "apps" / "frontier-detector" / "frontier_detector.db"


def _try_extract_funding_details(title: str, body: str) -> dict:
    """Use Claude Haiku to extract structured funding data from a press release.

    Only called for funding-related PRs. Returns extracted dict or empty dict.
    Uses the same lightweight approach as the existing pipeline_v2.py.
    """
    try:
        import anthropic
    except ImportError:
        logger.warning("anthropic package not installed, skipping AI extraction")
        return {}

    # Get API key from keychain (same pattern as other collectors)
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        try:
            import subprocess
            result = subprocess.run(
                ["security", "find-generic-password", "-s", "anthropic-api-key", "-w"],
                capture_output=True, text=True,
            )
            if result.returncode == 0:
                api_key = result.stdout.strip()
                os.environ["ANTHROPIC_API_KEY"] = api_key
        except Exception:
            pass

    if not api_key:
        logger.debug("No Anthropic API key available, skipping extraction")
        return {}

    client = anthropic.Anthropic(api_key=api_key)
    text_input = f"Title: {title}\n\nBody:\n{body[:3000]}"

    try:
        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=1024,
            system="You are an investment data extractor. Return ONLY valid JSON.",
            messages=[{
                "role": "user",
                "content": (
                    "Extract funding information from this press release.\n\n"
                    f"{text_input}\n\n"
                    "Return JSON with: company_name, round_type (seed/series_a/series_b/etc), "
                    "amount_raw (original text), amount_jpy (integer or null), "
                    "investors (array of {name, type, is_lead}), announced_date (YYYY-MM-DD), "
                    "sector (English category name). Return ONLY JSON."
                ),
            }],
        )
        text = response.content[0].text.strip()
        # Remove markdown code fences if present
        if text.startswith("```"):
            import re
            match = re.search(r'```(?:json)?\s*(\{.*?\})\s*```', text, re.DOTALL)
            if match:
                text = match.group(1)
        return json.loads(text)
    except Exception as e:
        logger.debug(f"Claude extraction failed: {e}")
        return {}


def store_press_release(conn, pr: PressRelease) -> bool:
    """Store a single PressRelease in the database.

    Returns True if a new record was created.
    """
    # Try to link to existing organization
    org_id = None
    if pr.company_name:
        org_id = find_organization_by_name(conn, pr.company_name)

    data = {
        "title": pr.title,
        "body_text": pr.body_text or pr.summary,
        "source": pr.source,
        "source_url": pr.source_url,
        "published_at": pr.published_at,
        "company_name": pr.company_name,
        "organization_id": org_id,
        "category": pr.category,
        "is_funding_related": pr.is_funding_related,
        "extracted_data": pr.extracted_data,
        "confidence_score": pr.confidence_score,
        "data_source_name": "prtimes_enhanced",
    }

    result = insert_press_release(conn, data)
    return result is not None


def run_press_release_pipeline(
    search_queries: list[str] = None,
    fetch_bodies: bool = False,
    extract_funding: bool = False,
    import_frontier: bool = False,
) -> dict:
    """Execute the full press release collection pipeline.

    Args:
        search_queries: DuckDuckGo search terms (defaults to funding queries).
        fetch_bodies: Whether to fetch full article bodies.
        extract_funding: Whether to use Claude to extract structured data
                        from funding-related PRs.
        import_frontier: Whether to import from Frontier Detector DB.

    Returns a summary dict with counts and timing.
    """
    start = datetime.now()
    queries = search_queries or DEFAULT_SEARCH_QUERIES

    logger.info(f"=== Press Release Pipeline started at {start.isoformat()} ===")
    logger.info(f"  Queries: {queries}")
    logger.info(f"  Fetch bodies: {fetch_bodies}")
    logger.info(f"  Extract funding: {extract_funding}")
    logger.info(f"  Import frontier: {import_frontier}")

    conn = get_conn()
    stored = 0
    skipped = 0
    errors = 0
    frontier_imported = 0

    try:
        # 1. Collect from PR TIMES (RSS + search)
        releases = collect_all(
            search_queries=queries,
            fetch_bodies=fetch_bodies,
        )
        logger.info(f"Collected {len(releases)} press releases")

        # 2. Optionally extract structured funding data
        if extract_funding:
            for pr in releases:
                if pr.is_funding_related and (pr.body_text or pr.summary):
                    details = _try_extract_funding_details(
                        pr.title, pr.body_text or pr.summary
                    )
                    if details:
                        pr.extracted_data = details
                        if details.get("company_name") and not pr.company_name:
                            pr.company_name = details["company_name"]

        # 3. Store in database
        for pr in releases:
            try:
                if store_press_release(conn, pr):
                    stored += 1
                else:
                    skipped += 1
            except Exception as e:
                logger.error(f"Error storing PR: {e}")
                errors += 1
        conn.commit()

        # 4. Optionally import from Frontier Detector
        if import_frontier and FRONTIER_DB_PATH.exists():
            logger.info(f"Importing from Frontier Detector: {FRONTIER_DB_PATH}")
            frontier_imported = import_frontier_detector_signals(
                conn, str(FRONTIER_DB_PATH)
            )
            logger.info(f"Imported {frontier_imported} signals from Frontier Detector")
        elif import_frontier:
            logger.warning(f"Frontier Detector DB not found: {FRONTIER_DB_PATH}")

        # 5. Get final stats
        stats = get_press_release_stats(conn)

    finally:
        conn.close()

    elapsed = (datetime.now() - start).total_seconds()
    result = {
        "collected": len(releases),
        "stored": stored,
        "skipped": skipped,
        "errors": errors,
        "frontier_imported": frontier_imported,
        "elapsed_seconds": round(elapsed, 1),
        "press_release_stats": stats,
    }

    logger.info("=== Press Release Pipeline complete ===")
    logger.info(f"  Collected: {len(releases)}")
    logger.info(f"  Stored: {stored}")
    logger.info(f"  Skipped: {skipped}")
    logger.info(f"  Errors: {errors}")
    logger.info(f"  Frontier imported: {frontier_imported}")
    logger.info(f"  Elapsed: {elapsed:.1f}s")
    logger.info(f"  Stats: {stats}")

    return result


if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    parser = argparse.ArgumentParser(description="Press release collection pipeline")
    parser.add_argument("--fetch-bodies", action="store_true",
                       help="Fetch full article bodies (slower)")
    parser.add_argument("--extract-funding", action="store_true",
                       help="Use Claude to extract structured funding data")
    parser.add_argument("--import-frontier", action="store_true",
                       help="Import signals from Frontier Detector DB")
    parser.add_argument("--queries", nargs="*",
                       help="Custom search queries (default: funding-related)")
    args = parser.parse_args()

    result = run_press_release_pipeline(
        search_queries=args.queries,
        fetch_bodies=args.fetch_bodies,
        extract_funding=args.extract_funding,
        import_frontier=args.import_frontier,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
