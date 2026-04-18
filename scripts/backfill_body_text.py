#!/usr/bin/env python3
"""
Backfill body_text for press_releases in the v2 database.

Fetches article body text from PR TIMES pages for records that are missing it.
Prioritizes funding-related records.

Usage:
    python3 scripts/backfill_body_text.py --limit 50 --dry-run
    python3 scripts/backfill_body_text.py --limit 500
    python3 scripts/backfill_body_text.py --funding-only
"""

import argparse
import logging
import sqlite3
import sys
import time
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.collector.prtimes_collector import extract_article_body

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "investment_signal_v2.db"
RATE_LIMIT_SEC = 2
BATCH_COMMIT_SIZE = 50


def fetch_records(conn: sqlite3.Connection, limit: int | None, funding_only: bool) -> list:
    """Fetch press_releases that need body_text backfill."""
    where = "source LIKE 'prtimes%' AND (body_text IS NULL OR body_text = '')"
    if funding_only:
        where += " AND is_funding_related = 1"
    # Prioritize funding-related records
    query = f"""
        SELECT id, source_url, title, is_funding_related
        FROM press_releases
        WHERE {where}
        ORDER BY is_funding_related DESC, id
    """
    if limit:
        query += f" LIMIT {limit}"
    return conn.execute(query).fetchall()


def main():
    parser = argparse.ArgumentParser(description="Backfill body_text for press_releases")
    parser.add_argument("--limit", type=int, default=None, help="Max records to process")
    parser.add_argument("--dry-run", action="store_true", help="Don't write to DB")
    parser.add_argument("--funding-only", action="store_true", help="Only process funding-related records")
    args = parser.parse_args()

    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")

    records = fetch_records(conn, args.limit, args.funding_only)
    total = len(records)
    log.info(f"Records to backfill: {total}")

    if total == 0:
        log.info("Nothing to process.")
        conn.close()
        return

    success = 0
    failed = 0
    not_found = 0

    for i, (rec_id, url, title, is_funding) in enumerate(records, 1):
        log.info(f"[{i}/{total}] id={rec_id}: {title[:60]}...")

        if args.dry_run:
            log.info(f"  [DRY RUN] Would fetch: {url}")
            continue

        body = extract_article_body(url)

        if body and len(body.strip()) > 50:
            conn.execute(
                "UPDATE press_releases SET body_text = ? WHERE id = ?",
                (body, rec_id),
            )
            success += 1
            log.info(f"  -> OK ({len(body)} chars)")
        elif body:
            not_found += 1
            log.info(f"  -> Too short ({len(body)} chars), skipping")
        else:
            failed += 1
            log.info(f"  -> FAILED (404 or parse error)")

        # Batch commit
        if i % BATCH_COMMIT_SIZE == 0:
            conn.commit()
            log.info(f"  --- Batch committed ({i}/{total}) ---")

        # Rate limit
        time.sleep(RATE_LIMIT_SEC)

    conn.commit()
    conn.close()
    log.info(f"Done. Success: {success}, Failed: {failed}, TooShort: {not_found}, Total: {total}")


if __name__ == "__main__":
    main()
