#!/usr/bin/env python3
"""
Enrich funding_releases with structured data extracted by Claude Haiku.

Reads title + body_text from funding_database.db, sends to Claude Haiku
for structured extraction, and writes results back to the DB.

Usage:
    python3 scripts/enrich_funding_data.py --limit 5 --dry-run
    python3 scripts/enrich_funding_data.py --limit 100
    python3 scripts/enrich_funding_data.py --force  # overwrite existing data
"""

import argparse
import json
import logging
import os
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "funding_database.db"
MODEL = "claude-haiku-4-5-20251001"
BATCH_SIZE = 20
# Haiku rate limits: be conservative with delays between batches
BATCH_DELAY_SEC = 2

SYSTEM_PROMPT = """\
You are a structured data extractor for Japanese startup funding announcements.
Given a title and body text, extract the following fields as JSON.
If a field cannot be determined, use null.

Output ONLY valid JSON with these keys:
- company_name: Official company name in Japanese (include 株式会社 etc.)
- round_type: One of: seed, pre_seed, pre_series_a, series_a, series_b, series_c, series_d, series_e, later_stage, ipo, ma, debt, grant, undisclosed
- amount_jpy: Funding amount in JPY as integer (e.g. 500000000 for 5億円). null if not disclosed.
- amount_raw: Original text for the amount (e.g. "5億円", "20億円"). null if not mentioned.
- investors: Array of objects [{name: string, type: "vc"|"cvc"|"angel"|"bank"|"corporate"|"government"|"other", is_lead: boolean}]. Empty array if none mentioned.
- sector: Business sector in English lowercase (e.g. "fintech", "healthcare", "saas", "edtech", "cleantech", "logistics", "ai", "biotech", "real_estate", "foodtech", "mobility", "cybersecurity", "hr_tech", "insurtech", "media", "e_commerce", "deep_tech", "other")
- announced_date: Announcement date as YYYY-MM-DD. null if not determinable.
"""

USER_PROMPT_TEMPLATE = """\
Title: {title}

Body:
{body}
"""


def get_api_key() -> str:
    """Retrieve Anthropic API key from macOS keychain."""
    try:
        result = subprocess.run(
            ["security", "find-generic-password", "-s", "ANTHROPIC_API_KEY", "-w"],
            capture_output=True, text=True, check=True,
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError:
        log.error("Failed to retrieve API key from keychain")
        sys.exit(1)


def call_claude(api_key: str, title: str, body: str) -> dict | None:
    """Call Claude Haiku to extract structured funding data."""
    import anthropic

    client = anthropic.Anthropic(api_key=api_key)

    # Truncate body to avoid excessive token usage
    body_truncated = (body or "")[:3000]

    try:
        response = client.messages.create(
            model=MODEL,
            max_tokens=1024,
            system=SYSTEM_PROMPT,
            messages=[{
                "role": "user",
                "content": USER_PROMPT_TEMPLATE.format(
                    title=title,
                    body=body_truncated,
                ),
            }],
        )
        raw = response.content[0].text.strip()

        # Strip markdown code fences if present
        if raw.startswith("```"):
            lines = raw.split("\n")
            # Remove first and last lines (``` markers)
            lines = [l for l in lines if not l.strip().startswith("```")]
            raw = "\n".join(lines)

        return json.loads(raw)
    except json.JSONDecodeError as e:
        log.warning(f"JSON parse error: {e} — raw: {raw[:200]}")
        return None
    except Exception as e:
        log.warning(f"API call failed: {e}")
        return None


def has_valid_extracted_data(extracted_data: str | None) -> bool:
    """Check if extracted_data already contains meaningful structured info."""
    if not extracted_data:
        return False
    try:
        data = json.loads(extracted_data)
        # Consider it valid only if it has key fields beyond just company_name
        meaningful_keys = {"round_type", "amount_jpy", "investors", "sector", "announced_date"}
        return bool(meaningful_keys & set(data.keys()))
    except (json.JSONDecodeError, TypeError):
        return False


def fetch_records(conn: sqlite3.Connection, limit: int | None, force: bool) -> list:
    """Fetch funding-related records that need enrichment."""
    query = """
        SELECT id, title, body_text, extracted_data
        FROM funding_releases
        WHERE is_funding_related = 1
        ORDER BY id
    """
    if limit:
        query += f" LIMIT {limit}"

    rows = conn.execute(query).fetchall()

    if not force:
        # Skip records that already have meaningful extracted_data
        rows = [r for r in rows if not has_valid_extracted_data(r[3])]

    return rows


def update_record(conn: sqlite3.Connection, record_id: int, data: dict, dry_run: bool):
    """Write extracted data back to the DB."""
    if dry_run:
        log.info(f"  [DRY RUN] Would update id={record_id}: {json.dumps(data, ensure_ascii=False)[:150]}")
        return

    extracted_json = json.dumps(data, ensure_ascii=False)
    conn.execute("""
        UPDATE funding_releases
        SET extracted_data = ?,
            company_name = ?,
            round_type = ?,
            amount_raw = ?,
            amount_jpy = ?
        WHERE id = ?
    """, (
        extracted_json,
        data.get("company_name"),
        data.get("round_type"),
        data.get("amount_raw"),
        data.get("amount_jpy"),
        record_id,
    ))


def main():
    parser = argparse.ArgumentParser(description="Enrich funding data with AI extraction")
    parser.add_argument("--limit", type=int, default=None, help="Max records to process")
    parser.add_argument("--dry-run", action="store_true", help="Don't write to DB")
    parser.add_argument("--force", action="store_true", help="Overwrite existing extracted_data")
    args = parser.parse_args()

    api_key = get_api_key()
    log.info(f"DB: {DB_PATH}")
    log.info(f"Model: {MODEL}")

    conn = sqlite3.connect(str(DB_PATH))
    records = fetch_records(conn, args.limit, args.force)
    total = len(records)
    log.info(f"Records to process: {total}")

    if total == 0:
        log.info("Nothing to process. Use --force to overwrite existing data.")
        conn.close()
        return

    success = 0
    failed = 0

    for i, (record_id, title, body, _) in enumerate(records, 1):
        log.info(f"[{i}/{total}] id={record_id}: {title[:60]}...")
        data = call_claude(api_key, title, body)

        if data:
            update_record(conn, record_id, data, args.dry_run)
            success += 1
            log.info(f"  -> {data.get('company_name')} | {data.get('round_type')} | {data.get('amount_raw')}")
        else:
            failed += 1
            log.warning(f"  -> FAILED to extract for id={record_id}")

        # Batch delay to respect rate limits
        if i % BATCH_SIZE == 0 and i < total:
            log.info(f"  Batch pause ({BATCH_DELAY_SEC}s)...")
            time.sleep(BATCH_DELAY_SEC)

    if not args.dry_run:
        conn.commit()
        log.info("Changes committed to DB.")

    conn.close()
    log.info(f"Done. Success: {success}, Failed: {failed}, Total: {total}")


if __name__ == "__main__":
    main()
