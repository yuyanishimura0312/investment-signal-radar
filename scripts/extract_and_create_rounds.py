#!/usr/bin/env python3
"""
Extract structured funding data from press_releases and create funding_rounds.

Reads funding-related press_releases with body_text from the v2 database,
sends them to Claude Haiku for structured extraction, and creates
funding_rounds + organizations + round_participants via models_v2.

Usage:
    python3 scripts/extract_and_create_rounds.py --limit 10 --dry-run
    python3 scripts/extract_and_create_rounds.py --limit 500
    python3 scripts/extract_and_create_rounds.py  # process all
"""

import argparse
import json
import logging
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.db.models_v2 import get_conn, insert_funding_round, round_exists

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "investment_signal_v2.db"
MODEL = "claude-haiku-4-5-20251001"
BATCH_SIZE = 20
BATCH_DELAY_SEC = 2

SYSTEM_PROMPT = """\
You are a structured data extractor for Japanese startup funding announcements.
Given a title and body text, extract the following fields as JSON.
If a field cannot be determined, use null.

Output ONLY valid JSON with these keys:
- company_name: Official company name in Japanese (include 株式会社 etc.)
- company_description: Brief description of the company (1-2 sentences in Japanese)
- round_type: One of: pre_seed, seed, series_a, series_b, series_c, series_d, series_e, strategic, debt, grant, ipo, angel, convertible_note, j_kiss, corporate_round, secondary, late_stage, unknown
- amount_jpy: Funding amount in JPY as integer (e.g. 500000000 for 5億円). null if not disclosed.
- amount_raw: Original text for the amount (e.g. "5億円", "20億円"). null if not mentioned.
- investors: Array of objects [{name: string, type: "vc"|"cvc"|"angel"|"bank"|"corporate"|"gov"|"other", is_lead: boolean}]. Empty array if none mentioned.
- sector: Business sector (e.g. "AI/機械学習", "ヘルスケア", "フィンテック", "SaaS", "バイオテック", "クリーンテック", "モビリティ", "EdTech", "不動産テック", "サイバーセキュリティ", "物流", "HR Tech", "InsurTech", "FoodTech", "ディープテック")
- announced_date: Announcement date as YYYY-MM-DD. null if not determinable.
- confidence: Extraction confidence (high/medium/low). high if clear funding announcement, medium if some inference needed, low if uncertain.
- is_funding: true if this is actually a funding announcement, false otherwise.

Rules:
- If this is NOT a funding announcement, return {"is_funding": false}
- For ambiguous amounts like "数億円", set amount_jpy to null and confidence to low
- Identify lead investors with is_lead: true
- Use 1 USD = 150 JPY for conversion
- Output ONLY valid JSON, no other text
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
    body_truncated = (body or "")[:4000]

    try:
        response = client.messages.create(
            model=MODEL,
            max_tokens=1024,
            system=SYSTEM_PROMPT,
            messages=[{
                "role": "user",
                "content": f"Title: {title}\n\nBody:\n{body_truncated}",
            }],
        )
        raw = response.content[0].text.strip()

        # Strip markdown code fences if present
        if raw.startswith("```"):
            lines = raw.split("\n")
            lines = [l for l in lines if not l.strip().startswith("```")]
            raw = "\n".join(lines)

        return json.loads(raw)
    except json.JSONDecodeError as e:
        log.warning(f"JSON parse error: {e} — raw: {raw[:200]}")
        return None
    except Exception as e:
        log.warning(f"API call failed: {e}")
        return None


def fetch_records(conn: sqlite3.Connection, limit: int | None, include_no_body: bool) -> list:
    """Fetch funding press_releases that need round creation."""
    where = "is_funding_related = 1 AND funding_round_id IS NULL"
    if not include_no_body:
        where += " AND body_text IS NOT NULL AND body_text != ''"
    query = f"""
        SELECT id, title, body_text, source_url, published_at, extracted_data
        FROM press_releases
        WHERE {where}
        ORDER BY published_at DESC
    """
    if limit:
        query += f" LIMIT {limit}"
    return conn.execute(query).fetchall()


def main():
    parser = argparse.ArgumentParser(description="Extract funding data and create rounds")
    parser.add_argument("--limit", type=int, default=None, help="Max records to process")
    parser.add_argument("--dry-run", action="store_true", help="Don't write to DB")
    parser.add_argument("--include-no-body", action="store_true",
                        help="Also process records without body_text (title-only extraction)")
    args = parser.parse_args()

    api_key = get_api_key()
    conn = get_conn(DB_PATH)
    conn.execute("PRAGMA busy_timeout = 30000")  # Wait up to 30s for lock

    records = fetch_records(conn, args.limit, args.include_no_body)
    total = len(records)
    log.info(f"Records to process: {total}")

    if total == 0:
        log.info("Nothing to process.")
        conn.close()
        return

    created = 0
    skipped_not_funding = 0
    skipped_duplicate = 0
    failed = 0

    for i, row in enumerate(records, 1):
        rec_id = row[0]
        title = row[1]
        body = row[2] or ""
        source_url = row[3]
        published_at = row[4]
        existing_data = row[5]

        log.info(f"[{i}/{total}] id={rec_id}: {title[:60]}...")

        # Check if round already exists for this URL
        if round_exists(conn, source_url):
            skipped_duplicate += 1
            log.info(f"  -> SKIP (round already exists)")
            continue

        if args.dry_run:
            log.info(f"  [DRY RUN] Would extract and create round")
            continue

        # Call Claude for extraction
        data = call_claude(api_key, title, body)
        if not data:
            failed += 1
            continue

        # Handle list responses (take first element)
        if isinstance(data, list):
            data = data[0] if data else {}
        if not isinstance(data, dict):
            failed += 1
            log.warning(f"  -> Unexpected response type: {type(data)}")
            continue

        # Check if actually funding
        if not data.get("is_funding", True):
            skipped_not_funding += 1
            try:
                conn.execute(
                    "UPDATE press_releases SET is_funding_related = 0 WHERE id = ?",
                    (rec_id,),
                )
            except sqlite3.OperationalError:
                pass  # Skip DB lock for non-critical update
            log.info(f"  -> NOT FUNDING (reclassified)")
            continue

        # Create funding round via models_v2
        company_name = data.get("company_name") or ""
        if not company_name:
            failed += 1
            log.warning(f"  -> No company_name extracted")
            continue

        investors = data.get("investors") or []
        amount_jpy = data.get("amount_jpy")
        amount_raw = data.get("amount_raw") or ""
        round_type = data.get("round_type") or "unknown"
        sector = data.get("sector") or ""
        announced_date = data.get("announced_date") or published_at or ""
        confidence = data.get("confidence") or "medium"
        description = data.get("company_description") or ""

        for retry in range(3):
            try:
                round_id = insert_funding_round(
                    conn,
                    company_name=company_name,
                    investors=investors,
                    amount_jpy=amount_jpy,
                    amount_raw=amount_raw,
                    round_type=round_type,
                    announced_date=announced_date,
                    source_url=source_url,
                    source_title=title,
                    sector=sector,
                    confidence=confidence,
                    description=description,
                    data_source_name="claude_extracted",
                )

                if round_id:
                    # Link press_release to funding_round
                    conn.execute(
                        "UPDATE press_releases SET funding_round_id = ?, extracted_data = ? WHERE id = ?",
                        (round_id, json.dumps(data, ensure_ascii=False), rec_id),
                    )
                    created += 1
                    inv_names = [inv.get("name", "?") for inv in investors[:3]]
                    log.info(f"  -> CREATED round_id={round_id}: {company_name} | {round_type} | {amount_raw} | investors={inv_names}")
                else:
                    skipped_duplicate += 1
                    log.info(f"  -> SKIP (duplicate URL)")
                break
            except sqlite3.OperationalError as e:
                if "locked" in str(e) and retry < 2:
                    log.warning(f"  -> DB locked, retry {retry+1}/3 in 10s...")
                    time.sleep(10)
                    continue
                failed += 1
                log.error(f"  -> ERROR: {e}")
                break
            except Exception as e:
                failed += 1
                log.error(f"  -> ERROR: {e}")
                break

        # Batch commit + delay
        if i % BATCH_SIZE == 0:
            conn.commit()
            log.info(f"  --- Batch committed ({i}/{total}) ---")
            time.sleep(BATCH_DELAY_SEC)

    conn.commit()
    conn.close()
    log.info(f"Done. Created: {created}, NotFunding: {skipped_not_funding}, "
             f"Duplicate: {skipped_duplicate}, Failed: {failed}, Total: {total}")


if __name__ == "__main__":
    main()
