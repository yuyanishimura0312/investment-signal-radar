#!/usr/bin/env python3
"""
Extract structured funding data from unused funding press releases.

Targets press_releases where category='funding' AND funding_round_id IS NULL,
sends title + body_text to Claude Haiku for extraction, creates funding_rounds,
and links the press_release back to the created round.

Usage:
    python3 scripts/extract_remaining_funding_prs.py --limit 300
    python3 scripts/extract_remaining_funding_prs.py --limit 50 --dry-run
    python3 scripts/extract_remaining_funding_prs.py  # process all with body_text
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
project_root = str(Path(__file__).resolve().parent.parent)
sys.path.insert(0, project_root)
from src.db.models_v2 import get_conn, insert_funding_round, round_exists

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "investment_signal_v2.db"
MODEL = "claude-haiku-4-5-20251001"
BATCH_SIZE = 10
BATCH_DELAY_SEC = 1
BODY_TRUNCATE = 3000

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
    result = subprocess.run(
        ["security", "find-generic-password", "-s", "anthropic-api-key", "-w"],
        capture_output=True, text=True,
    )
    key = result.stdout.strip()
    if not key:
        # Try alternate service name
        result = subprocess.run(
            ["security", "find-generic-password", "-s", "ANTHROPIC_API_KEY", "-w"],
            capture_output=True, text=True,
        )
        key = result.stdout.strip()
    if not key:
        log.error("Failed to retrieve Anthropic API key from keychain")
        sys.exit(1)
    return key


def call_claude(api_key: str, title: str, body: str) -> dict | None:
    """Call Claude Haiku to extract structured funding data."""
    import anthropic

    client = anthropic.Anthropic(api_key=api_key)
    body_truncated = (body or "")[:BODY_TRUNCATE]

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


def fetch_records(conn: sqlite3.Connection, limit: int | None) -> list:
    """Fetch funding press_releases (category='funding', no round yet, has body_text)."""
    query = """
        SELECT id, title, body_text, source_url, published_at
        FROM press_releases
        WHERE category = 'funding'
          AND funding_round_id IS NULL
          AND body_text IS NOT NULL
          AND body_text != ''
        ORDER BY published_at DESC
    """
    if limit:
        query += f" LIMIT {limit}"
    return conn.execute(query).fetchall()


def main():
    parser = argparse.ArgumentParser(
        description="Extract funding data from unused funding press releases"
    )
    parser.add_argument("--limit", type=int, default=300,
                        help="Max records to process (default: 300)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Don't write to DB — just log what would happen")
    args = parser.parse_args()

    api_key = get_api_key()
    conn = get_conn(DB_PATH)
    conn.execute("PRAGMA busy_timeout = 30000")

    records = fetch_records(conn, args.limit)
    total = len(records)
    log.info(f"Records to process: {total} (limit={args.limit})")

    if total == 0:
        log.info("Nothing to process — all funding PRs already have rounds or no body_text.")
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

        log.info(f"[{i}/{total}] id={rec_id}: {title[:70]}")

        # Skip if a round already exists for this URL
        if round_exists(conn, source_url):
            skipped_duplicate += 1
            log.info(f"  -> SKIP (round already exists for URL)")
            continue

        if args.dry_run:
            log.info(f"  [DRY RUN] Would call Claude and create round")
            continue

        # Call Claude Haiku for extraction
        data = call_claude(api_key, title, body)
        time.sleep(1)  # 1s delay between API calls

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

        # Not actually a funding announcement
        if not data.get("is_funding", True):
            skipped_not_funding += 1
            try:
                conn.execute(
                    "UPDATE press_releases SET is_funding_related = 0 WHERE id = ?",
                    (rec_id,),
                )
                conn.commit()
            except sqlite3.OperationalError:
                pass
            log.info(f"  -> NOT FUNDING (skipped)")
            continue

        company_name = (data.get("company_name") or "").strip()
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
                    # Link press_release to the new funding_round
                    conn.execute(
                        """UPDATE press_releases
                           SET funding_round_id = ?,
                               extracted_data = ?,
                               is_funding_related = 1
                           WHERE id = ?""",
                        (round_id, json.dumps(data, ensure_ascii=False), rec_id),
                    )
                    created += 1
                    inv_names = [inv.get("name", "?") for inv in investors[:3]]
                    log.info(
                        f"  -> CREATED round_id={round_id}: {company_name} | "
                        f"{round_type} | {amount_raw} | investors={inv_names}"
                    )
                else:
                    skipped_duplicate += 1
                    log.info(f"  -> SKIP (duplicate URL hash)")
                break

            except sqlite3.OperationalError as e:
                if "locked" in str(e) and retry < 2:
                    log.warning(f"  -> DB locked, retry {retry+1}/3 in 10s...")
                    time.sleep(10)
                    continue
                failed += 1
                log.error(f"  -> DB ERROR: {e}")
                break
            except Exception as e:
                failed += 1
                log.error(f"  -> ERROR: {e}")
                break

        # Batch commit every BATCH_SIZE records
        if i % BATCH_SIZE == 0:
            conn.commit()
            log.info(f"  --- Batch committed ({i}/{total}) | "
                     f"created={created} dup={skipped_duplicate} "
                     f"notfunding={skipped_not_funding} failed={failed} ---")

    conn.commit()
    conn.close()

    log.info(
        f"\n=== DONE ===\n"
        f"  Processed : {total}\n"
        f"  Created   : {created}\n"
        f"  Duplicate : {skipped_duplicate}\n"
        f"  NotFunding: {skipped_not_funding}\n"
        f"  Failed    : {failed}"
    )


if __name__ == "__main__":
    main()
