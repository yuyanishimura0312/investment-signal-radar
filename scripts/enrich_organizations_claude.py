#!/usr/bin/env python3
"""
Enrich organizations in the investment_signal_v2.db using Claude Haiku API.
Fetches website, founded_date, and description for companies (is_company=1)
that have not yet been enriched (enriched_at IS NULL).

Usage:
    python3 scripts/enrich_organizations_claude.py [--limit N] [--db PATH]
"""

import argparse
import json
import sqlite3
import subprocess
import sys
import time
from datetime import datetime

import anthropic

DB_PATH = "data/investment_signal_v2.db"
MODEL = "claude-haiku-4-5-20251001"
BATCH_SIZE = 10
RATE_LIMIT_SECONDS = 1.0


def get_api_key() -> str:
    result = subprocess.run(
        ["security", "find-generic-password", "-s", "ANTHROPIC_API_KEY", "-w"],
        capture_output=True,
        text=True,
    )
    key = result.stdout.strip()
    if not key:
        print("ERROR: Could not retrieve Anthropic API key from keychain.", file=sys.stderr)
        sys.exit(1)
    return key


def fetch_companies(conn: sqlite3.Connection, limit: int) -> list[tuple]:
    """Fetch companies that have not been enriched yet."""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, name, name_en, description
        FROM organizations
        WHERE enriched_at IS NULL
          AND is_company = 1
        ORDER BY id ASC
        LIMIT ?
        """,
        (limit,),
    )
    return cur.fetchall()


def build_prompt(batch: list[tuple]) -> str:
    """Build a batch prompt asking Claude to enrich company data."""
    lines = []
    for i, (org_id, name, name_en, description) in enumerate(batch, 1):
        display_name = name_en if name_en else name
        lines.append(f"{i}. ID={org_id} | name={display_name} (Japanese: {name})")

    companies_text = "\n".join(lines)

    return f"""以下の{len(batch)}社について、それぞれの公式ウェブサイト、設立年、および説明文を調べてください。

{companies_text}

各企業について以下の情報をJSONで返してください：
- website: 公式URLまたはnull（不明な場合）
- founded_date: 設立年（YYYY形式）または設立日（YYYY-MM-DD形式）、不明な場合はnull
- description: 1〜2文の日本語による企業説明（何をしている会社か）。既に説明がある場合は改善してもよい。不明な場合はnull

必ずJSONオブジェクトの配列で返してください。各要素にはIDも含めてください。
例：
[
  {{"id": 123, "website": "https://example.com", "founded_date": "2020", "description": "AIを活用した〜を提供する企業。"}},
  {{"id": 456, "website": null, "founded_date": null, "description": null}}
]

重要：
- 確実に知っている情報のみを記入してください。不確かな場合はnullにしてください
- websiteは公式サイトのURLのみ（SNSやプレスリリースサイト等は除く）
- founded_dateは設立・創業年を記入
- descriptionは簡潔に1〜2文（日本語）
- JSONのみを返し、説明文や前置きは不要です"""


def call_claude(client: anthropic.Anthropic, prompt: str) -> list[dict] | None:
    """Call Claude API and parse the JSON response."""
    try:
        message = client.messages.create(
            model=MODEL,
            max_tokens=2000,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = message.content[0].text.strip()

        # Extract JSON array from response (handle markdown code blocks)
        if "```" in raw:
            start = raw.find("[")
            end = raw.rfind("]") + 1
            if start != -1 and end > start:
                raw = raw[start:end]
        elif raw.startswith("["):
            pass  # already clean JSON
        else:
            # Try to find the array
            start = raw.find("[")
            end = raw.rfind("]") + 1
            if start != -1 and end > start:
                raw = raw[start:end]

        return json.loads(raw)
    except json.JSONDecodeError as e:
        print(f"  [WARN] JSON parse error: {e}")
        print(f"  Raw response (first 300 chars): {raw[:300]}")
        return None
    except anthropic.APIError as e:
        print(f"  [WARN] API error: {e}")
        return None
    except Exception as e:
        print(f"  [WARN] Unexpected error: {e}")
        return None


def update_organizations(conn: sqlite3.Connection, results: list[dict], batch: list[tuple]) -> int:
    """Update database with enrichment results. Returns number of records updated."""
    cur = conn.cursor()
    now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    # Build a lookup of existing descriptions by ID
    existing_desc = {org_id: desc for org_id, _, _, desc in batch}

    updated = 0
    for item in results:
        org_id = item.get("id")
        if org_id is None:
            continue

        website = item.get("website")
        founded_date = item.get("founded_date")
        new_desc = item.get("description")

        # Only update description if it was null/empty before
        use_desc = existing_desc.get(org_id)
        if not use_desc and new_desc:
            use_desc = new_desc

        cur.execute(
            """
            UPDATE organizations
            SET website = COALESCE(?, website),
                founded_date = COALESCE(?, founded_date),
                description = COALESCE(?, description),
                enriched_at = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (website, founded_date, use_desc, now, now, org_id),
        )
        updated += cur.rowcount

    # Mark all batch IDs as enriched even if no data was returned
    result_ids = {item.get("id") for item in results if item.get("id")}
    batch_ids = [org_id for org_id, _, _, _ in batch]
    unenriched_ids = [oid for oid in batch_ids if oid not in result_ids]
    for org_id in unenriched_ids:
        cur.execute(
            "UPDATE organizations SET enriched_at = ?, updated_at = ? WHERE id = ?",
            (now, now, org_id),
        )

    conn.commit()
    return updated


def main():
    parser = argparse.ArgumentParser(description="Enrich organizations using Claude Haiku API")
    parser.add_argument("--limit", type=int, default=100, help="Max number of companies to process (default: 100)")
    parser.add_argument("--db", default=DB_PATH, help=f"Database path (default: {DB_PATH})")
    args = parser.parse_args()

    print(f"=== Organization Enrichment via Claude Haiku ===")
    print(f"Database: {args.db}")
    print(f"Limit: {args.limit} companies")
    print(f"Batch size: {BATCH_SIZE}")
    print()

    # Connect to DB
    conn = sqlite3.connect(args.db)

    # Fetch companies
    print("Fetching companies to enrich...")
    companies = fetch_companies(conn, args.limit)
    print(f"Found {len(companies)} companies to process")
    print()

    if not companies:
        print("Nothing to enrich. All companies already have enriched_at set.")
        conn.close()
        return

    # Init Claude client
    api_key = get_api_key()
    client = anthropic.Anthropic(api_key=api_key)

    # Process in batches
    total_updated = 0
    total_batches = (len(companies) + BATCH_SIZE - 1) // BATCH_SIZE
    failed_batches = 0

    for batch_num in range(total_batches):
        start_idx = batch_num * BATCH_SIZE
        batch = companies[start_idx : start_idx + BATCH_SIZE]

        print(f"Batch {batch_num + 1}/{total_batches} ({len(batch)} companies):")
        for _, name, _, _ in batch:
            print(f"  - {name}")

        prompt = build_prompt(batch)
        results = call_claude(client, prompt)

        if results is None:
            print(f"  [SKIP] Batch failed, marking as enriched_at to avoid re-processing.")
            failed_batches += 1
            # Mark as enriched with null data to avoid re-processing forever
            now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
            cur = conn.cursor()
            for org_id, _, _, _ in batch:
                cur.execute(
                    "UPDATE organizations SET enriched_at = ?, updated_at = ? WHERE id = ?",
                    (now, now, org_id),
                )
            conn.commit()
        else:
            updated = update_organizations(conn, results, batch)
            total_updated += updated
            websites_found = sum(1 for r in results if r.get("website"))
            dates_found = sum(1 for r in results if r.get("founded_date"))
            descs_found = sum(1 for r in results if r.get("description"))
            print(f"  -> Updated {updated} records | websites={websites_found}, dates={dates_found}, descs={descs_found}")

        # Rate limiting between batches (not needed after last batch)
        if batch_num < total_batches - 1:
            time.sleep(RATE_LIMIT_SECONDS)

    conn.close()

    # Summary
    print()
    print("=== Summary ===")
    print(f"Companies processed: {len(companies)}")
    print(f"Records updated:     {total_updated}")
    print(f"Failed batches:      {failed_batches}")
    print(f"Successful batches:  {total_batches - failed_batches}")


if __name__ == "__main__":
    main()
