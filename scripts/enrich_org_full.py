#!/usr/bin/env python3
"""
Full organization enrichment: website, region/city, status, name_en.

Extends enrich_organizations_claude.py to cover all empty fields.
Processes companies first, then top investors.

Usage:
    python3 scripts/enrich_org_full.py --target companies --limit 200
    python3 scripts/enrich_org_full.py --target investors --limit 100
    python3 scripts/enrich_org_full.py --target companies --skip-enriched
"""

import argparse
import json
import sqlite3
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

try:
    import anthropic
except ImportError:
    print("pip install anthropic")
    sys.exit(1)

DB_PATH = Path(__file__).parent.parent / "data" / "investment_signal_v2.db"
MODEL = "claude-haiku-4-5-20251001"
BATCH_SIZE = 15
RATE_LIMIT_SECONDS = 1.0


def get_api_key() -> str:
    result = subprocess.run(
        ["security", "find-generic-password", "-s", "ANTHROPIC_API_KEY", "-w"],
        capture_output=True, text=True,
    )
    key = result.stdout.strip()
    if not key:
        print("ERROR: Could not retrieve Anthropic API key from keychain.", file=sys.stderr)
        sys.exit(1)
    return key


def fetch_targets(conn: sqlite3.Connection, target: str, limit: int,
                  skip_enriched: bool) -> list[dict]:
    """Fetch organizations needing enrichment."""
    if target == "companies":
        where = "is_company = 1"
        order = """
            CASE
                WHEN website IS NULL OR website = '' THEN 0
                ELSE 1
            END ASC,
            CASE
                WHEN region IS NULL OR region = '' THEN 0
                ELSE 1
            END ASC,
            id ASC
        """
    else:  # investors
        where = "is_investor = 1"
        order = """
            CASE
                WHEN description IS NULL OR description = '' THEN 0
                ELSE 1
            END ASC,
            id ASC
        """

    if skip_enriched:
        where += " AND (enriched_at IS NULL OR region IS NULL OR region = '')"

    rows = conn.execute(f"""
        SELECT id, name, name_en, description, website, founded_date,
               region, city, status, investor_type
        FROM organizations
        WHERE {where}
        ORDER BY {order}
        LIMIT ?
    """, (limit,)).fetchall()

    return [dict(zip(
        ['id', 'name', 'name_en', 'description', 'website', 'founded_date',
         'region', 'city', 'status', 'investor_type'], row
    )) for row in rows]


def build_company_prompt(batch: list[dict]) -> str:
    lines = []
    for org in batch:
        desc_hint = f" (概要: {org['description'][:60]})" if org.get('description') else ""
        web_hint = f" (web: {org['website']})" if org.get('website') else ""
        lines.append(f"  ID={org['id']} | {org['name']}{desc_hint}{web_hint}")

    return f"""以下の{len(batch)}社の日本のスタートアップについて、各社の情報を調べてください。

{chr(10).join(lines)}

各企業について以下をJSON配列で返してください:
- id: 企業ID（そのまま）
- name_en: 英語名（公式英語名があれば。なければnull）
- website: 公式サイトURL（既にある場合はそのまま。不明ならnull）
- region: 本社所在地の都道府県（例: "東京都", "大阪府"）。不明ならnull
- city: 本社所在地の市区町村（例: "渋谷区", "中央区"）。不明ならnull
- status: 現在の状態。"active"（営業中）, "acquired"（買収済み）, "ipo"（上場済み）, "closed"（閉鎖）のいずれか。不明なら"active"
- employee_estimate: 従業員数の概算（不明ならnull）

重要ルール:
- 確実に知っている情報のみ。不確かならnull
- regionは日本の都道府県名で統一
- statusは上記4種のみ
- JSONのみを返し、前置き・説明は不要"""


def build_investor_prompt(batch: list[dict]) -> str:
    lines = []
    for org in batch:
        itype = f" ({org['investor_type']})" if org.get('investor_type') else ""
        lines.append(f"  ID={org['id']} | {org['name']}{itype}")

    return f"""以下の{len(batch)}社の投資家・VC・CVCについて情報を調べてください。

{chr(10).join(lines)}

各投資家について以下をJSON配列で返してください:
- id: ID（そのまま）
- name_en: 英語名（あれば）
- website: 公式サイトURL（不明ならnull）
- description: 1文の日本語説明（何に投資するVCか）。不明ならnull
- region: 本社所在地の都道府県。不明ならnull
- founded_date: 設立年（YYYY形式）。不明ならnull

重要: 確実な情報のみ。JSONのみ返してください。"""


def call_claude(client: anthropic.Anthropic, prompt: str) -> list[dict] | None:
    try:
        message = client.messages.create(
            model=MODEL,
            max_tokens=4000,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = message.content[0].text.strip()
        start = raw.find("[")
        end = raw.rfind("]") + 1
        if start != -1 and end > start:
            raw = raw[start:end]
        return json.loads(raw)
    except json.JSONDecodeError as e:
        print(f"  [WARN] JSON parse error: {e}")
        return None
    except anthropic.APIError as e:
        print(f"  [WARN] API error: {e}")
        return None
    except Exception as e:
        print(f"  [WARN] Unexpected error: {e}")
        return None


def apply_results(conn: sqlite3.Connection, results: list[dict],
                  target: str) -> int:
    now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    updated = 0

    for item in results:
        org_id = item.get("id")
        if org_id is None:
            continue

        if target == "companies":
            conn.execute("""
                UPDATE organizations SET
                    name_en = COALESCE(?, name_en),
                    website = COALESCE(?, website),
                    region = COALESCE(?, region),
                    city = COALESCE(?, city),
                    status = CASE
                        WHEN ? IN ('active','acquired','ipo','closed') THEN ?
                        ELSE status
                    END,
                    enriched_at = ?,
                    updated_at = ?
                WHERE id = ?
            """, (
                item.get("name_en"),
                item.get("website"),
                item.get("region"),
                item.get("city"),
                item.get("status", "active"), item.get("status", "active"),
                now, now, org_id
            ))
        else:  # investors
            conn.execute("""
                UPDATE organizations SET
                    name_en = COALESCE(?, name_en),
                    website = COALESCE(?, website),
                    description = COALESCE(?, description),
                    region = COALESCE(?, region),
                    founded_date = COALESCE(?, founded_date),
                    enriched_at = ?,
                    updated_at = ?
                WHERE id = ?
            """, (
                item.get("name_en"),
                item.get("website"),
                item.get("description"),
                item.get("region"),
                item.get("founded_date"),
                now, now, org_id
            ))

        updated += 1

    conn.commit()
    return updated


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", choices=["companies", "investors"], default="companies")
    parser.add_argument("--limit", type=int, default=200)
    parser.add_argument("--skip-enriched", action="store_true")
    parser.add_argument("--db", type=str, default=str(DB_PATH))
    args = parser.parse_args()

    conn = sqlite3.connect(args.db, timeout=30)
    conn.execute("PRAGMA journal_mode = WAL")

    targets = fetch_targets(conn, args.target, args.limit, args.skip_enriched)
    print(f"Target: {args.target}, found {len(targets)} to enrich")

    if not targets:
        print("Nothing to enrich.")
        conn.close()
        return

    client = anthropic.Anthropic(api_key=get_api_key())
    total_updated = 0

    for i in range(0, len(targets), BATCH_SIZE):
        batch = targets[i:i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        total_batches = (len(targets) + BATCH_SIZE - 1) // BATCH_SIZE

        print(f"  Batch {batch_num}/{total_batches} ({len(batch)} orgs)...", end=" ", flush=True)

        if args.target == "companies":
            prompt = build_company_prompt(batch)
        else:
            prompt = build_investor_prompt(batch)

        results = call_claude(client, prompt)
        if results:
            updated = apply_results(conn, results, args.target)
            total_updated += updated
            print(f"updated {updated}")
        else:
            print("failed")

        time.sleep(RATE_LIMIT_SECONDS)

    # Print coverage summary
    print(f"\nTotal updated: {total_updated}")
    print("\n--- Post-enrichment coverage ---")
    total = conn.execute("SELECT COUNT(*) FROM organizations").fetchone()[0]
    for field in ['name_en', 'website', 'region', 'city', 'description']:
        r = conn.execute(
            f"SELECT COUNT(*) FROM organizations WHERE {field} IS NOT NULL AND {field} != ''"
        ).fetchone()[0]
        print(f"  {field}: {r}/{total} ({100*r/total:.1f}%)")

    status_dist = conn.execute(
        "SELECT status, COUNT(*) FROM organizations GROUP BY status ORDER BY COUNT(*) DESC"
    ).fetchall()
    print("  status distribution:", dict(status_dist))

    conn.close()


if __name__ == "__main__":
    main()
