#!/usr/bin/env python3
"""
Migrate data from v1 schema (investment_signal.db) to v2 schema
(investment_signal_v2.db).

Migration mapping:
  v1.companies                -> v2.organizations (primary_role=company, is_company=1)
  v1.investors                -> v2.organizations (primary_role=investor, is_investor=1)
  v1.investments              -> v2.funding_rounds + v2.events(event_type=funding)
  v1.investment_investors     -> v2.round_participants
  v1.sectors                  -> v2.sectors (merge with seed data)
  v1.signals                  -> v2.signals

Usage:
    python3 scripts/migrate_v1_to_v2.py
    python3 scripts/migrate_v1_to_v2.py --dry-run
    python3 scripts/migrate_v1_to_v2.py --v1 data/investment_signal.db --v2 data/investment_signal_v2.db
"""

import argparse
import hashlib
import json
import re
import sqlite3
import sys
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(__file__).parent.parent
DEFAULT_V1 = PROJECT_ROOT / "data" / "investment_signal.db"
DEFAULT_V2 = PROJECT_ROOT / "data" / "investment_signal_v2.db"


# ---------------------------------------------------------------
# Round type mapping (v1 -> v2)
# ---------------------------------------------------------------
ROUND_TYPE_MAP = {
    "pre-seed": "pre_seed",
    "seed": "seed",
    "pre-a": "series_a",
    "a": "series_a",
    "b": "series_b",
    "c": "series_c",
    "d": "series_d",
    "e": "series_e",
    "f": "series_f",
    "g": "series_g",
    "strategic": "strategic",
    "debt": "debt",
    "grant": "grant",
    "ipo": "ipo",
    "angel": "angel",
    "unknown": "unknown",
}


def slugify(name: str) -> str:
    """Generate URL-safe slug from name (ASCII-only).

    Non-ASCII names (Japanese etc.) fall back to a deterministic md5-based
    slug so the result can safely appear in URLs and file paths.
    """
    if not name:
        return f"org-{int(datetime.now().timestamp() * 1000)}"
    s = re.sub(r"[^a-z0-9\s-]", "", name.lower())
    s = re.sub(r"[\s_]+", "-", s).strip("-")
    if not s:
        s = "org-" + hashlib.md5(name.encode("utf-8")).hexdigest()[:12]
    return s[:80]


def ensure_unique_slug(conn_v2: sqlite3.Connection, base_slug: str) -> str:
    """Ensure slug uniqueness by appending a counter if needed."""
    slug = base_slug
    counter = 1
    while conn_v2.execute(
        "SELECT 1 FROM organizations WHERE slug = ?", (slug,)
    ).fetchone():
        slug = f"{base_slug}-{counter}"
        counter += 1
    return slug


def get_data_source_id(conn_v2: sqlite3.Connection, name: str) -> int:
    """Get data_source id by name (must exist in seed data)."""
    row = conn_v2.execute(
        "SELECT id FROM data_sources WHERE name = ?", (name,)
    ).fetchone()
    if row is None:
        raise ValueError(f"Data source '{name}' not found. Run init_db_v2.py first.")
    return row[0]


def migrate_sectors(conn_v1: sqlite3.Connection, conn_v2: sqlite3.Connection,
                    dry_run: bool = False) -> dict[int, int]:
    """Migrate sectors. Returns v1_id -> v2_id mapping."""
    v1_sectors = conn_v1.execute(
        "SELECT id, name, description FROM sectors"
    ).fetchall()
    mapping: dict[int, int] = {}
    added = 0
    matched = 0

    for v1_id, name, desc in v1_sectors:
        if not name:
            continue
        # Try match with v2 seed sectors by name (case-insensitive, ja or en)
        row = conn_v2.execute(
            "SELECT id FROM sectors WHERE LOWER(name) = LOWER(?) OR name_ja = ?",
            (name, name),
        ).fetchone()
        if row:
            mapping[v1_id] = row[0]
            matched += 1
            continue
        # Otherwise insert as new sector
        if not dry_run:
            cur = conn_v2.execute(
                """INSERT INTO sectors (name, name_ja, description, sort_order)
                   VALUES (?, ?, ?, 100)""",
                (name, name, desc),
            )
            mapping[v1_id] = cur.lastrowid
        added += 1

    print(f"  Sectors: matched={matched}, added={added}")
    return mapping


def migrate_companies(conn_v1: sqlite3.Connection, conn_v2: sqlite3.Connection,
                      sector_map: dict[int, int], migrated_ds_id: int,
                      dry_run: bool = False) -> dict[int, int]:
    """Migrate companies to organizations. Idempotent: reuses existing orgs by name."""
    rows = conn_v1.execute("""
        SELECT id, canonical_name, aliases, website_url, founded_year,
               description, sector_id, pestle_category, country,
               created_at, updated_at
        FROM companies
    """).fetchall()

    mapping: dict[int, int] = {}
    reused = 0
    created = 0
    for row in rows:
        v1_id = row["id"]
        name = row["canonical_name"] or "Unknown"
        aliases = row["aliases"] or "[]"
        founded_date = f"{row['founded_year']}-01-01" if row["founded_year"] else None

        # Idempotency: reuse existing organization with same name
        existing = conn_v2.execute(
            "SELECT id FROM organizations WHERE LOWER(name) = LOWER(?)", (name,)
        ).fetchone()
        if existing:
            mapping[v1_id] = existing[0]
            if not dry_run:
                # Ensure company role is marked
                conn_v2.execute(
                    "UPDATE organizations SET is_company = 1 WHERE id = ?",
                    (existing[0],),
                )
            reused += 1
            continue

        if dry_run:
            mapping[v1_id] = -v1_id
            created += 1
            continue

        slug = ensure_unique_slug(conn_v2, slugify(name))
        cur = conn_v2.execute(
            """INSERT INTO organizations (
                slug, name, aliases, primary_role, is_company, is_investor,
                description, founded_date, country_code,
                data_source_id, confidence_score, collected_at, updated_at
            ) VALUES (?, ?, ?, 'company', 1, 0, ?, ?, ?, ?, 0.5, ?, ?)""",
            (
                slug, name, aliases, row["description"], founded_date,
                row["country"] or "JP",
                migrated_ds_id, row["created_at"], row["updated_at"],
            ),
        )
        v2_id = cur.lastrowid
        mapping[v1_id] = v2_id
        created += 1

        # Link to sector
        if row["sector_id"] and row["sector_id"] in sector_map:
            conn_v2.execute(
                """INSERT OR IGNORE INTO organization_sectors
                   (organization_id, sector_id, is_primary) VALUES (?, ?, 1)""",
                (v2_id, sector_map[row["sector_id"]]),
            )

    print(f"  Companies -> Organizations: created={created}, reused={reused}")
    return mapping


def migrate_investors(conn_v1: sqlite3.Connection, conn_v2: sqlite3.Connection,
                       migrated_ds_id: int, dry_run: bool = False) -> dict[int, int]:
    """Migrate investors to organizations. Returns v1_investor_id -> v2_org_id."""
    rows = conn_v1.execute("""
        SELECT id, canonical_name, aliases, type, website_url, country, is_active, created_at
        FROM investors
    """).fetchall()

    mapping: dict[int, int] = {}
    for row in rows:
        v1_id = row["id"]
        name = row["canonical_name"] or "Unknown Investor"
        aliases = row["aliases"] or "[]"
        inv_type = row["type"] or "vc"
        # Map old 'other' etc., should already be valid in v2
        if inv_type not in ('vc', 'cvc', 'angel', 'gov', 'bank', 'corporate', 'accelerator', 'other'):
            inv_type = "other"

        # Check if a company with the same name already exists (same entity in dual role)
        existing = conn_v2.execute(
            "SELECT id FROM organizations WHERE LOWER(name) = LOWER(?)",
            (name,),
        ).fetchone()

        if existing:
            v2_id = existing[0]
            if not dry_run:
                # Upgrade to dual role: mark as investor
                conn_v2.execute(
                    """UPDATE organizations
                       SET is_investor = 1, investor_type = COALESCE(investor_type, ?)
                       WHERE id = ?""",
                    (inv_type, v2_id),
                )
            mapping[v1_id] = v2_id
            continue

        base_slug = slugify(name) + "-inv"
        slug = base_slug
        if not dry_run:
            slug = ensure_unique_slug(conn_v2, base_slug)

        if dry_run:
            mapping[v1_id] = -v1_id
            continue

        cur = conn_v2.execute(
            """INSERT INTO organizations (
                slug, name, aliases, primary_role, is_company, is_investor,
                investor_type, website, country_code, status,
                data_source_id, confidence_score, collected_at
            ) VALUES (?, ?, ?, 'investor', 0, 1, ?, ?, ?, ?, ?, 0.5, ?)""",
            (
                slug, name, aliases, inv_type, row["website_url"],
                row["country"] or "JP",
                "active" if row["is_active"] else "closed",
                migrated_ds_id, row["created_at"],
            ),
        )
        mapping[v1_id] = cur.lastrowid

    print(f"  Investors -> Organizations: {len(mapping)}")
    return mapping


def migrate_investments(conn_v1: sqlite3.Connection, conn_v2: sqlite3.Connection,
                         company_map: dict[int, int], investor_map: dict[int, int],
                         migrated_ds_id: int, dry_run: bool = False) -> dict[int, int]:
    """Migrate investments to funding_rounds + events(type=funding).

    Idempotent: skips rounds whose url_hash already exists in v2, and reuses
    the existing round_id for the returned mapping so downstream steps stay
    consistent on re-runs.
    """
    rows = conn_v1.execute("""
        SELECT id, company_id, source_id, announced_date, amount_jpy, amount_raw,
               currency, round_type, confidence, source_url, source_title, url_hash,
               pestle_category, notes, is_duplicate, extracted_at, created_at
        FROM investments
    """).fetchall()

    confidence_score_map = {"high": 0.9, "medium": 0.6, "low": 0.3}
    round_mapping: dict[int, int] = {}
    created = 0
    reused = 0
    events_created = 0
    events_reused = 0

    for row in rows:
        v1_id = row["id"]
        v1_round = row["round_type"] or "unknown"
        v2_round = ROUND_TYPE_MAP.get(v1_round, "unknown")
        conf_val = confidence_score_map.get(row["confidence"] or "medium", 0.6)
        org_id = company_map.get(row["company_id"])

        if org_id is None:
            print(f"  WARN: investment {v1_id} has no company mapping, skipping")
            continue

        # Idempotency: reuse existing funding round by url_hash
        existing = conn_v2.execute(
            "SELECT id FROM funding_rounds WHERE url_hash = ?",
            (row["url_hash"],),
        ).fetchone()
        if existing:
            round_mapping[v1_id] = existing[0]
            reused += 1
            # Check if corresponding funding event already exists
            ev = conn_v2.execute(
                """SELECT 1 FROM events
                   WHERE organization_id = ? AND event_type = 'funding'
                     AND source_url = ?""",
                (org_id, row["source_url"]),
            ).fetchone()
            events_reused += 1 if ev else 0
            continue

        if dry_run:
            round_mapping[v1_id] = -v1_id
            created += 1
            events_created += 1
            continue

        cur = conn_v2.execute(
            """INSERT INTO funding_rounds (
                organization_id, round_type, announced_date, amount_jpy, amount_raw,
                currency, data_source_id, confidence_score, source_url, source_title,
                url_hash, notes, is_duplicate, pestle_category, collected_at, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                org_id, v2_round, row["announced_date"], row["amount_jpy"],
                row["amount_raw"], row["currency"] or "JPY",
                migrated_ds_id, conf_val,
                row["source_url"], row["source_title"], row["url_hash"],
                row["notes"], row["is_duplicate"] or 0, row["pestle_category"],
                row["extracted_at"], row["created_at"],
            ),
        )
        round_id = cur.lastrowid
        round_mapping[v1_id] = round_id
        created += 1

        significance = min(1.0, max(0.3, conf_val))
        event_data = {
            "round_type": v2_round,
            "amount_jpy": row["amount_jpy"],
            "amount_raw": row["amount_raw"],
            "funding_round_id": round_id,
        }
        conn_v2.execute(
            """INSERT INTO events (
                organization_id, event_type, event_date, title, description,
                event_data, significance_score, data_source_id, confidence_score,
                source_url, collected_at, created_at
            ) VALUES (?, 'funding', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                org_id,
                row["announced_date"] or datetime.now().strftime("%Y-%m-%d"),
                row["source_title"] or f"Funding round: {v2_round}",
                row["notes"],
                json.dumps(event_data, ensure_ascii=False),
                significance, migrated_ds_id, conf_val,
                row["source_url"], row["extracted_at"], row["created_at"],
            ),
        )
        events_created += 1

    print(f"  Investments -> Funding Rounds: created={created}, reused={reused}")
    print(f"  Funding Events: created={events_created}, already_present={events_reused}")
    return round_mapping


def migrate_investment_investors(conn_v1: sqlite3.Connection, conn_v2: sqlite3.Connection,
                                  round_map: dict[int, int], investor_map: dict[int, int],
                                  dry_run: bool = False) -> int:
    """Migrate investment_investors to round_participants."""
    rows = conn_v1.execute("""
        SELECT investment_id, investor_id, is_lead FROM investment_investors
    """).fetchall()

    count = 0
    for inv_id, investor_id, is_lead in rows:
        round_id = round_map.get(inv_id)
        org_id = investor_map.get(investor_id)
        if not round_id or not org_id:
            continue
        if not dry_run:
            conn_v2.execute(
                """INSERT OR IGNORE INTO round_participants
                   (funding_round_id, investor_id, is_lead) VALUES (?, ?, ?)""",
                (round_id, org_id, is_lead or 0),
            )
        count += 1

    print(f"  Round Participants: {count}")
    return count


def migrate_signals(conn_v1: sqlite3.Connection, conn_v2: sqlite3.Connection,
                    sector_map: dict[int, int], dry_run: bool = False) -> int:
    """Migrate signals (schema is mostly compatible)."""
    rows = conn_v1.execute("""
        SELECT signal_type, sector_id, detected_at, period_start, period_end,
               baseline_count, current_count, acceleration_ratio, description,
               related_investment_ids, is_reported
        FROM signals
    """).fetchall()

    count = 0
    for r in rows:
        sector_v2 = sector_map.get(r["sector_id"]) if r["sector_id"] else None
        if not dry_run:
            conn_v2.execute(
                """INSERT INTO signals (
                    signal_type, sector_id, detected_at, period_start, period_end,
                    baseline_count, current_count, acceleration_ratio, description,
                    related_round_ids, is_reported
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    r["signal_type"], sector_v2, r["detected_at"],
                    r["period_start"], r["period_end"],
                    r["baseline_count"], r["current_count"],
                    r["acceleration_ratio"], r["description"],
                    r["related_investment_ids"] or "[]", r["is_reported"],
                ),
            )
        count += 1

    print(f"  Signals: {count}")
    return count


def main():
    parser = argparse.ArgumentParser(description="Migrate v1 to v2 schema")
    parser.add_argument("--v1", type=Path, default=DEFAULT_V1,
                        help="Source v1 database path")
    parser.add_argument("--v2", type=Path, default=DEFAULT_V2,
                        help="Target v2 database path")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be migrated without writing")
    args = parser.parse_args()

    if not args.v1.exists():
        print(f"ERROR: v1 database not found: {args.v1}", file=sys.stderr)
        sys.exit(1)
    if not args.v2.exists():
        print(f"ERROR: v2 database not found: {args.v2}", file=sys.stderr)
        print("Run: python3 src/db/init_db_v2.py", file=sys.stderr)
        sys.exit(1)

    print(f"Migrating: {args.v1} -> {args.v2}")
    if args.dry_run:
        print("(DRY RUN - no changes will be written)")
    print()

    conn_v1 = sqlite3.connect(str(args.v1))
    conn_v1.row_factory = sqlite3.Row
    conn_v2 = sqlite3.connect(str(args.v2))
    conn_v2.execute("PRAGMA foreign_keys = ON")
    # Explicit transaction boundary for the whole migration
    conn_v2.execute("BEGIN IMMEDIATE")

    try:
        migrated_ds_id = get_data_source_id(conn_v2, "migrated_v1")

        print("Step 1/5: Migrating sectors...")
        sector_map = migrate_sectors(conn_v1, conn_v2, args.dry_run)

        print("Step 2/5: Migrating companies -> organizations...")
        company_map = migrate_companies(
            conn_v1, conn_v2, sector_map, migrated_ds_id, args.dry_run
        )

        print("Step 3/5: Migrating investors -> organizations...")
        investor_map = migrate_investors(
            conn_v1, conn_v2, migrated_ds_id, args.dry_run
        )

        print("Step 4/5: Migrating investments -> funding_rounds + events...")
        round_map = migrate_investments(
            conn_v1, conn_v2, company_map, investor_map, migrated_ds_id, args.dry_run
        )

        print("Step 5/5: Migrating investment_investors & signals...")
        migrate_investment_investors(
            conn_v1, conn_v2, round_map, investor_map, args.dry_run
        )
        migrate_signals(conn_v1, conn_v2, sector_map, args.dry_run)

        if args.dry_run:
            conn_v2.rollback()
            print("\n(DRY RUN completed - no changes committed)")
        else:
            conn_v2.commit()
            print("\nMigration completed successfully.")

        # Verification
        print("\nFinal v2 stats:")
        for table in ["organizations", "funding_rounds", "round_participants",
                      "events", "sectors", "signals"]:
            count = conn_v2.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            print(f"  {table}: {count}")

    except Exception as e:
        conn_v2.rollback()
        print(f"\nERROR during migration: {e}", file=sys.stderr)
        raise
    finally:
        conn_v1.close()
        conn_v2.close()


if __name__ == "__main__":
    main()
