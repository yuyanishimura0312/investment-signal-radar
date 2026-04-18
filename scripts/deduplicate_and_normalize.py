#!/usr/bin/env python3
"""
Deduplicate funding_rounds and normalize investor names in the v2 database.

Fixes:
1. Investor name variants (表記ゆれ): merge duplicate organizations
2. Duplicate funding_rounds: same company + date + round_type

Usage:
    python3 scripts/deduplicate_and_normalize.py --dry-run
    python3 scripts/deduplicate_and_normalize.py
"""

import argparse
import logging
import re
import sqlite3
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "investment_signal_v2.db"


def normalize_name(name: str) -> str:
    """Normalize Japanese company/investor name for comparison."""
    s = name.strip()
    # Remove legal entity suffixes/prefixes
    s = re.sub(r'株式会社|有限会社|合同会社|一般社団法人|公益財団法人', '', s)
    # Remove spaces
    s = s.replace(' ', '').replace('　', '').replace('\u3000', '')
    # Normalize unicode
    s = s.strip()
    return s


def merge_investor_variants(conn: sqlite3.Connection, dry_run: bool) -> int:
    """Find and merge investor name variants."""
    # Get all investors
    rows = conn.execute(
        "SELECT id, name FROM organizations WHERE is_investor = 1 ORDER BY id"
    ).fetchall()

    # Group by normalized name
    groups: dict[str, list] = {}
    for row in rows:
        norm = normalize_name(row[1])
        if not norm:
            continue
        groups.setdefault(norm, []).append((row[0], row[1]))

    merged = 0
    for norm, variants in groups.items():
        if len(variants) <= 1:
            continue

        # Keep the first (oldest) ID as canonical
        canonical_id, canonical_name = variants[0]
        duplicates = variants[1:]

        if dry_run:
            dup_names = [f"{n}(id={i})" for i, n in duplicates]
            log.info(f"[DRY RUN] Would merge: {dup_names} -> {canonical_name}(id={canonical_id})")
            merged += len(duplicates)
            continue

        for dup_id, dup_name in duplicates:
            # Update round_participants to point to canonical
            conn.execute("""
                UPDATE OR IGNORE round_participants
                SET investor_id = ?
                WHERE investor_id = ?
            """, (canonical_id, dup_id))

            # Delete participants that would violate unique constraint
            conn.execute("""
                DELETE FROM round_participants
                WHERE investor_id = ? AND funding_round_id IN (
                    SELECT funding_round_id FROM round_participants WHERE investor_id = ?
                )
            """, (dup_id, canonical_id))

            # Update any remaining round_participants
            conn.execute(
                "UPDATE round_participants SET investor_id = ? WHERE investor_id = ?",
                (canonical_id, dup_id),
            )

            # Update organization_sectors
            conn.execute(
                "UPDATE OR IGNORE organization_sectors SET organization_id = ? WHERE organization_id = ?",
                (canonical_id, dup_id),
            )
            conn.execute(
                "DELETE FROM organization_sectors WHERE organization_id = ?",
                (dup_id,),
            )

            # Update events
            conn.execute(
                "UPDATE events SET organization_id = ? WHERE organization_id = ?",
                (canonical_id, dup_id),
            )

            # Update funding_rounds (if this org is also a company)
            conn.execute(
                "UPDATE funding_rounds SET organization_id = ? WHERE organization_id = ?",
                (canonical_id, dup_id),
            )

            # Update press_releases
            conn.execute(
                "UPDATE press_releases SET organization_id = ? WHERE organization_id = ?",
                (canonical_id, dup_id),
            )

            # Update organization_tags
            conn.execute(
                "UPDATE OR IGNORE organization_tags SET organization_id = ? WHERE organization_id = ?",
                (canonical_id, dup_id),
            )
            conn.execute(
                "DELETE FROM organization_tags WHERE organization_id = ?", (dup_id,),
            )

            # Update signal_scores
            conn.execute(
                "UPDATE signal_scores SET organization_id = ? WHERE organization_id = ?",
                (canonical_id, dup_id),
            )

            # Update network_metrics
            conn.execute(
                "UPDATE OR IGNORE network_metrics SET organization_id = ? WHERE organization_id = ?",
                (canonical_id, dup_id),
            )
            conn.execute(
                "DELETE FROM network_metrics WHERE organization_id = ?", (dup_id,),
            )

            # Add alias
            aliases = conn.execute(
                "SELECT aliases FROM organizations WHERE id = ?", (canonical_id,)
            ).fetchone()[0] or "[]"
            import json
            alias_list = json.loads(aliases)
            if dup_name not in alias_list:
                alias_list.append(dup_name)
            conn.execute(
                "UPDATE organizations SET aliases = ? WHERE id = ?",
                (json.dumps(alias_list, ensure_ascii=False), canonical_id),
            )

            # Delete duplicate organization
            conn.execute("DELETE FROM organizations WHERE id = ?", (dup_id,))
            log.info(f"  Merged: {dup_name}(id={dup_id}) -> {canonical_name}(id={canonical_id})")
            merged += 1

    return merged


def deduplicate_funding_rounds(conn: sqlite3.Connection, dry_run: bool) -> int:
    """Remove duplicate funding_rounds (same company + date + round_type)."""
    dupes = conn.execute("""
        SELECT organization_id, announced_date, round_type, count(*) as cnt,
               GROUP_CONCAT(id) as ids
        FROM funding_rounds
        GROUP BY organization_id, announced_date, round_type
        HAVING cnt > 1
        ORDER BY cnt DESC
    """).fetchall()

    removed = 0
    for row in dupes:
        org_id, date, rtype, cnt, id_str = row
        ids = [int(x) for x in id_str.split(',')]
        keep_id = ids[0]  # Keep the first one
        remove_ids = ids[1:]

        # Get company name for logging
        name = conn.execute(
            "SELECT name FROM organizations WHERE id = ?", (org_id,)
        ).fetchone()[0]

        if dry_run:
            log.info(f"[DRY RUN] Would remove {len(remove_ids)} dupes: {name} | {date} | {rtype}")
            removed += len(remove_ids)
            continue

        for rid in remove_ids:
            # Move any unique round_participants to the kept round
            conn.execute("""
                UPDATE OR IGNORE round_participants
                SET funding_round_id = ?
                WHERE funding_round_id = ?
            """, (keep_id, rid))
            # Delete remaining (duplicate) participants
            conn.execute(
                "DELETE FROM round_participants WHERE funding_round_id = ?", (rid,)
            )
            # Update press_releases to point to kept round
            conn.execute(
                "UPDATE press_releases SET funding_round_id = ? WHERE funding_round_id = ?",
                (keep_id, rid),
            )
            # Delete event
            conn.execute(
                "DELETE FROM events WHERE event_type = 'funding' AND event_data LIKE ?",
                (f'%"funding_round_id": {rid}%',),
            )
            # Delete duplicate funding_round
            conn.execute("DELETE FROM funding_rounds WHERE id = ?", (rid,))
            log.info(f"  Removed: round_id={rid} ({name} | {date} | {rtype})")
            removed += 1

    return removed


def fix_invalid_dates(conn: sqlite3.Connection, dry_run: bool) -> int:
    """Fix funding_rounds with invalid date formats."""
    bad = conn.execute("""
        SELECT id, announced_date, source_url
        FROM funding_rounds
        WHERE announced_date != '' AND announced_date NOT LIKE '____-__-__'
    """).fetchall()

    fixed = 0
    for row in bad:
        rid, date, url = row
        # Try to parse and fix — add -01 for YYYY-MM format
        clean = date.strip()
        if re.match(r'^\d{4}-\d{2}$', clean):
            clean = clean + '-01'
        else:
            clean = clean[:10]
        if dry_run:
            log.info(f"[DRY RUN] Would fix date: id={rid}, '{date}' -> '{clean}'")
        else:
            conn.execute(
                "UPDATE funding_rounds SET announced_date = ? WHERE id = ?",
                (clean, rid),
            )
            log.info(f"  Fixed date: id={rid}, '{date}' -> '{clean}'")
        fixed += 1
    return fixed


def main():
    parser = argparse.ArgumentParser(description="Deduplicate and normalize funding DB")
    parser.add_argument("--dry-run", action="store_true", help="Preview changes")
    args = parser.parse_args()

    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout = 30000")

    log.info("--- Phase 1: Normalize investor names ---")
    merged = merge_investor_variants(conn, args.dry_run)
    log.info(f"Investor variants merged: {merged}")

    log.info("--- Phase 2: Deduplicate funding_rounds ---")
    removed = deduplicate_funding_rounds(conn, args.dry_run)
    log.info(f"Duplicate rounds removed: {removed}")

    log.info("--- Phase 3: Fix invalid dates ---")
    fixed = fix_invalid_dates(conn, args.dry_run)
    log.info(f"Invalid dates fixed: {fixed}")

    if not args.dry_run:
        conn.commit()
        log.info("Changes committed.")

        # Print final stats
        stats = {
            'funding_rounds': conn.execute("SELECT count(*) FROM funding_rounds").fetchone()[0],
            'organizations': conn.execute("SELECT count(*) FROM organizations").fetchone()[0],
            'investors': conn.execute("SELECT count(*) FROM organizations WHERE is_investor=1").fetchone()[0],
            'round_participants': conn.execute("SELECT count(*) FROM round_participants").fetchone()[0],
        }
        log.info(f"Final stats: {stats}")

    conn.close()


if __name__ == "__main__":
    main()
