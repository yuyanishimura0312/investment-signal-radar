"""
Enrichment pipeline using gBizINFO to fill missing corporate data.

Finds organizations in the DB that lack a corporate_number and attempts
to match them via the gBizINFO name search API. When a match is found,
updates the organization record with official government data.
"""

import logging
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# Default DB location
DEFAULT_DB_PATH = str(
    Path(__file__).parent.parent.parent / "data" / "investment_signal_v2.db"
)


def _get_conn(db_path: str) -> sqlite3.Connection:
    """Open a connection with row_factory enabled."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def _ensure_enrichment_columns(conn: sqlite3.Connection) -> None:
    """Add enrichment columns if they don't already exist (idempotent)."""
    alter_statements = [
        "ALTER TABLE organizations ADD COLUMN capital_yen INTEGER",
        "ALTER TABLE organizations ADD COLUMN employee_count INTEGER",
        "ALTER TABLE organizations ADD COLUMN address TEXT",
        "ALTER TABLE organizations ADD COLUMN enriched_at TEXT",
    ]
    for stmt in alter_statements:
        try:
            conn.execute(stmt)
        except sqlite3.OperationalError:
            # Column already exists — safe to ignore
            pass
    # Index for fast lookup by corporate_number (already in schema, but be safe)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_org_corp_number "
        "ON organizations(corporate_number)"
    )
    conn.commit()


def _select_name_match(org_name: str, candidates: list) -> Optional[object]:
    """Pick the best match from gBizINFO search results.

    Uses simple heuristics: exact match first, then longest common substring.
    Returns None if no confident match found.
    """
    if not candidates:
        return None

    # Normalize for comparison
    normalized = org_name.strip().lower()

    # Pass 1: exact name match
    for c in candidates:
        if c.name.strip().lower() == normalized:
            return c

    # Pass 2: name contains the search term (or vice versa)
    for c in candidates:
        c_lower = c.name.strip().lower()
        if normalized in c_lower or c_lower in normalized:
            return c

    # Pass 3: if only one result, accept it
    if len(candidates) == 1:
        return candidates[0]

    # No confident match
    return None


def enrich_organizations(
    db_path: str = DEFAULT_DB_PATH,
    limit: int = 50,
    dry_run: bool = False,
) -> dict:
    """
    Find organizations without corporate_number and try to enrich them via gBizINFO.

    Args:
        db_path: Path to the SQLite database
        limit: Max number of organizations to process in one run
        dry_run: If True, search but don't update the DB

    Returns:
        Summary dict: searched, matched, updated, errors, skipped_no_token
    """
    # Late import to avoid errors when token isn't set
    try:
        from src.integrations.gbizinfo import GBizInfoClient
        client = GBizInfoClient()
    except ValueError as e:
        logger.warning(f"gBizINFO client unavailable: {e}")
        return {
            "searched": 0, "matched": 0, "updated": 0,
            "errors": 0, "skipped_no_token": True,
            "message": str(e),
        }

    conn = _get_conn(db_path)
    _ensure_enrichment_columns(conn)

    # Find JP companies without a corporate_number
    rows = conn.execute("""
        SELECT id, name, name_local
        FROM organizations
        WHERE country_code = 'JP'
          AND (corporate_number IS NULL OR corporate_number = '')
          AND (enriched_at IS NULL)
          AND is_company = 1
        ORDER BY updated_at DESC
        LIMIT ?
    """, (limit,)).fetchall()

    stats = {"searched": 0, "matched": 0, "updated": 0, "errors": 0, "skipped_no_token": False}

    for row in rows:
        org_id = row["id"]
        # Prefer name_local (Japanese) for searching, fall back to name
        search_name = row["name_local"] or row["name"]
        if not search_name:
            continue

        stats["searched"] += 1
        try:
            results = client.search_by_name(search_name, limit=5)
        except Exception as e:
            logger.warning(f"Search error for org {org_id} ('{search_name}'): {e}")
            stats["errors"] += 1
            continue

        if not client.available:
            logger.error("gBizINFO API became unavailable; stopping enrichment.")
            break

        match = _select_name_match(search_name, results)
        if match is None:
            logger.debug(f"No match for org {org_id}: '{search_name}'")
            # Mark as attempted so we don't retry every run
            if not dry_run:
                conn.execute(
                    "UPDATE organizations SET enriched_at = ? WHERE id = ?",
                    (datetime.now().isoformat(), org_id),
                )
                conn.commit()
            continue

        stats["matched"] += 1
        logger.info(
            f"Matched org {org_id} '{search_name}' -> "
            f"'{match.name}' (corp#{match.corporate_number})"
        )

        if not dry_run:
            try:
                conn.execute("""
                    UPDATE organizations SET
                        corporate_number = ?,
                        capital_yen = ?,
                        employee_count = ?,
                        address = ?,
                        enriched_at = ?
                    WHERE id = ?
                """, (
                    match.corporate_number,
                    match.capital,
                    match.employee_count,
                    match.address,
                    datetime.now().isoformat(),
                    org_id,
                ))
                conn.commit()
                stats["updated"] += 1
            except Exception as e:
                logger.error(f"DB update failed for org {org_id}: {e}")
                stats["errors"] += 1

    conn.close()
    return stats


def get_enrichment_stats(db_path: str = DEFAULT_DB_PATH) -> dict:
    """Return enrichment statistics for dashboard export.

    Returns:
        enriched_count: Number of orgs with corporate_number set
        total_organizations: Total org count (JP companies)
        enrichment_rate: Ratio of enriched orgs
        capital_distribution: Breakdown by capital range
    """
    conn = _get_conn(db_path)
    _ensure_enrichment_columns(conn)

    # Total JP companies
    total_row = conn.execute(
        "SELECT COUNT(*) as c FROM organizations WHERE is_company = 1 AND country_code = 'JP'"
    ).fetchone()
    total = total_row["c"] if total_row else 0

    # Enriched (have corporate_number)
    enriched_row = conn.execute(
        "SELECT COUNT(*) as c FROM organizations "
        "WHERE is_company = 1 AND country_code = 'JP' "
        "AND corporate_number IS NOT NULL AND corporate_number != ''"
    ).fetchone()
    enriched = enriched_row["c"] if enriched_row else 0

    # Capital distribution
    capital_ranges = [
        ("~1000万", 0, 10_000_000),
        ("1000万~1億", 10_000_000, 100_000_000),
        ("1億~10億", 100_000_000, 1_000_000_000),
        ("10億~", 1_000_000_000, None),
    ]
    capital_distribution = []
    for label, low, high in capital_ranges:
        if high is not None:
            row = conn.execute(
                "SELECT COUNT(*) as c FROM organizations "
                "WHERE capital_yen IS NOT NULL AND capital_yen >= ? AND capital_yen < ?",
                (low, high),
            ).fetchone()
        else:
            row = conn.execute(
                "SELECT COUNT(*) as c FROM organizations "
                "WHERE capital_yen IS NOT NULL AND capital_yen >= ?",
                (low,),
            ).fetchone()
        capital_distribution.append({"range": label, "count": row["c"] if row else 0})

    conn.close()

    return {
        "enriched_count": enriched,
        "total_organizations": total,
        "enrichment_rate": round(enriched / total, 3) if total > 0 else 0.0,
        "capital_distribution": capital_distribution,
    }
