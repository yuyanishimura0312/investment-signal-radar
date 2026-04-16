#!/usr/bin/env python3
"""
Investment trend analysis for v2 schema.

Exports aggregated data for the web dashboard, using the Organization-centric
+ event-driven model. Backward-compatible with v1 data.json structure while
adding v2-specific fields (event momentum, score distribution, data freshness).
"""

import contextlib
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))
from db.models_v2 import get_conn, get_stats  # noqa: E402


@contextlib.contextmanager
def _conn():
    """Yield a DB connection that is always closed, even on exception."""
    conn = get_conn()
    try:
        yield conn
    finally:
        conn.close()


# ================================================================
# Core aggregations (compatible with v1 dashboard)
# ================================================================

def sector_trends(months: int = 12) -> list[dict]:
    """Monthly funding counts/amounts by primary sector."""
    cutoff = (datetime.now() - timedelta(days=months * 31)).strftime("%Y-%m-%d")
    with _conn() as conn:
        rows = conn.execute("""
            SELECT
                strftime('%Y-%m', fr.announced_date) AS month,
                COALESCE(s.name_ja, s.name, 'Unknown') AS sector,
                COUNT(*) AS count,
                SUM(COALESCE(fr.amount_jpy, 0)) AS total_amount_jpy
            FROM funding_rounds fr
            JOIN organizations o ON fr.organization_id = o.id
            LEFT JOIN organization_sectors os
                ON o.id = os.organization_id AND os.is_primary = 1
            LEFT JOIN sectors s ON os.sector_id = s.id
            WHERE fr.announced_date >= ?
              AND (fr.is_duplicate IS NULL OR fr.is_duplicate = 0)
            GROUP BY month, sector
            ORDER BY month, count DESC
        """, (cutoff,)).fetchall()
    return [dict(r) for r in rows]


def round_distribution() -> list[dict]:
    """Funding counts by round_type."""
    with _conn() as conn:
        rows = conn.execute("""
            SELECT round_type, COUNT(*) AS count,
                   SUM(COALESCE(amount_jpy, 0)) AS total_amount_jpy
            FROM funding_rounds
            WHERE is_duplicate IS NULL OR is_duplicate = 0
            GROUP BY round_type
            ORDER BY count DESC
        """).fetchall()
    return [dict(r) for r in rows]


def top_investors(limit: int = 20) -> list[dict]:
    """Most active investors by funding round participation."""
    with _conn() as conn:
        rows = conn.execute("""
            SELECT
                o.name AS investor_name,
                o.investor_type,
                COUNT(DISTINCT rp.funding_round_id) AS deal_count,
                SUM(CASE WHEN rp.is_lead = 1 THEN 1 ELSE 0 END) AS lead_count
            FROM round_participants rp
            JOIN organizations o ON rp.investor_id = o.id
            WHERE o.is_investor = 1
            GROUP BY o.id
            ORDER BY deal_count DESC, lead_count DESC
            LIMIT ?
        """, (limit,)).fetchall()
    return [dict(r) for r in rows]


def co_investment_pairs(min_count: int = 2) -> list[dict]:
    """Investor pairs that co-invested in >= min_count rounds."""
    with _conn() as conn:
        rows = conn.execute("""
            SELECT
                oa.name AS investor_a,
                ob.name AS investor_b,
                COUNT(DISTINCT a.funding_round_id) AS shared_deals
            FROM round_participants a
            JOIN round_participants b
                ON a.funding_round_id = b.funding_round_id
                AND a.investor_id < b.investor_id
            JOIN organizations oa ON a.investor_id = oa.id
            JOIN organizations ob ON b.investor_id = ob.id
            GROUP BY a.investor_id, b.investor_id
            HAVING shared_deals >= ?
            ORDER BY shared_deals DESC
        """, (min_count,)).fetchall()
    return [dict(r) for r in rows]


def monthly_summary() -> list[dict]:
    """Monthly aggregate: deal count, total amount, unique companies."""
    with _conn() as conn:
        rows = conn.execute("""
            SELECT
                strftime('%Y-%m', announced_date) AS month,
                COUNT(*) AS deal_count,
                SUM(COALESCE(amount_jpy, 0)) AS total_amount_jpy,
                COUNT(DISTINCT organization_id) AS unique_companies
            FROM funding_rounds
            WHERE is_duplicate IS NULL OR is_duplicate = 0
            GROUP BY month
            ORDER BY month
        """).fetchall()
    return [dict(r) for r in rows]


# ================================================================
# v2-specific aggregations (new capabilities)
# ================================================================

def event_momentum(days: int = 90) -> list[dict]:
    """90-day rolling event counts by type (cross-organization)."""
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    with _conn() as conn:
        rows = conn.execute("""
            SELECT event_type, COUNT(*) AS count,
                   AVG(significance_score) AS avg_significance
            FROM events
            WHERE event_date >= ?
            GROUP BY event_type
            ORDER BY count DESC
        """, (cutoff,)).fetchall()
    return [dict(r) for r in rows]


def tag_distribution() -> list[dict]:
    """Organization count by tag (for technology/business model tags)."""
    with _conn() as conn:
        rows = conn.execute("""
            SELECT
                t.tag_category,
                COALESCE(t.name_ja, t.name) AS tag_name,
                COUNT(DISTINCT ot.organization_id) AS org_count
            FROM tags t
            JOIN organization_tags ot ON t.id = ot.tag_id
            GROUP BY t.id
            ORDER BY org_count DESC
            LIMIT 50
        """).fetchall()
    return [dict(r) for r in rows]


def data_freshness_summary() -> dict:
    """Summary of data freshness (how many records are stale / expired)."""
    with _conn() as conn:
        row = conn.execute("""
            SELECT
                COUNT(*) AS total,
                SUM(CASE
                    WHEN julianday('now') - julianday(collected_at) > ttl_hard_days
                    THEN 1 ELSE 0 END) AS expired,
                SUM(CASE
                    WHEN julianday('now') - julianday(collected_at) BETWEEN ttl_soft_days AND ttl_hard_days
                    THEN 1 ELSE 0 END) AS stale,
                SUM(CASE
                    WHEN julianday('now') - julianday(collected_at) <= ttl_soft_days
                    THEN 1 ELSE 0 END) AS fresh
            FROM organizations
        """).fetchone()
    return dict(row) if row else {}


def data_source_breakdown() -> list[dict]:
    """How many organizations/rounds came from each data source."""
    with _conn() as conn:
        rows = conn.execute("""
            SELECT
                ds.name AS source,
                ds.source_type,
                COUNT(DISTINCT o.id) AS org_count,
                COUNT(DISTINCT fr.id) AS round_count
            FROM data_sources ds
            LEFT JOIN organizations o ON o.data_source_id = ds.id
            LEFT JOIN funding_rounds fr ON fr.data_source_id = ds.id
            GROUP BY ds.id
            HAVING org_count > 0 OR round_count > 0
            ORDER BY org_count DESC
        """).fetchall()
    return [dict(r) for r in rows]


def top_organizations_by_score(limit: int = 20) -> list[dict]:
    """Top companies by latest composite score (empty if no scores yet)."""
    with _conn() as conn:
        rows = conn.execute("""
            SELECT o.id, o.name, o.primary_role,
                   v.composite_score, v.calculated_at, v.model_version
            FROM v_latest_scores v
            JOIN organizations o ON v.id = o.id
            WHERE o.is_company = 1
            ORDER BY v.composite_score DESC
            LIMIT ?
        """, (limit,)).fetchall()
    return [dict(r) for r in rows]


# ================================================================
# Full export
# ================================================================

def export_dashboard_data(output_path: Optional[str] = None) -> dict:
    """Generate the full dashboard data.json for the web UI."""
    with _conn() as conn:
        stats = get_stats(conn)

    data = {
        "generated_at": datetime.now().isoformat(),
        "schema_version": "v2.0",
        "stats": stats,
        "sector_trends": sector_trends(),
        "round_distribution": round_distribution(),
        "top_investors": top_investors(),
        "co_investment_pairs": co_investment_pairs(),
        "monthly_summary": monthly_summary(),
        # v2-specific
        "event_momentum": event_momentum(),
        "tag_distribution": tag_distribution(),
        "data_freshness": data_freshness_summary(),
        "data_source_breakdown": data_source_breakdown(),
        "top_organizations_by_score": top_organizations_by_score(),
    }

    if output_path is None:
        output_path = str(
            Path(__file__).parent.parent.parent / "web" / "public" / "data.json"
        )
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"Dashboard data exported to {output_path}")
    return data


if __name__ == "__main__":
    export_dashboard_data()
