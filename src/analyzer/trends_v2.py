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


def investor_profiles(limit: int = 50) -> list[dict]:
    """Detailed investor profiles with sector breakdown and portfolio info."""
    with _conn() as conn:
        rows = conn.execute("""
            SELECT
                o.id, o.name AS investor_name, o.investor_type,
                COUNT(DISTINCT rp.funding_round_id) AS deal_count,
                COUNT(DISTINCT fr.organization_id) AS portfolio_count,
                SUM(fr.amount_jpy) AS total_invested_jpy,
                AVG(fr.amount_jpy) AS avg_deal_size_jpy,
                MIN(fr.announced_date) AS first_deal,
                MAX(fr.announced_date) AS last_deal,
                SUM(CASE WHEN rp.is_lead = 1 THEN 1 ELSE 0 END) AS lead_count,
                GROUP_CONCAT(DISTINCT fr.round_type) AS round_types
            FROM organizations o
            JOIN round_participants rp ON o.id = rp.investor_id
            JOIN funding_rounds fr ON rp.funding_round_id = fr.id
            WHERE o.is_investor = 1
            GROUP BY o.id
            ORDER BY deal_count DESC
            LIMIT ?
        """, (limit,)).fetchall()
    return [dict(r) for r in rows]


def investor_sector_matrix() -> list[dict]:
    """Investor x Sector cross-analysis for heatmap visualization."""
    with _conn() as conn:
        rows = conn.execute("""
            SELECT
                inv.name AS investor_name,
                COALESCE(s.name_ja, s.name, 'Unknown') AS sector_name,
                COUNT(DISTINCT fr.id) AS deal_count,
                SUM(fr.amount_jpy) AS total_invested_jpy
            FROM organizations inv
            JOIN round_participants rp ON inv.id = rp.investor_id
            JOIN funding_rounds fr ON rp.funding_round_id = fr.id
            JOIN organizations co ON fr.organization_id = co.id
            LEFT JOIN organization_sectors os
                ON co.id = os.organization_id AND os.is_primary = 1
            LEFT JOIN sectors s ON os.sector_id = s.id
            WHERE inv.is_investor = 1
            GROUP BY inv.id, s.id
            HAVING deal_count >= 1
            ORDER BY deal_count DESC
        """).fetchall()
    return [dict(r) for r in rows]


def sector_summary() -> list[dict]:
    """Sector-level investment summary."""
    with _conn() as conn:
        rows = conn.execute("""
            SELECT
                COALESCE(s.name_ja, s.name, 'Unknown') AS sector_name,
                COUNT(DISTINCT fr.id) AS deal_count,
                COUNT(DISTINCT fr.organization_id) AS company_count,
                COUNT(DISTINCT rp.investor_id) AS investor_count,
                SUM(fr.amount_jpy) AS total_raised_jpy,
                AVG(fr.amount_jpy) AS avg_deal_size_jpy,
                MIN(fr.announced_date) AS first_deal,
                MAX(fr.announced_date) AS last_deal
            FROM funding_rounds fr
            JOIN organizations co ON fr.organization_id = co.id
            LEFT JOIN organization_sectors os
                ON co.id = os.organization_id AND os.is_primary = 1
            LEFT JOIN sectors s ON os.sector_id = s.id
            LEFT JOIN round_participants rp ON fr.id = rp.funding_round_id
            WHERE fr.is_duplicate IS NULL OR fr.is_duplicate = 0
            GROUP BY s.id
            ORDER BY deal_count DESC
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


def export_press_release_data() -> dict:
    """Aggregate press release data for the dashboard.

    Returns a summary dict with counts by source/category/month,
    recent releases, and top companies by PR count.
    Safe to call even if press_releases table is empty or missing.
    """
    with _conn() as conn:
        # Check if press_releases table exists
        table_check = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='press_releases'"
        ).fetchone()
        if not table_check:
            return {
                "total_count": 0,
                "funding_related_count": 0,
                "by_source": {},
                "by_category": {},
                "by_month": [],
                "recent_releases": [],
                "top_companies_by_pr_count": [],
            }

        # Total and funding count
        row = conn.execute("SELECT COUNT(*) as c FROM press_releases").fetchone()
        total = row["c"]

        row = conn.execute(
            "SELECT COUNT(*) as c FROM press_releases WHERE is_funding_related = 1"
        ).fetchone()
        funding_count = row["c"]

        # By source
        rows = conn.execute(
            "SELECT source, COUNT(*) as c FROM press_releases GROUP BY source"
        ).fetchall()
        by_source = {r["source"]: r["c"] for r in rows}

        # By category
        rows = conn.execute(
            "SELECT COALESCE(category, 'unknown') as cat, COUNT(*) as c "
            "FROM press_releases GROUP BY cat ORDER BY c DESC"
        ).fetchall()
        by_category = {r["cat"]: r["c"] for r in rows}

        # By month (last 24 months)
        rows = conn.execute("""
            SELECT
                strftime('%Y-%m', published_at) AS month,
                COUNT(*) AS count,
                SUM(CASE WHEN is_funding_related = 1 THEN 1 ELSE 0 END) AS funding_count
            FROM press_releases
            WHERE published_at IS NOT NULL
            GROUP BY month
            ORDER BY month DESC
            LIMIT 24
        """).fetchall()
        by_month = [dict(r) for r in rows]

        # Recent releases (last 50)
        rows = conn.execute("""
            SELECT title, source, published_at, company_name,
                   is_funding_related, source_url
            FROM press_releases
            ORDER BY published_at DESC NULLS LAST, id DESC
            LIMIT 50
        """).fetchall()
        recent = [dict(r) for r in rows]

        # Top companies by press release count
        rows = conn.execute("""
            SELECT company_name, COUNT(*) as count
            FROM press_releases
            WHERE company_name IS NOT NULL AND company_name != ''
            GROUP BY company_name
            ORDER BY count DESC
            LIMIT 20
        """).fetchall()
        top_companies = [dict(r) for r in rows]

    return {
        "total_count": total,
        "funding_related_count": funding_count,
        "by_source": by_source,
        "by_category": by_category,
        "by_month": by_month,
        "recent_releases": recent,
        "top_companies_by_pr_count": top_companies,
    }


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
# Enrichment data (gBizINFO corporate info)
# ================================================================

def _safe_enrichment_data() -> dict:
    """Wrapper that returns empty stats if enrichment is not yet set up."""
    try:
        return export_enrichment_data()
    except Exception:
        return {
            "enriched_count": 0,
            "total_organizations": 0,
            "enrichment_rate": 0.0,
            "capital_distribution": [],
        }


def export_enrichment_data() -> dict:
    """Export corporate enrichment stats for the dashboard.

    Returns aggregated statistics on how many organizations have been
    enriched with official government data via gBizINFO, plus a capital
    distribution breakdown.
    """
    from integrations.enrichment_pipeline import get_enrichment_stats

    db_path = str(Path(__file__).parent.parent.parent / "data" / "investment_signal_v2.db")
    return get_enrichment_stats(db_path)


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
        # Corporate enrichment via gBizINFO
        "corporate_enrichment": _safe_enrichment_data(),
        # Press releases (PR TIMES, Frontier Detector, etc.)
        "press_releases": export_press_release_data(),
        # Investor analysis (Phase 3)
        "investor_profiles": investor_profiles(),
        "investor_sector_matrix": investor_sector_matrix(),
        "sector_summary": sector_summary(),
        # Signals & Network (Phase 3a/3b enrichment)
        "signals": _export_signals(),
        "network_top_investors": _export_network_top(),
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


def _export_signals() -> list[dict]:
    """Export detected signals for dashboard display."""
    with _conn() as conn:
        rows = conn.execute("""
            SELECT signal_type, s.name AS sector_name, si.detected_at,
                   si.baseline_count, si.current_count, si.acceleration_ratio,
                   si.description
            FROM signals si
            LEFT JOIN sectors s ON si.sector_id = s.id
            ORDER BY si.detected_at DESC
            LIMIT 50
        """).fetchall()
        return [dict(r) for r in rows]


def _export_network_top() -> list[dict]:
    """Export top investors by network centrality."""
    with _conn() as conn:
        rows = conn.execute("""
            SELECT o.name AS investor_name,
                   MAX(CASE WHEN nm.metric_type='degree_centrality' THEN nm.metric_value END) AS degree,
                   MAX(CASE WHEN nm.metric_type='betweenness_centrality' THEN nm.metric_value END) AS betweenness,
                   MAX(CASE WHEN nm.metric_type='eigenvector_centrality' THEN nm.metric_value END) AS eigenvector,
                   MAX(CASE WHEN nm.metric_type='co_investment_count' THEN nm.metric_value END) AS co_investments,
                   MAX(CASE WHEN nm.metric_type='unique_co_investors' THEN nm.metric_value END) AS unique_partners
            FROM network_metrics nm
            JOIN organizations o ON nm.organization_id = o.id
            GROUP BY nm.organization_id
            ORDER BY degree DESC
            LIMIT 30
        """).fetchall()
        return [dict(r) for r in rows]


if __name__ == "__main__":
    export_dashboard_data()
