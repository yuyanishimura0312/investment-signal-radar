#!/usr/bin/env python3
"""
Investment trend analysis module.
Computes sector-level and time-series investment statistics.
"""

import json
import sys
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))
from db.models import get_conn


def sector_trends(months: int = 12) -> list[dict]:
    """
    Compute monthly investment counts and amounts by sector.
    Returns a list of {month, sector, count, total_amount_jpy}.
    """
    conn = get_conn()
    cutoff = (datetime.now() - timedelta(days=months * 30)).strftime("%Y-%m-%d")

    rows = conn.execute("""
        SELECT
            strftime('%Y-%m', i.announced_date) as month,
            COALESCE(s.name, 'Unknown') as sector,
            COUNT(*) as count,
            SUM(COALESCE(i.amount_jpy, 0)) as total_amount_jpy
        FROM investments i
        LEFT JOIN companies c ON i.company_id = c.id
        LEFT JOIN sectors s ON c.sector_id = s.id
        WHERE i.announced_date >= ? AND i.is_duplicate = 0
        GROUP BY month, sector
        ORDER BY month, count DESC
    """, (cutoff,)).fetchall()
    conn.close()

    return [dict(r) for r in rows]


def round_distribution() -> list[dict]:
    """Get investment count by round type."""
    conn = get_conn()
    rows = conn.execute("""
        SELECT round_type, COUNT(*) as count,
               SUM(COALESCE(amount_jpy, 0)) as total_amount_jpy
        FROM investments
        WHERE is_duplicate = 0
        GROUP BY round_type
        ORDER BY count DESC
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def top_investors(limit: int = 20) -> list[dict]:
    """Get most active investors by deal count."""
    conn = get_conn()
    rows = conn.execute("""
        SELECT
            inv.canonical_name as investor_name,
            inv.type as investor_type,
            COUNT(DISTINCT ii.investment_id) as deal_count,
            SUM(CASE WHEN ii.is_lead = 1 THEN 1 ELSE 0 END) as lead_count
        FROM investment_investors ii
        JOIN investors inv ON ii.investor_id = inv.id
        GROUP BY inv.id
        ORDER BY deal_count DESC
        LIMIT ?
    """, (limit,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def co_investment_pairs(min_count: int = 2) -> list[dict]:
    """
    Find investor pairs that frequently co-invest.
    Returns pairs with their shared deal count.
    """
    conn = get_conn()
    rows = conn.execute("""
        SELECT
            a_inv.canonical_name as investor_a,
            b_inv.canonical_name as investor_b,
            COUNT(*) as shared_deals
        FROM investment_investors a
        JOIN investment_investors b ON a.investment_id = b.investment_id AND a.investor_id < b.investor_id
        JOIN investors a_inv ON a.investor_id = a_inv.id
        JOIN investors b_inv ON b.investor_id = b_inv.id
        GROUP BY a.investor_id, b.investor_id
        HAVING shared_deals >= ?
        ORDER BY shared_deals DESC
    """, (min_count,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def monthly_summary() -> list[dict]:
    """Get monthly aggregate statistics."""
    conn = get_conn()
    rows = conn.execute("""
        SELECT
            strftime('%Y-%m', announced_date) as month,
            COUNT(*) as deal_count,
            SUM(COALESCE(amount_jpy, 0)) as total_amount_jpy,
            COUNT(DISTINCT company_id) as unique_companies
        FROM investments
        WHERE is_duplicate = 0
        GROUP BY month
        ORDER BY month
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def export_dashboard_data(output_path: str = None):
    """Export all trend data as JSON for the web dashboard."""
    data = {
        "generated_at": datetime.now().isoformat(),
        "sector_trends": sector_trends(),
        "round_distribution": round_distribution(),
        "top_investors": top_investors(),
        "co_investment_pairs": co_investment_pairs(),
        "monthly_summary": monthly_summary(),
    }

    if output_path is None:
        output_path = str(Path(__file__).parent.parent.parent / "web" / "data.json")

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"Dashboard data exported to {output_path}")
    return data


if __name__ == "__main__":
    export_dashboard_data()
