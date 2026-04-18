#!/usr/bin/env python3
"""
Investor analysis module for the Investment Signal Radar.

Provides detailed analytics on investor behavior, sector preferences,
co-investment networks, and portfolio composition.

Usage:
    python3 -m src.analyzer.investor_analysis
    python3 -m src.analyzer.investor_analysis --format json
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from src.db.models_v2 import get_conn


def investor_rankings(conn, limit: int = 30) -> list[dict]:
    """Top investors ranked by deal count and total investment."""
    rows = conn.execute("""
        SELECT
            o.id, o.name, o.investor_type,
            COUNT(DISTINCT rp.funding_round_id) AS deal_count,
            COUNT(DISTINCT fr.organization_id) AS portfolio_size,
            SUM(CASE WHEN rp.is_lead = 1 THEN 1 ELSE 0 END) AS lead_deals,
            SUM(fr.amount_jpy) AS total_invested_jpy,
            MIN(fr.announced_date) AS first_deal,
            MAX(fr.announced_date) AS last_deal
        FROM organizations o
        JOIN round_participants rp ON o.id = rp.investor_id
        JOIN funding_rounds fr ON rp.funding_round_id = fr.id
        WHERE o.is_investor = 1
        GROUP BY o.id
        ORDER BY deal_count DESC
        LIMIT ?
    """, (limit,)).fetchall()
    return [dict(r) for r in rows]


def sector_distribution(conn) -> list[dict]:
    """Investment distribution across sectors."""
    rows = conn.execute("""
        SELECT
            COALESCE(s.name_ja, s.name, 'Unknown') AS sector,
            COUNT(DISTINCT fr.id) AS deal_count,
            COUNT(DISTINCT fr.organization_id) AS companies,
            COUNT(DISTINCT rp.investor_id) AS investors,
            SUM(fr.amount_jpy) AS total_jpy,
            AVG(fr.amount_jpy) AS avg_jpy
        FROM funding_rounds fr
        LEFT JOIN organization_sectors os
            ON fr.organization_id = os.organization_id AND os.is_primary = 1
        LEFT JOIN sectors s ON os.sector_id = s.id
        LEFT JOIN round_participants rp ON fr.id = rp.funding_round_id
        WHERE fr.is_duplicate IS NULL OR fr.is_duplicate = 0
        GROUP BY s.id
        ORDER BY deal_count DESC
    """).fetchall()
    return [dict(r) for r in rows]


def co_investment_network(conn, min_shared: int = 2) -> list[dict]:
    """Co-investment network: pairs of investors who frequently invest together."""
    rows = conn.execute("""
        SELECT
            oa.name AS investor_a,
            ob.name AS investor_b,
            COUNT(DISTINCT a.funding_round_id) AS shared_deals,
            GROUP_CONCAT(DISTINCT COALESCE(s.name_ja, s.name)) AS shared_sectors
        FROM round_participants a
        JOIN round_participants b
            ON a.funding_round_id = b.funding_round_id
            AND a.investor_id < b.investor_id
        JOIN organizations oa ON a.investor_id = oa.id
        JOIN organizations ob ON b.investor_id = ob.id
        JOIN funding_rounds fr ON a.funding_round_id = fr.id
        LEFT JOIN organization_sectors os
            ON fr.organization_id = os.organization_id AND os.is_primary = 1
        LEFT JOIN sectors s ON os.sector_id = s.id
        GROUP BY a.investor_id, b.investor_id
        HAVING shared_deals >= ?
        ORDER BY shared_deals DESC
    """, (min_shared,)).fetchall()
    return [dict(r) for r in rows]


def round_stage_analysis(conn) -> list[dict]:
    """Deal count and amount by round stage."""
    rows = conn.execute("""
        SELECT
            round_type,
            COUNT(*) AS deal_count,
            SUM(amount_jpy) AS total_jpy,
            AVG(amount_jpy) AS avg_jpy,
            COUNT(DISTINCT organization_id) AS companies
        FROM funding_rounds
        WHERE is_duplicate IS NULL OR is_duplicate = 0
        GROUP BY round_type
        ORDER BY deal_count DESC
    """).fetchall()
    return [dict(r) for r in rows]


def print_report(conn):
    """Print a summary report to stdout."""
    print("=" * 60)
    print("投資家分析レポート")
    print("=" * 60)

    # Stats
    stats = conn.execute("SELECT count(*) FROM funding_rounds").fetchone()[0]
    inv_count = conn.execute(
        "SELECT count(DISTINCT investor_id) FROM round_participants"
    ).fetchone()[0]
    co_count = conn.execute(
        "SELECT count(DISTINCT organization_id) FROM funding_rounds"
    ).fetchone()[0]
    print(f"\n資金調達ラウンド: {stats}件")
    print(f"投資家: {inv_count}社")
    print(f"投資先企業: {co_count}社")

    # Top investors
    print("\n--- トップ投資家（ディール数順） ---")
    for r in investor_rankings(conn, 15):
        amt = f"¥{r['total_invested_jpy']:,.0f}" if r['total_invested_jpy'] else "非公開"
        print(f"  {r['name']}: {r['deal_count']}件 (リード{r['lead_deals']}件) | {r['portfolio_size']}社 | {amt}")

    # Sectors
    print("\n--- セクター分布 ---")
    for r in sector_distribution(conn):
        print(f"  {r['sector']}: {r['deal_count']}件 | {r['companies']}社 | {r['investors']}投資家")

    # Round stages
    print("\n--- ラウンドステージ別 ---")
    for r in round_stage_analysis(conn):
        avg = f"¥{r['avg_jpy']:,.0f}" if r['avg_jpy'] else "N/A"
        print(f"  {r['round_type']}: {r['deal_count']}件 | 平均{avg}")

    # Co-investment
    pairs = co_investment_network(conn)
    if pairs:
        print(f"\n--- 共同投資ペア（{len(pairs)}組） ---")
        for r in pairs[:10]:
            print(f"  {r['investor_a']} × {r['investor_b']}: {r['shared_deals']}件 ({r['shared_sectors']})")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Investor analysis")
    parser.add_argument("--format", choices=["text", "json"], default="text")
    args = parser.parse_args()

    conn = get_conn()

    if args.format == "json":
        result = {
            "investor_rankings": investor_rankings(conn),
            "sector_distribution": sector_distribution(conn),
            "co_investment_network": co_investment_network(conn),
            "round_stage_analysis": round_stage_analysis(conn),
        }
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print_report(conn)

    conn.close()


if __name__ == "__main__":
    main()
