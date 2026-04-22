"""
Enrich investor organizations with sector assignments and tags based on their portfolio.

For each investor with no sector assignment:
  1. Collect all companies they invested in via round_participants -> funding_rounds
  2. Look up the primary sector for each company via organization_sectors
  3. Assign the most common sector as the investor's primary sector
  4. Assign secondary sectors if the investor spans 2+ sectors

For investors with 5+ deals, also apply tags:
  - "multi_sector"   (business_model) if they invest in 3+ distinct sectors
  - "active_investor" (market) if they have 10+ deals
  - <sector_name>    (technology) if >50% of deals concentrate in one sector
"""

import sys
from collections import Counter
from pathlib import Path

# Allow imports from src/
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from src.db.models_v2 import (
    assign_primary_sector,
    get_conn,
    tag_organization,
)


# ----------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------

def get_investors_without_sectors(conn):
    """Return list of (id, name) for investors that have no sector assignment."""
    rows = conn.execute("""
        SELECT o.id, o.name
        FROM organizations o
        LEFT JOIN organization_sectors os ON os.organization_id = o.id
        WHERE o.is_investor = 1
          AND os.organization_id IS NULL
        ORDER BY o.id
    """).fetchall()
    return [(r["id"], r["name"]) for r in rows]


def get_portfolio_sectors(conn, investor_id):
    """Return a list of sector names for all companies this investor backed.

    Each deal contributes one entry (the company's primary sector if it exists).
    Companies without a primary sector are skipped.
    """
    rows = conn.execute("""
        SELECT s.name AS sector
        FROM round_participants rp
        JOIN funding_rounds fr ON fr.id = rp.funding_round_id
        JOIN organization_sectors cs
            ON cs.organization_id = fr.organization_id
            AND cs.is_primary = 1
        JOIN sectors s ON s.id = cs.sector_id
        WHERE rp.investor_id = ?
    """, (investor_id,)).fetchall()
    return [r["sector"] for r in rows]


def assign_secondary_sectors(conn, investor_id, secondary_sectors):
    """Insert non-primary sector links for an investor."""
    for sector_name in secondary_sectors:
        # find_or_create_sector lives inside assign_primary_sector; replicate
        # the lookup pattern to avoid touching is_primary flag.
        from src.db.models_v2 import find_or_create_sector
        sector_id = find_or_create_sector(conn, sector_name)
        # Only insert if not already present (either as primary or secondary)
        existing = conn.execute("""
            SELECT 1 FROM organization_sectors
            WHERE organization_id = ? AND sector_id = ?
        """, (investor_id, sector_id)).fetchone()
        if not existing:
            conn.execute("""
                INSERT INTO organization_sectors (organization_id, sector_id, is_primary)
                VALUES (?, ?, 0)
            """, (investor_id, sector_id))


# ----------------------------------------------------------------
# Main enrichment logic
# ----------------------------------------------------------------

def enrich_investor_sectors():
    conn = get_conn()

    investors = get_investors_without_sectors(conn)
    print(f"Investors without sector assignment: {len(investors)}")

    enriched_count = 0
    sectors_assigned = 0
    tags_created = 0

    for inv_id, inv_name in investors:
        portfolio_sectors = get_portfolio_sectors(conn, inv_id)
        if not portfolio_sectors:
            continue  # No linked company with sector data — skip

        deal_count = len(portfolio_sectors)
        counter = Counter(portfolio_sectors)
        most_common_sector, most_common_count = counter.most_common(1)[0]
        all_sectors = [s for s, _ in counter.most_common()]

        # --- 1. Primary sector: most frequent sector in portfolio ---
        assign_primary_sector(conn, inv_id, most_common_sector)
        sectors_assigned += 1

        # --- 2. Secondary sectors (if investor spans 2+ distinct sectors) ---
        if len(all_sectors) >= 2:
            secondary = all_sectors[1:]  # everything except primary
            assign_secondary_sectors(conn, inv_id, secondary)
            sectors_assigned += len(secondary)

        enriched_count += 1

        # --- 3. Tags for investors with 5+ deals ---
        if deal_count >= 5:
            distinct_sectors = len(all_sectors)

            # multi_sector: 3+ distinct sectors
            if distinct_sectors >= 3:
                tag_organization(
                    conn, inv_id, "multi_sector",
                    category="business_model", assigned_by="rule", confidence=0.9
                )
                tags_created += 1

            # active_investor: 10+ deals
            if deal_count >= 10:
                tag_organization(
                    conn, inv_id, "active_investor",
                    category="market", assigned_by="rule", confidence=0.9
                )
                tags_created += 1

            # dominant sector tag: >50% of deals in one sector
            dominance_ratio = most_common_count / deal_count
            if dominance_ratio > 0.5:
                tag_organization(
                    conn, inv_id, most_common_sector,
                    category="technology", assigned_by="rule",
                    confidence=round(dominance_ratio, 2)
                )
                tags_created += 1

    conn.commit()
    conn.close()

    print()
    print("=== Enrichment Summary ===")
    print(f"Investors enriched:   {enriched_count}")
    print(f"Sector assignments:   {sectors_assigned}")
    print(f"Tags created:         {tags_created}")
    print()

    # --- Spot-check: show top 10 enriched investors ---
    conn2 = get_conn()
    rows = conn2.execute("""
        SELECT o.name, s.name AS primary_sector,
               COUNT(DISTINCT os2.sector_id) AS sector_count,
               COUNT(DISTINCT rp.funding_round_id) AS deal_count
        FROM organizations o
        JOIN organization_sectors os ON os.organization_id = o.id AND os.is_primary = 1
        JOIN sectors s ON s.id = os.sector_id
        LEFT JOIN organization_sectors os2 ON os2.organization_id = o.id
        LEFT JOIN round_participants rp ON rp.investor_id = o.id
        WHERE o.is_investor = 1
        GROUP BY o.id
        ORDER BY deal_count DESC
        LIMIT 10
    """).fetchall()
    print("Top 10 enriched investors by deal count:")
    print(f"{'Name':<40} {'Primary Sector':<30} {'Sectors':>7} {'Deals':>6}")
    print("-" * 90)
    for r in rows:
        print(f"{r['name'][:40]:<40} {r['primary_sector'][:30]:<30} {r['sector_count']:>7} {r['deal_count']:>6}")

    conn2.close()


if __name__ == "__main__":
    enrich_investor_sectors()
