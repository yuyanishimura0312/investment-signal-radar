#!/usr/bin/env python3
"""
Analyze startup emergence patterns and detect signals.

Rebuilds startup_cohorts from milestones + funding data, then runs
signal detection to find:
  - Founding surges (spike in new startups per sector)
  - Funding acceleration (time-to-funding getting shorter)
  - Sector emergence (new sectors appearing)
  - VC convergence (multiple VCs entering same new area)
  - Cohort outperformance (a vintage outperforming averages)

Usage:
    python3 scripts/analyze_startup_emergence.py
    python3 scripts/analyze_startup_emergence.py --rebuild-cohorts
    python3 scripts/analyze_startup_emergence.py --detect-signals
    python3 scripts/analyze_startup_emergence.py --report
"""

import argparse
import json
import sqlite3
import statistics
from collections import defaultdict
from datetime import datetime
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "investment_signal_v2.db"


def get_conn(db_path: Path = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def rebuild_cohorts(conn: sqlite3.Connection):
    """Rebuild startup_cohorts from milestones and funding data."""
    print("Rebuilding startup cohorts...")

    snapshot_date = datetime.now().strftime("%Y-%m-%d")

    # Clear existing cohorts for this snapshot
    conn.execute("DELETE FROM startup_cohorts WHERE snapshot_date = ?", (snapshot_date,))

    # Build cohorts by year × sector
    rows = conn.execute("""
        SELECT
            sm.milestone_year AS cohort_year,
            os.sector_id,
            COUNT(DISTINCT sm.organization_id) AS startup_count,
            COUNT(DISTINCT CASE WHEN fr.id IS NOT NULL THEN sm.organization_id END)
                AS funded_count,
            COALESCE(SUM(fr_total.total_jpy), 0) AS total_raised_jpy,
            COALESCE(SUM(fr_total.total_usd), 0) AS total_raised_usd,
            -- Outcome counts
            COUNT(DISTINCT CASE WHEN o.status = 'ipo' THEN o.id END) AS ipo_count,
            COUNT(DISTINCT CASE WHEN o.status = 'acquired' THEN o.id END) AS acquired_count,
            COUNT(DISTINCT CASE WHEN o.status = 'closed' THEN o.id END) AS shutdown_count,
            COUNT(DISTINCT CASE WHEN o.status = 'active' THEN o.id END) AS still_active_count
        FROM startup_milestones sm
        JOIN organizations o ON sm.organization_id = o.id
        LEFT JOIN organization_sectors os ON o.id = os.organization_id AND os.is_primary = 1
        LEFT JOIN funding_rounds fr ON sm.organization_id = fr.organization_id
        LEFT JOIN (
            SELECT organization_id,
                   SUM(amount_jpy) AS total_jpy,
                   SUM(amount_usd) AS total_usd
            FROM funding_rounds
            GROUP BY organization_id
        ) fr_total ON sm.organization_id = fr_total.organization_id
        WHERE sm.milestone_type = 'founded'
          AND sm.milestone_year IS NOT NULL
          AND os.sector_id IS NOT NULL
        GROUP BY sm.milestone_year, os.sector_id
    """).fetchall()

    if not rows:
        print("  No cohort data found. Run migration first.")
        return

    # Calculate YoY growth rates
    sector_year_counts = defaultdict(dict)
    for row in rows:
        sector_year_counts[row["sector_id"]][row["cohort_year"]] = row["startup_count"]

    inserted = 0
    for row in rows:
        year = row["cohort_year"]
        sector_id = row["sector_id"]
        prev_count = sector_year_counts[sector_id].get(year - 1)
        yoy_growth = None
        if prev_count and prev_count > 0:
            yoy_growth = (row["startup_count"] - prev_count) / prev_count

        # Calculate emergence score (normalized growth signal)
        # Higher when: growing fast AND from a low base (= truly emerging)
        emergence = 0.0
        if yoy_growth is not None and yoy_growth > 0:
            base_factor = min(1.0, 5.0 / max(row["startup_count"], 1))
            emergence = min(1.0, yoy_growth * (1 + base_factor) / 3.0)

        conn.execute("""
            INSERT INTO startup_cohorts
                (cohort_year, sector_id, tag_id, startup_count, funded_count,
                 total_raised_jpy, total_raised_usd,
                 ipo_count, acquired_count, shutdown_count, still_active_count,
                 yoy_growth_rate, emergence_score, snapshot_date)
            VALUES (?, ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (year, sector_id, row["startup_count"], row["funded_count"],
              row["total_raised_jpy"], row["total_raised_usd"],
              row["ipo_count"], row["acquired_count"],
              row["shutdown_count"], row["still_active_count"],
              yoy_growth, emergence, snapshot_date))
        inserted += 1

    # Also build tag-based cohorts
    tag_rows = conn.execute("""
        SELECT
            sm.milestone_year AS cohort_year,
            ot.tag_id,
            COUNT(DISTINCT sm.organization_id) AS startup_count,
            COUNT(DISTINCT CASE WHEN fr.id IS NOT NULL THEN sm.organization_id END)
                AS funded_count
        FROM startup_milestones sm
        JOIN organizations o ON sm.organization_id = o.id
        JOIN organization_tags ot ON o.id = ot.organization_id
        JOIN tags t ON ot.tag_id = t.id AND t.tag_category = 'technology'
        LEFT JOIN funding_rounds fr ON sm.organization_id = fr.organization_id
        WHERE sm.milestone_type = 'founded'
          AND sm.milestone_year IS NOT NULL
        GROUP BY sm.milestone_year, ot.tag_id
        HAVING startup_count >= 2
    """).fetchall()

    for row in tag_rows:
        conn.execute("""
            INSERT OR IGNORE INTO startup_cohorts
                (cohort_year, sector_id, tag_id, startup_count, funded_count,
                 snapshot_date)
            VALUES (?, NULL, ?, ?, ?, ?)
        """, (row["cohort_year"], row["tag_id"],
              row["startup_count"], row["funded_count"], snapshot_date))
        inserted += 1

    conn.commit()
    print(f"  Inserted {inserted} cohort records.")


def detect_signals(conn: sqlite3.Connection):
    """Detect startup emergence signals from cohort data."""
    print("Detecting emergence signals...")

    current_year = datetime.now().year
    signals_found = 0

    # 1. Founding surge: sectors with 2x+ YoY growth in recent years
    surges = conn.execute("""
        SELECT sc.*, s.name AS sector_name, s.name_ja
        FROM startup_cohorts sc
        JOIN sectors s ON sc.sector_id = s.id
        WHERE sc.cohort_year >= ? - 2
          AND sc.yoy_growth_rate > 1.0
          AND sc.startup_count >= 3
          AND sc.tag_id IS NULL
        ORDER BY sc.yoy_growth_rate DESC
    """, (current_year,)).fetchall()

    for surge in surges:
        conn.execute("""
            INSERT INTO startup_emergence_signals
                (signal_type, sector_id, observation_period,
                 signal_strength, baseline_value, current_value,
                 change_ratio, description)
            VALUES ('founding_surge', ?, ?, ?, ?, ?, ?, ?)
        """, (
            surge["sector_id"],
            f"{surge['cohort_year']-1} to {surge['cohort_year']}",
            min(1.0, surge["yoy_growth_rate"] / 3.0),
            surge["startup_count"] / (1 + surge["yoy_growth_rate"]),
            surge["startup_count"],
            1 + surge["yoy_growth_rate"],
            f"{surge['sector_name']}({surge['name_ja']}): "
            f"新規スタートアップ数が前年比{surge['yoy_growth_rate']*100:.0f}%増 "
            f"(n={surge['startup_count']})"
        ))
        signals_found += 1

    # 2. Funding acceleration: sectors where time-to-seed is shrinking
    accel_rows = conn.execute("""
        SELECT
            s.id AS sector_id, s.name AS sector_name, s.name_ja,
            sm_founded.milestone_year AS cohort_year,
            AVG(julianday(sm_seed.milestone_date) - julianday(sm_founded.milestone_date))
                AS avg_days_to_seed,
            COUNT(*) AS sample_size
        FROM startup_milestones sm_founded
        JOIN startup_milestones sm_seed
            ON sm_founded.organization_id = sm_seed.organization_id
            AND sm_seed.milestone_type IN ('seed', 'first_funding')
        JOIN organizations o ON sm_founded.organization_id = o.id
        JOIN organization_sectors os ON o.id = os.organization_id AND os.is_primary = 1
        JOIN sectors s ON os.sector_id = s.id
        WHERE sm_founded.milestone_type = 'founded'
          AND sm_founded.milestone_date IS NOT NULL
          AND sm_seed.milestone_date IS NOT NULL
          AND sm_founded.milestone_year >= ? - 5
        GROUP BY s.id, sm_founded.milestone_year
        HAVING sample_size >= 3
        ORDER BY s.id, cohort_year
    """, (current_year,)).fetchall()

    # Group by sector and check for acceleration
    sector_timings = defaultdict(list)
    for row in accel_rows:
        sector_timings[row["sector_id"]].append({
            "year": row["cohort_year"],
            "days": row["avg_days_to_seed"],
            "name": row["sector_name"],
            "name_ja": row["name_ja"],
            "n": row["sample_size"]
        })

    for sector_id, timings in sector_timings.items():
        if len(timings) < 2:
            continue
        timings.sort(key=lambda x: x["year"])
        first = timings[0]["days"]
        last = timings[-1]["days"]
        if first and last and first > 0 and last < first * 0.7:
            conn.execute("""
                INSERT INTO startup_emergence_signals
                    (signal_type, sector_id, observation_period,
                     signal_strength, baseline_value, current_value,
                     change_ratio, description)
                VALUES ('funding_acceleration', ?, ?, ?, ?, ?, ?, ?)
            """, (
                sector_id,
                f"{timings[0]['year']} to {timings[-1]['year']}",
                min(1.0, (first - last) / first),
                first, last, last / first if first > 0 else None,
                f"{timings[0]['name']}({timings[0]['name_ja']}): "
                f"Seed到達日数が{first:.0f}日→{last:.0f}日に短縮 "
                f"({(1-last/first)*100:.0f}%減)"
            ))
            signals_found += 1

    # 3. VC convergence: sectors where multiple new VCs entered recently
    vc_entries = conn.execute("""
        SELECT
            s.id AS sector_id, s.name AS sector_name, s.name_ja,
            COUNT(DISTINCT rp.investor_id) AS new_vc_count,
            GROUP_CONCAT(DISTINCT inv.name) AS vc_names
        FROM round_participants rp
        JOIN funding_rounds fr ON rp.funding_round_id = fr.id
        JOIN organizations inv ON rp.investor_id = inv.id AND inv.is_investor = 1
        JOIN organization_sectors os ON fr.organization_id = os.organization_id AND os.is_primary = 1
        JOIN sectors s ON os.sector_id = s.id
        WHERE fr.announced_date >= date('now', '-18 months')
          AND NOT EXISTS (
              -- This VC had no prior deals in this sector
              SELECT 1 FROM round_participants rp2
              JOIN funding_rounds fr2 ON rp2.funding_round_id = fr2.id
              JOIN organization_sectors os2 ON fr2.organization_id = os2.organization_id
                  AND os2.is_primary = 1
              WHERE rp2.investor_id = rp.investor_id
                AND os2.sector_id = os.sector_id
                AND fr2.announced_date < date('now', '-18 months')
          )
        GROUP BY s.id
        HAVING new_vc_count >= 3
        ORDER BY new_vc_count DESC
    """).fetchall()

    for entry in vc_entries:
        conn.execute("""
            INSERT INTO startup_emergence_signals
                (signal_type, sector_id, observation_period,
                 signal_strength, current_value, description,
                 evidence_data)
            VALUES ('vc_convergence', ?, ?, ?, ?, ?, ?)
        """, (
            entry["sector_id"],
            "past 18 months",
            min(1.0, entry["new_vc_count"] / 10.0),
            entry["new_vc_count"],
            f"{entry['sector_name']}({entry['name_ja']}): "
            f"{entry['new_vc_count']}社のVCが新規参入",
            json.dumps({"vc_names": entry["vc_names"].split(",")
                        if entry["vc_names"] else []})
        ))
        signals_found += 1

    conn.commit()
    print(f"  Detected {signals_found} emergence signals.")


def generate_report(conn: sqlite3.Connection):
    """Generate a summary report of the startup ecosystem."""
    print("\n" + "=" * 60)
    print("  Startup Ecosystem Report")
    print("=" * 60)

    # Overall stats
    stats = conn.execute("""
        SELECT
            (SELECT COUNT(DISTINCT organization_id) FROM startup_milestones) AS total_startups,
            (SELECT COUNT(*) FROM startup_milestones) AS total_milestones,
            (SELECT COUNT(*) FROM startup_cohorts
             WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM startup_cohorts))
                AS cohort_records,
            (SELECT COUNT(*) FROM ecosystem_rankings) AS ranking_records,
            (SELECT COUNT(*) FROM nedo_projects) AS nedo_projects,
            (SELECT COUNT(*) FROM startup_emergence_signals) AS signals
    """).fetchone()

    print(f"\n  Tracked startups:      {stats['total_startups']}")
    print(f"  Lifecycle milestones:  {stats['total_milestones']}")
    print(f"  Cohort records:        {stats['cohort_records']}")
    print(f"  Ecosystem rankings:    {stats['ranking_records']}")
    print(f"  NEDO projects:         {stats['nedo_projects']}")
    print(f"  Emergence signals:     {stats['signals']}")

    # Top emerging sectors
    print("\n--- Top Emerging Sectors (by emergence score) ---")
    emerging = conn.execute("""
        SELECT sc.cohort_year, s.name, s.name_ja,
               sc.startup_count, sc.funded_count,
               sc.yoy_growth_rate, sc.emergence_score
        FROM startup_cohorts sc
        JOIN sectors s ON sc.sector_id = s.id
        WHERE sc.emergence_score > 0.1
          AND sc.tag_id IS NULL
          AND sc.snapshot_date = (SELECT MAX(snapshot_date) FROM startup_cohorts)
        ORDER BY sc.emergence_score DESC
        LIMIT 10
    """).fetchall()

    for row in emerging:
        yoy = f"{row['yoy_growth_rate']*100:+.0f}%" if row["yoy_growth_rate"] else "N/A"
        print(f"  {row['cohort_year']} {row['name']:<30} "
              f"n={row['startup_count']:>3}  funded={row['funded_count']:>3}  "
              f"YoY={yoy:>6}  score={row['emergence_score']:.2f}")

    # Recent signals
    print("\n--- Recent Emergence Signals ---")
    signals = conn.execute("""
        SELECT ses.*, s.name AS sector_name
        FROM startup_emergence_signals ses
        LEFT JOIN sectors s ON ses.sector_id = s.id
        ORDER BY ses.detected_at DESC
        LIMIT 10
    """).fetchall()

    for sig in signals:
        print(f"  [{sig['signal_type']}] {sig['description']}")
        if sig["signal_strength"]:
            print(f"    strength={sig['signal_strength']:.2f}")

    # Milestone distribution
    print("\n--- Milestone Distribution ---")
    milestones = conn.execute("""
        SELECT milestone_type, COUNT(*) AS cnt
        FROM startup_milestones
        GROUP BY milestone_type
        ORDER BY cnt DESC
    """).fetchall()

    for m in milestones:
        print(f"  {m['milestone_type']:<25} {m['cnt']:>5}")

    print("\n" + "=" * 60)


def main():
    parser = argparse.ArgumentParser(description="Analyze startup emergence patterns")
    parser.add_argument("--path", type=Path, default=DB_PATH)
    parser.add_argument("--rebuild-cohorts", action="store_true",
                        help="Rebuild cohort aggregation tables")
    parser.add_argument("--detect-signals", action="store_true",
                        help="Run signal detection")
    parser.add_argument("--report", action="store_true",
                        help="Generate summary report")
    parser.add_argument("--all", action="store_true",
                        help="Run all steps")
    args = parser.parse_args()

    if not args.path.exists():
        print(f"ERROR: Database not found at {args.path}")
        return

    conn = get_conn(args.path)

    try:
        if args.all or (not args.rebuild_cohorts and not args.detect_signals
                        and not args.report):
            rebuild_cohorts(conn)
            detect_signals(conn)
            generate_report(conn)
        else:
            if args.rebuild_cohorts:
                rebuild_cohorts(conn)
            if args.detect_signals:
                detect_signals(conn)
            if args.report:
                generate_report(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
