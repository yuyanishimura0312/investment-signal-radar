#!/usr/bin/env python3
"""
Startup Ecosystem DB: Coverage targets and measurement.

Defines theoretical ideal values for each coverage dimension,
measures current state, and reports coverage ratios.

Usage:
    python3 scripts/coverage_targets.py
    python3 scripts/coverage_targets.py --json
"""

import json
import sqlite3
import sys
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "investment_signal_v2.db"

# ============================================================
# THEORETICAL IDEAL VALUES
# ============================================================
# Based on:
#   - Japan startup ecosystem size: ~10,000 active startups (METI estimate)
#   - Annual VC deals in Japan: ~2,000-2,500 (INITIAL/JVR data)
#   - NEDO active projects: ~500-800/year
#   - Startup Genome covers 300+ ecosystems globally
#   - Our primary focus: Japanese startups with VC funding
# ============================================================

TARGETS = {
    # --- Entity Coverage ---
    "organizations_total": {
        "ideal": 5000,
        "description": "Total tracked organizations (companies + investors)",
        "rationale": "日本のVC投資先スタートアップ上位5,000社をカバー"
    },
    "companies_total": {
        "ideal": 3000,
        "description": "Tracked startup companies",
        "rationale": "日本の資金調達実績のあるスタートアップ約3,000社"
    },
    "investors_total": {
        "ideal": 500,
        "description": "Tracked investors (VC/CVC/Angel)",
        "rationale": "日本の主要VC/CVC/エンジェル投資家500名・社"
    },

    # --- Lifecycle Data Quality ---
    "founded_date_rate": {
        "ideal": 0.85,
        "description": "Companies with known founding date",
        "rationale": "法人番号DBやG-Biz Infoで85%以上のカバーが可能"
    },
    "sector_assignment_rate": {
        "ideal": 0.95,
        "description": "Companies with at least one sector assigned",
        "rationale": "Claude API分類により95%以上のセクター付与が可能"
    },
    "tag_assignment_rate": {
        "ideal": 0.80,
        "description": "Companies with at least one technology tag",
        "rationale": "プレスリリース・Web情報からの自動タグ付けで80%"
    },
    "milestone_coverage_rate": {
        "ideal": 0.90,
        "description": "Companies with at least one lifecycle milestone",
        "rationale": "設立・資金調達のいずれかが記録されていれば達成"
    },
    "milestones_per_company": {
        "ideal": 3.0,
        "description": "Average milestones per tracked company",
        "rationale": "設立+初回資金調達+最新ラウンドで最低3件"
    },

    # --- Funding Round Coverage ---
    "funding_rounds_total": {
        "ideal": 5000,
        "description": "Total recorded funding rounds",
        "rationale": "2020-2026の日本スタートアップ資金調達約5,000件"
    },
    "rounds_with_amount": {
        "ideal": 0.70,
        "description": "Rounds with known amount (JPY or USD)",
        "rationale": "非公開ラウンド30%を除き70%の金額が判明"
    },
    "rounds_with_investors": {
        "ideal": 0.60,
        "description": "Rounds with at least one identified investor",
        "rationale": "リード投資家の特定率60%が現実的な目標"
    },

    # --- Temporal Coverage ---
    "year_coverage_2020_plus": {
        "ideal": 0.90,
        "description": "Coverage ratio for deals from 2020 onward",
        "rationale": "PR TIMES/The Bridge主要ソースの本格収集開始以降"
    },
    "year_coverage_2015_2019": {
        "ideal": 0.50,
        "description": "Coverage ratio for deals from 2015-2019",
        "rationale": "過去データは主要案件のみ歴史的バックフィル"
    },

    # --- Ecosystem Context ---
    "ecosystem_rankings_years": {
        "ideal": 5,
        "description": "Years of ecosystem ranking data",
        "rationale": "Startup Genome/Blink 2020-2024の5年分"
    },
    "nedo_projects": {
        "ideal": 200,
        "description": "Tracked NEDO research projects",
        "rationale": "スタートアップ関連NEDOプロジェクト上位200件"
    },
    "nedo_with_spinoff": {
        "ideal": 50,
        "description": "NEDO projects with identified startup spinoffs",
        "rationale": "大学発ベンチャー創出に繋がったプロジェクト50件"
    },

    # --- Signal Detection ---
    "emergence_signals": {
        "ideal": 50,
        "description": "Detected emergence signals (meaningful, not noise)",
        "rationale": "33セクター×2-3年のうち有意なシグナル約50件"
    },
    "cohort_records": {
        "ideal": 300,
        "description": "Year × sector cohort records",
        "rationale": "33セクター×10年分のコホート集計"
    },
}


def measure_current(conn: sqlite3.Connection) -> dict:
    """Measure current coverage against targets."""
    results = {}

    # Entity counts
    results["organizations_total"] = conn.execute(
        "SELECT COUNT(*) FROM organizations").fetchone()[0]
    results["companies_total"] = conn.execute(
        "SELECT COUNT(*) FROM organizations WHERE is_company=1").fetchone()[0]
    results["investors_total"] = conn.execute(
        "SELECT COUNT(*) FROM organizations WHERE is_investor=1").fetchone()[0]

    # Data quality rates
    companies = max(results["companies_total"], 1)
    results["founded_date_rate"] = conn.execute(
        "SELECT COUNT(*) FROM organizations WHERE is_company=1 AND founded_date IS NOT NULL"
    ).fetchone()[0] / companies

    results["sector_assignment_rate"] = 1.0 - conn.execute("""
        SELECT COUNT(*) FROM organizations o WHERE o.is_company=1
        AND NOT EXISTS (SELECT 1 FROM organization_sectors os WHERE os.organization_id=o.id)
    """).fetchone()[0] / companies

    results["tag_assignment_rate"] = 1.0 - conn.execute("""
        SELECT COUNT(*) FROM organizations o WHERE o.is_company=1
        AND NOT EXISTS (SELECT 1 FROM organization_tags ot WHERE ot.organization_id=o.id)
    """).fetchone()[0] / companies

    results["milestone_coverage_rate"] = 1.0 - conn.execute("""
        SELECT COUNT(*) FROM organizations o WHERE o.is_company=1
        AND NOT EXISTS (SELECT 1 FROM startup_milestones sm WHERE sm.organization_id=o.id)
    """).fetchone()[0] / companies

    ms_total = conn.execute("SELECT COUNT(*) FROM startup_milestones").fetchone()[0]
    ms_orgs = conn.execute(
        "SELECT COUNT(DISTINCT organization_id) FROM startup_milestones").fetchone()[0]
    results["milestones_per_company"] = ms_total / max(ms_orgs, 1)

    # Funding rounds
    results["funding_rounds_total"] = conn.execute(
        "SELECT COUNT(*) FROM funding_rounds").fetchone()[0]
    fr_total = max(results["funding_rounds_total"], 1)
    results["rounds_with_amount"] = conn.execute(
        "SELECT COUNT(*) FROM funding_rounds WHERE amount_jpy IS NOT NULL OR amount_usd IS NOT NULL"
    ).fetchone()[0] / fr_total
    results["rounds_with_investors"] = conn.execute("""
        SELECT COUNT(DISTINCT fr.id) FROM funding_rounds fr
        WHERE EXISTS (SELECT 1 FROM round_participants rp WHERE rp.funding_round_id=fr.id)
    """).fetchone()[0] / fr_total

    # Temporal coverage (estimated by deal density vs expected)
    r2020 = conn.execute("""
        SELECT COUNT(*) FROM funding_rounds WHERE announced_date >= '2020-01-01'
    """).fetchone()[0]
    results["year_coverage_2020_plus"] = min(1.0, r2020 / 3500)  # ~3500 expected deals 2020-2026

    r2015 = conn.execute("""
        SELECT COUNT(*) FROM funding_rounds
        WHERE announced_date >= '2015-01-01' AND announced_date < '2020-01-01'
    """).fetchone()[0]
    results["year_coverage_2015_2019"] = min(1.0, r2015 / 2500)  # ~2500 expected deals 2015-2019

    # Ecosystem context
    results["ecosystem_rankings_years"] = conn.execute(
        "SELECT COUNT(DISTINCT report_year) FROM ecosystem_rankings").fetchone()[0]
    results["nedo_projects"] = conn.execute(
        "SELECT COUNT(*) FROM nedo_projects").fetchone()[0]
    results["nedo_with_spinoff"] = conn.execute(
        "SELECT COUNT(*) FROM nedo_projects WHERE has_spinoff=1").fetchone()[0]

    # Signals
    results["emergence_signals"] = conn.execute(
        "SELECT COUNT(*) FROM startup_emergence_signals").fetchone()[0]
    results["cohort_records"] = conn.execute(
        "SELECT COUNT(*) FROM startup_cohorts").fetchone()[0]

    return results


def report(conn: sqlite3.Connection, as_json: bool = False):
    """Generate coverage report."""
    current = measure_current(conn)

    if as_json:
        output = {}
        total_score = 0
        count = 0
        for key, target in TARGETS.items():
            ideal = target["ideal"]
            actual = current.get(key, 0)
            if isinstance(ideal, float) and ideal <= 1.0:
                ratio = actual / ideal if ideal > 0 else 0
            else:
                ratio = actual / ideal if ideal > 0 else 0
            ratio = min(ratio, 1.0)
            total_score += ratio
            count += 1
            output[key] = {
                "ideal": ideal, "actual": round(actual, 3),
                "coverage": round(ratio, 3),
                "description": target["description"]
            }
        output["_overall_coverage"] = round(total_score / count, 3)
        print(json.dumps(output, indent=2, ensure_ascii=False))
        return

    print("=" * 72)
    print("  Startup Ecosystem DB — Coverage Report")
    print("=" * 72)

    categories = [
        ("Entity Coverage", [
            "organizations_total", "companies_total", "investors_total"]),
        ("Lifecycle Data Quality", [
            "founded_date_rate", "sector_assignment_rate", "tag_assignment_rate",
            "milestone_coverage_rate", "milestones_per_company"]),
        ("Funding Round Coverage", [
            "funding_rounds_total", "rounds_with_amount", "rounds_with_investors"]),
        ("Temporal Coverage", [
            "year_coverage_2020_plus", "year_coverage_2015_2019"]),
        ("Ecosystem Context", [
            "ecosystem_rankings_years", "nedo_projects", "nedo_with_spinoff"]),
        ("Signal Detection", [
            "emergence_signals", "cohort_records"]),
    ]

    total_score = 0
    total_count = 0

    for cat_name, keys in categories:
        print(f"\n--- {cat_name} ---")
        cat_score = 0
        for key in keys:
            target = TARGETS[key]
            ideal = target["ideal"]
            actual = current.get(key, 0)

            if isinstance(ideal, float) and ideal <= 1.0:
                ratio = actual / ideal if ideal > 0 else 0
                actual_str = f"{actual*100:.1f}%"
                ideal_str = f"{ideal*100:.0f}%"
            else:
                ratio = actual / ideal if ideal > 0 else 0
                actual_str = f"{actual:,.0f}" if isinstance(actual, (int, float)) else str(actual)
                ideal_str = f"{ideal:,}"

            ratio = min(ratio, 1.0)
            cat_score += ratio
            total_score += ratio
            total_count += 1

            bar_len = 20
            filled = int(ratio * bar_len)
            bar = "█" * filled + "░" * (bar_len - filled)

            print(f"  {target['description']:<40} {actual_str:>8} / {ideal_str:>8}  "
                  f"|{bar}| {ratio*100:5.1f}%")

        cat_avg = cat_score / len(keys) * 100
        print(f"  {'Category average:':>40} {' ':>19} {cat_avg:5.1f}%")

    overall = total_score / total_count * 100
    print(f"\n{'=' * 72}")
    print(f"  OVERALL COVERAGE: {overall:.1f}%")
    print(f"{'=' * 72}")

    # Priority gaps
    print("\n--- Priority Gaps (lowest coverage) ---")
    gaps = []
    for key, target in TARGETS.items():
        actual = current.get(key, 0)
        ideal = target["ideal"]
        if isinstance(ideal, float) and ideal <= 1.0:
            ratio = actual / ideal
        else:
            ratio = actual / ideal if ideal > 0 else 0
        ratio = min(ratio, 1.0)
        gaps.append((key, ratio, target["description"], target["rationale"]))

    gaps.sort(key=lambda x: x[1])
    for key, ratio, desc, rationale in gaps[:8]:
        print(f"  {ratio*100:5.1f}%  {desc}")
        print(f"         → {rationale}")


if __name__ == "__main__":
    conn = sqlite3.connect(str(DB_PATH))
    report(conn, as_json="--json" in sys.argv)
    conn.close()
