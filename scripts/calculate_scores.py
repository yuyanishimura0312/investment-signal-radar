#!/usr/bin/env python3
"""
Part 3: Signal Score Calculation
Calculates momentum, funding, and market scores for each company with ≥1
funding round, then computes a composite score using v1.0 model weights.

Scores are normalized to [0, 1] range:
  momentum  — recency + frequency of funding rounds
  funding   — total amount raised vs. sector median
  market    — sector growth trend (deal count growth rate)
  composite — weighted sum per score_models v1.0
"""

import sys
import sqlite3
import json
import logging
import math
from datetime import datetime, date
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

DB_PATH = project_root / "data" / "investment_signal_v2.db"
MODEL_VERSION = "v1.0"

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

TODAY = date.today()


# ================================================================
# Scoring helpers
# ================================================================

def sigmoid(x: float) -> float:
    """Sigmoid squashing to (0, 1)."""
    return 1.0 / (1.0 + math.exp(-x))


def clamp(value: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, value))


def compute_momentum(rounds: list[dict]) -> tuple[float, dict]:
    """
    Momentum score based on:
    - Recency: days since last round (recent = higher)
    - Frequency: number of rounds (more = higher)

    Returns (score, components_dict)
    """
    if not rounds:
        return 0.0, {}

    # Recency: days since most recent round
    dates = [r["announced_date"] for r in rounds if r["announced_date"]]
    if not dates:
        return 0.0, {}

    most_recent = max(dates)
    try:
        last_date = datetime.strptime(most_recent, "%Y-%m-%d").date()
    except ValueError:
        return 0.0, {}

    days_ago = (TODAY - last_date).days

    # Recency score: 1.0 if same day, decays exponentially over 2 years
    recency_score = math.exp(-days_ago / 365.0)

    # Frequency score: sigmoid on round count (3+ rounds → ~0.75, 6+ → ~0.95)
    n_rounds = len(rounds)
    frequency_score = sigmoid((n_rounds - 2) * 0.8)

    momentum = clamp(0.6 * recency_score + 0.4 * frequency_score)
    components = {
        "recency_days": days_ago,
        "last_round_date": most_recent,
        "n_rounds": n_rounds,
        "recency_score": round(recency_score, 4),
        "frequency_score": round(frequency_score, 4),
    }
    return round(momentum, 4), components


def compute_funding_score(
    total_amount: float,
    sector_median: float,
    sector_p75: float,
) -> tuple[float, dict]:
    """
    Funding score based on total amount raised vs. sector median.
    Ratio > 1 → above median. Ratio > 2 → well above. Capped at ~1.0.
    """
    if sector_median <= 0 or total_amount <= 0:
        # No amount data: give a minimal baseline
        score = 0.1 if total_amount > 0 else 0.05
        return score, {"total_amount_jpy": total_amount, "sector_median": sector_median}

    ratio = total_amount / sector_median
    # sigmoid centering: ratio=1 (at median) → ~0.5, ratio=3 → ~0.88
    score = clamp(sigmoid(ratio - 1.0))
    components = {
        "total_amount_jpy": total_amount,
        "sector_median_jpy": sector_median,
        "sector_p75_jpy": sector_p75,
        "amount_to_median_ratio": round(ratio, 3),
    }
    return round(score, 4), components


def compute_market_score(
    sector_id: int,
    sector_growth_map: dict,
) -> tuple[float, dict]:
    """
    Market score based on the sector's own growth trend.
    Sectors with more recent acceleration get higher scores.
    """
    growth = sector_growth_map.get(sector_id, {})
    growth_rate = growth.get("growth_rate", 0.0)
    deal_count = growth.get("recent_count", 0)

    # Sigmoid on growth_rate centered at 0 (flat = 0.5)
    score = clamp(sigmoid(growth_rate * 2.0))
    components = {
        "sector_id": sector_id,
        "sector_growth_rate": round(growth_rate, 4),
        "sector_recent_deals": deal_count,
    }
    return round(score, 4), components


# ================================================================
# Main calculation
# ================================================================

def build_sector_growth_map(conn: sqlite3.Connection) -> dict[int, dict]:
    """
    Compute a growth rate for each sector.
    growth_rate = (recent_monthly_avg / baseline_monthly_avg) - 1
    recent = last 3 months, baseline = prior 3 months
    """
    recent_rows = conn.execute("""
        SELECT COALESCE(s.id, 0) AS sector_id, COUNT(*) AS cnt
        FROM funding_rounds fr
        JOIN organizations o ON fr.organization_id = o.id
        LEFT JOIN organization_sectors os
            ON o.id = os.organization_id AND os.is_primary = 1
        LEFT JOIN sectors s ON os.sector_id = s.id
        WHERE fr.announced_date >= date('now', '-3 months')
          AND (fr.is_duplicate IS NULL OR fr.is_duplicate = 0)
        GROUP BY s.id
    """).fetchall()

    baseline_rows = conn.execute("""
        SELECT COALESCE(s.id, 0) AS sector_id, COUNT(*) AS cnt
        FROM funding_rounds fr
        JOIN organizations o ON fr.organization_id = o.id
        LEFT JOIN organization_sectors os
            ON o.id = os.organization_id AND os.is_primary = 1
        LEFT JOIN sectors s ON os.sector_id = s.id
        WHERE fr.announced_date >= date('now', '-6 months')
          AND fr.announced_date < date('now', '-3 months')
          AND (fr.is_duplicate IS NULL OR fr.is_duplicate = 0)
        GROUP BY s.id
    """).fetchall()

    recent_map = {r["sector_id"]: r["cnt"] for r in recent_rows}
    baseline_map = {r["sector_id"]: r["cnt"] for r in baseline_rows}

    all_sectors = set(list(recent_map.keys()) + list(baseline_map.keys()))
    result = {}
    for sid in all_sectors:
        recent_cnt = recent_map.get(sid, 0)
        baseline_cnt = baseline_map.get(sid, 0)
        if baseline_cnt > 0:
            growth_rate = (recent_cnt - baseline_cnt) / baseline_cnt
        elif recent_cnt > 0:
            growth_rate = 1.0  # New activity
        else:
            growth_rate = 0.0
        result[sid] = {
            "growth_rate": growth_rate,
            "recent_count": recent_cnt,
            "baseline_count": baseline_cnt,
        }
    return result


def build_sector_amount_stats(conn: sqlite3.Connection) -> dict[int, dict]:
    """
    For each sector, compute median and 75th percentile of total amounts raised
    per company. Returns {sector_id: {median, p75}}.
    """
    # Per-company total amounts by sector
    rows = conn.execute("""
        SELECT COALESCE(s.id, 0) AS sector_id,
               fr.organization_id,
               SUM(COALESCE(fr.amount_jpy, 0)) AS total_amount
        FROM funding_rounds fr
        JOIN organizations o ON fr.organization_id = o.id AND o.is_company = 1
        LEFT JOIN organization_sectors os
            ON o.id = os.organization_id AND os.is_primary = 1
        LEFT JOIN sectors s ON os.sector_id = s.id
        WHERE (fr.is_duplicate IS NULL OR fr.is_duplicate = 0)
        GROUP BY s.id, fr.organization_id
    """).fetchall()

    # Group amounts by sector
    from collections import defaultdict
    sector_amounts: dict[int, list[float]] = defaultdict(list)
    for r in rows:
        if r["total_amount"] > 0:
            sector_amounts[r["sector_id"]].append(r["total_amount"])

    result = {}
    for sector_id, amounts in sector_amounts.items():
        amounts_sorted = sorted(amounts)
        n = len(amounts_sorted)
        if n == 0:
            median = 0.0
            p75 = 0.0
        elif n == 1:
            median = amounts_sorted[0]
            p75 = amounts_sorted[0]
        else:
            mid = n // 2
            median = (
                amounts_sorted[mid]
                if n % 2 == 1
                else (amounts_sorted[mid - 1] + amounts_sorted[mid]) / 2
            )
            p75_idx = int(n * 0.75)
            p75 = amounts_sorted[min(p75_idx, n - 1)]
        result[sector_id] = {"median": median, "p75": p75}

    return result


def main():
    logger.info(f"Connecting to {DB_PATH}")
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")

    # Load model weights
    model_row = conn.execute(
        "SELECT weights, version FROM score_models WHERE version = ?",
        (MODEL_VERSION,),
    ).fetchone()
    if not model_row:
        logger.error(f"Model {MODEL_VERSION} not found in score_models table")
        conn.close()
        return

    weights = json.loads(model_row["weights"])
    logger.info(f"Loaded model {MODEL_VERSION}: {weights}")

    # Precompute sector stats
    logger.info("Computing sector growth rates...")
    sector_growth = build_sector_growth_map(conn)
    logger.info(f"  {len(sector_growth)} sectors with activity")

    logger.info("Computing sector amount statistics...")
    sector_amounts = build_sector_amount_stats(conn)

    # Fetch all companies with ≥1 funding round
    companies = conn.execute("""
        SELECT o.id, o.name,
               COALESCE(os.sector_id, 0) AS sector_id
        FROM organizations o
        LEFT JOIN organization_sectors os
            ON o.id = os.organization_id AND os.is_primary = 1
        WHERE o.is_company = 1
          AND EXISTS (
              SELECT 1 FROM funding_rounds fr
              WHERE fr.organization_id = o.id
                AND (fr.is_duplicate IS NULL OR fr.is_duplicate = 0)
          )
    """).fetchall()

    logger.info(f"Scoring {len(companies)} companies with funding rounds...")

    now_str = datetime.now().isoformat()
    inserted = 0

    for company in companies:
        org_id = company["id"]
        sector_id = company["sector_id"]

        # Fetch this company's funding rounds
        rounds = conn.execute("""
            SELECT id, amount_jpy, announced_date, round_type
            FROM funding_rounds
            WHERE organization_id = ?
              AND (is_duplicate IS NULL OR is_duplicate = 0)
            ORDER BY announced_date ASC
        """, (org_id,)).fetchall()

        rounds_list = [dict(r) for r in rounds]

        # --- Momentum score ---
        momentum_score, momentum_components = compute_momentum(rounds_list)

        # --- Funding score ---
        total_amount = sum(
            r["amount_jpy"] for r in rounds_list if r["amount_jpy"]
        )
        stats = sector_amounts.get(sector_id, {"median": 0, "p75": 0})
        funding_score, funding_components = compute_funding_score(
            total_amount, stats["median"], stats["p75"]
        )

        # --- Market score ---
        market_score, market_components = compute_market_score(
            sector_id, sector_growth
        )

        # --- Composite score using v1.0 weights ---
        composite_score = (
            weights.get("momentum", 0.35) * momentum_score
            + weights.get("funding", 0.25) * funding_score
            + weights.get("market", 0.15) * market_score
            # team / technology / network default to 0.5 × weight (no data)
            + weights.get("team", 0.10) * 0.5
            + weights.get("technology", 0.10) * 0.5
            + weights.get("network", 0.05) * 0.5
        )
        composite_score = clamp(round(composite_score, 4))

        # Collect all score rows to insert
        score_rows = [
            ("momentum", momentum_score, json.dumps(momentum_components, ensure_ascii=False)),
            ("funding", funding_score, json.dumps(funding_components, ensure_ascii=False)),
            ("market", market_score, json.dumps(market_components, ensure_ascii=False)),
            ("composite", composite_score, json.dumps({
                "weights": weights,
                "momentum": momentum_score,
                "funding": funding_score,
                "market": market_score,
                "team": 0.5,
                "technology": 0.5,
                "network": 0.5,
            }, ensure_ascii=False)),
        ]

        for score_type, score_value, components_json in score_rows:
            conn.execute(
                """INSERT INTO signal_scores
                   (organization_id, score_type, score_value, model_version,
                    components, calculated_at)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (org_id, score_type, score_value, MODEL_VERSION,
                 components_json, now_str),
            )
            inserted += 1

    conn.commit()

    # Final counts
    total = conn.execute("SELECT COUNT(*) FROM signal_scores").fetchone()[0]
    by_type = conn.execute(
        "SELECT score_type, COUNT(*) as c FROM signal_scores GROUP BY score_type"
    ).fetchall()
    conn.close()

    print(f"\n=== Score Calculation Complete ===")
    print(f"signal_scores: {total} total records")
    for row in by_type:
        print(f"  {row['score_type']}: {row['c']}")
    print(f"  (inserted {inserted} rows for {len(companies)} companies)")


if __name__ == "__main__":
    main()
