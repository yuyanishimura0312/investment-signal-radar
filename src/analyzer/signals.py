#!/usr/bin/env python3
"""
Signal of Change detection engine (v2 schema).

Detects multiple signal types:
  1. investment_surge   — sector with accelerating deal count
  2. new_sector         — previously quiet sector with sudden activity
  3. round_size_anomaly — unusually large rounds relative to sector average
  4. new_investor_entry — established investors entering new sectors
  5. co_investment_cluster — new co-investment patterns forming
"""

import sys
import json
import subprocess
import logging
from pathlib import Path
from datetime import datetime, timedelta

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))
from db.models_v2 import get_conn

logger = logging.getLogger(__name__)

# Thresholds
SURGE_THRESHOLD = 1.5       # 50% increase over baseline
MIN_RECENT_COUNT = 2        # Minimum deals in recent period
ROUND_SIZE_ZSCORE = 2.0     # Standard deviations above mean for anomaly
NEW_INVESTOR_MIN_DEALS = 3  # Investor must have >= N total deals to be "established"


def detect_investment_surges(
    recent_weeks: int = 4,
    baseline_months: int = 3,
) -> list[dict]:
    """Detect sectors with accelerating investment activity."""
    conn = get_conn()
    try:
        now = datetime.now()
        recent_start = (now - timedelta(weeks=recent_weeks)).strftime("%Y-%m-%d")
        baseline_start = (now - timedelta(days=baseline_months * 30)).strftime("%Y-%m-%d")

        # Recent period counts by sector
        recent = conn.execute("""
            SELECT COALESCE(s.name, 'Unknown') AS sector,
                   s.id AS sector_id,
                   COUNT(*) AS count
            FROM funding_rounds fr
            JOIN organizations o ON fr.organization_id = o.id
            LEFT JOIN organization_sectors os
                ON o.id = os.organization_id AND os.is_primary = 1
            LEFT JOIN sectors s ON os.sector_id = s.id
            WHERE fr.announced_date >= ?
              AND (fr.is_duplicate IS NULL OR fr.is_duplicate = 0)
            GROUP BY s.id
        """, (recent_start,)).fetchall()

        # Baseline period counts by sector (monthly average)
        baseline = conn.execute("""
            SELECT COALESCE(s.name, 'Unknown') AS sector,
                   s.id AS sector_id,
                   COUNT(*) * 1.0 / ? AS monthly_avg
            FROM funding_rounds fr
            JOIN organizations o ON fr.organization_id = o.id
            LEFT JOIN organization_sectors os
                ON o.id = os.organization_id AND os.is_primary = 1
            LEFT JOIN sectors s ON os.sector_id = s.id
            WHERE fr.announced_date >= ? AND fr.announced_date < ?
              AND (fr.is_duplicate IS NULL OR fr.is_duplicate = 0)
            GROUP BY s.id
        """, (baseline_months, baseline_start, recent_start)).fetchall()
    finally:
        conn.close()

    baseline_map = {r["sector"]: (r["monthly_avg"], r["sector_id"]) for r in baseline}

    signals = []
    for r in recent:
        sector = r["sector"]
        sector_id = r["sector_id"]
        current = r["count"]
        baseline_avg, _ = baseline_map.get(sector, (0, None))

        if current < MIN_RECENT_COUNT:
            continue

        if baseline_avg > 0:
            recent_monthly = current * (30.0 / (recent_weeks * 7))
            ratio = recent_monthly / baseline_avg
        else:
            ratio = float("inf")

        if ratio >= SURGE_THRESHOLD or baseline_avg == 0:
            sig_type = "new_sector" if baseline_avg == 0 else "investment_surge"
            signals.append({
                "signal_type": sig_type,
                "sector": sector,
                "sector_id": sector_id,
                "period_start": recent_start,
                "period_end": now.strftime("%Y-%m-%d"),
                "baseline_monthly_avg": round(baseline_avg, 1),
                "recent_count": current,
                "acceleration_ratio": round(ratio, 2) if ratio != float("inf") else None,
                "description": (
                    f"Sector '{sector}': {current} deals in last {recent_weeks} weeks "
                    f"(baseline avg: {baseline_avg:.1f}/month, ratio: {ratio:.1f}x)"
                    if ratio != float("inf")
                    else f"New sector '{sector}': {current} deals in last {recent_weeks} weeks (no prior baseline)"
                ),
            })

    signals.sort(key=lambda s: s["acceleration_ratio"] or 999, reverse=True)
    return signals


def detect_round_size_anomalies(months: int = 3) -> list[dict]:
    """Detect unusually large funding rounds relative to sector average."""
    conn = get_conn()
    try:
        cutoff = (datetime.now() - timedelta(days=months * 30)).strftime("%Y-%m-%d")

        # Sector average and stddev
        sector_stats = conn.execute("""
            SELECT s.id AS sector_id, COALESCE(s.name, 'Unknown') AS sector,
                   AVG(fr.amount_jpy) AS avg_amount,
                   -- sample stddev (manual calc for SQLite)
                   SQRT(AVG(fr.amount_jpy * fr.amount_jpy) - AVG(fr.amount_jpy) * AVG(fr.amount_jpy)) AS stddev_amount,
                   COUNT(*) AS deal_count
            FROM funding_rounds fr
            JOIN organizations o ON fr.organization_id = o.id
            LEFT JOIN organization_sectors os
                ON o.id = os.organization_id AND os.is_primary = 1
            LEFT JOIN sectors s ON os.sector_id = s.id
            WHERE fr.amount_jpy IS NOT NULL AND fr.amount_jpy > 0
              AND (fr.is_duplicate IS NULL OR fr.is_duplicate = 0)
            GROUP BY s.id
            HAVING deal_count >= 5
        """).fetchall()

        stats_map = {}
        for row in sector_stats:
            stats_map[row["sector_id"]] = {
                "sector": row["sector"],
                "avg": row["avg_amount"],
                "stddev": row["stddev_amount"] or 0,
                "count": row["deal_count"],
            }

        # Recent large rounds
        recent_rounds = conn.execute("""
            SELECT fr.id, fr.amount_jpy, fr.announced_date, fr.round_type,
                   o.name AS company_name, s.id AS sector_id,
                   COALESCE(s.name, 'Unknown') AS sector
            FROM funding_rounds fr
            JOIN organizations o ON fr.organization_id = o.id
            LEFT JOIN organization_sectors os
                ON o.id = os.organization_id AND os.is_primary = 1
            LEFT JOIN sectors s ON os.sector_id = s.id
            WHERE fr.announced_date >= ?
              AND fr.amount_jpy IS NOT NULL AND fr.amount_jpy > 0
              AND (fr.is_duplicate IS NULL OR fr.is_duplicate = 0)
            ORDER BY fr.amount_jpy DESC
        """, (cutoff,)).fetchall()
    finally:
        conn.close()

    signals = []
    for r in recent_rounds:
        s = stats_map.get(r["sector_id"])
        if not s or s["stddev"] == 0:
            continue
        zscore = (r["amount_jpy"] - s["avg"]) / s["stddev"]
        if zscore >= ROUND_SIZE_ZSCORE:
            amount_oku = r["amount_jpy"] / 100_000_000
            avg_oku = s["avg"] / 100_000_000
            signals.append({
                "signal_type": "round_size_anomaly",
                "sector": r["sector"],
                "sector_id": r["sector_id"],
                "period_start": r["announced_date"],
                "period_end": r["announced_date"],
                "baseline_monthly_avg": round(avg_oku, 1),
                "recent_count": 1,
                "acceleration_ratio": round(zscore, 2),
                "description": (
                    f"{r['company_name']} ({r['round_type']}): "
                    f"{amount_oku:.1f}億円 — sector avg {avg_oku:.1f}億円, "
                    f"z-score {zscore:.1f}"
                ),
                "related_round_ids": [r["id"]],
            })

    signals.sort(key=lambda s: s["acceleration_ratio"], reverse=True)
    return signals[:20]  # Top 20


def detect_new_investor_entries(months: int = 3) -> list[dict]:
    """Detect established investors entering new sectors for the first time."""
    conn = get_conn()
    try:
        cutoff = (datetime.now() - timedelta(days=months * 30)).strftime("%Y-%m-%d")

        rows = conn.execute("""
            WITH investor_history AS (
                SELECT rp.investor_id, s.id AS sector_id,
                       COALESCE(s.name, 'Unknown') AS sector,
                       MIN(fr.announced_date) AS first_deal_in_sector,
                       COUNT(*) AS deals_in_sector
                FROM round_participants rp
                JOIN funding_rounds fr ON rp.funding_round_id = fr.id
                JOIN organizations co ON fr.organization_id = co.id
                LEFT JOIN organization_sectors os
                    ON co.id = os.organization_id AND os.is_primary = 1
                LEFT JOIN sectors s ON os.sector_id = s.id
                WHERE fr.is_duplicate IS NULL OR fr.is_duplicate = 0
                GROUP BY rp.investor_id, s.id
            ),
            established_investors AS (
                SELECT investor_id, COUNT(DISTINCT sector_id) AS sector_count,
                       SUM(deals_in_sector) AS total_deals
                FROM investor_history
                GROUP BY investor_id
                HAVING total_deals >= ?
            )
            SELECT ih.investor_id, inv.name AS investor_name,
                   ih.sector, ih.sector_id,
                   ih.first_deal_in_sector, ih.deals_in_sector,
                   ei.total_deals
            FROM investor_history ih
            JOIN established_investors ei ON ih.investor_id = ei.investor_id
            JOIN organizations inv ON ih.investor_id = inv.id
            WHERE ih.first_deal_in_sector >= ?
              AND ih.deals_in_sector <= 2
            ORDER BY ei.total_deals DESC
        """, (NEW_INVESTOR_MIN_DEALS, cutoff)).fetchall()
    finally:
        conn.close()

    signals = []
    for r in rows:
        signals.append({
            "signal_type": "new_investor_entry",
            "sector": r["sector"],
            "sector_id": r["sector_id"],
            "period_start": r["first_deal_in_sector"],
            "period_end": r["first_deal_in_sector"],
            "baseline_monthly_avg": 0,
            "recent_count": r["deals_in_sector"],
            "acceleration_ratio": None,
            "description": (
                f"{r['investor_name']} (total {r['total_deals']} deals) "
                f"entered '{r['sector']}' for the first time on {r['first_deal_in_sector']}"
            ),
        })

    return signals


def store_signals(signals: list[dict]):
    """Save detected signals to the database."""
    if not signals:
        logger.info("No signals to store")
        return

    conn = get_conn()
    try:
        for sig in signals:
            sector_id = sig.get("sector_id")
            related = json.dumps(sig.get("related_round_ids", []))

            conn.execute("""
                INSERT INTO signals
                (signal_type, sector_id, detected_at, period_start, period_end,
                 baseline_count, current_count, acceleration_ratio,
                 description, related_round_ids)
                VALUES (?, ?, datetime('now'), ?, ?, ?, ?, ?, ?, ?)
            """, (
                sig["signal_type"],
                sector_id,
                sig["period_start"],
                sig["period_end"],
                int(sig["baseline_monthly_avg"]) if sig["baseline_monthly_avg"] else 0,
                sig["recent_count"],
                sig["acceleration_ratio"],
                sig["description"],
                related,
            ))

        conn.commit()
        logger.info(f"Stored {len(signals)} signals")
    except Exception as e:
        logger.error(f"Failed to store signals: {e}")
        conn.rollback()
    finally:
        conn.close()


def report_to_dashboard(signals: list[dict]):
    """Save signal report to Research Dashboard via save-research.sh."""
    if not signals:
        return

    # Group by type
    by_type: dict[str, list] = {}
    for sig in signals:
        by_type.setdefault(sig["signal_type"], []).append(sig)

    type_labels = {
        "investment_surge": "Investment Surge (セクター加速)",
        "new_sector": "New Sector Activity (新規セクター)",
        "round_size_anomaly": "Round Size Anomaly (異常値ラウンド)",
        "new_investor_entry": "New Investor Entry (投資家の新規参入)",
    }

    report_lines = [
        "# Investment Signal Radar: Signal of Change Report",
        f"\nDetected at: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        f"\n## Summary: {len(signals)} signals detected\n",
    ]

    for sig_type, sigs in by_type.items():
        label = type_labels.get(sig_type, sig_type)
        report_lines.append(f"\n### {label} ({len(sigs)} signals)\n")
        for i, sig in enumerate(sigs, 1):
            ratio_str = f"{sig['acceleration_ratio']}x" if sig["acceleration_ratio"] else "NEW"
            report_lines.append(f"{i}. **{sig['sector']}** ({ratio_str}): {sig['description']}\n")

    report_content = "\n".join(report_lines)

    tmp_path = Path("/tmp/isr_signal_report.md")
    tmp_path.write_text(report_content, encoding="utf-8")

    dashboard_script = Path.home() / "projects/research/research-dashboard/save-research.sh"
    if dashboard_script.exists():
        try:
            subprocess.run([
                "bash", str(dashboard_script),
                "--title", f"Investment Signal Radar: {len(signals)} Signals Detected",
                "--category", "signal",
                "--tags", "VC,investment,signal,foresight",
                "--content-file", str(tmp_path),
            ], check=True, capture_output=True)
            logger.info("Signal report saved to Research Dashboard")
        except subprocess.CalledProcessError as e:
            logger.warning(f"Failed to save to dashboard: {e}")
    else:
        logger.warning("Research Dashboard save-research.sh not found")

    print(report_content)


def run_signal_detection():
    """Full signal detection pipeline."""
    logging.basicConfig(level=logging.INFO)

    all_signals = []

    logger.info("Detecting investment surges...")
    surges = detect_investment_surges()
    all_signals.extend(surges)
    logger.info(f"  Found {len(surges)} surge signals")

    logger.info("Detecting round size anomalies...")
    anomalies = detect_round_size_anomalies()
    all_signals.extend(anomalies)
    logger.info(f"  Found {len(anomalies)} anomaly signals")

    logger.info("Detecting new investor entries...")
    entries = detect_new_investor_entries()
    all_signals.extend(entries)
    logger.info(f"  Found {len(entries)} new entry signals")

    logger.info(f"Total: {len(all_signals)} signals detected")

    if all_signals:
        store_signals(all_signals)
        report_to_dashboard(all_signals)
    else:
        logger.info("No signals detected")

    return all_signals


if __name__ == "__main__":
    run_signal_detection()
