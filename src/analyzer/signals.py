#!/usr/bin/env python3
"""
Signal of Change detection engine.
Identifies sectors with accelerating investment activity.
"""

import sys
import json
import subprocess
import logging
from pathlib import Path
from datetime import datetime, timedelta

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))
from db.models import get_conn

logger = logging.getLogger(__name__)

# Threshold: sector count must exceed baseline by this ratio to trigger
SURGE_THRESHOLD = 1.5  # 50% increase
MIN_RECENT_COUNT = 3   # Minimum deals in recent period to be significant


def detect_investment_surges(
    recent_weeks: int = 4,
    baseline_months: int = 3,
) -> list[dict]:
    """
    Detect sectors with accelerating investment activity.
    Compares recent period against baseline average.
    """
    conn = get_conn()
    now = datetime.now()
    recent_start = (now - timedelta(weeks=recent_weeks)).strftime("%Y-%m-%d")
    baseline_start = (now - timedelta(days=baseline_months * 30)).strftime("%Y-%m-%d")

    # Recent period counts by sector
    recent = conn.execute("""
        SELECT s.name as sector, COUNT(*) as count
        FROM investments i
        JOIN companies c ON i.company_id = c.id
        JOIN sectors s ON c.sector_id = s.id
        WHERE i.announced_date >= ? AND i.is_duplicate = 0
        GROUP BY s.id
    """, (recent_start,)).fetchall()

    # Baseline period counts by sector (monthly average)
    baseline = conn.execute("""
        SELECT s.name as sector,
               COUNT(*) * 1.0 / ? as monthly_avg
        FROM investments i
        JOIN companies c ON i.company_id = c.id
        JOIN sectors s ON c.sector_id = s.id
        WHERE i.announced_date >= ? AND i.announced_date < ?
              AND i.is_duplicate = 0
        GROUP BY s.id
    """, (baseline_months, baseline_start, recent_start)).fetchall()

    conn.close()

    # Build baseline lookup
    baseline_map = {r["sector"]: r["monthly_avg"] for r in baseline}

    # Detect surges
    signals = []
    for r in recent:
        sector = r["sector"]
        current = r["count"]
        baseline_avg = baseline_map.get(sector, 0)

        if current < MIN_RECENT_COUNT:
            continue

        # Calculate acceleration ratio
        if baseline_avg > 0:
            # Normalize recent to monthly equivalent for comparison
            recent_monthly = current * (30.0 / (recent_weeks * 7))
            ratio = recent_monthly / baseline_avg
        else:
            # New sector with no baseline
            ratio = float('inf')

        if ratio >= SURGE_THRESHOLD or baseline_avg == 0:
            signal = {
                "signal_type": "investment_surge",
                "sector": sector,
                "period_start": recent_start,
                "period_end": now.strftime("%Y-%m-%d"),
                "baseline_monthly_avg": round(baseline_avg, 1),
                "recent_count": current,
                "acceleration_ratio": round(ratio, 2) if ratio != float('inf') else None,
                "description": (
                    f"Sector '{sector}': {current} deals in last {recent_weeks} weeks "
                    f"(baseline avg: {baseline_avg:.1f}/month, "
                    f"ratio: {ratio:.1f}x)" if ratio != float('inf')
                    else f"New sector '{sector}': {current} deals in last {recent_weeks} weeks (no baseline)"
                ),
            }
            signals.append(signal)

    # Sort by acceleration ratio (highest first)
    signals.sort(
        key=lambda s: s["acceleration_ratio"] or 999,
        reverse=True,
    )

    return signals


def store_signals(signals: list[dict]):
    """Save detected signals to the database."""
    if not signals:
        logger.info("No signals to store")
        return

    conn = get_conn()
    try:
        for sig in signals:
            row = conn.execute(
                "SELECT id FROM sectors WHERE name = ?",
                (sig["sector"],)
            ).fetchone()
            sector_id = row["id"] if row else None

            conn.execute("""
                INSERT INTO signals
                (signal_type, sector_id, period_start, period_end,
                 baseline_count, current_count, acceleration_ratio, description)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                sig["signal_type"],
                sector_id,
                sig["period_start"],
                sig["period_end"],
                int(sig["baseline_monthly_avg"]) if sig["baseline_monthly_avg"] else 0,
                sig["recent_count"],
                sig["acceleration_ratio"],
                sig["description"],
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

    report_lines = [
        "# Investment Signal Radar: Signal of Change Report",
        f"\nDetected at: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        f"\n## Detected Signals ({len(signals)} total)\n",
    ]

    for i, sig in enumerate(signals, 1):
        ratio_str = f"{sig['acceleration_ratio']}x" if sig['acceleration_ratio'] else "NEW"
        report_lines.append(
            f"### {i}. {sig['sector']} ({ratio_str})\n"
            f"{sig['description']}\n"
        )

    report_content = "\n".join(report_lines)

    # Write temp file for save-research.sh
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


def run_signal_detection():
    """Full signal detection pipeline."""
    logging.basicConfig(level=logging.INFO)

    logger.info("Running signal detection...")
    signals = detect_investment_surges()

    if signals:
        logger.info(f"Detected {len(signals)} signals:")
        for sig in signals:
            logger.info(f"  - {sig['sector']}: {sig['description']}")
        store_signals(signals)
        report_to_dashboard(signals)
    else:
        logger.info("No investment surge signals detected")

    return signals


if __name__ == "__main__":
    run_signal_detection()
