#!/usr/bin/env python3
"""
Part 2: Signal Detection
Runs the three signal detectors and persists results to the signals table.
"""

import sys
import sqlite3
import logging
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

DB_PATH = project_root / "data" / "investment_signal_v2.db"

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def main():
    # Import signal functions after sys.path is set
    from src.analyzer.signals import (
        detect_investment_surges,
        detect_round_size_anomalies,
        detect_new_investor_entries,
        store_signals,
    )

    logger.info("=== Investment Signal Detection ===")

    all_signals = []

    logger.info("Detecting investment surges (sector acceleration)...")
    surges = detect_investment_surges()
    logger.info(f"  Found: {len(surges)} surge/new-sector signals")
    all_signals.extend(surges)

    logger.info("Detecting round size anomalies...")
    anomalies = detect_round_size_anomalies()
    logger.info(f"  Found: {len(anomalies)} anomaly signals")
    all_signals.extend(anomalies)

    logger.info("Detecting new investor entries...")
    entries = detect_new_investor_entries()
    logger.info(f"  Found: {len(entries)} new-entry signals")
    all_signals.extend(entries)

    logger.info(f"Total signals detected: {len(all_signals)}")

    # The signals table CHECK constraint only allows:
    # investment_surge, new_sector, co_investment_cluster, cross_radar, event_surge, network_shift
    # Map detector outputs to allowed types before storing.
    SIGNAL_TYPE_MAP = {
        "investment_surge": "investment_surge",
        "new_sector": "new_sector",
        "round_size_anomaly": "event_surge",     # large rounds → event surge
        "new_investor_entry": "network_shift",   # investor entering new sector → network shift
    }

    mapped_signals = []
    for sig in all_signals:
        original_type = sig.get("signal_type", "")
        mapped_type = SIGNAL_TYPE_MAP.get(original_type)
        if mapped_type is None:
            logger.warning(f"Unknown signal_type '{original_type}', skipping")
            continue
        mapped = dict(sig)
        mapped["signal_type"] = mapped_type
        mapped_signals.append(mapped)

    if mapped_signals:
        logger.info(f"Storing {len(mapped_signals)} signals to database...")
        store_signals(mapped_signals)
    else:
        logger.warning("No signals to store.")

    # Report final count
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    count = conn.execute("SELECT COUNT(*) FROM signals").fetchone()[0]
    by_type = conn.execute(
        "SELECT signal_type, COUNT(*) as c FROM signals GROUP BY signal_type"
    ).fetchall()
    conn.close()

    print(f"\n=== Signal Detection Complete ===")
    print(f"signals: {count} total")
    for row in by_type:
        print(f"  {row['signal_type']}: {row['c']}")


if __name__ == "__main__":
    main()
