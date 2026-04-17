#!/usr/bin/env python3
"""
Import signals from Frontier Detector into the Investment Signal Radar.

Reads the 92 existing signals from ~/projects/apps/frontier-detector/frontier_detector.db
and imports them as press_releases in the investment signal radar DB.

Usage:
    python3 scripts/import_frontier_signals.py
    python3 scripts/import_frontier_signals.py --frontier-db /path/to/frontier_detector.db
    python3 scripts/import_frontier_signals.py --dry-run
"""

import argparse
import sys
from pathlib import Path

# Ensure src/ is importable
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from db.init_db_v2 import init_db, DEFAULT_DB_PATH  # noqa: E402
from db.models_v2 import get_conn, import_frontier_detector_signals, get_press_release_stats  # noqa: E402


DEFAULT_FRONTIER_DB = Path.home() / "projects" / "apps" / "frontier-detector" / "frontier_detector.db"


def main():
    parser = argparse.ArgumentParser(
        description="Import Frontier Detector signals into Investment Signal Radar"
    )
    parser.add_argument(
        "--frontier-db", type=Path, default=DEFAULT_FRONTIER_DB,
        help=f"Path to frontier_detector.db (default: {DEFAULT_FRONTIER_DB})"
    )
    parser.add_argument(
        "--radar-db", type=Path, default=DEFAULT_DB_PATH,
        help=f"Path to investment signal radar DB (default: {DEFAULT_DB_PATH})"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Count signals without importing"
    )
    args = parser.parse_args()

    # Validate paths
    if not args.frontier_db.exists():
        print(f"ERROR: Frontier Detector DB not found: {args.frontier_db}")
        sys.exit(1)

    # Ensure radar DB is initialized (idempotent)
    init_db(args.radar_db)

    if args.dry_run:
        import sqlite3
        fconn = sqlite3.connect(str(args.frontier_db))
        count = fconn.execute("SELECT COUNT(*) FROM signals").fetchone()[0]
        fconn.close()
        print(f"Frontier Detector has {count} signals ready to import.")
        print("Run without --dry-run to actually import.")
        return

    # Import
    conn = get_conn(args.radar_db)
    try:
        imported = import_frontier_detector_signals(conn, str(args.frontier_db))
        stats = get_press_release_stats(conn)
    finally:
        conn.close()

    print(f"\nImport complete:")
    print(f"  New records imported: {imported}")
    print(f"  Press release stats: {stats}")


if __name__ == "__main__":
    main()
