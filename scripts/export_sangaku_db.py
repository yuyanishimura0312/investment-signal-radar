#!/usr/bin/env python3
"""Export prtimes_sangaku data as a standalone academia-industry DB.

Separated from VC funding DB to maintain clean signal-to-noise ratio.
This DB contains industry-academia collaboration press releases
originally collected by sangaku-matcher-v2.

Usage:
    python3 scripts/export_sangaku_db.py
"""
from __future__ import annotations

import os
import sqlite3
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_DB = PROJECT_ROOT / "data" / "investment_signal_v2.db"
DEFAULT_OUTPUT = PROJECT_ROOT / "data" / "sangaku_press_releases.db"

SCHEMA = """
CREATE TABLE IF NOT EXISTS press_releases (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    source_url TEXT UNIQUE,
    published_at TEXT,
    company_name TEXT,
    category TEXT,
    is_funding_related INTEGER DEFAULT 0,
    collected_at TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS metadata (
    key TEXT PRIMARY KEY,
    value TEXT
);

CREATE INDEX IF NOT EXISTS idx_pr_published ON press_releases(published_at);
CREATE INDEX IF NOT EXISTS idx_pr_category ON press_releases(category);
"""


def export(src_path: Path, dst_path: Path) -> None:
    src = sqlite3.connect(src_path)
    src.row_factory = sqlite3.Row

    if dst_path.exists():
        dst_path.unlink()

    dst = sqlite3.connect(dst_path)
    dst.executescript(SCHEMA)

    rows = src.execute("""
        SELECT title, source_url, published_at, company_name,
               category, is_funding_related, collected_at
        FROM press_releases
        WHERE source = 'prtimes_sangaku'
        ORDER BY published_at DESC
    """).fetchall()

    for r in rows:
        dst.execute("""
            INSERT OR IGNORE INTO press_releases
                (title, source_url, published_at, company_name,
                 category, is_funding_related, collected_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (r["title"], r["source_url"], r["published_at"],
              r["company_name"], r["category"], r["is_funding_related"],
              r["collected_at"]))

    dst.execute("INSERT OR REPLACE INTO metadata VALUES (?, ?)",
                ("exported_at", datetime.now().isoformat()))
    dst.execute("INSERT OR REPLACE INTO metadata VALUES (?, ?)",
                ("source", "prtimes_sangaku from investment_signal_v2.db"))

    dst.commit()
    count = dst.execute("SELECT COUNT(*) FROM press_releases").fetchone()[0]
    dst.close()
    src.close()

    size_mb = os.path.getsize(dst_path) / 1024 / 1024
    print(f"Exported to {dst_path} ({size_mb:.1f} MB)")
    print(f"  Press releases: {count}")


if __name__ == "__main__":
    export(SRC_DB, DEFAULT_OUTPUT)
