#!/usr/bin/env python3
"""Export a standalone funding database from Investment Signal Radar v2.

Creates a self-contained SQLite database focused on VC/funding data,
modeled after the successful sangaku-matcher-v2/export_prtimes_db.py pattern.

Output schema:
  - funding_releases: Press releases classified as funding-related
  - companies: Unique companies mentioned in funding releases
  - investors: VCs and investors extracted from press releases
  - rounds: Structured funding round data (when extractable)
  - monthly_stats: Aggregated monthly funding statistics

Usage:
    python3 scripts/export_funding_db.py
    python3 scripts/export_funding_db.py --output ~/data/funding.db
    python3 scripts/export_funding_db.py --all  # Include non-funding PRs too
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_DB = PROJECT_ROOT / "data" / "investment_signal_v2.db"
DEFAULT_OUTPUT = PROJECT_ROOT / "data" / "funding_database.db"

SCHEMA = """
-- Core: Funding-related press releases
CREATE TABLE IF NOT EXISTS funding_releases (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    body_text TEXT,
    source TEXT NOT NULL,
    source_url TEXT UNIQUE,
    published_at TEXT,
    company_name TEXT,
    category TEXT,
    is_funding_related INTEGER DEFAULT 0,
    amount_raw TEXT,        -- e.g. "5億円", "20億円"
    amount_jpy INTEGER,     -- Normalized to JPY (億→100000000)
    round_type TEXT,        -- seed, pre_series_a, series_a, etc.
    extracted_data TEXT,     -- JSON with structured details
    confidence_score REAL,
    search_keyword TEXT,
    collected_at TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Aggregated company data
CREATE TABLE IF NOT EXISTS companies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL,
    pr_count INTEGER DEFAULT 0,
    funding_count INTEGER DEFAULT 0,
    total_raised_text TEXT,  -- Aggregate of all known amounts
    first_seen TEXT,
    last_seen TEXT,
    sectors TEXT             -- JSON array
);

-- Monthly aggregation for trends
CREATE TABLE IF NOT EXISTS monthly_stats (
    month TEXT PRIMARY KEY,  -- YYYY-MM
    total_releases INTEGER DEFAULT 0,
    funding_releases INTEGER DEFAULT 0,
    exit_releases INTEGER DEFAULT 0,
    partnership_releases INTEGER DEFAULT 0,
    accelerator_releases INTEGER DEFAULT 0
);

-- Metadata
CREATE TABLE IF NOT EXISTS metadata (
    key TEXT PRIMARY KEY,
    value TEXT
);

CREATE INDEX IF NOT EXISTS idx_fr_published ON funding_releases(published_at);
CREATE INDEX IF NOT EXISTS idx_fr_category ON funding_releases(category);
CREATE INDEX IF NOT EXISTS idx_fr_company ON funding_releases(company_name);
CREATE INDEX IF NOT EXISTS idx_fr_funding ON funding_releases(is_funding_related);
CREATE INDEX IF NOT EXISTS idx_fr_round ON funding_releases(round_type);
"""

# Round type detection
ROUND_PATTERNS = [
    (r"プレシード|プレシード|pre.?seed", "pre_seed"),
    (r"シード|シード|seed", "seed"),
    (r"プレシリーズ\s*[AＡ]|pre.?series\s*a", "pre_series_a"),
    (r"シリーズ\s*[AＡ]|series\s*a", "series_a"),
    (r"シリーズ\s*[BＢ]|series\s*b", "series_b"),
    (r"シリーズ\s*[CＣ]|series\s*c", "series_c"),
    (r"シリーズ\s*[DＤ]|series\s*d", "series_d"),
    (r"シリーズ\s*[EＥ]|series\s*e", "series_e"),
    (r"シリーズ\s*[FＦ]|series\s*f", "series_f"),
    (r"第三者割当増資", "third_party_allotment"),
    (r"IPO|ＩＰＯ|新規上場|株式公開", "ipo"),
    (r"M&A|Ｍ＆Ａ|買収|事業譲渡", "ma"),
]


def detect_round_type(text: str) -> str | None:
    """Detect funding round type from text."""
    text_lower = text.lower()
    for pattern, round_type in ROUND_PATTERNS:
        if re.search(pattern, text, re.IGNORECASE):
            return round_type
    return None


def parse_amount_jpy(amount_raw: str | None) -> int | None:
    """Parse amount string to JPY integer."""
    if not amount_raw:
        return None
    # "X.X億円" -> int
    m = re.search(r"([\d,.]+)\s*億", amount_raw)
    if m:
        val = float(m.group(1).replace(",", ""))
        return int(val * 100_000_000)
    # "XXX万円" -> int
    m = re.search(r"([\d,.]+)\s*万", amount_raw)
    if m:
        val = float(m.group(1).replace(",", ""))
        return int(val * 10_000)
    return None


def extract_amount(title: str, body: str = "") -> str | None:
    """Extract amount from text."""
    text = f"{title} {body}"
    # Japanese yen amounts
    m = re.search(r"([\d,.]+\s*億円)", text)
    if m:
        return m.group(1)
    m = re.search(r"([\d,.]+\s*万円)", text)
    if m:
        return m.group(1)
    # Dollar amounts
    m = re.search(r"([\d,.]+\s*億ドル)", text)
    if m:
        return m.group(1)
    m = re.search(r"(\$[\d,.]+\s*[MB])", text)
    if m:
        return m.group(1)
    m = re.search(r"([\d,.]+\s*million\s*(?:USD|dollars?))", text, re.IGNORECASE)
    if m:
        return m.group(1)
    return None


def export(src_path: Path, dst_path: Path, include_all: bool = False) -> None:
    """Export funding database."""
    src = sqlite3.connect(src_path)
    src.row_factory = sqlite3.Row

    if dst_path.exists():
        dst_path.unlink()

    dst = sqlite3.connect(dst_path)
    dst.executescript(SCHEMA)

    # Export press releases (filter out pre-2021 anomalies and sangaku data)
    date_filter = "AND (published_at IS NULL OR published_at >= '2021-01-01')"
    # Exclude prtimes_sangaku (academia/industry data) from VC funding DB
    source_filter = "AND source != 'prtimes_sangaku'"
    if include_all:
        rows = src.execute(f"""
            SELECT * FROM press_releases
            WHERE 1=1 {date_filter} {source_filter}
            ORDER BY published_at DESC
        """).fetchall()
    else:
        rows = src.execute(f"""
            SELECT * FROM press_releases
            WHERE (is_funding_related = 1
               OR category IN ('funding', 'exit', 'accelerator', 'partnership'))
               {date_filter} {source_filter}
            ORDER BY published_at DESC
        """).fetchall()

    pr_count = 0
    company_data: dict[str, dict] = {}
    monthly_data: dict[str, dict] = {}

    for r in rows:
        title = r["title"] or ""
        body = r["body_text"] or ""

        # Extract/enhance data
        amount_raw = extract_amount(title, body)
        extracted = {}
        if r["extracted_data"]:
            try:
                extracted = json.loads(r["extracted_data"])
            except (json.JSONDecodeError, TypeError):
                pass

        if not amount_raw and extracted.get("amount_raw"):
            amount_raw = extracted["amount_raw"]

        amount_jpy = parse_amount_jpy(amount_raw)
        round_type = detect_round_type(f"{title} {body}")
        search_kw = extracted.get("search_keyword", "")

        dst.execute("""
            INSERT OR IGNORE INTO funding_releases
                (title, body_text, source, source_url, published_at,
                 company_name, category, is_funding_related,
                 amount_raw, amount_jpy, round_type,
                 extracted_data, confidence_score, search_keyword, collected_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            title, body, r["source"], r["source_url"], r["published_at"],
            r["company_name"], r["category"], r["is_funding_related"],
            amount_raw, amount_jpy, round_type,
            r["extracted_data"], r["confidence_score"], search_kw, r["collected_at"],
        ))
        pr_count += 1

        # Aggregate company data
        company = r["company_name"] or ""
        if company:
            if company not in company_data:
                company_data[company] = {
                    "pr_count": 0, "funding_count": 0,
                    "first_seen": r["published_at"], "last_seen": r["published_at"],
                }
            cd = company_data[company]
            cd["pr_count"] += 1
            if r["is_funding_related"]:
                cd["funding_count"] += 1
            if r["published_at"]:
                if not cd["first_seen"] or r["published_at"] < cd["first_seen"]:
                    cd["first_seen"] = r["published_at"]
                if not cd["last_seen"] or r["published_at"] > cd["last_seen"]:
                    cd["last_seen"] = r["published_at"]

        # Monthly aggregation
        if r["published_at"] and len(r["published_at"]) >= 7:
            month = r["published_at"][:7]
            if month not in monthly_data:
                monthly_data[month] = {
                    "total": 0, "funding": 0, "exit": 0,
                    "partnership": 0, "accelerator": 0,
                }
            md = monthly_data[month]
            md["total"] += 1
            cat = r["category"] or "other"
            if cat in md:
                md[cat] += 1
            # Also count is_funding_related=1 records in funding
            # (covers exit category items marked as funding-related)
            if r["is_funding_related"] and cat != "funding":
                md["funding"] += 1

    # Insert companies
    for name, data in company_data.items():
        dst.execute("""
            INSERT OR REPLACE INTO companies
                (name, pr_count, funding_count, first_seen, last_seen)
            VALUES (?, ?, ?, ?, ?)
        """, (name, data["pr_count"], data["funding_count"],
              data["first_seen"], data["last_seen"]))

    # Insert monthly stats
    for month, data in sorted(monthly_data.items()):
        dst.execute("""
            INSERT OR REPLACE INTO monthly_stats
                (month, total_releases, funding_releases, exit_releases,
                 partnership_releases, accelerator_releases)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (month, data["total"], data["funding"], data["exit"],
              data["partnership"], data["accelerator"]))

    # Metadata
    dst.execute("INSERT OR REPLACE INTO metadata VALUES (?, ?)",
                ("exported_at", datetime.now().isoformat()))
    dst.execute("INSERT OR REPLACE INTO metadata VALUES (?, ?)",
                ("source_db", str(src_path)))
    dst.execute("INSERT OR REPLACE INTO metadata VALUES (?, ?)",
                ("include_all", str(include_all)))

    dst.commit()
    dst.close()
    src.close()

    size_mb = os.path.getsize(dst_path) / 1024 / 1024
    print(f"Exported to {dst_path} ({size_mb:.1f} MB)")
    print(f"  Press releases: {pr_count}")
    print(f"  Companies: {len(company_data)}")
    print(f"  Monthly stats: {len(monthly_data)} months")
    print(f"  Include all: {include_all}")


def main():
    parser = argparse.ArgumentParser(description="Export funding database")
    parser.add_argument("--source", default=str(SRC_DB))
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--all", action="store_true",
                       help="Include all PRs, not just funding-related")
    args = parser.parse_args()
    export(Path(args.source), Path(args.output), include_all=args.all)


if __name__ == "__main__":
    main()
