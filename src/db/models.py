"""
Database helper functions for Investment Signal Radar.
Provides CRUD operations for all tables.
"""

import sqlite3
import hashlib
import json
from pathlib import Path
from datetime import datetime
from typing import Optional

DB_PATH = Path(__file__).parent.parent.parent / "data" / "investment_signal.db"


def get_conn() -> sqlite3.Connection:
    """Get a database connection with row_factory."""
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def url_hash(url: str) -> str:
    """Generate a hash for deduplication."""
    return hashlib.sha256(url.encode()).hexdigest()[:16]


def source_exists(conn: sqlite3.Connection, source_url: str) -> bool:
    """Check if a source URL has already been processed."""
    h = url_hash(source_url)
    row = conn.execute(
        "SELECT 1 FROM investments WHERE url_hash = ?", (h,)
    ).fetchone()
    return row is not None


def find_or_create_company(
    conn: sqlite3.Connection,
    name: str,
    description: str = "",
    sector_name: str = "",
    country: str = "JP",
) -> int:
    """Find existing company by name or create new one."""
    # Exact match
    row = conn.execute(
        "SELECT id FROM companies WHERE canonical_name = ?", (name,)
    ).fetchone()
    if row:
        return row["id"]

    # Check aliases
    rows = conn.execute("SELECT id, aliases FROM companies").fetchall()
    for r in rows:
        aliases = json.loads(r["aliases"] or "[]")
        if name in aliases:
            return r["id"]

    # Create new
    sector_id = None
    if sector_name:
        sector_id = find_or_create_sector(conn, sector_name)

    cur = conn.execute(
        """INSERT INTO companies (canonical_name, description, sector_id, country)
           VALUES (?, ?, ?, ?)""",
        (name, description, sector_id, country),
    )
    return cur.lastrowid


def find_or_create_investor(
    conn: sqlite3.Connection,
    name: str,
    investor_type: str = "vc",
    country: str = "JP",
) -> int:
    """Find existing investor by name or create new one."""
    row = conn.execute(
        "SELECT id FROM investors WHERE canonical_name = ?", (name,)
    ).fetchone()
    if row:
        return row["id"]

    # Check aliases
    rows = conn.execute("SELECT id, aliases FROM investors").fetchall()
    for r in rows:
        aliases = json.loads(r["aliases"] or "[]")
        if name in aliases:
            return r["id"]

    cur = conn.execute(
        """INSERT INTO investors (canonical_name, type, country)
           VALUES (?, ?, ?)""",
        (name, investor_type, country),
    )
    return cur.lastrowid


def find_or_create_sector(conn: sqlite3.Connection, name: str) -> int:
    """Find existing sector or create new one."""
    row = conn.execute(
        "SELECT id FROM sectors WHERE name = ?", (name,)
    ).fetchone()
    if row:
        return row["id"]
    cur = conn.execute("INSERT INTO sectors (name) VALUES (?)", (name,))
    return cur.lastrowid


def insert_investment(
    conn: sqlite3.Connection,
    company_name: str,
    investors: list[dict],
    amount_jpy: Optional[int],
    amount_raw: str,
    round_type: str,
    announced_date: str,
    source_url: str,
    source_title: str,
    sector: str = "",
    pestle_category: str = "",
    confidence: str = "medium",
    description: str = "",
    source_id: int = 1,
) -> Optional[int]:
    """
    Insert a complete investment record with company and investors.
    Returns investment_id or None if duplicate.
    """
    h = url_hash(source_url)

    # Skip if duplicate
    if source_exists(conn, source_url):
        return None

    # Find or create company
    company_id = find_or_create_company(
        conn, company_name, description=description,
        sector_name=sector, country="JP"
    )

    # Validate round_type
    valid_rounds = {
        'pre-seed', 'seed', 'pre-a', 'a', 'b', 'c', 'd', 'e', 'f', 'g',
        'strategic', 'debt', 'grant', 'ipo', 'angel', 'unknown'
    }
    if round_type not in valid_rounds:
        round_type = 'unknown'

    # Validate confidence
    if confidence not in ('high', 'medium', 'low'):
        confidence = 'medium'

    # Insert investment
    cur = conn.execute(
        """INSERT INTO investments
           (company_id, source_id, announced_date, amount_jpy, amount_raw,
            currency, round_type, confidence, source_url, source_title,
            url_hash, pestle_category)
           VALUES (?, ?, ?, ?, ?, 'JPY', ?, ?, ?, ?, ?, ?)""",
        (company_id, source_id, announced_date, amount_jpy, amount_raw,
         round_type, confidence, source_url, source_title, h, pestle_category),
    )
    investment_id = cur.lastrowid

    # Link investors
    for inv in investors:
        inv_name = inv.get("name", "").strip()
        if not inv_name:
            continue
        investor_id = find_or_create_investor(
            conn, inv_name,
            investor_type=inv.get("type", "vc"),
            country=inv.get("country", "JP"),
        )
        is_lead = 1 if inv.get("is_lead", False) else 0
        conn.execute(
            """INSERT OR IGNORE INTO investment_investors
               (investment_id, investor_id, is_lead)
               VALUES (?, ?, ?)""",
            (investment_id, investor_id, is_lead),
        )

    return investment_id


def get_stats(conn: sqlite3.Connection) -> dict:
    """Get database statistics."""
    stats = {}
    for table in ["investments", "companies", "investors", "sectors", "signals"]:
        row = conn.execute(f"SELECT COUNT(*) as c FROM {table}").fetchone()
        stats[table] = row["c"]
    return stats
