#!/usr/bin/env python3
"""
Initialize the Investment Signal Radar SQLite database.
Creates all tables and indexes idempotently.

Usage:
    python3 src/db/init_db.py
"""

import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent.parent.parent / "data" / "investment_signal.db"

SCHEMA_SQL = """
-- Data sources (RSS feeds, scrape targets, APIs)
CREATE TABLE IF NOT EXISTS sources (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    name            TEXT    NOT NULL,
    type            TEXT    NOT NULL CHECK(type IN ('rss', 'scrape', 'api')),
    url             TEXT    NOT NULL UNIQUE,
    last_fetched_at TEXT,
    is_active       INTEGER NOT NULL DEFAULT 1
);

-- Sectors / industry classification
CREATE TABLE IF NOT EXISTS sectors (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    name            TEXT    NOT NULL UNIQUE,
    parent_id       INTEGER REFERENCES sectors(id),
    description     TEXT
);

-- Portfolio companies (investment targets)
CREATE TABLE IF NOT EXISTS companies (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    canonical_name  TEXT    NOT NULL,
    aliases         TEXT    DEFAULT '[]',
    website_url     TEXT,
    founded_year    INTEGER,
    description     TEXT,
    sector_id       INTEGER REFERENCES sectors(id),
    pestle_category TEXT,
    country         TEXT    DEFAULT 'JP',
    needs_review    INTEGER NOT NULL DEFAULT 0,
    created_at      TEXT    NOT NULL DEFAULT (datetime('now')),
    updated_at      TEXT    NOT NULL DEFAULT (datetime('now'))
);

-- Investors (VCs, CVCs, angels, etc.)
CREATE TABLE IF NOT EXISTS investors (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    canonical_name  TEXT    NOT NULL,
    aliases         TEXT    DEFAULT '[]',
    type            TEXT    CHECK(type IN ('vc', 'cvc', 'angel', 'gov', 'bank', 'corporate', 'other')),
    website_url     TEXT,
    country         TEXT    DEFAULT 'JP',
    is_active       INTEGER NOT NULL DEFAULT 1,
    created_at      TEXT    NOT NULL DEFAULT (datetime('now'))
);

-- Investment events (central table)
CREATE TABLE IF NOT EXISTS investments (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    company_id      INTEGER REFERENCES companies(id),
    source_id       INTEGER REFERENCES sources(id),
    announced_date  TEXT,
    amount_jpy      INTEGER,
    amount_raw      TEXT,
    currency        TEXT    DEFAULT 'JPY',
    round_type      TEXT    CHECK(round_type IN (
        'pre-seed', 'seed', 'pre-a', 'a', 'b', 'c', 'd', 'e', 'f', 'g',
        'strategic', 'debt', 'grant', 'ipo', 'angel', 'unknown'
    )),
    confidence      TEXT    CHECK(confidence IN ('high', 'medium', 'low')) DEFAULT 'medium',
    source_url      TEXT    NOT NULL,
    source_title    TEXT,
    url_hash        TEXT    NOT NULL UNIQUE,
    pestle_category TEXT,
    notes           TEXT,
    is_duplicate    INTEGER NOT NULL DEFAULT 0,
    extracted_at    TEXT    NOT NULL DEFAULT (datetime('now')),
    created_at      TEXT    NOT NULL DEFAULT (datetime('now'))
);

-- Many-to-many: investments <-> investors
CREATE TABLE IF NOT EXISTS investment_investors (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    investment_id   INTEGER NOT NULL REFERENCES investments(id) ON DELETE CASCADE,
    investor_id     INTEGER NOT NULL REFERENCES investors(id),
    is_lead         INTEGER NOT NULL DEFAULT 0,
    UNIQUE(investment_id, investor_id)
);

-- Detected signals (Phase 3)
CREATE TABLE IF NOT EXISTS signals (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    signal_type             TEXT    NOT NULL CHECK(signal_type IN (
        'investment_surge', 'new_sector', 'co_investment_cluster', 'cross_radar'
    )),
    sector_id               INTEGER REFERENCES sectors(id),
    detected_at             TEXT    NOT NULL DEFAULT (datetime('now')),
    period_start            TEXT,
    period_end              TEXT,
    baseline_count          INTEGER,
    current_count           INTEGER,
    acceleration_ratio      REAL,
    description             TEXT,
    related_investment_ids  TEXT    DEFAULT '[]',
    is_reported             INTEGER NOT NULL DEFAULT 0
);

-- Indexes for fast queries
CREATE INDEX IF NOT EXISTS idx_investments_date
    ON investments(announced_date);
CREATE INDEX IF NOT EXISTS idx_investments_company
    ON investments(company_id);
CREATE INDEX IF NOT EXISTS idx_investments_round
    ON investments(round_type);
CREATE INDEX IF NOT EXISTS idx_investments_pestle
    ON investments(pestle_category);
CREATE INDEX IF NOT EXISTS idx_investments_url_hash
    ON investments(url_hash);
CREATE INDEX IF NOT EXISTS idx_companies_name
    ON companies(canonical_name);
CREATE INDEX IF NOT EXISTS idx_companies_sector
    ON companies(sector_id);
CREATE INDEX IF NOT EXISTS idx_investors_name
    ON investors(canonical_name);
CREATE INDEX IF NOT EXISTS idx_investment_investors_inv
    ON investment_investors(investment_id);
CREATE INDEX IF NOT EXISTS idx_signals_type
    ON signals(signal_type);
CREATE INDEX IF NOT EXISTS idx_signals_sector
    ON signals(sector_id);

-- Seed default RSS sources
INSERT OR IGNORE INTO sources (name, type, url) VALUES
    ('PR TIMES', 'rss', 'https://prtimes.jp/index.rdf'),
    ('The Bridge', 'rss', 'https://thebridge.jp/feed');
"""


def init_db():
    """Create database and all tables/indexes."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.executescript(SCHEMA_SQL)
    conn.close()
    print(f"Database initialized: {DB_PATH}")
    return DB_PATH


if __name__ == "__main__":
    init_db()
