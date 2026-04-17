#!/usr/bin/env python3
"""
Initialize the Investment Signal Radar SQLite database v2.0.

New schema based on best practice research (see /tmp/best-practice-report.md):
  - Organization-centric entity design (Crunchbase-style)
  - Event-driven data model (Harmonic.ai-style)
  - Statement-style data quality management (OpenCorporates-style)
  - Faceted 3-layer classification (Dealroom-style)
  - Component-separated scoring (CB Insights Mosaic-style)

Usage:
    python3 src/db/init_db_v2.py
    python3 src/db/init_db_v2.py --path data/investment_signal_v2.db
"""

import argparse
import sqlite3
from pathlib import Path

DEFAULT_DB_PATH = Path(__file__).parent.parent.parent / "data" / "investment_signal_v2.db"


SCHEMA_SQL = """
-- ============================================================
-- Investment Signal Radar v2.0 Schema
-- ============================================================
-- Design principles:
--   1. Organization-centric (Crunchbase)
--   2. Event-driven (Harmonic.ai)
--   3. Statement-style quality management (OpenCorporates)
--   4. Faceted 3-layer classification (Dealroom)
--   5. Component-separated scoring (CB Insights Mosaic)
-- ============================================================

PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

-- ------------------------------------------------------------
-- 1. Data Sources (provenance master)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS data_sources (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    name                    TEXT    UNIQUE NOT NULL,
    source_type             TEXT    NOT NULL
        CHECK (source_type IN ('official', 'commercial', 'news', 'social', 'manual', 'ml_inferred')),
    base_confidence         REAL    DEFAULT 0.5,
    default_ttl_soft_days   INTEGER DEFAULT 30,
    default_ttl_hard_days   INTEGER DEFAULT 90,
    api_endpoint            TEXT,
    notes                   TEXT
);

-- ------------------------------------------------------------
-- 2. Organizations (central entity: companies + investors + etc.)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS organizations (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    slug                TEXT    UNIQUE NOT NULL,
    name                TEXT    NOT NULL,
    name_en             TEXT,
    name_local          TEXT,
    aliases             TEXT    DEFAULT '[]',
    primary_role        TEXT    NOT NULL DEFAULT 'company'
        CHECK (primary_role IN ('company', 'investor', 'acquirer', 'accelerator', 'government')),
    status              TEXT    DEFAULT 'active'
        CHECK (status IN ('active', 'closed', 'acquired', 'ipo', 'unknown')),
    -- Additional role flags (Crunchbase-style multi-role)
    is_company          INTEGER DEFAULT 0,
    is_investor         INTEGER DEFAULT 0,
    -- Descriptive fields
    description         TEXT,
    founded_date        TEXT,
    country_code        TEXT    DEFAULT 'JP',
    region              TEXT,
    city                TEXT,
    website             TEXT,
    -- Japanese-specific identifier
    corporate_number    TEXT,
    -- Global identifiers
    crunchbase_uuid     TEXT,
    pitchbook_id        TEXT,
    sec_cik             TEXT,
    -- Investor-specific metadata
    investor_type       TEXT
        CHECK (investor_type IS NULL OR investor_type IN (
            'vc', 'cvc', 'angel', 'gov', 'bank', 'corporate', 'accelerator', 'other'
        )),
    -- Extension for future fields without schema migration
    extra_data          TEXT    DEFAULT '{}',
    -- Data provenance
    data_source_id      INTEGER REFERENCES data_sources(id),
    confidence_score    REAL    DEFAULT 0.5 CHECK (confidence_score BETWEEN 0.0 AND 1.0),
    collected_at        TEXT    NOT NULL DEFAULT (datetime('now')),
    updated_at          TEXT    NOT NULL DEFAULT (datetime('now')),
    ttl_soft_days       INTEGER DEFAULT 30,
    ttl_hard_days       INTEGER DEFAULT 90,
    created_at          TEXT    NOT NULL DEFAULT (datetime('now'))
);

-- ------------------------------------------------------------
-- 3. Funding Rounds
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS funding_rounds (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    organization_id         INTEGER NOT NULL REFERENCES organizations(id),
    round_type              TEXT    NOT NULL
        CHECK (round_type IN (
            'pre_seed', 'seed', 'series_a', 'series_b', 'series_c',
            'series_d', 'series_e', 'series_f', 'series_g', 'late_stage',
            'grant', 'debt', 'convertible_note', 'j_kiss',
            'corporate_round', 'strategic', 'ipo', 'secondary',
            'angel', 'unknown'
        )),
    announced_date          TEXT,
    closed_date             TEXT,
    amount_jpy              INTEGER,
    amount_usd              INTEGER,
    amount_raw              TEXT,
    currency                TEXT    DEFAULT 'JPY',
    pre_valuation_jpy       INTEGER,
    pre_valuation_usd       INTEGER,
    post_valuation_jpy      INTEGER,
    post_valuation_usd      INTEGER,
    investor_count          INTEGER,
    -- Data provenance
    data_source_id          INTEGER REFERENCES data_sources(id),
    confidence_score        REAL    DEFAULT 0.5
        CHECK (confidence_score IS NULL OR (confidence_score BETWEEN 0.0 AND 1.0)),
    source_url              TEXT,
    source_title            TEXT,
    url_hash                TEXT    UNIQUE,
    notes                   TEXT,
    is_duplicate            INTEGER DEFAULT 0,
    pestle_category         TEXT,
    collected_at            TEXT    NOT NULL DEFAULT (datetime('now')),
    created_at              TEXT    NOT NULL DEFAULT (datetime('now'))
);

-- ------------------------------------------------------------
-- 4. Round Participants (investors <-> funding_rounds, M:N)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS round_participants (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    funding_round_id        INTEGER NOT NULL REFERENCES funding_rounds(id) ON DELETE CASCADE,
    investor_id             INTEGER NOT NULL REFERENCES organizations(id),
    is_lead                 INTEGER DEFAULT 0,
    investment_amount_jpy   INTEGER,
    investment_amount_usd   INTEGER,
    UNIQUE(funding_round_id, investor_id)
);

-- ------------------------------------------------------------
-- 5. Events (central event log for signal detection)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS events (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    organization_id     INTEGER NOT NULL REFERENCES organizations(id),
    event_type          TEXT    NOT NULL
        CHECK (event_type IN (
            'funding', 'hiring', 'patent', 'grant', 'accelerator',
            'partnership', 'product_launch', 'media_mention',
            'domain_registration', 'incorporation', 'acquisition',
            'executive_change', 'office_expansion', 'regulatory',
            'award', 'ipo_filing', 'pivot', 'layoff', 'shutdown', 'other'
        )),
    event_date          TEXT    NOT NULL,
    title               TEXT,
    description         TEXT,
    event_data          TEXT    DEFAULT '{}',
    significance_score  REAL    DEFAULT 0.5 CHECK (significance_score BETWEEN 0.0 AND 1.0),
    -- Data provenance
    data_source_id      INTEGER REFERENCES data_sources(id),
    confidence_score    REAL    DEFAULT 0.5
        CHECK (confidence_score IS NULL OR (confidence_score BETWEEN 0.0 AND 1.0)),
    source_url          TEXT,
    collected_at        TEXT    NOT NULL DEFAULT (datetime('now')),
    created_at          TEXT    NOT NULL DEFAULT (datetime('now'))
);

-- ------------------------------------------------------------
-- 6. Sectors (30-50 fixed top-level industries)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sectors (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    name                TEXT    UNIQUE NOT NULL,
    name_ja             TEXT,
    parent_id           INTEGER REFERENCES sectors(id),
    description         TEXT,
    sort_order          INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS organization_sectors (
    organization_id     INTEGER NOT NULL REFERENCES organizations(id),
    sector_id           INTEGER NOT NULL REFERENCES sectors(id),
    is_primary          INTEGER DEFAULT 0,
    PRIMARY KEY (organization_id, sector_id)
);

-- ------------------------------------------------------------
-- 7. Tags (dynamic facets: technology, business_model, market)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS tags (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    tag_category        TEXT    NOT NULL
        CHECK (tag_category IN ('technology', 'business_model', 'market')),
    name                TEXT    NOT NULL,
    name_ja             TEXT,
    UNIQUE(tag_category, name)
);

CREATE TABLE IF NOT EXISTS organization_tags (
    organization_id     INTEGER NOT NULL REFERENCES organizations(id),
    tag_id              INTEGER NOT NULL REFERENCES tags(id),
    confidence_score    REAL    DEFAULT 1.0
        CHECK (confidence_score IS NULL OR (confidence_score BETWEEN 0.0 AND 1.0)),
    assigned_by         TEXT    DEFAULT 'manual'
        CHECK (assigned_by IN ('manual', 'ml', 'rule', 'import')),
    PRIMARY KEY (organization_id, tag_id)
);

CREATE TABLE IF NOT EXISTS tag_synonyms (
    synonym             TEXT    NOT NULL PRIMARY KEY,
    canonical_tag_id    INTEGER NOT NULL REFERENCES tags(id)
);

-- ------------------------------------------------------------
-- 8. Signal Scores (component-separated; composite calc in app layer)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS signal_scores (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    organization_id     INTEGER NOT NULL REFERENCES organizations(id),
    score_type          TEXT    NOT NULL
        CHECK (score_type IN (
            'momentum', 'funding', 'market', 'team',
            'technology', 'network', 'composite'
        )),
    score_value         REAL    NOT NULL CHECK (score_value BETWEEN 0.0 AND 1.0),
    model_version       TEXT    NOT NULL,
    components          TEXT    DEFAULT '{}',
    calculated_at       TEXT    NOT NULL DEFAULT (datetime('now')),
    UNIQUE(organization_id, score_type, model_version, calculated_at)
);

CREATE TABLE IF NOT EXISTS score_models (
    version             TEXT    PRIMARY KEY,
    description         TEXT,
    weights             TEXT    NOT NULL DEFAULT '{}',
    is_active           INTEGER DEFAULT 0,
    created_at          TEXT    NOT NULL DEFAULT (datetime('now'))
);

-- ------------------------------------------------------------
-- 9. People
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS people (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    name            TEXT    NOT NULL,
    name_en         TEXT,
    name_local      TEXT,
    linkedin_url    TEXT,
    extra_data      TEXT    DEFAULT '{}',
    created_at      TEXT    NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS organization_people (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    organization_id     INTEGER NOT NULL REFERENCES organizations(id),
    person_id           INTEGER NOT NULL REFERENCES people(id),
    role                TEXT,
    is_current          INTEGER DEFAULT 1,
    start_date          TEXT,
    end_date            TEXT,
    UNIQUE(organization_id, person_id, role)
);

-- ------------------------------------------------------------
-- 10. Network Metrics Cache (co-investment centrality, etc.)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS network_metrics (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    organization_id     INTEGER NOT NULL REFERENCES organizations(id),
    metric_type         TEXT    NOT NULL
        CHECK (metric_type IN (
            'degree_centrality', 'eigenvector_centrality',
            'betweenness_centrality', 'co_investment_count',
            'unique_co_investors', 'community_id'
        )),
    metric_value        REAL    NOT NULL,
    calculated_at       TEXT    NOT NULL DEFAULT (datetime('now')),
    UNIQUE(organization_id, metric_type, calculated_at)
);

-- ------------------------------------------------------------
-- 11. Trend Snapshots (weak signal analysis)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS trend_snapshots (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_date       TEXT    NOT NULL,
    dimension_type      TEXT    NOT NULL
        CHECK (dimension_type IN ('sector', 'tag', 'round_type', 'country')),
    dimension_value     TEXT    NOT NULL,
    deal_count          INTEGER DEFAULT 0,
    total_amount_jpy    INTEGER DEFAULT 0,
    total_amount_usd    INTEGER DEFAULT 0,
    avg_deal_size_usd   INTEGER,
    velocity            REAL,
    acceleration        REAL,
    created_at          TEXT    NOT NULL DEFAULT (datetime('now')),
    UNIQUE(snapshot_date, dimension_type, dimension_value)
);

-- ------------------------------------------------------------
-- 12. Signals (detected foresight signals - legacy compatible)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS signals (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    signal_type             TEXT    NOT NULL
        CHECK (signal_type IN (
            'investment_surge', 'new_sector', 'co_investment_cluster',
            'cross_radar', 'event_surge', 'network_shift'
        )),
    sector_id               INTEGER REFERENCES sectors(id),
    tag_id                  INTEGER REFERENCES tags(id),
    detected_at             TEXT    NOT NULL DEFAULT (datetime('now')),
    period_start            TEXT,
    period_end              TEXT,
    baseline_count          INTEGER,
    current_count           INTEGER,
    acceleration_ratio      REAL,
    description             TEXT,
    related_round_ids       TEXT    DEFAULT '[]',
    is_reported             INTEGER NOT NULL DEFAULT 0
);

-- ------------------------------------------------------------
-- 13. Watchlists
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS watchlists (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    name            TEXT    NOT NULL,
    description     TEXT,
    created_at      TEXT    NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS watchlist_items (
    watchlist_id        INTEGER NOT NULL REFERENCES watchlists(id),
    organization_id     INTEGER NOT NULL REFERENCES organizations(id),
    added_at            TEXT    NOT NULL DEFAULT (datetime('now')),
    notes               TEXT,
    PRIMARY KEY (watchlist_id, organization_id)
);

-- ============================================================
-- INDEXES
-- ============================================================

-- Organizations
CREATE INDEX IF NOT EXISTS idx_org_slug ON organizations(slug);
CREATE INDEX IF NOT EXISTS idx_org_role ON organizations(primary_role);
CREATE INDEX IF NOT EXISTS idx_org_country ON organizations(country_code);
CREATE INDEX IF NOT EXISTS idx_org_status ON organizations(status);
CREATE INDEX IF NOT EXISTS idx_org_corporate_number
    ON organizations(corporate_number) WHERE corporate_number IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_org_updated ON organizations(updated_at);
CREATE INDEX IF NOT EXISTS idx_org_name ON organizations(name);
CREATE INDEX IF NOT EXISTS idx_org_is_company ON organizations(is_company) WHERE is_company = 1;
CREATE INDEX IF NOT EXISTS idx_org_is_investor ON organizations(is_investor) WHERE is_investor = 1;

-- Funding Rounds
CREATE INDEX IF NOT EXISTS idx_fr_org ON funding_rounds(organization_id);
CREATE INDEX IF NOT EXISTS idx_fr_date ON funding_rounds(announced_date);
CREATE INDEX IF NOT EXISTS idx_fr_type ON funding_rounds(round_type);
CREATE INDEX IF NOT EXISTS idx_fr_amount ON funding_rounds(amount_usd);
CREATE INDEX IF NOT EXISTS idx_fr_url_hash ON funding_rounds(url_hash);

-- Round Participants
CREATE INDEX IF NOT EXISTS idx_rp_investor ON round_participants(investor_id);
CREATE INDEX IF NOT EXISTS idx_rp_round ON round_participants(funding_round_id);

-- Events
CREATE INDEX IF NOT EXISTS idx_ev_org ON events(organization_id);
CREATE INDEX IF NOT EXISTS idx_ev_type ON events(event_type);
CREATE INDEX IF NOT EXISTS idx_ev_date ON events(event_date);
CREATE INDEX IF NOT EXISTS idx_ev_org_type_date
    ON events(organization_id, event_type, event_date);
CREATE INDEX IF NOT EXISTS idx_ev_significance
    ON events(significance_score DESC);

-- Signal Scores
CREATE INDEX IF NOT EXISTS idx_ss_org_type
    ON signal_scores(organization_id, score_type);
CREATE INDEX IF NOT EXISTS idx_ss_calculated ON signal_scores(calculated_at);

-- Network Metrics
CREATE INDEX IF NOT EXISTS idx_nm_org ON network_metrics(organization_id);

-- Trend Snapshots
CREATE INDEX IF NOT EXISTS idx_ts_date_dim
    ON trend_snapshots(snapshot_date, dimension_type);

-- Tags & Sectors
CREATE INDEX IF NOT EXISTS idx_ot_tag ON organization_tags(tag_id);
CREATE INDEX IF NOT EXISTS idx_os_sector ON organization_sectors(sector_id);

-- Signals
CREATE INDEX IF NOT EXISTS idx_signals_type ON signals(signal_type);
CREATE INDEX IF NOT EXISTS idx_signals_sector ON signals(sector_id);

-- ------------------------------------------------------------
-- 14. Press Releases (PR TIMES, Bridge, Frontier Detector imports)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS press_releases (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    title               TEXT    NOT NULL,
    body_text           TEXT,
    source              TEXT    NOT NULL,  -- 'prtimes', 'bridge', 'frontier_detector', 'other'
    source_url          TEXT    NOT NULL,
    url_hash            TEXT    UNIQUE,
    published_at        TEXT,
    company_name        TEXT,
    organization_id     INTEGER REFERENCES organizations(id),
    category            TEXT,   -- 'funding', 'partnership', 'product_launch', 'hiring', 'other'
    is_funding_related  INTEGER DEFAULT 0,
    funding_round_id    INTEGER REFERENCES funding_rounds(id),
    extracted_data      TEXT,   -- JSON blob for structured extraction results
    confidence_score    REAL    DEFAULT 0.5,
    data_source_id      INTEGER REFERENCES data_sources(id),
    collected_at        TEXT    DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    created_at          TEXT    DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

CREATE INDEX IF NOT EXISTS idx_pr_url_hash ON press_releases(url_hash);
CREATE INDEX IF NOT EXISTS idx_pr_published ON press_releases(published_at);
CREATE INDEX IF NOT EXISTS idx_pr_org ON press_releases(organization_id);
CREATE INDEX IF NOT EXISTS idx_pr_funding ON press_releases(is_funding_related);
CREATE INDEX IF NOT EXISTS idx_pr_source ON press_releases(source);
CREATE INDEX IF NOT EXISTS idx_pr_category ON press_releases(category);

CREATE TABLE IF NOT EXISTS press_release_tags (
    press_release_id    INTEGER REFERENCES press_releases(id) ON DELETE CASCADE,
    tag_id              INTEGER REFERENCES tags(id),
    PRIMARY KEY (press_release_id, tag_id)
);

-- ============================================================
-- VIEWS
-- ============================================================

-- Latest composite score per organization
CREATE VIEW IF NOT EXISTS v_latest_scores AS
SELECT
    o.id, o.name, o.primary_role,
    s.score_value AS composite_score,
    s.calculated_at, s.model_version
FROM organizations o
JOIN signal_scores s ON o.id = s.organization_id
WHERE s.score_type = 'composite'
  AND s.calculated_at = (
      SELECT MAX(s2.calculated_at) FROM signal_scores s2
      WHERE s2.organization_id = o.id AND s2.score_type = 'composite'
  );

-- Organizations with stale data
CREATE VIEW IF NOT EXISTS v_stale_data AS
SELECT
    id, name, slug, collected_at, ttl_soft_days, ttl_hard_days,
    CAST(julianday('now') - julianday(collected_at) AS INTEGER) AS days_since_collection,
    CASE
        WHEN julianday('now') - julianday(collected_at) > ttl_hard_days THEN 'expired'
        WHEN julianday('now') - julianday(collected_at) > ttl_soft_days THEN 'stale'
        ELSE 'fresh'
    END AS freshness_status
FROM organizations
WHERE julianday('now') - julianday(collected_at) > ttl_soft_days;

-- Co-investment pairs
CREATE VIEW IF NOT EXISTS v_co_investments AS
SELECT
    rp1.investor_id AS investor_a,
    rp2.investor_id AS investor_b,
    COUNT(DISTINCT rp1.funding_round_id) AS shared_rounds,
    MIN(fr.announced_date) AS first_co_investment,
    MAX(fr.announced_date) AS last_co_investment
FROM round_participants rp1
JOIN round_participants rp2
    ON rp1.funding_round_id = rp2.funding_round_id
    AND rp1.investor_id < rp2.investor_id
JOIN funding_rounds fr ON rp1.funding_round_id = fr.id
GROUP BY rp1.investor_id, rp2.investor_id;

-- Event momentum (90-day rolling per org)
CREATE VIEW IF NOT EXISTS v_event_momentum AS
SELECT
    organization_id, event_type,
    COUNT(*) AS event_count_90d,
    AVG(significance_score) AS avg_significance
FROM events
WHERE event_date >= date('now', '-90 days')
GROUP BY organization_id, event_type;
"""


# Seed data: default data sources based on research findings
SEED_DATA_SOURCES = [
    ("manual", "manual", 1.0, 365, 365, None, "Manual entry"),
    ("pr_times_rss", "news", 0.7, 7, 30, "https://prtimes.jp/index.rdf", "PR TIMES RSS feed"),
    ("the_bridge_rss", "news", 0.7, 7, 30, "https://thebridge.jp/feed", "The Bridge RSS feed"),
    ("houjin_bangou", "official", 1.0, 90, 365, "https://www.houjin-bangou.nta.go.jp/webapi/", "Japan corporate number DB"),
    ("sec_edgar_form_d", "official", 1.0, 30, 90, "https://data.sec.gov/submissions/", "SEC EDGAR Form D filings"),
    ("claude_extracted", "ml_inferred", 0.6, 30, 90, None, "Extracted from news via Claude API"),
    ("migrated_v1", "manual", 0.5, 30, 90, None, "Migrated from schema v1"),
    ("prtimes_enhanced", "news", 0.7, 7, 30, "https://prtimes.jp/", "Enhanced PR TIMES collector (search + RSS + body extraction)"),
    ("frontier_detector_import", "news", 0.6, 30, 90, None, "Imported from Frontier Detector signals DB"),
]

# Seed sectors based on Dealroom's 32 fixed industries (adapted for investment radar)
SEED_SECTORS = [
    ("AI/Machine Learning", "AI/機械学習"),
    ("Fintech", "フィンテック"),
    ("Healthcare/Biotech", "ヘルスケア/バイオ"),
    ("Climate/CleanTech", "気候/クリーンテック"),
    ("Energy", "エネルギー"),
    ("Mobility/Transportation", "モビリティ/交通"),
    ("Space", "宇宙"),
    ("Robotics", "ロボティクス"),
    ("Quantum Computing", "量子コンピューティング"),
    ("Cybersecurity", "サイバーセキュリティ"),
    ("Enterprise Software/SaaS", "エンタープライズ/SaaS"),
    ("Consumer/E-commerce", "コンシューマー/EC"),
    ("Real Estate/PropTech", "不動産/プロップテック"),
    ("Construction Tech", "建設テック"),
    ("Food/AgTech", "フード/アグリテック"),
    ("Education/EdTech", "教育/エドテック"),
    ("HR/WorkTech", "人事/ワークテック"),
    ("Media/Entertainment", "メディア/エンタメ"),
    ("Gaming", "ゲーム"),
    ("Logistics/Supply Chain", "物流/サプライチェーン"),
    ("Manufacturing/Industrial", "製造/産業"),
    ("Materials/Chemistry", "素材/化学"),
    ("Semiconductor", "半導体"),
    ("Hardware/IoT", "ハードウェア/IoT"),
    ("Blockchain/Web3", "ブロックチェーン/Web3"),
    ("Legal Tech", "リーガルテック"),
    ("GovTech", "ガブテック"),
    ("InsurTech", "インシュアテック"),
    ("Marketing/AdTech", "マーケティング/アドテック"),
    ("Travel/Hospitality", "旅行/ホスピタリティ"),
    ("Retail Tech", "リテールテック"),
    ("Developer Tools", "デベロッパーツール"),
    ("Other", "その他"),
]


def seed_initial_data(conn: sqlite3.Connection) -> None:
    """Seed default data sources and sectors."""
    # Data sources
    for name, stype, conf, ttl_s, ttl_h, api, note in SEED_DATA_SOURCES:
        conn.execute(
            """INSERT OR IGNORE INTO data_sources
               (name, source_type, base_confidence, default_ttl_soft_days,
                default_ttl_hard_days, api_endpoint, notes)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (name, stype, conf, ttl_s, ttl_h, api, note),
        )

    # Sectors
    for idx, (name, name_ja) in enumerate(SEED_SECTORS):
        conn.execute(
            """INSERT OR IGNORE INTO sectors (name, name_ja, sort_order)
               VALUES (?, ?, ?)""",
            (name, name_ja, idx),
        )

    # Initial score model
    conn.execute(
        """INSERT OR IGNORE INTO score_models (version, description, weights, is_active)
           VALUES (?, ?, ?, ?)""",
        (
            "v1.0",
            "Initial composite score model based on CB Insights 4M adapted for early-stage radar",
            '{"momentum": 0.35, "funding": 0.25, "market": 0.15, "team": 0.10, "technology": 0.10, "network": 0.05}',
            1,
        ),
    )

    conn.commit()


def _apply_enrichment_columns(conn: sqlite3.Connection) -> None:
    """Add enrichment columns for gBizINFO integration (idempotent).

    These columns store official corporate data fetched from the
    gBizINFO government API (capital, employee count, address).
    """
    alter_statements = [
        "ALTER TABLE organizations ADD COLUMN capital_yen INTEGER",
        "ALTER TABLE organizations ADD COLUMN employee_count INTEGER",
        "ALTER TABLE organizations ADD COLUMN address TEXT",
        "ALTER TABLE organizations ADD COLUMN enriched_at TEXT",
    ]
    for stmt in alter_statements:
        try:
            conn.execute(stmt)
        except sqlite3.OperationalError:
            # Column already exists — safe to ignore
            pass

    # Index for corporate_number lookups (covers enrichment queries)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_org_corp_number "
        "ON organizations(corporate_number)"
    )
    conn.commit()


def init_db(db_path: Path = DEFAULT_DB_PATH, seed: bool = True) -> Path:
    """Create database with v2 schema. Idempotent."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    try:
        conn.executescript(SCHEMA_SQL)
        _apply_enrichment_columns(conn)
        if seed:
            seed_initial_data(conn)
    finally:
        conn.close()
    print(f"v2 database initialized: {db_path}")
    return db_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Initialize v2 schema")
    parser.add_argument("--path", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--no-seed", action="store_true", help="Skip seed data")
    args = parser.parse_args()
    init_db(args.path, seed=not args.no_seed)
