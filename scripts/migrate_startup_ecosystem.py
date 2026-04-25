#!/usr/bin/env python3
"""
Migrate IR v2 database to add startup ecosystem tables.

Adds:
  - startup_milestones: lifecycle events (founding → growth → exit)
  - startup_cohorts: aggregated cohort analysis by year × sector
  - ecosystem_rankings: Startup Genome / StartupBlink annual data
  - nedo_projects: NEDO research projects linked to startups
  - startup_emergence_signals: detected emergence patterns

These tables extend the existing organizations/funding_rounds/events
tables to enable temporal analysis of startup emergence as foresight signals.

Usage:
    python3 scripts/migrate_startup_ecosystem.py
"""

import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "investment_signal_v2.db"

MIGRATION_SQL = """
-- ============================================================
-- Startup Ecosystem Extension Tables
-- ============================================================
-- Purpose: Track startup lifecycle as foresight signals.
--   "When did startups in theme X start appearing?"
--   "How did they grow? What happened to them?"
--   Combined with VC data → investment-side signal detection.
-- ============================================================

-- ------------------------------------------------------------
-- 15. Startup Milestones (lifecycle tracking per organization)
-- ------------------------------------------------------------
-- Each row = one milestone in a startup's lifecycle.
-- Complements the events table with structured lifecycle phases.
CREATE TABLE IF NOT EXISTS startup_milestones (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    organization_id     INTEGER NOT NULL REFERENCES organizations(id),
    milestone_type      TEXT    NOT NULL
        CHECK (milestone_type IN (
            'founded',          -- incorporation / founding
            'first_product',    -- MVP or first product launch
            'first_funding',    -- first external funding
            'seed',             -- seed round
            'series_a',         -- Series A
            'series_b_plus',    -- Series B or later
            'revenue_start',    -- first revenue
            'profitability',    -- first profitable quarter/year
            'pivot',            -- major business model change
            'international',    -- international expansion
            'ipo_filing',       -- IPO filing
            'ipo',              -- IPO completed
            'acquisition',      -- acquired by another company
            'merger',           -- merged with another company
            'shutdown',         -- ceased operations
            'unicorn',          -- reached $1B valuation
            'nedo_adoption',    -- adopted NEDO project results
            'accelerator_entry',-- entered accelerator program
            'accelerator_grad'  -- graduated from accelerator
        )),
    milestone_date      TEXT,           -- ISO date, NULL if unknown
    milestone_year      INTEGER,        -- year only, for when exact date unknown
    description         TEXT,
    -- Quantitative context at milestone time
    valuation_jpy       INTEGER,        -- valuation at this milestone
    valuation_usd       INTEGER,
    employee_count      INTEGER,        -- headcount at this milestone
    cumulative_funding_jpy  INTEGER,    -- total raised up to this point
    cumulative_funding_usd  INTEGER,
    -- Linked records
    funding_round_id    INTEGER REFERENCES funding_rounds(id),
    event_id            INTEGER REFERENCES events(id),
    -- Provenance
    data_source_id      INTEGER REFERENCES data_sources(id),
    confidence_score    REAL    DEFAULT 0.5
        CHECK (confidence_score BETWEEN 0.0 AND 1.0),
    source_url          TEXT,
    extra_data          TEXT    DEFAULT '{}',
    created_at          TEXT    NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_sm_org ON startup_milestones(organization_id);
CREATE INDEX IF NOT EXISTS idx_sm_type ON startup_milestones(milestone_type);
CREATE INDEX IF NOT EXISTS idx_sm_date ON startup_milestones(milestone_date);
CREATE INDEX IF NOT EXISTS idx_sm_year ON startup_milestones(milestone_year);
CREATE INDEX IF NOT EXISTS idx_sm_org_type
    ON startup_milestones(organization_id, milestone_type);

-- ------------------------------------------------------------
-- 16. Startup Cohorts (year × sector aggregation for trend analysis)
-- ------------------------------------------------------------
-- Pre-computed cohort metrics for "when did theme X emerge?" analysis.
-- Snapshot table: regenerated periodically from milestones + funding data.
CREATE TABLE IF NOT EXISTS startup_cohorts (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    cohort_year         INTEGER NOT NULL,       -- founding year
    sector_id           INTEGER REFERENCES sectors(id),
    tag_id              INTEGER REFERENCES tags(id),
    -- At least one of sector_id or tag_id must be set
    -- Cohort metrics
    startup_count       INTEGER DEFAULT 0,      -- companies founded in this year×sector
    funded_count        INTEGER DEFAULT 0,      -- of those, how many received funding
    total_raised_jpy    INTEGER DEFAULT 0,      -- total funding raised by cohort
    total_raised_usd    INTEGER DEFAULT 0,
    median_time_to_seed_days    INTEGER,         -- median days from founding to seed
    median_time_to_a_days       INTEGER,         -- median days from founding to Series A
    -- Outcome tracking
    ipo_count           INTEGER DEFAULT 0,
    acquired_count      INTEGER DEFAULT 0,
    shutdown_count      INTEGER DEFAULT 0,
    still_active_count  INTEGER DEFAULT 0,
    -- Temporal signal metrics
    yoy_growth_rate     REAL,                   -- year-over-year growth in startup_count
    emergence_score     REAL,                   -- normalized emergence signal strength
    -- Snapshot metadata
    snapshot_date       TEXT    NOT NULL DEFAULT (datetime('now')),
    UNIQUE(cohort_year, sector_id, tag_id, snapshot_date)
);

CREATE INDEX IF NOT EXISTS idx_sc_year ON startup_cohorts(cohort_year);
CREATE INDEX IF NOT EXISTS idx_sc_sector ON startup_cohorts(sector_id);
CREATE INDEX IF NOT EXISTS idx_sc_tag ON startup_cohorts(tag_id);
CREATE INDEX IF NOT EXISTS idx_sc_emergence
    ON startup_cohorts(emergence_score DESC);

-- ------------------------------------------------------------
-- 17. Ecosystem Rankings (Startup Genome / StartupBlink annual data)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ecosystem_rankings (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    source              TEXT    NOT NULL
        CHECK (source IN ('startup_genome', 'startupblink', 'other')),
    report_year         INTEGER NOT NULL,
    -- Geography
    ecosystem_name      TEXT    NOT NULL,        -- e.g., "Tokyo", "Silicon Valley"
    country_code        TEXT,
    -- Rankings
    global_rank         INTEGER,
    regional_rank       INTEGER,
    -- Metrics (as reported)
    total_startup_count INTEGER,
    total_funding_usd   INTEGER,
    unicorn_count       INTEGER,
    exit_count          INTEGER,
    -- Sector specializations
    top_sectors         TEXT    DEFAULT '[]',    -- JSON array of sector names
    sector_strengths    TEXT    DEFAULT '{}',    -- JSON: {sector: score}
    -- Raw data
    raw_data            TEXT    DEFAULT '{}',    -- full JSON of extracted metrics
    source_url          TEXT,
    notes               TEXT,
    created_at          TEXT    NOT NULL DEFAULT (datetime('now')),
    UNIQUE(source, report_year, ecosystem_name)
);

CREATE INDEX IF NOT EXISTS idx_er_source_year
    ON ecosystem_rankings(source, report_year);
CREATE INDEX IF NOT EXISTS idx_er_ecosystem
    ON ecosystem_rankings(ecosystem_name);

-- ------------------------------------------------------------
-- 18. NEDO Projects (public R&D linked to startup formation)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS nedo_projects (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    nedo_project_id     TEXT    UNIQUE,          -- NEDO's own project identifier
    title               TEXT    NOT NULL,
    title_en            TEXT,
    program_name        TEXT,                    -- NEDO program / scheme name
    -- Timeline
    start_date          TEXT,
    end_date            TEXT,
    fiscal_year         INTEGER,
    -- Participants
    lead_organization   TEXT,                    -- PI's institution
    lead_researcher     TEXT,
    participating_orgs  TEXT    DEFAULT '[]',    -- JSON array
    -- Classification
    research_field      TEXT,                    -- NEDO's own classification
    sector_id           INTEGER REFERENCES sectors(id),
    technology_tags     TEXT    DEFAULT '[]',    -- JSON array of technology keywords
    -- Budget
    budget_jpy          INTEGER,
    -- Outcome tracking
    has_spinoff         INTEGER DEFAULT 0,       -- did a startup emerge?
    spinoff_org_id      INTEGER REFERENCES organizations(id),
    patent_count        INTEGER DEFAULT 0,
    publication_count   INTEGER DEFAULT 0,
    -- Content
    abstract            TEXT,
    outcome_summary     TEXT,
    -- Source
    source_url          TEXT,
    pdf_url             TEXT,
    extra_data          TEXT    DEFAULT '{}',
    created_at          TEXT    NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_nedo_year ON nedo_projects(fiscal_year);
CREATE INDEX IF NOT EXISTS idx_nedo_sector ON nedo_projects(sector_id);
CREATE INDEX IF NOT EXISTS idx_nedo_spinoff
    ON nedo_projects(has_spinoff) WHERE has_spinoff = 1;
CREATE INDEX IF NOT EXISTS idx_nedo_spinoff_org
    ON nedo_projects(spinoff_org_id) WHERE spinoff_org_id IS NOT NULL;

-- ------------------------------------------------------------
-- 19. Startup Emergence Signals (detected patterns)
-- ------------------------------------------------------------
-- Signals detected from startup lifecycle analysis.
-- "Startups in sector X are being founded 3x faster than last year"
-- "Time-to-seed in sector Y has halved"
CREATE TABLE IF NOT EXISTS startup_emergence_signals (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    signal_type         TEXT    NOT NULL
        CHECK (signal_type IN (
            'founding_surge',       -- spike in new startups in a sector
            'funding_acceleration', -- time-to-funding getting shorter
            'sector_emergence',     -- new sector appearing in startup data
            'cohort_outperformance',-- a cohort outperforming historical averages
            'vc_convergence',       -- multiple VCs entering the same new sector
            'nedo_to_startup',      -- NEDO project results being commercialized
            'cross_border',         -- international expansion pattern
            'exit_wave',            -- cluster of exits in a sector
            'pivot_cluster'         -- multiple companies pivoting to same direction
        )),
    -- What sector/theme
    sector_id           INTEGER REFERENCES sectors(id),
    tag_id              INTEGER REFERENCES tags(id),
    -- When detected
    detected_at         TEXT    NOT NULL DEFAULT (datetime('now')),
    observation_period  TEXT,                    -- e.g., "2024-Q3 to 2025-Q1"
    -- Signal strength
    signal_strength     REAL    CHECK (signal_strength BETWEEN 0.0 AND 1.0),
    statistical_significance    REAL,            -- p-value or z-score
    -- Evidence
    baseline_value      REAL,                   -- historical average
    current_value       REAL,                   -- current observed value
    change_ratio        REAL,                   -- current / baseline
    description         TEXT,
    evidence_data       TEXT    DEFAULT '{}',    -- JSON with supporting data
    related_org_ids     TEXT    DEFAULT '[]',    -- JSON array of organization IDs
    -- Integration with other signal systems
    related_signal_id   INTEGER REFERENCES signals(id),
    pestle_category     TEXT,
    -- Status
    is_verified         INTEGER DEFAULT 0,
    is_reported         INTEGER DEFAULT 0,
    created_at          TEXT    NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_ses_type
    ON startup_emergence_signals(signal_type);
CREATE INDEX IF NOT EXISTS idx_ses_sector
    ON startup_emergence_signals(sector_id);
CREATE INDEX IF NOT EXISTS idx_ses_detected
    ON startup_emergence_signals(detected_at);
CREATE INDEX IF NOT EXISTS idx_ses_strength
    ON startup_emergence_signals(signal_strength DESC);

-- ============================================================
-- VIEWS for Startup Ecosystem Analysis
-- ============================================================

-- Startup lifecycle timeline (all milestones for each company)
CREATE VIEW IF NOT EXISTS v_startup_timeline AS
SELECT
    o.id AS org_id,
    o.name,
    o.founded_date,
    o.status,
    sm.milestone_type,
    sm.milestone_date,
    sm.milestone_year,
    sm.valuation_usd,
    sm.employee_count,
    sm.cumulative_funding_usd,
    sm.description AS milestone_desc,
    GROUP_CONCAT(DISTINCT s.name) AS sectors,
    GROUP_CONCAT(DISTINCT t.name) AS tags
FROM organizations o
JOIN startup_milestones sm ON o.id = sm.organization_id
LEFT JOIN organization_sectors os ON o.id = os.organization_id AND os.is_primary = 1
LEFT JOIN sectors s ON os.sector_id = s.id
LEFT JOIN organization_tags ot ON o.id = ot.organization_id
LEFT JOIN tags t ON ot.tag_id = t.id AND t.tag_category = 'technology'
WHERE o.is_company = 1
GROUP BY o.id, sm.id
ORDER BY o.id, sm.milestone_date;

-- Sector emergence heatmap (founding counts by year × sector)
CREATE VIEW IF NOT EXISTS v_sector_emergence_heatmap AS
SELECT
    sc.cohort_year,
    s.name AS sector_name,
    s.name_ja AS sector_name_ja,
    sc.startup_count,
    sc.funded_count,
    sc.total_raised_usd,
    sc.yoy_growth_rate,
    sc.emergence_score
FROM startup_cohorts sc
JOIN sectors s ON sc.sector_id = s.id
WHERE sc.tag_id IS NULL
ORDER BY sc.cohort_year DESC, sc.emergence_score DESC;

-- VC sector entry timing (when did VCs first invest in each sector?)
CREATE VIEW IF NOT EXISTS v_vc_sector_entry AS
SELECT
    inv.id AS investor_id,
    inv.name AS investor_name,
    inv.investor_type,
    s.id AS sector_id,
    s.name AS sector_name,
    MIN(fr.announced_date) AS first_investment_date,
    COUNT(DISTINCT fr.id) AS total_deals,
    SUM(fr.amount_usd) AS total_invested_usd
FROM organizations inv
JOIN round_participants rp ON inv.id = rp.investor_id
JOIN funding_rounds fr ON rp.funding_round_id = fr.id
JOIN organization_sectors os ON fr.organization_id = os.organization_id AND os.is_primary = 1
JOIN sectors s ON os.sector_id = s.id
WHERE inv.is_investor = 1
GROUP BY inv.id, s.id
ORDER BY s.id, first_investment_date;

-- NEDO → Startup pipeline (research projects that spawned companies)
CREATE VIEW IF NOT EXISTS v_nedo_startup_pipeline AS
SELECT
    np.nedo_project_id,
    np.title AS project_title,
    np.program_name,
    np.fiscal_year,
    np.lead_organization,
    np.budget_jpy,
    o.name AS startup_name,
    o.founded_date AS startup_founded,
    o.status AS startup_status,
    fr_summary.total_rounds,
    fr_summary.total_raised_jpy
FROM nedo_projects np
JOIN organizations o ON np.spinoff_org_id = o.id
LEFT JOIN (
    SELECT organization_id,
           COUNT(*) AS total_rounds,
           SUM(amount_jpy) AS total_raised_jpy
    FROM funding_rounds
    GROUP BY organization_id
) fr_summary ON o.id = fr_summary.organization_id
WHERE np.has_spinoff = 1
ORDER BY np.fiscal_year DESC;

-- Time-to-funding analysis by sector (median days from founding to each stage)
CREATE VIEW IF NOT EXISTS v_time_to_funding AS
SELECT
    s.name AS sector_name,
    s.name_ja,
    sm_founded.milestone_year AS cohort_year,
    COUNT(DISTINCT sm_founded.organization_id) AS cohort_size,
    -- Time to seed (median approximated by avg for SQLite)
    AVG(CASE WHEN sm_seed.milestone_date IS NOT NULL AND sm_founded.milestone_date IS NOT NULL
        THEN julianday(sm_seed.milestone_date) - julianday(sm_founded.milestone_date)
    END) AS avg_days_to_seed,
    -- Time to Series A
    AVG(CASE WHEN sm_a.milestone_date IS NOT NULL AND sm_founded.milestone_date IS NOT NULL
        THEN julianday(sm_a.milestone_date) - julianday(sm_founded.milestone_date)
    END) AS avg_days_to_series_a,
    -- Funding rate
    ROUND(100.0 * COUNT(sm_seed.id) / COUNT(sm_founded.id), 1) AS seed_rate_pct,
    ROUND(100.0 * COUNT(sm_a.id) / COUNT(sm_founded.id), 1) AS series_a_rate_pct
FROM startup_milestones sm_founded
JOIN organizations o ON sm_founded.organization_id = o.id
JOIN organization_sectors os ON o.id = os.organization_id AND os.is_primary = 1
JOIN sectors s ON os.sector_id = s.id
LEFT JOIN startup_milestones sm_seed
    ON sm_founded.organization_id = sm_seed.organization_id
    AND sm_seed.milestone_type = 'seed'
LEFT JOIN startup_milestones sm_a
    ON sm_founded.organization_id = sm_a.organization_id
    AND sm_a.milestone_type = 'series_a'
WHERE sm_founded.milestone_type = 'founded'
GROUP BY s.id, sm_founded.milestone_year
HAVING cohort_size >= 3
ORDER BY s.name, cohort_year;
"""

# Seed data for new data sources
SEED_NEW_SOURCES = [
    ("nedo_seika", "official", 0.9, 365, 730,
     "https://seika.nedo.go.jp/", "NEDO research outcome reports"),
    ("startup_genome", "commercial", 0.7, 365, 730,
     "https://startupgenome.com/", "Startup Genome annual GSER report"),
    ("startupblink", "commercial", 0.7, 365, 730,
     "https://www.startupblink.com/", "StartupBlink ecosystem rankings"),
    ("univ_venture_survey", "official", 0.8, 365, 730,
     None, "MEXT/METI university venture survey data"),
]


def migrate(db_path: Path = DB_PATH):
    """Run the startup ecosystem migration."""
    if not db_path.exists():
        print(f"ERROR: Database not found at {db_path}")
        return False

    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA foreign_keys = ON")

    try:
        # Check if already migrated
        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='startup_milestones'"
        )
        if cursor.fetchone():
            print("Startup ecosystem tables already exist. Checking for updates...")
        else:
            print("Creating startup ecosystem tables...")

        conn.executescript(MIGRATION_SQL)
        print("  Tables and indexes created.")

        # Seed new data sources
        for src in SEED_NEW_SOURCES:
            conn.execute("""
                INSERT OR IGNORE INTO data_sources
                (name, source_type, base_confidence, default_ttl_soft_days,
                 default_ttl_hard_days, api_endpoint, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, src)
        conn.commit()
        print("  Data sources seeded.")

        # Backfill startup_milestones from existing data
        backfill_count = backfill_milestones_from_existing(conn)
        print(f"  Backfilled {backfill_count} milestones from existing data.")

        conn.commit()
        print("Migration complete.")
        return True

    except Exception as e:
        conn.rollback()
        print(f"ERROR: {e}")
        raise
    finally:
        conn.close()


def backfill_milestones_from_existing(conn: sqlite3.Connection) -> int:
    """Create startup_milestones from existing organizations and funding_rounds."""
    count = 0

    # 1. Founded milestones from organizations.founded_date
    cursor = conn.execute("""
        INSERT OR IGNORE INTO startup_milestones
            (organization_id, milestone_type, milestone_date, milestone_year,
             data_source_id, confidence_score)
        SELECT
            o.id,
            'founded',
            o.founded_date,
            CAST(SUBSTR(o.founded_date, 1, 4) AS INTEGER),
            o.data_source_id,
            COALESCE(o.confidence_score, 0.5)
        FROM organizations o
        WHERE o.is_company = 1
          AND o.founded_date IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM startup_milestones sm
              WHERE sm.organization_id = o.id AND sm.milestone_type = 'founded'
          )
    """)
    count += cursor.rowcount

    # 2. Funding milestones from funding_rounds
    round_type_map = {
        'pre_seed': 'first_funding',
        'seed': 'seed',
        'angel': 'seed',
        'j_kiss': 'seed',
        'convertible_note': 'seed',
        'series_a': 'series_a',
    }
    for round_type, milestone_type in round_type_map.items():
        cursor = conn.execute(f"""
            INSERT OR IGNORE INTO startup_milestones
                (organization_id, milestone_type, milestone_date, milestone_year,
                 funding_round_id, valuation_usd, cumulative_funding_usd,
                 data_source_id, confidence_score)
            SELECT
                fr.organization_id,
                '{milestone_type}',
                fr.announced_date,
                CAST(SUBSTR(fr.announced_date, 1, 4) AS INTEGER),
                fr.id,
                fr.post_valuation_usd,
                (SELECT SUM(fr2.amount_usd) FROM funding_rounds fr2
                 WHERE fr2.organization_id = fr.organization_id
                   AND fr2.announced_date <= fr.announced_date),
                fr.data_source_id,
                COALESCE(fr.confidence_score, 0.5)
            FROM funding_rounds fr
            WHERE fr.round_type = '{round_type}'
              AND fr.announced_date IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM startup_milestones sm
                  WHERE sm.organization_id = fr.organization_id
                    AND sm.milestone_type = '{milestone_type}'
                    AND sm.funding_round_id = fr.id
              )
        """)
        count += cursor.rowcount

    # 3. Series B+ milestones
    cursor = conn.execute("""
        INSERT OR IGNORE INTO startup_milestones
            (organization_id, milestone_type, milestone_date, milestone_year,
             funding_round_id, valuation_usd, cumulative_funding_usd,
             data_source_id, confidence_score)
        SELECT
            fr.organization_id,
            'series_b_plus',
            fr.announced_date,
            CAST(SUBSTR(fr.announced_date, 1, 4) AS INTEGER),
            fr.id,
            fr.post_valuation_usd,
            (SELECT SUM(fr2.amount_usd) FROM funding_rounds fr2
             WHERE fr2.organization_id = fr.organization_id
               AND fr2.announced_date <= fr.announced_date),
            fr.data_source_id,
            COALESCE(fr.confidence_score, 0.5)
        FROM funding_rounds fr
        WHERE fr.round_type IN ('series_b', 'series_c', 'series_d',
                                 'series_e', 'series_f', 'series_g', 'late_stage')
          AND fr.announced_date IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM startup_milestones sm
              WHERE sm.organization_id = fr.organization_id
                AND sm.milestone_type = 'series_b_plus'
                AND sm.funding_round_id = fr.id
          )
    """)
    count += cursor.rowcount

    # 4. IPO milestones from events
    cursor = conn.execute("""
        INSERT OR IGNORE INTO startup_milestones
            (organization_id, milestone_type, milestone_date, milestone_year,
             event_id, data_source_id, confidence_score)
        SELECT
            e.organization_id,
            CASE e.event_type
                WHEN 'ipo_filing' THEN 'ipo_filing'
                WHEN 'acquisition' THEN 'acquisition'
                WHEN 'shutdown' THEN 'shutdown'
            END,
            e.event_date,
            CAST(SUBSTR(e.event_date, 1, 4) AS INTEGER),
            e.id,
            e.data_source_id,
            COALESCE(e.confidence_score, 0.5)
        FROM events e
        WHERE e.event_type IN ('ipo_filing', 'acquisition', 'shutdown')
          AND NOT EXISTS (
              SELECT 1 FROM startup_milestones sm
              WHERE sm.organization_id = e.organization_id
                AND sm.event_id = e.id
          )
    """)
    count += cursor.rowcount

    # 5. Status-based milestones
    status_map = {'acquired': 'acquisition', 'ipo': 'ipo', 'closed': 'shutdown'}
    for status, milestone_type in status_map.items():
        cursor = conn.execute(f"""
            INSERT OR IGNORE INTO startup_milestones
                (organization_id, milestone_type, description,
                 data_source_id, confidence_score)
            SELECT
                o.id,
                '{milestone_type}',
                'Inferred from organization status: {status}',
                o.data_source_id,
                0.3
            FROM organizations o
            WHERE o.is_company = 1
              AND o.status = '{status}'
              AND NOT EXISTS (
                  SELECT 1 FROM startup_milestones sm
                  WHERE sm.organization_id = o.id
                    AND sm.milestone_type = '{milestone_type}'
              )
        """)
        count += cursor.rowcount

    return count


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Migrate IR DB for startup ecosystem")
    parser.add_argument("--path", type=Path, default=DB_PATH)
    args = parser.parse_args()
    migrate(args.path)
