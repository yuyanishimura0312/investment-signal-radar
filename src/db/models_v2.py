"""
Database helper functions for Investment Signal Radar v2.0 schema.

Provides CRUD operations based on the Organization-centric, event-driven
data model designed from best practice research.
"""

import hashlib
import json
import re
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Optional

DB_PATH = Path(__file__).parent.parent.parent / "data" / "investment_signal_v2.db"


# ================================================================
# Connection management
# ================================================================

def get_conn(db_path: Optional[Path] = None) -> sqlite3.Connection:
    """Get a database connection with row_factory and WAL mode."""
    path = db_path or DB_PATH
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


# ================================================================
# Utilities
# ================================================================

def url_hash(url: str) -> str:
    """Generate a short hash for URL deduplication."""
    return hashlib.sha256(url.encode()).hexdigest()[:16]


def slugify(name: str) -> str:
    """Generate URL-safe slug from name."""
    if not name:
        return f"org-{int(datetime.now().timestamp() * 1000)}"
    s = re.sub(r"[^\w\s-]", "", name.lower(), flags=re.UNICODE)
    s = re.sub(r"[\s_]+", "-", s).strip("-")
    if not s:
        s = "org-" + hashlib.md5(name.encode()).hexdigest()[:10]
    return s[:80]


def ensure_unique_slug(conn: sqlite3.Connection, base_slug: str) -> str:
    """Ensure slug uniqueness by appending a counter if needed."""
    slug = base_slug
    counter = 1
    while conn.execute(
        "SELECT 1 FROM organizations WHERE slug = ?", (slug,)
    ).fetchone():
        slug = f"{base_slug}-{counter}"
        counter += 1
    return slug


# ================================================================
# Data source helpers
# ================================================================

def get_data_source_id(conn: sqlite3.Connection, name: str) -> int:
    """Get data_source id by name. Returns 'manual' id if not found."""
    row = conn.execute(
        "SELECT id FROM data_sources WHERE name = ?", (name,)
    ).fetchone()
    if row:
        return row["id"]
    # Fallback to manual
    row = conn.execute(
        "SELECT id FROM data_sources WHERE name = 'manual'"
    ).fetchone()
    return row["id"] if row else 1


# ================================================================
# Deduplication
# ================================================================

def round_exists(conn: sqlite3.Connection, source_url: str) -> bool:
    """Check if a funding round with this source URL was already ingested."""
    h = url_hash(source_url)
    row = conn.execute(
        "SELECT 1 FROM funding_rounds WHERE url_hash = ?", (h,)
    ).fetchone()
    return row is not None


# ================================================================
# Organization (unified entity: companies + investors)
# ================================================================

def find_organization_by_name(
    conn: sqlite3.Connection, name: str
) -> Optional[int]:
    """Find organization by canonical name or alias."""
    if not name:
        return None
    # Exact match
    row = conn.execute(
        "SELECT id FROM organizations WHERE LOWER(name) = LOWER(?)", (name,)
    ).fetchone()
    if row:
        return row["id"]
    # Check aliases (JSON array)
    rows = conn.execute(
        "SELECT id, aliases FROM organizations WHERE aliases != '[]'"
    ).fetchall()
    for r in rows:
        try:
            aliases = json.loads(r["aliases"] or "[]")
            if name in aliases:
                return r["id"]
        except (json.JSONDecodeError, TypeError):
            continue
    return None


def find_or_create_company(
    conn: sqlite3.Connection,
    name: str,
    description: str = "",
    country: str = "JP",
    data_source_id: Optional[int] = None,
) -> int:
    """Find existing company org or create a new one."""
    existing = find_organization_by_name(conn, name)
    if existing:
        # Ensure company role is set
        conn.execute(
            "UPDATE organizations SET is_company = 1 WHERE id = ?", (existing,)
        )
        return existing

    slug = ensure_unique_slug(conn, slugify(name))
    ds_id = data_source_id or get_data_source_id(conn, "claude_extracted")
    cur = conn.execute(
        """INSERT INTO organizations (
            slug, name, primary_role, is_company, is_investor,
            description, country_code, data_source_id, confidence_score
        ) VALUES (?, ?, 'company', 1, 0, ?, ?, ?, 0.6)""",
        (slug, name, description, country, ds_id),
    )
    return cur.lastrowid


def find_or_create_investor(
    conn: sqlite3.Connection,
    name: str,
    investor_type: str = "vc",
    country: str = "JP",
    data_source_id: Optional[int] = None,
) -> int:
    """Find existing investor org or create a new one (dual-role aware)."""
    existing = find_organization_by_name(conn, name)
    if existing:
        # Upgrade to investor role
        conn.execute(
            """UPDATE organizations
               SET is_investor = 1,
                   investor_type = COALESCE(investor_type, ?)
               WHERE id = ?""",
            (investor_type, existing),
        )
        return existing

    slug = ensure_unique_slug(conn, slugify(name) + "-inv")
    ds_id = data_source_id or get_data_source_id(conn, "claude_extracted")
    cur = conn.execute(
        """INSERT INTO organizations (
            slug, name, primary_role, is_company, is_investor,
            investor_type, country_code, data_source_id, confidence_score
        ) VALUES (?, ?, 'investor', 0, 1, ?, ?, ?, 0.6)""",
        (slug, name, investor_type, country, ds_id),
    )
    return cur.lastrowid


# ================================================================
# Sector / Tag assignment
# ================================================================

def find_or_create_sector(conn: sqlite3.Connection, name: str) -> int:
    """Find sector by name (en or ja) or create a new one."""
    if not name:
        name = "Other"
    row = conn.execute(
        "SELECT id FROM sectors WHERE LOWER(name) = LOWER(?) OR name_ja = ?",
        (name, name),
    ).fetchone()
    if row:
        return row["id"]
    cur = conn.execute(
        "INSERT INTO sectors (name, name_ja, sort_order) VALUES (?, ?, 100)",
        (name, name),
    )
    return cur.lastrowid


def assign_primary_sector(
    conn: sqlite3.Connection, organization_id: int, sector_name: str
) -> None:
    """Assign a primary sector to an organization."""
    if not sector_name:
        return
    sector_id = find_or_create_sector(conn, sector_name)
    # Clear existing primary if any, then insert
    conn.execute(
        """UPDATE organization_sectors SET is_primary = 0
           WHERE organization_id = ?""",
        (organization_id,),
    )
    conn.execute(
        """INSERT OR REPLACE INTO organization_sectors
           (organization_id, sector_id, is_primary)
           VALUES (?, ?, 1)""",
        (organization_id, sector_id),
    )


def find_or_create_tag(
    conn: sqlite3.Connection, name: str, category: str = "technology"
) -> int:
    """Find or create a tag. Checks synonyms first."""
    # Synonym lookup
    row = conn.execute(
        "SELECT canonical_tag_id FROM tag_synonyms WHERE synonym = ?", (name,)
    ).fetchone()
    if row:
        return row["canonical_tag_id"]
    # Direct lookup
    row = conn.execute(
        "SELECT id FROM tags WHERE tag_category = ? AND LOWER(name) = LOWER(?)",
        (category, name),
    ).fetchone()
    if row:
        return row["id"]
    cur = conn.execute(
        "INSERT INTO tags (tag_category, name, name_ja) VALUES (?, ?, ?)",
        (category, name, name),
    )
    return cur.lastrowid


def tag_organization(
    conn: sqlite3.Connection,
    organization_id: int,
    tag_name: str,
    category: str = "technology",
    assigned_by: str = "manual",
    confidence: float = 1.0,
) -> None:
    """Attach a tag to an organization."""
    tag_id = find_or_create_tag(conn, tag_name, category)
    conn.execute(
        """INSERT OR REPLACE INTO organization_tags
           (organization_id, tag_id, confidence_score, assigned_by)
           VALUES (?, ?, ?, ?)""",
        (organization_id, tag_id, confidence, assigned_by),
    )


# ================================================================
# Funding Rounds + Events (core signal ingestion)
# ================================================================

# Map legacy v1 round types to v2 canonical names
ROUND_TYPE_NORMALIZATION = {
    "pre-seed": "pre_seed", "pre_seed": "pre_seed",
    "seed": "seed",
    "pre-a": "series_a", "pre_a": "series_a",
    "a": "series_a", "series_a": "series_a", "series-a": "series_a",
    "b": "series_b", "series_b": "series_b", "series-b": "series_b",
    "c": "series_c", "series_c": "series_c", "series-c": "series_c",
    "d": "series_d", "series_d": "series_d", "series-d": "series_d",
    "e": "series_e", "series_e": "series_e",
    "f": "series_f", "series_f": "series_f",
    "g": "series_g", "series_g": "series_g",
    "strategic": "strategic",
    "debt": "debt",
    "grant": "grant",
    "ipo": "ipo",
    "angel": "angel",
    "convertible": "convertible_note", "convertible_note": "convertible_note",
    "j-kiss": "j_kiss", "j_kiss": "j_kiss",
    "corporate_round": "corporate_round", "corporate": "corporate_round",
    "secondary": "secondary",
    "late_stage": "late_stage",
    "unknown": "unknown",
}


def normalize_round_type(raw: str) -> str:
    """Normalize various round type labels to the canonical v2 values."""
    if not raw:
        return "unknown"
    key = raw.strip().lower()
    return ROUND_TYPE_NORMALIZATION.get(key, "unknown")


def insert_funding_round(
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
    data_source_name: str = "claude_extracted",
) -> Optional[int]:
    """
    Insert a complete funding round with company, investors, and a funding event.

    Returns funding_round_id, or None if duplicate URL.
    """
    if round_exists(conn, source_url):
        return None

    ds_id = get_data_source_id(conn, data_source_name)
    conf_score_map = {"high": 0.9, "medium": 0.6, "low": 0.3}
    conf_val = conf_score_map.get(confidence, 0.6)

    # 1. Find/create company organization
    company_id = find_or_create_company(
        conn, company_name, description=description, data_source_id=ds_id
    )
    if sector:
        assign_primary_sector(conn, company_id, sector)

    # 2. Normalize round type
    v2_round = normalize_round_type(round_type)

    # 3. Fallback date
    if not announced_date:
        announced_date = datetime.now().strftime("%Y-%m-%d")

    # 4. Insert funding_round
    cur = conn.execute(
        """INSERT INTO funding_rounds (
            organization_id, round_type, announced_date, amount_jpy, amount_raw,
            currency, data_source_id, confidence_score, source_url, source_title,
            url_hash, pestle_category
        ) VALUES (?, ?, ?, ?, ?, 'JPY', ?, ?, ?, ?, ?, ?)""",
        (
            company_id, v2_round, announced_date, amount_jpy, amount_raw,
            ds_id, conf_val, source_url, source_title, url_hash(source_url),
            pestle_category,
        ),
    )
    round_id = cur.lastrowid

    # 5. Link investors via round_participants
    for inv in investors:
        inv_name = (inv.get("name") or "").strip()
        if not inv_name:
            continue
        investor_id = find_or_create_investor(
            conn, inv_name,
            investor_type=inv.get("type") or "vc",
            country=inv.get("country") or "JP",
            data_source_id=ds_id,
        )
        is_lead = 1 if inv.get("is_lead") else 0
        conn.execute(
            """INSERT OR IGNORE INTO round_participants
               (funding_round_id, investor_id, is_lead) VALUES (?, ?, ?)""",
            (round_id, investor_id, is_lead),
        )

    # 6. Create the corresponding funding event (first-class event model)
    event_payload = {
        "round_type": v2_round,
        "amount_jpy": amount_jpy,
        "amount_raw": amount_raw,
        "funding_round_id": round_id,
        "investor_count": len(investors),
    }
    conn.execute(
        """INSERT INTO events (
            organization_id, event_type, event_date, title, description,
            event_data, significance_score, data_source_id, confidence_score,
            source_url
        ) VALUES (?, 'funding', ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            company_id, announced_date, source_title, description,
            json.dumps(event_payload, ensure_ascii=False),
            max(0.3, conf_val), ds_id, conf_val, source_url,
        ),
    )

    return round_id


# ================================================================
# Generic event insertion (for future non-funding signals)
# ================================================================

def insert_event(
    conn: sqlite3.Connection,
    organization_id: int,
    event_type: str,
    event_date: str,
    title: str = "",
    description: str = "",
    event_data: Optional[dict] = None,
    significance: float = 0.5,
    confidence: float = 0.5,
    source_url: str = "",
    data_source_name: str = "manual",
) -> int:
    """Insert a generic event (hiring, patent, partnership, etc.)."""
    ds_id = get_data_source_id(conn, data_source_name)
    cur = conn.execute(
        """INSERT INTO events (
            organization_id, event_type, event_date, title, description,
            event_data, significance_score, data_source_id, confidence_score,
            source_url
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            organization_id, event_type, event_date, title, description,
            json.dumps(event_data or {}, ensure_ascii=False),
            significance, ds_id, confidence, source_url,
        ),
    )
    return cur.lastrowid


# ================================================================
# Stats
# ================================================================

_STATS_TABLES = (
    "organizations", "funding_rounds", "round_participants",
    "events", "sectors", "tags", "signals", "signal_scores"
)


def get_stats(conn: sqlite3.Connection) -> dict:
    """Get v2 database statistics."""
    stats: dict[str, int] = {}
    for table in _STATS_TABLES:
        row = conn.execute(f"SELECT COUNT(*) as c FROM {table}").fetchone()
        stats[table] = row["c"]
    # Derived counts
    row = conn.execute(
        "SELECT COUNT(*) as c FROM organizations WHERE is_company = 1"
    ).fetchone()
    stats["companies"] = row["c"]
    row = conn.execute(
        "SELECT COUNT(*) as c FROM organizations WHERE is_investor = 1"
    ).fetchone()
    stats["investors"] = row["c"]
    return stats
