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
    """Generate URL-safe slug from name.

    Strictly ASCII-only (a-z0-9-). Japanese and other non-ASCII text falls back
    to a deterministic md5-based slug so slugs can safely appear in URLs.
    """
    if not name:
        return f"org-{int(datetime.now().timestamp() * 1000)}"
    # ASCII-only: strip anything that is not a-z, 0-9, space, or hyphen
    s = re.sub(r"[^a-z0-9\s-]", "", name.lower())
    s = re.sub(r"[\s_]+", "-", s).strip("-")
    if not s:
        # Fallback: deterministic hash of the original name
        s = "org-" + hashlib.md5(name.encode("utf-8")).hexdigest()[:12]
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
    """Find sector by name (en or ja) or create a new one.

    Normalizes the input name to a canonical sector before lookup/insert,
    preventing fragmentation of the sectors table.
    """
    from src.normalizer.sector_normalizer import normalize_sector

    canonical = normalize_sector(name)
    row = conn.execute(
        "SELECT id FROM sectors WHERE LOWER(name) = LOWER(?) OR name_ja = ?",
        (canonical, canonical),
    ).fetchone()
    if row:
        return row["id"]
    # Fallback: create with canonical name (should rarely happen)
    cur = conn.execute(
        "INSERT INTO sectors (name, name_ja, sort_order) VALUES (?, ?, 100)",
        (canonical, canonical),
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
    # Press release count (table may not exist in older DBs)
    try:
        row = conn.execute(
            "SELECT COUNT(*) as c FROM press_releases"
        ).fetchone()
        stats["press_releases"] = row["c"]
    except Exception:
        stats["press_releases"] = 0
    return stats


# ================================================================
# Press Release CRUD
# ================================================================

def insert_press_release(
    conn: sqlite3.Connection,
    data: dict,
) -> Optional[int]:
    """Insert a press release. Returns id, or None if duplicate URL.

    Expected keys in data:
        title, source, source_url (required)
        body_text, published_at, company_name, organization_id,
        category, is_funding_related, funding_round_id,
        extracted_data (dict or JSON string), confidence_score,
        data_source_name (str, looked up to data_source_id)
    """
    source_url = data.get("source_url", "")
    if not source_url:
        return None

    h = url_hash(source_url)
    # Deduplicate by URL hash
    if conn.execute(
        "SELECT 1 FROM press_releases WHERE url_hash = ?", (h,)
    ).fetchone():
        return None

    # Resolve data_source_id
    ds_name = data.get("data_source_name", "manual")
    ds_id = data.get("data_source_id") or get_data_source_id(conn, ds_name)

    # extracted_data can be dict or string
    extracted = data.get("extracted_data")
    if isinstance(extracted, dict):
        extracted = json.dumps(extracted, ensure_ascii=False)

    cur = conn.execute(
        """INSERT INTO press_releases (
            title, body_text, source, source_url, url_hash,
            published_at, company_name, organization_id,
            category, is_funding_related, funding_round_id,
            extracted_data, confidence_score, data_source_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            data.get("title", ""),
            data.get("body_text"),
            data.get("source", "other"),
            source_url,
            h,
            data.get("published_at"),
            data.get("company_name"),
            data.get("organization_id"),
            data.get("category"),
            1 if data.get("is_funding_related") else 0,
            data.get("funding_round_id"),
            extracted,
            data.get("confidence_score", 0.5),
            ds_id,
        ),
    )
    return cur.lastrowid


def get_press_releases(
    conn: sqlite3.Connection,
    limit: int = 50,
    offset: int = 0,
    source: Optional[str] = None,
    funding_only: bool = False,
) -> list[dict]:
    """Retrieve press releases with optional filters."""
    where_parts = []
    params: list = []
    if source:
        where_parts.append("source = ?")
        params.append(source)
    if funding_only:
        where_parts.append("is_funding_related = 1")

    where_clause = ""
    if where_parts:
        where_clause = "WHERE " + " AND ".join(where_parts)

    rows = conn.execute(
        f"""SELECT id, title, source, source_url, published_at,
                   company_name, category, is_funding_related,
                   confidence_score, collected_at
            FROM press_releases
            {where_clause}
            ORDER BY published_at DESC NULLS LAST, id DESC
            LIMIT ? OFFSET ?""",
        params + [limit, offset],
    ).fetchall()
    return [dict(r) for r in rows]


def get_press_release_stats(conn: sqlite3.Connection) -> dict:
    """Aggregate stats about press releases."""
    stats: dict = {}

    row = conn.execute("SELECT COUNT(*) as c FROM press_releases").fetchone()
    stats["total"] = row["c"]

    row = conn.execute(
        "SELECT COUNT(*) as c FROM press_releases WHERE is_funding_related = 1"
    ).fetchone()
    stats["funding_related"] = row["c"]

    # By source
    rows = conn.execute(
        "SELECT source, COUNT(*) as c FROM press_releases GROUP BY source"
    ).fetchall()
    stats["by_source"] = {r["source"]: r["c"] for r in rows}

    # By category
    rows = conn.execute(
        "SELECT COALESCE(category, 'unknown') as cat, COUNT(*) as c "
        "FROM press_releases GROUP BY cat"
    ).fetchall()
    stats["by_category"] = {r["cat"]: r["c"] for r in rows}

    return stats


def link_press_release_to_funding(
    conn: sqlite3.Connection,
    pr_id: int,
    funding_round_id: int,
) -> None:
    """Link a press release to a funding round."""
    conn.execute(
        """UPDATE press_releases
           SET funding_round_id = ?, is_funding_related = 1
           WHERE id = ?""",
        (funding_round_id, pr_id),
    )


def import_frontier_detector_signals(
    conn: sqlite3.Connection,
    frontier_db_path: str,
) -> int:
    """Import signals from Frontier Detector DB as press releases.

    Maps frontier signals to press_releases table. All signal types are imported
    (academic, funding, keyword, patent) to provide a comprehensive view.
    Returns the count of newly imported records.
    """
    import sqlite3 as _sqlite3

    fconn = _sqlite3.connect(frontier_db_path)
    fconn.row_factory = _sqlite3.Row
    try:
        rows = fconn.execute("""
            SELECT s.id, s.technology_id, s.agent_type, s.source_type,
                   s.source_url, s.source_name, s.title, s.content,
                   s.signal_date, s.metadata_json, s.collected_at,
                   t.name_ja AS tech_name_ja, t.name_en AS tech_name_en,
                   t.domain AS tech_domain
            FROM signals s
            LEFT JOIN technologies t ON s.technology_id = t.id
        """).fetchall()
    finally:
        fconn.close()

    ds_id = get_data_source_id(conn, "frontier_detector_import")
    imported = 0

    # Map frontier agent_type to press release category
    category_map = {
        "prtimes": "product_launch",
        "academic": "other",
        "funding": "funding",
        "patent": "other",
        "keyword": "other",
    }

    for row in rows:
        source_url = row["source_url"] or ""
        if not source_url:
            # Build a synthetic URL from the signal ID so we can deduplicate
            source_url = f"frontier-detector://signal/{row['id']}"

        is_funding = 1 if row["agent_type"] == "funding" else 0
        category = category_map.get(row["agent_type"], "other")

        # Build extracted_data with frontier-specific metadata
        extracted = {
            "frontier_signal_id": row["id"],
            "technology_id": row["technology_id"],
            "tech_name_ja": row["tech_name_ja"],
            "tech_name_en": row["tech_name_en"],
            "tech_domain": row["tech_domain"],
            "agent_type": row["agent_type"],
            "source_type": row["source_type"],
        }
        if row["metadata_json"]:
            try:
                extracted["original_metadata"] = json.loads(row["metadata_json"])
            except (json.JSONDecodeError, TypeError):
                pass

        result = insert_press_release(conn, {
            "title": row["title"] or f"Signal: {row['agent_type']}",
            "body_text": row["content"],
            "source": "frontier_detector",
            "source_url": source_url,
            "published_at": row["signal_date"] or row["collected_at"],
            "company_name": row["source_name"],
            "category": category,
            "is_funding_related": is_funding,
            "extracted_data": extracted,
            "confidence_score": 0.6,
            "data_source_id": ds_id,
        })
        if result is not None:
            imported += 1

    conn.commit()
    return imported
