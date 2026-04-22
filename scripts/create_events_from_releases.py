"""
Create events from non-funding press releases.

Processes press_releases with category in (partnership, hiring, product_launch,
exit, accelerator) and creates corresponding events in the events table.

Also attempts to link unlinked press_releases (organization_id IS NULL) to
organizations via company_name matching before creating events.

Usage:
    python scripts/create_events_from_releases.py [--dry-run]
"""

import argparse
import json
import sqlite3
import sys
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "investment_signal_v2.db"

# Categories to process and their significance scores
CATEGORY_SIGNIFICANCE = {
    "partnership": 0.5,
    "product_launch": 0.5,
    "hiring": 0.6,
    "exit": 0.7,
    "accelerator": 0.4,
}

# Map press_release category → valid events.event_type
# 'exit' is not a valid event_type; map to 'acquisition' (most exits are M&A)
CATEGORY_TO_EVENT_TYPE = {
    "partnership": "partnership",
    "product_launch": "product_launch",
    "hiring": "hiring",
    "exit": "acquisition",
    "accelerator": "accelerator",
}

# Categories where org linking is attempted (all non-funding categories)
LINK_CATEGORIES = {"partnership", "exit", "hiring", "product_launch", "accelerator"}

CONFIDENCE_SCORE = 0.7  # category was auto-classified
BATCH_SIZE = 100


def get_conn(db_path: Path = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def get_data_source_id(conn: sqlite3.Connection, pr_data_source_id: int) -> int:
    """Return the press release's data_source_id, fall back to manual."""
    if pr_data_source_id:
        row = conn.execute(
            "SELECT id FROM data_sources WHERE id = ?", (pr_data_source_id,)
        ).fetchone()
        if row:
            return row["id"]
    row = conn.execute(
        "SELECT id FROM data_sources WHERE name = 'manual'"
    ).fetchone()
    return row["id"] if row else 1


def event_exists(
    conn: sqlite3.Connection,
    organization_id: int,
    event_type: str,
    event_date: str,
    source_url: str,
) -> bool:
    """Check if a matching event already exists to avoid duplicates."""
    row = conn.execute(
        """SELECT 1 FROM events
           WHERE organization_id = ?
             AND event_type = ?
             AND event_date = ?
             AND source_url = ?""",
        (organization_id, event_type, event_date, source_url),
    ).fetchone()
    return row is not None


def try_link_organization(
    conn: sqlite3.Connection, company_name: str
) -> int | None:
    """Try to find an organization matching company_name via LIKE match.

    Returns organization_id if found, else None.
    Strips common Japanese corporate suffixes for broader matching.
    """
    if not company_name or not company_name.strip():
        return None

    name = company_name.strip()

    # 1. Exact match
    row = conn.execute(
        "SELECT id FROM organizations WHERE LOWER(name) = LOWER(?)",
        (name,),
    ).fetchone()
    if row:
        return row["id"]

    # 2. LIKE match (press release company name contains org name or vice versa)
    row = conn.execute(
        "SELECT id FROM organizations WHERE LOWER(?) LIKE '%' || LOWER(name) || '%' AND LENGTH(name) >= 4",
        (name,),
    ).fetchone()
    if row:
        return row["id"]

    row = conn.execute(
        "SELECT id FROM organizations WHERE LOWER(name) LIKE '%' || LOWER(?) || '%' AND LENGTH(?) >= 4",
        (name, name),
    ).fetchone()
    if row:
        return row["id"]

    # 3. Strip common corporate suffixes and retry
    suffixes = ["株式会社", "合同会社", "有限会社", "一般社団法人", "NPO法人",
                "Inc.", "Inc", "Co., Ltd.", "Ltd.", "LLC", "Corporation", "Corp."]
    clean = name
    for suffix in suffixes:
        clean = clean.replace(suffix, "").strip()
    if clean and clean != name and len(clean) >= 4:
        row = conn.execute(
            "SELECT id FROM organizations WHERE LOWER(name) LIKE '%' || LOWER(?) || '%' AND LENGTH(?) >= 4",
            (clean, clean),
        ).fetchone()
        if row:
            return row["id"]

    return None


def try_link_from_title(
    conn: sqlite3.Connection, title: str
) -> int | None:
    """Try to find an organization by checking if its name appears in the title.

    Scans organization names (length >= 4) against the press release title.
    Returns the first matching organization_id, or None.
    """
    if not title or len(title) < 4:
        return None

    # Fetch all org names (id + name) and check inclusion in title
    # Use SQL LIKE for efficiency: title LIKE '%name%'
    row = conn.execute(
        """SELECT id FROM organizations
           WHERE LENGTH(name) >= 4
             AND INSTR(LOWER(?), LOWER(name)) > 0
           ORDER BY LENGTH(name) DESC
           LIMIT 1""",
        (title,),
    ).fetchone()
    return row["id"] if row else None


def update_pr_organization(
    conn: sqlite3.Connection, pr_id: int, org_id: int
) -> None:
    """Update a press release with the resolved organization_id."""
    conn.execute(
        "UPDATE press_releases SET organization_id = ? WHERE id = ?",
        (org_id, pr_id),
    )


def create_event_from_pr(
    conn: sqlite3.Connection,
    pr: sqlite3.Row,
    organization_id: int,
    dry_run: bool = False,
) -> bool:
    """Create an event from a press release row. Returns True if created."""
    category = pr["category"]
    # Map press_release category to a valid event_type (CHECK constraint)
    event_type = CATEGORY_TO_EVENT_TYPE.get(category, "other")
    event_date = pr["published_at"] or ""
    source_url = pr["source_url"] or ""
    title = pr["title"] or ""

    # Build description from body_text (first 200 chars)
    body_text = pr["body_text"] or ""
    description = body_text[:200].strip() if body_text else ""

    # event_data JSON (store original category for traceability)
    event_data = json.dumps(
        {
            "press_release_id": pr["id"],
            "category": category,
            "original_category": category,
        },
        ensure_ascii=False,
    )

    significance = CATEGORY_SIGNIFICANCE.get(category, 0.5)
    ds_id = get_data_source_id(conn, pr["data_source_id"])

    # Skip if event_date is missing (required field)
    if not event_date:
        return False

    # Duplicate check
    if event_exists(conn, organization_id, event_type, event_date, source_url):
        return False

    if dry_run:
        return True

    conn.execute(
        """INSERT INTO events (
            organization_id, event_type, event_date, title, description,
            event_data, significance_score, data_source_id, confidence_score,
            source_url
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            organization_id,
            event_type,
            event_date,
            title,
            description,
            event_data,
            significance,
            ds_id,
            CONFIDENCE_SCORE,
            source_url,
        ),
    )
    return True


def main():
    parser = argparse.ArgumentParser(description="Create events from non-funding press releases")
    parser.add_argument("--dry-run", action="store_true", help="Preview without writing to DB")
    args = parser.parse_args()

    dry_run = args.dry_run
    if dry_run:
        print("[DRY RUN] No changes will be written to the database.\n")

    conn = get_conn()

    categories = list(CATEGORY_SIGNIFICANCE.keys())
    placeholders = ",".join("?" * len(categories))

    # --- Step 1: Process press_releases with organization_id already set ---
    linked_rows = conn.execute(
        f"""SELECT id, organization_id, title, published_at, source_url,
                   category, body_text, data_source_id
            FROM press_releases
            WHERE category IN ({placeholders})
              AND organization_id IS NOT NULL""",
        categories,
    ).fetchall()

    # --- Step 2: Fetch unlinked press_releases and try to match orgs ---
    unlinked_rows = conn.execute(
        f"""SELECT id, organization_id, title, published_at, source_url,
                   category, body_text, data_source_id, company_name
            FROM press_releases
            WHERE category IN ({placeholders})
              AND organization_id IS NULL""",
        categories,
    ).fetchall()

    print(f"Found {len(linked_rows)} already-linked press releases")
    print(f"Found {len(unlinked_rows)} unlinked press releases — attempting org matching...\n")

    # Process unlinked: try to match org
    newly_linked = []
    link_attempts = {cat: {"matched": 0, "unmatched": 0} for cat in categories}

    for pr in unlinked_rows:
        company_name = (pr["company_name"] or "").strip()
        category = pr["category"]
        # Try company_name first; fall back to title-based org search
        org_id = try_link_organization(conn, company_name)
        if not org_id:
            # Try matching organization names against the press release title
            title = pr["title"] or ""
            if title:
                org_id = try_link_from_title(conn, title)

        if org_id:
            if not dry_run:
                update_pr_organization(conn, pr["id"], org_id)
            newly_linked.append((pr, org_id))
            link_attempts[category]["matched"] += 1
        else:
            link_attempts[category]["unmatched"] += 1

    print("Org-linking results by category:")
    total_linked = 0
    for cat, counts in link_attempts.items():
        matched = counts["matched"]
        unmatched = counts["unmatched"]
        total = matched + unmatched
        total_linked += matched
        print(f"  {cat:15s}: {matched}/{total} matched")
    print(f"  Total newly linked: {total_linked}\n")

    # Combine all processable press releases
    all_processable = []
    for pr in linked_rows:
        all_processable.append((pr, pr["organization_id"]))
    for pr, org_id in newly_linked:
        all_processable.append((pr, org_id))

    print(f"Total press releases to process for events: {len(all_processable)}\n")

    # --- Step 3: Create events in batches ---
    counts_created = {cat: 0 for cat in categories}
    counts_skipped = {cat: 0 for cat in categories}
    total_created = 0
    total_skipped = 0

    for batch_start in range(0, len(all_processable), BATCH_SIZE):
        batch = all_processable[batch_start: batch_start + BATCH_SIZE]
        batch_end = min(batch_start + BATCH_SIZE, len(all_processable))

        for pr, org_id in batch:
            category = pr["category"]
            created = create_event_from_pr(conn, pr, org_id, dry_run=dry_run)
            if created:
                counts_created[category] += 1
                total_created += 1
            else:
                counts_skipped[category] += 1
                total_skipped += 1

        if not dry_run:
            conn.commit()

        print(f"  Processed batch {batch_start // BATCH_SIZE + 1}: "
              f"rows {batch_start + 1}-{batch_end} "
              f"(created so far: {total_created})")

    # --- Summary ---
    print("\n" + "=" * 50)
    print("SUMMARY")
    print("=" * 50)
    print(f"{'Category':<20} {'Created':>8} {'Skipped':>8}")
    print("-" * 40)
    for cat in categories:
        print(f"{cat:<20} {counts_created[cat]:>8} {counts_skipped[cat]:>8}")
    print("-" * 40)
    print(f"{'TOTAL':<20} {total_created:>8} {total_skipped:>8}")
    print()

    # Final events table count
    row = conn.execute("SELECT COUNT(*) as c FROM events").fetchone()
    print(f"Events table total: {row['c']}")

    row_by_type = conn.execute(
        "SELECT event_type, COUNT(*) as c FROM events GROUP BY event_type ORDER BY c DESC"
    ).fetchall()
    print("\nEvents by type:")
    for r in row_by_type:
        print(f"  {r['event_type']:<20} {r['c']}")

    conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
