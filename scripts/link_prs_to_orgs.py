"""
link_prs_to_orgs.py

Match press_releases to organizations by finding organization names
(or their aliases) as substrings in PR titles.

Rules:
- Minimum name length: 4 chars (avoids false positives on short names)
- Skip common generic words that could false-match
- For names starting with 株式会社/合同会社/有限会社: also try the bare name without the prefix
- If exactly one match: assign it
- If multiple matches: pick the longest matching name (more specific)
- Commit in batches of 500
"""

import sqlite3
import json
import re
from pathlib import Path
from collections import defaultdict

DB_PATH = Path(__file__).parent.parent / "data" / "investment_signal_v2.db"

# Common short/generic words to skip as standalone match candidates
SKIP_WORDS = {
    "AI", "IT", "IoT", "DX", "HR", "SaaS", "VC", "LP", "GP",
    "株式", "会社", "有限", "合同", "事業", "部門", "グループ",
    "Japan", "Asia", "Tech", "Lab", "Labs", "Fund", "Capital",
    "株式会社", "有限会社", "合同会社", "一般社団法人", "一般財団法人",
    "特定非営利活動法人", "NPO", "公益財団法人", "公益社団法人",
}

# Prefixes that, when stripped, reveal the "bare" company name
CORP_PREFIXES = [
    "株式会社",
    "合同会社",
    "有限会社",
    "一般社団法人",
    "一般財団法人",
    "特定非営利活動法人",
    "公益財団法人",
    "公益社団法人",
]


def expand_name_variants(name: str) -> list[str]:
    """Return list of searchable variants for a given org name."""
    variants = [name]
    for prefix in CORP_PREFIXES:
        if name.startswith(prefix):
            bare = name[len(prefix):]
            if bare:
                variants.append(bare)
        elif name.endswith(prefix.rstrip("社法人")):
            pass  # suffix case — less common, skip
    return variants


def is_valid_candidate(name: str) -> bool:
    """Return True if this name is worth matching against titles."""
    if len(name) < 4:
        return False
    if name in SKIP_WORDS:
        return False
    return True


def load_orgs(conn: sqlite3.Connection) -> list[dict]:
    """Load all organizations with their searchable name variants."""
    c = conn.cursor()
    c.execute("SELECT id, name, aliases FROM organizations")
    orgs = []
    for org_id, name, aliases_json in c.fetchall():
        # Collect all name variants from name + aliases
        all_names = [name] if name else []
        try:
            aliases = json.loads(aliases_json) if aliases_json else []
            if isinstance(aliases, list):
                all_names.extend(aliases)
        except (json.JSONDecodeError, TypeError):
            pass

        # Build expanded set of searchable strings
        search_terms = set()
        for n in all_names:
            if n:
                for variant in expand_name_variants(n.strip()):
                    if is_valid_candidate(variant):
                        search_terms.add(variant)

        if search_terms:
            orgs.append({
                "id": org_id,
                "name": name,
                "search_terms": search_terms,
            })
    return orgs


def find_best_match(title: str, orgs: list[dict]) -> int | None:
    """
    Find the best organization match for a PR title.

    Returns org_id if a unique or best match is found, else None.
    """
    if not title:
        return None

    matched: list[tuple[int, int, str]] = []  # (org_id, match_len, matched_term)

    for org in orgs:
        best_term = None
        best_len = 0
        for term in org["search_terms"]:
            if term in title and len(term) > best_len:
                best_len = len(term)
                best_term = term
        if best_term is not None:
            matched.append((org["id"], best_len, best_term))

    if not matched:
        return None

    # De-duplicate by org_id (keep longest match per org)
    by_org: dict[int, tuple[int, str]] = {}
    for org_id, length, term in matched:
        if org_id not in by_org or length > by_org[org_id][0]:
            by_org[org_id] = (length, term)

    if len(by_org) == 1:
        return next(iter(by_org))

    # Multiple orgs matched — pick the one with the longest term
    best_org_id = max(by_org, key=lambda oid: by_org[oid][0])
    best_len = by_org[best_org_id][0]

    # If there's a tie among multiple orgs at the top length, skip (ambiguous)
    top_orgs = [oid for oid, (l, _) in by_org.items() if l == best_len]
    if len(top_orgs) > 1:
        return None

    return best_org_id


def main():
    print(f"Connecting to {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)

    print("Loading organizations...")
    orgs = load_orgs(conn)
    print(f"  Loaded {len(orgs)} organizations with searchable terms")

    c = conn.cursor()
    c.execute(
        "SELECT id, title FROM press_releases WHERE organization_id IS NULL AND title IS NOT NULL"
    )
    prs = c.fetchall()
    print(f"Loading press_releases without organization_id: {len(prs):,}")

    updates: list[tuple[int, int]] = []  # (org_id, pr_id)
    no_match = 0
    ambiguous = 0

    for pr_id, title in prs:
        org_id = find_best_match(title, orgs)
        if org_id is not None:
            updates.append((org_id, pr_id))
        else:
            no_match += 1

    print(f"\nMatching complete:")
    print(f"  Matched:    {len(updates):,}")
    print(f"  No match:   {no_match:,}")
    print(f"  Total:      {len(prs):,}")

    # Commit in batches of 500
    batch_size = 500
    committed = 0
    for i in range(0, len(updates), batch_size):
        batch = updates[i : i + batch_size]
        c.executemany(
            "UPDATE press_releases SET organization_id = ? WHERE id = ?", batch
        )
        conn.commit()
        committed += len(batch)
        print(f"  Committed {committed:,} / {len(updates):,} updates...")

    print("\nDone. Running breakdown by category...")

    # Category breakdown of matched PRs
    if updates:
        pr_ids = [str(u[1]) for u in updates]
        # SQLite has a limit on IN clause size; query in chunks
        cat_counts: dict[str, int] = defaultdict(int)
        chunk = 500
        for i in range(0, len(pr_ids), chunk):
            ids_str = ",".join(pr_ids[i : i + chunk])
            c.execute(
                f"SELECT category, COUNT(*) FROM press_releases WHERE id IN ({ids_str}) GROUP BY category"
            )
            for cat, cnt in c.fetchall():
                cat_counts[cat] += cnt

        print("\nMatched PRs by category:")
        for cat, cnt in sorted(cat_counts.items(), key=lambda x: -x[1]):
            print(f"  {cat or '(none)':25s}: {cnt:,}")

    # Final totals
    c.execute("SELECT COUNT(*) FROM press_releases WHERE organization_id IS NOT NULL")
    total_linked = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM press_releases")
    total_prs = c.fetchone()[0]
    print(f"\nFinal DB state:")
    print(f"  press_releases with organization_id: {total_linked:,} / {total_prs:,} ({total_linked/total_prs*100:.1f}%)")

    conn.close()


if __name__ == "__main__":
    main()
