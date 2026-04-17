#!/usr/bin/env python3
"""Collect VC/funding-related press releases from PR TIMES.

Replicates the proven pattern from sangaku-matcher-v2/fetch_press_releases.py
but with funding/investment-focused keywords instead of R&D/academia ones.

Method: HTML scraping of PR TIMES search results pages.
This is the same approach that successfully collected 11,357 releases
for sangaku-matcher-v2.

Usage:
    # Collect with default funding keywords
    python3 scripts/collect_funding_prtimes.py

    # Custom keywords
    python3 scripts/collect_funding_prtimes.py --keywords "資金調達" "シリーズA"

    # Limit pages per keyword (default: 5)
    python3 scripts/collect_funding_prtimes.py --max-pages 10
"""
from __future__ import annotations

import hashlib
import html as htmlmod
import json
import re
import sqlite3
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "data" / "investment_signal_v2.db"

# PR TIMES search page URL (HTML scraping, same as sangaku-matcher-v2)
PRTIMES_SEARCH_URL = "https://prtimes.jp/main/action.php?run=html&page=searchkey&search_word={keyword}"
# Pagination: PR TIMES uses &page_num=N for pagination
PRTIMES_SEARCH_PAGED = "https://prtimes.jp/main/action.php?run=html&page=searchkey&search_word={keyword}&page_num={page}"

# Rate limit between requests (seconds)
RATE_LIMIT = 2.0
REQUEST_TIMEOUT = 30

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

# ================================================================
# Funding/VC-focused search keywords
# ================================================================

FUNDING_SEARCH_KEYWORDS = [
    # Direct funding terms
    "資金調達",
    "シリーズA",
    "シリーズB",
    "シリーズC",
    "シードラウンド 調達",
    "プレシリーズA",
    "第三者割当増資",
    # VC/investor terms
    "ベンチャーキャピタル 出資",
    "VC 投資 スタートアップ",
    "CVC 出資",
    "リード投資家",
    # Specific round types
    "億円 調達 スタートアップ",
    "エクイティ調達",
    # Exit events
    "IPO 上場 スタートアップ",
    "M&A 買収 スタートアップ",
    # Ecosystem
    "アクセラレーター 採択",
    "インキュベーション プログラム",
]

# Classification keywords (applied to title + description)
FUNDING_CLASSIFY_KW = [
    "資金調達", "シリーズA", "シリーズB", "シリーズC", "シリーズD", "シリーズE",
    "シードラウンド", "プレシリーズ", "エクイティ調達",
    "リード投資", "第三者割当増資", "調達額", "億円を調達",
    "万円を調達", "出資", "増資", "ラウンド",
]

PARTNERSHIP_CLASSIFY_KW = [
    "業務提携", "資本提携", "資本業務提携", "パートナーシップ", "協業",
    "共同開発", "提携", "アライアンス", "合弁",
]

EXIT_CLASSIFY_KW = [
    "IPO", "上場", "M&A", "買収", "事業譲渡", "株式公開",
]

ACCELERATOR_CLASSIFY_KW = [
    "アクセラレーター", "インキュベーター", "採択", "デモデイ",
    "ピッチ", "スタートアップ支援",
]

# ================================================================
# PR TIMES HTML scraping (proven pattern from sangaku-matcher-v2)
# ================================================================

def fetch_search_page(url: str) -> list[dict]:
    """Fetch and parse PR TIMES search results page via HTML scraping.

    Same approach as sangaku-matcher-v2/fetch_press_releases.py.
    """
    try:
        req = urllib.request.Request(url)
        req.add_header("User-Agent", USER_AGENT)
        with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
            html_data = resp.read().decode("utf-8", errors="replace")

        items = []
        # Extract <article> blocks from the search results page
        articles = re.findall(r"<article[^>]*>(.*?)</article>", html_data, re.DOTALL)
        for art in articles:
            # Title from <h3>
            title_m = re.search(r"<h3[^>]*>(.*?)</h3>", art, re.DOTALL)
            title = re.sub(r"<[^>]+>", "", title_m.group(1)).strip() if title_m else ""
            title = htmlmod.unescape(title)

            # Link
            link_m = re.search(r'href="(/main/html/rd/p/[^"]+)"', art)
            link = f"https://prtimes.jp{link_m.group(1)}" if link_m else ""

            # Date from <time>
            date_m = re.search(r"<time[^>]*>(.*?)</time>", art, re.DOTALL)
            date_str = re.sub(r"<[^>]+>", "", date_m.group(1)).strip() if date_m else ""

            # Description
            desc_m = re.search(
                r'class="[^"]*description[^"]*"[^>]*>(.*?)</(?:div|p|span)>',
                art, re.DOTALL,
            )
            description = re.sub(r"<[^>]+>", "", desc_m.group(1)).strip() if desc_m else ""
            description = htmlmod.unescape(description)

            # Company name (often in a separate element)
            company_m = re.search(
                r'class="[^"]*company[^"]*"[^>]*>(.*?)</(?:a|span|div)>',
                art, re.DOTALL,
            )
            company = re.sub(r"<[^>]+>", "", company_m.group(1)).strip() if company_m else ""
            company = htmlmod.unescape(company)

            if title:
                items.append({
                    "title": title,
                    "url": link,
                    "published": date_str,
                    "description": description,
                    "company_name": company,
                })
        return items
    except Exception as e:
        print(f"  Fetch error: {e}", file=sys.stderr)
        return []


def fetch_all_pages(keyword: str, max_pages: int = 5) -> list[dict]:
    """Fetch multiple pages of search results for a keyword."""
    all_items = []
    encoded = urllib.parse.quote(keyword)

    for page in range(1, max_pages + 1):
        if page == 1:
            url = PRTIMES_SEARCH_URL.format(keyword=encoded)
        else:
            url = PRTIMES_SEARCH_PAGED.format(keyword=encoded, page=page)

        items = fetch_search_page(url)
        if not items:
            break  # No more results

        all_items.extend(items)
        print(f"  Page {page}: {len(items)} items")
        time.sleep(RATE_LIMIT)

    return all_items


# ================================================================
# Classification
# ================================================================

def parse_pub_date(date_str: str) -> str | None:
    """Parse date string to ISO format (handles Japanese dates)."""
    if not date_str:
        return None
    # Japanese format: 2026年4月14日 10時10分
    m = re.search(r"(\d{4})年(\d{1,2})月(\d{1,2})日", date_str)
    if m:
        return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
    for fmt in ["%a, %d %b %Y %H:%M:%S %z", "%Y-%m-%dT%H:%M:%S%z"]:
        try:
            dt = datetime.strptime(date_str.strip(), fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def classify(title: str, description: str = "") -> tuple[str, bool]:
    """Classify a press release. Returns (category, is_funding_related)."""
    text = f"{title} {description}"

    # Check funding first (highest priority for this radar)
    if any(kw in text for kw in FUNDING_CLASSIFY_KW):
        return "funding", True

    if any(kw in text for kw in EXIT_CLASSIFY_KW):
        return "exit", True

    if any(kw in text for kw in PARTNERSHIP_CLASSIFY_KW):
        return "partnership", False

    if any(kw in text for kw in ACCELERATOR_CLASSIFY_KW):
        return "accelerator", False

    return "other", False


def extract_amount(title: str, description: str = "") -> str | None:
    """Try to extract funding amount from text."""
    text = f"{title} {description}"
    # Match patterns like "XX億円", "X.X億円", "XXX万円"
    m = re.search(r"([\d,.]+\s*億円)", text)
    if m:
        return m.group(1)
    m = re.search(r"([\d,.]+\s*万円)", text)
    if m:
        return m.group(1)
    return None


# ================================================================
# Database operations
# ================================================================

def store_releases(db_path: Path, releases: list[dict]) -> dict:
    """Store press releases in the investment_signal_v2 database.

    Returns stats dict.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    # Get existing URLs to avoid duplicates
    existing = set(
        r[0] for r in conn.execute("SELECT source_url FROM press_releases").fetchall()
    )

    stored = 0
    skipped = 0

    for pr in releases:
        url = pr["url"]
        if url in existing or not url:
            skipped += 1
            continue

        title = pr["title"]
        category, is_funding = pr["category"], pr["is_funding_related"]
        url_hash = hashlib.sha256(url.encode()).hexdigest()[:16]

        extracted = {}
        amount = pr.get("amount")
        if amount:
            extracted["amount_raw"] = amount
        if pr.get("company_name"):
            extracted["company_name"] = pr["company_name"]

        conn.execute("""
            INSERT INTO press_releases
                (title, body_text, source, source_url, url_hash, published_at,
                 company_name, organization_id, category, is_funding_related,
                 extracted_data, confidence_score, collected_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
        """, (
            title, pr.get("description", ""),
            "prtimes_funding", url, url_hash, pr.get("published_at"),
            pr.get("company_name", ""), None, category,
            1 if is_funding else 0,
            json.dumps(extracted, ensure_ascii=False) if extracted else "{}",
            0.8,  # Higher confidence since keyword-targeted
        ))
        existing.add(url)
        stored += 1

    conn.commit()

    # Get final stats
    total = conn.execute("SELECT COUNT(*) FROM press_releases").fetchone()[0]
    funding = conn.execute(
        "SELECT COUNT(*) FROM press_releases WHERE is_funding_related = 1"
    ).fetchone()[0]
    by_source = dict(conn.execute(
        "SELECT source, COUNT(*) FROM press_releases GROUP BY source"
    ).fetchall())
    by_cat = dict(conn.execute(
        "SELECT category, COUNT(*) FROM press_releases GROUP BY category ORDER BY COUNT(*) DESC"
    ).fetchall())

    conn.close()

    return {
        "stored": stored,
        "skipped": skipped,
        "total_in_db": total,
        "funding_in_db": funding,
        "by_source": by_source,
        "by_category": by_cat,
    }


# ================================================================
# Main pipeline
# ================================================================

def collect_funding_releases(
    keywords: list[str] | None = None,
    max_pages: int = 5,
    db_path: Path | None = None,
) -> dict:
    """Full collection pipeline for funding/VC press releases.

    Args:
        keywords: Search keywords (defaults to FUNDING_SEARCH_KEYWORDS).
        max_pages: Max pages to scrape per keyword.
        db_path: Database path (defaults to investment_signal_v2.db).

    Returns summary stats.
    """
    kws = keywords or FUNDING_SEARCH_KEYWORDS
    db = db_path or DB_PATH
    start = datetime.now()

    print(f"=== Funding PR TIMES Collector ===")
    print(f"  Keywords: {len(kws)}")
    print(f"  Max pages/keyword: {max_pages}")
    print(f"  DB: {db}")
    print()

    all_items = []
    seen_urls: set[str] = set()

    for i, kw in enumerate(kws):
        print(f"[{i+1}/{len(kws)}] Searching: {kw}")
        items = fetch_all_pages(kw, max_pages=max_pages)

        # Deduplicate and classify
        new_count = 0
        for item in items:
            url = item.get("url", "")
            if not url or url in seen_urls:
                continue
            seen_urls.add(url)

            title = item["title"]
            desc = item.get("description", "")
            category, is_funding = classify(title, desc)
            amount = extract_amount(title, desc)

            all_items.append({
                "title": title,
                "url": url,
                "published_at": parse_pub_date(item.get("published", "")),
                "description": desc,
                "company_name": item.get("company_name", ""),
                "category": category,
                "is_funding_related": is_funding,
                "amount": amount,
                "search_keyword": kw,
            })
            new_count += 1

        print(f"  -> {new_count} unique items")
        time.sleep(RATE_LIMIT)

    print(f"\nTotal collected: {len(all_items)}")

    # Classify summary before storing
    funding_count = sum(1 for x in all_items if x["is_funding_related"])
    print(f"Funding-related: {funding_count}")

    # Store in database
    stats = store_releases(db, all_items)

    elapsed = (datetime.now() - start).total_seconds()
    stats["collected"] = len(all_items)
    stats["elapsed_seconds"] = round(elapsed, 1)

    print(f"\n=== Collection Complete ({elapsed:.0f}s) ===")
    print(f"  Collected: {len(all_items)}")
    print(f"  Stored (new): {stats['stored']}")
    print(f"  Skipped (dup): {stats['skipped']}")
    print(f"  Total in DB: {stats['total_in_db']}")
    print(f"  Funding in DB: {stats['funding_in_db']}")
    print(f"  By source: {stats['by_source']}")
    print(f"  By category: {stats['by_category']}")

    return stats


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Collect VC/funding press releases from PR TIMES"
    )
    parser.add_argument(
        "--keywords", nargs="*",
        help="Custom search keywords (default: built-in funding keywords)",
    )
    parser.add_argument(
        "--max-pages", type=int, default=5,
        help="Max pages to scrape per keyword (default: 5)",
    )
    parser.add_argument(
        "--db", type=str, default=str(DB_PATH),
        help="Database path",
    )
    args = parser.parse_args()

    result = collect_funding_releases(
        keywords=args.keywords,
        max_pages=args.max_pages,
        db_path=Path(args.db),
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
