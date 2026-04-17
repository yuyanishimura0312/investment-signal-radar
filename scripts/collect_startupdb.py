#!/usr/bin/env python3
"""Collect funding news related to STARTUP DB (by for Startups, Inc.).

Uses two data sources:
  1. Google News RSS — searches for "STARTUP DB 資金調達" and related queries
  2. PR TIMES HTML scraping — searches for "STARTUP DB" press releases

STARTUP DB (startup-db.com) is a SPA with no public RSS/API,
so we collect news *about* STARTUP DB's funding reports and rankings
from external aggregators.

Usage:
    # Run with defaults
    python3 scripts/collect_startupdb.py

    # Custom max pages for PR TIMES scraping
    python3 scripts/collect_startupdb.py --max-pages 3

    # Dry run (don't write to DB)
    python3 scripts/collect_startupdb.py --dry-run
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

# Try to import optional dependencies for richer parsing
try:
    import feedparser
    HAS_FEEDPARSER = True
except ImportError:
    HAS_FEEDPARSER = False
    print("Warning: feedparser not installed. Using built-in XML parser.", file=sys.stderr)

try:
    from bs4 import BeautifulSoup
    HAS_BS4 = True
except ImportError:
    HAS_BS4 = False
    print("Warning: beautifulsoup4 not installed. Using regex-based parser.", file=sys.stderr)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "data" / "investment_signal_v2.db"

# Rate limit between HTTP requests (seconds)
RATE_LIMIT = 2.0
REQUEST_TIMEOUT = 30

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

# ================================================================
# Google News RSS queries for STARTUP DB funding news
# ================================================================

GOOGLE_NEWS_QUERIES = [
    "STARTUP DB 資金調達",
    "STARTUP DB 資金調達ランキング",
    "STARTUP DB スタートアップ 調達",
    "フォースタートアップス STARTUP DB",
    "startup-db.com 資金調達",
]

GOOGLE_NEWS_RSS_BASE = (
    "https://news.google.com/rss/search?"
    "q={query}&hl=ja&gl=JP&ceid=JP:ja"
)

# ================================================================
# PR TIMES search keywords
# ================================================================

PRTIMES_SEARCH_KEYWORDS = [
    "STARTUP DB",
    "STARTUP DB 資金調達",
    "フォースタートアップス STARTUP DB",
    "スタートアップデータベース 調達",
]

PRTIMES_SEARCH_URL = (
    "https://prtimes.jp/main/action.php"
    "?run=html&page=searchkey&search_word={keyword}"
)
PRTIMES_SEARCH_PAGED = (
    "https://prtimes.jp/main/action.php"
    "?run=html&page=searchkey&search_word={keyword}&page_num={page}"
)

# ================================================================
# Classification keywords
# ================================================================

FUNDING_CLASSIFY_KW = [
    "資金調達", "シリーズA", "シリーズB", "シリーズC", "シリーズD", "シリーズE",
    "シードラウンド", "プレシリーズ", "エクイティ調達",
    "リード投資", "第三者割当増資", "調達額", "億円を調達",
    "万円を調達", "出資", "増資", "ラウンド", "資金調達ランキング",
    "調達金額", "調達総額",
]

EXIT_CLASSIFY_KW = [
    "IPO", "上場", "M&A", "買収", "事業譲渡", "株式公開",
]

PARTNERSHIP_CLASSIFY_KW = [
    "業務提携", "資本提携", "資本業務提携", "パートナーシップ", "協業",
    "共同開発", "提携", "アライアンス",
]

ACCELERATOR_CLASSIFY_KW = [
    "アクセラレーター", "インキュベーター", "採択", "デモデイ",
    "ピッチ", "スタートアップ支援",
]


# ================================================================
# Utility functions
# ================================================================

def make_request(url: str) -> str | None:
    """Fetch URL content as UTF-8 string. Returns None on error."""
    try:
        req = urllib.request.Request(url)
        req.add_header("User-Agent", USER_AGENT)
        with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
            return resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        print(f"  Fetch error ({url[:80]}...): {e}", file=sys.stderr)
        return None


def url_hash(url: str) -> str:
    """Generate 16-char hash for deduplication."""
    return hashlib.sha256(url.encode()).hexdigest()[:16]


def parse_pub_date(date_str: str) -> str | None:
    """Parse various date formats to ISO YYYY-MM-DD."""
    if not date_str:
        return None

    # Japanese format: 2026年4月14日
    m = re.search(r"(\d{4})年(\d{1,2})月(\d{1,2})日", date_str)
    if m:
        return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"

    # RFC 2822: Thu, 05 Mar 2026 08:00:00 GMT
    for fmt in [
        "%a, %d %b %Y %H:%M:%S %Z",
        "%a, %d %b %Y %H:%M:%S %z",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S+09:00",
        "%Y-%m-%d",
    ]:
        try:
            dt = datetime.strptime(date_str.strip(), fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue

    # Last resort: extract YYYY-MM-DD pattern
    m = re.search(r"(\d{4}-\d{2}-\d{2})", date_str)
    if m:
        return m.group(1)

    return None


def classify(title: str, description: str = "") -> tuple[str, bool]:
    """Classify a news item. Returns (category, is_funding_related)."""
    text = f"{title} {description}"

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
    m = re.search(r"([\d,.]+\s*億円)", text)
    if m:
        return m.group(1)
    m = re.search(r"([\d,.]+\s*万円)", text)
    if m:
        return m.group(1)
    return None


def resolve_google_news_url(gnews_url: str) -> str:
    """Attempt to resolve Google News redirect URL to the original.

    Google News RSS links go through a redirect. We try to follow it
    to get the actual article URL. Falls back to the Google News URL.
    """
    try:
        req = urllib.request.Request(gnews_url)
        req.add_header("User-Agent", USER_AGENT)
        # Follow redirect manually to capture the final URL
        opener = urllib.request.build_opener(urllib.request.HTTPRedirectHandler)
        resp = opener.open(req, timeout=10)
        return resp.url
    except Exception:
        return gnews_url


# ================================================================
# Source 1: Google News RSS
# ================================================================

def fetch_google_news_rss(query: str) -> list[dict]:
    """Fetch news items from Google News RSS for a given query."""
    encoded = urllib.parse.quote(query)
    rss_url = GOOGLE_NEWS_RSS_BASE.format(query=encoded)

    items = []

    if HAS_FEEDPARSER:
        # Use feedparser for robust RSS parsing
        feed = feedparser.parse(rss_url)
        for entry in feed.entries:
            title = entry.get("title", "")
            link = entry.get("link", "")
            pub_date = entry.get("published", "")
            source_name = ""
            if hasattr(entry, "source") and hasattr(entry.source, "title"):
                source_name = entry.source.title

            # Extract description text (strip HTML)
            desc = entry.get("description", "")
            desc = re.sub(r"<[^>]+>", "", desc).strip()

            items.append({
                "title": title,
                "url": link,
                "published": pub_date,
                "description": desc,
                "source_name": source_name,
            })
    else:
        # Fallback: parse RSS XML with regex
        xml_data = make_request(rss_url)
        if not xml_data:
            return []

        # Extract <item> blocks
        item_blocks = re.findall(r"<item>(.*?)</item>", xml_data, re.DOTALL)
        for block in item_blocks:
            title_m = re.search(r"<title>(.*?)</title>", block, re.DOTALL)
            link_m = re.search(r"<link>(.*?)</link>", block, re.DOTALL)
            pub_m = re.search(r"<pubDate>(.*?)</pubDate>", block, re.DOTALL)
            desc_m = re.search(r"<description>(.*?)</description>", block, re.DOTALL)
            source_m = re.search(r"<source[^>]*>(.*?)</source>", block, re.DOTALL)

            title = htmlmod.unescape(title_m.group(1).strip()) if title_m else ""
            link = link_m.group(1).strip() if link_m else ""
            pub = pub_m.group(1).strip() if pub_m else ""
            desc = re.sub(r"<[^>]+>", "", htmlmod.unescape(desc_m.group(1))) if desc_m else ""
            source_name = htmlmod.unescape(source_m.group(1).strip()) if source_m else ""

            items.append({
                "title": title,
                "url": link,
                "published": pub,
                "description": desc.strip(),
                "source_name": source_name,
            })

    return items


def collect_from_google_news() -> list[dict]:
    """Collect news from Google News RSS across all queries."""
    all_items = []
    seen_titles: set[str] = set()

    for i, query in enumerate(GOOGLE_NEWS_QUERIES):
        print(f"  [Google News {i+1}/{len(GOOGLE_NEWS_QUERIES)}] Query: {query}")
        items = fetch_google_news_rss(query)

        new_count = 0
        for item in items:
            title = item["title"]
            # Deduplicate by title (Google News URLs are redirects)
            if not title or title in seen_titles:
                continue
            seen_titles.add(title)

            # Resolve redirect URL to get the actual article URL
            original_url = resolve_google_news_url(item["url"])

            category, is_funding = classify(title, item.get("description", ""))
            amount = extract_amount(title, item.get("description", ""))

            all_items.append({
                "title": title,
                "url": original_url,
                "published_at": parse_pub_date(item.get("published", "")),
                "description": item.get("description", ""),
                "company_name": item.get("source_name", ""),
                "category": category,
                "is_funding_related": is_funding,
                "amount": amount,
                "collection_source": "google_news",
            })
            new_count += 1

        print(f"    -> {new_count} unique items")
        time.sleep(RATE_LIMIT)

    return all_items


# ================================================================
# Source 2: PR TIMES HTML scraping
# ================================================================

def fetch_prtimes_search_page(url: str) -> list[dict]:
    """Fetch and parse PR TIMES search results via HTML scraping.

    Uses BeautifulSoup if available, falls back to regex parsing.
    Same proven approach as collect_funding_prtimes.py.
    """
    html_data = make_request(url)
    if not html_data:
        return []

    items = []

    if HAS_BS4:
        soup = BeautifulSoup(html_data, "html.parser")
        articles = soup.find_all("article")
        for art in articles:
            # Title
            h3 = art.find("h3")
            title = h3.get_text(strip=True) if h3 else ""

            # Link
            link_tag = art.find("a", href=re.compile(r"/main/html/rd/p/"))
            link = f"https://prtimes.jp{link_tag['href']}" if link_tag else ""

            # Date
            time_tag = art.find("time")
            date_str = time_tag.get_text(strip=True) if time_tag else ""

            # Description
            desc_tag = art.find(class_=re.compile(r"description"))
            description = desc_tag.get_text(strip=True) if desc_tag else ""

            # Company
            company_tag = art.find(class_=re.compile(r"company"))
            company = company_tag.get_text(strip=True) if company_tag else ""

            if title:
                items.append({
                    "title": title,
                    "url": link,
                    "published": date_str,
                    "description": description,
                    "company_name": company,
                })
    else:
        # Regex fallback (same as collect_funding_prtimes.py)
        articles = re.findall(r"<article[^>]*>(.*?)</article>", html_data, re.DOTALL)
        for art in articles:
            title_m = re.search(r"<h3[^>]*>(.*?)</h3>", art, re.DOTALL)
            title = re.sub(r"<[^>]+>", "", title_m.group(1)).strip() if title_m else ""
            title = htmlmod.unescape(title)

            link_m = re.search(r'href="(/main/html/rd/p/[^"]+)"', art)
            link = f"https://prtimes.jp{link_m.group(1)}" if link_m else ""

            date_m = re.search(r"<time[^>]*>(.*?)</time>", art, re.DOTALL)
            date_str = re.sub(r"<[^>]+>", "", date_m.group(1)).strip() if date_m else ""

            desc_m = re.search(
                r'class="[^"]*description[^"]*"[^>]*>(.*?)</(?:div|p|span)>',
                art, re.DOTALL,
            )
            description = re.sub(r"<[^>]+>", "", desc_m.group(1)).strip() if desc_m else ""
            description = htmlmod.unescape(description)

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


def fetch_prtimes_all_pages(keyword: str, max_pages: int = 3) -> list[dict]:
    """Fetch multiple pages of PR TIMES search results."""
    all_items = []
    encoded = urllib.parse.quote(keyword)

    for page in range(1, max_pages + 1):
        if page == 1:
            url = PRTIMES_SEARCH_URL.format(keyword=encoded)
        else:
            url = PRTIMES_SEARCH_PAGED.format(keyword=encoded, page=page)

        items = fetch_prtimes_search_page(url)
        if not items:
            break

        all_items.extend(items)
        print(f"    Page {page}: {len(items)} items")
        time.sleep(RATE_LIMIT)

    return all_items


def collect_from_prtimes(max_pages: int = 3) -> list[dict]:
    """Collect STARTUP DB-related press releases from PR TIMES."""
    all_items = []
    seen_urls: set[str] = set()

    for i, kw in enumerate(PRTIMES_SEARCH_KEYWORDS):
        print(f"  [PR TIMES {i+1}/{len(PRTIMES_SEARCH_KEYWORDS)}] Search: {kw}")
        items = fetch_prtimes_all_pages(kw, max_pages=max_pages)

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
                "collection_source": "prtimes_search",
            })
            new_count += 1

        print(f"    -> {new_count} unique items")
        time.sleep(RATE_LIMIT)

    return all_items


# ================================================================
# Database operations
# ================================================================

def store_releases(db_path: Path, releases: list[dict]) -> dict:
    """Store collected items into the press_releases table.

    Returns stats dict with counts.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    # Load existing url_hashes for dedup
    existing = set(
        r[0] for r in conn.execute(
            "SELECT url_hash FROM press_releases WHERE source = 'startupdb'"
        ).fetchall()
    )
    # Also check all source_urls to avoid cross-source duplicates
    existing_urls = set(
        r[0] for r in conn.execute(
            "SELECT source_url FROM press_releases"
        ).fetchall()
    )

    stored = 0
    skipped = 0

    for item in releases:
        url = item["url"]
        if not url:
            skipped += 1
            continue

        h = url_hash(url)
        if h in existing or url in existing_urls:
            skipped += 1
            continue

        title = item["title"]
        category = item["category"]
        is_funding = item["is_funding_related"]

        extracted = {}
        amount = item.get("amount")
        if amount:
            extracted["amount_raw"] = amount
        if item.get("company_name"):
            extracted["company_name"] = item["company_name"]
        if item.get("collection_source"):
            extracted["collection_source"] = item["collection_source"]

        conn.execute("""
            INSERT INTO press_releases
                (title, body_text, source, source_url, url_hash, published_at,
                 company_name, organization_id, category, is_funding_related,
                 extracted_data, confidence_score, collected_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
        """, (
            title,
            item.get("description", ""),
            "startupdb",  # source identifier
            url,
            h,
            item.get("published_at"),
            item.get("company_name", ""),
            None,
            category,
            1 if is_funding else 0,
            json.dumps(extracted, ensure_ascii=False) if extracted else "{}",
            0.7,  # Moderate confidence — aggregated from external sources
        ))

        existing.add(h)
        existing_urls.add(url)
        stored += 1

    conn.commit()

    # Summary stats
    total = conn.execute("SELECT COUNT(*) FROM press_releases").fetchone()[0]
    funding = conn.execute(
        "SELECT COUNT(*) FROM press_releases WHERE is_funding_related = 1"
    ).fetchone()[0]
    startupdb_count = conn.execute(
        "SELECT COUNT(*) FROM press_releases WHERE source = 'startupdb'"
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
        "startupdb_in_db": startupdb_count,
        "total_in_db": total,
        "funding_in_db": funding,
        "by_source": by_source,
        "by_category": by_cat,
    }


# ================================================================
# Main pipeline
# ================================================================

def collect_startupdb_news(
    max_pages: int = 3,
    db_path: Path | None = None,
    dry_run: bool = False,
) -> dict:
    """Full collection pipeline for STARTUP DB funding news.

    Args:
        max_pages: Max pages per PR TIMES search keyword.
        db_path: Database path (defaults to investment_signal_v2.db).
        dry_run: If True, collect but don't write to DB.

    Returns summary stats.
    """
    db = db_path or DB_PATH
    start = datetime.now()

    print("=== STARTUP DB News Collector ===")
    print(f"  Google News queries: {len(GOOGLE_NEWS_QUERIES)}")
    print(f"  PR TIMES keywords: {len(PRTIMES_SEARCH_KEYWORDS)}")
    print(f"  Max pages/keyword: {max_pages}")
    print(f"  DB: {db}")
    print(f"  Dry run: {dry_run}")
    print()

    all_items: list[dict] = []
    seen_titles: set[str] = set()

    # --- Source 1: Google News RSS ---
    print("[Source 1] Google News RSS")
    gnews_items = collect_from_google_news()
    for item in gnews_items:
        if item["title"] not in seen_titles:
            seen_titles.add(item["title"])
            all_items.append(item)
    print(f"  Google News total: {len(gnews_items)}\n")

    # --- Source 2: PR TIMES HTML scraping ---
    print("[Source 2] PR TIMES search")
    prtimes_items = collect_from_prtimes(max_pages=max_pages)
    for item in prtimes_items:
        if item["title"] not in seen_titles:
            seen_titles.add(item["title"])
            all_items.append(item)
    print(f"  PR TIMES total: {len(prtimes_items)}\n")

    # Summary before storing
    print(f"Total collected (deduplicated): {len(all_items)}")
    funding_count = sum(1 for x in all_items if x["is_funding_related"])
    print(f"Funding-related: {funding_count}")

    cats = {}
    for item in all_items:
        cats[item["category"]] = cats.get(item["category"], 0) + 1
    print(f"Categories: {cats}")

    if dry_run:
        elapsed = (datetime.now() - start).total_seconds()
        print(f"\n=== Dry Run Complete ({elapsed:.0f}s) ===")
        print("  No data written to database.")
        return {
            "collected": len(all_items),
            "funding_collected": funding_count,
            "categories": cats,
            "elapsed_seconds": round(elapsed, 1),
            "dry_run": True,
        }

    # Store in database
    stats = store_releases(db, all_items)

    elapsed = (datetime.now() - start).total_seconds()
    stats["collected"] = len(all_items)
    stats["elapsed_seconds"] = round(elapsed, 1)

    print(f"\n=== Collection Complete ({elapsed:.0f}s) ===")
    print(f"  Collected: {len(all_items)}")
    print(f"  Stored (new): {stats['stored']}")
    print(f"  Skipped (dup): {stats['skipped']}")
    print(f"  STARTUP DB in DB: {stats['startupdb_in_db']}")
    print(f"  Total in DB: {stats['total_in_db']}")
    print(f"  Funding in DB: {stats['funding_in_db']}")
    print(f"  By source: {stats['by_source']}")
    print(f"  By category: {stats['by_category']}")

    return stats


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Collect STARTUP DB funding news from Google News RSS and PR TIMES"
    )
    parser.add_argument(
        "--max-pages", type=int, default=3,
        help="Max pages to scrape per PR TIMES keyword (default: 3)",
    )
    parser.add_argument(
        "--db", type=str, default=str(DB_PATH),
        help="Database path",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Collect data but don't write to database",
    )
    args = parser.parse_args()

    result = collect_startupdb_news(
        max_pages=args.max_pages,
        db_path=Path(args.db),
        dry_run=args.dry_run,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
