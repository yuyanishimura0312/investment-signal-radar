#!/usr/bin/env python3
"""Collect historical VC/funding press releases from PR TIMES (past 5 years).

Strategy:
  PR TIMES search doesn't support deep pagination or date filtering.
  Instead, we use Web Search with year+keyword combinations to discover
  prtimes.jp URLs, then fetch article details directly from each URL.

  Coverage matrix: 17 funding keywords × 5 years × 2 halves = 170 searches
  Each search returns ~10 results → estimated 1,000-1,500 unique URLs.

Usage:
    python3 scripts/collect_historical_funding.py
    python3 scripts/collect_historical_funding.py --year 2023
    python3 scripts/collect_historical_funding.py --dry-run
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

REQUEST_TIMEOUT = 20
RATE_LIMIT = 2.0
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

# ================================================================
# Search keyword matrix for historical collection
# ================================================================

# ================================================================
# Granular search keywords for maximum coverage
# ================================================================
# Strategy: PR TIMES search returns ~40 results per query (no deep pagination).
# To collect 5 years of data, we use highly specific keyword combinations:
#   - Round types × year
#   - Major VC names (to find portfolio investment PRs)
#   - Amount ranges (e.g. "X億円 調達")
#   - Company-specific terms

# Round-type keywords (combined with years in search)
ROUND_KEYWORDS = [
    "資金調達",
    "シリーズA 資金調達",
    "シリーズB 資金調達",
    "シリーズC 資金調達",
    "シリーズD 資金調達",
    "シードラウンド 資金調達",
    "プレシリーズA 資金調達",
    "プレシード 資金調達",
    "第三者割当増資 スタートアップ",
    "エクイティファイナンス",
]

# Major Japanese VCs (searching by VC name finds their portfolio PRs)
VC_KEYWORDS = [
    "JAFCO 出資",
    "グロービス・キャピタル 出資",
    "SBIインベストメント 出資",
    "ジェネシア 出資",
    "グローバル・ブレイン 出資",
    "DNX Ventures 出資",
    "Coral Capital 出資",
    "East Ventures 出資",
    "ANRI 出資",
    "インキュベイトファンド 出資",
    "DBJキャピタル 出資",
    "ニッセイ・キャピタル 出資",
    "WiL 出資",
    "B Dash Ventures 出資",
    "YJ Capital 出資",
    "みずほキャピタル 出資",
    "三井住友海上キャピタル 出資",
    "伊藤忠テクノロジーベンチャーズ 出資",
]

# Amount-based keywords
AMOUNT_KEYWORDS = [
    "1億円 資金調達",
    "3億円 資金調達",
    "5億円 資金調達",
    "10億円 資金調達",
    "20億円 資金調達",
    "50億円 資金調達",
    "100億円 資金調達",
]

# Exit/M&A keywords
EXIT_KEYWORDS = [
    "IPO 新規上場 スタートアップ",
    "M&A 買収 スタートアップ",
    "事業譲渡 テック",
    "株式公開 スタートアップ",
]

# Ecosystem keywords
ECOSYSTEM_KEYWORDS = [
    "アクセラレーター 採択 スタートアップ",
    "デモデイ スタートアップ",
    "CVC 出資 スタートアップ",
    "ベンチャーキャピタル ファンド 組成",
]

# Combine all keywords
ALL_SEARCH_KEYWORDS = ROUND_KEYWORDS + VC_KEYWORDS + AMOUNT_KEYWORDS + EXIT_KEYWORDS + ECOSYSTEM_KEYWORDS

# Years to cover (5 years back from 2026)
DEFAULT_YEARS = [2021, 2022, 2023, 2024, 2025, 2026]

# Classification keywords
FUNDING_KW = [
    "資金調達", "シリーズA", "シリーズB", "シリーズC", "シリーズD", "シリーズE",
    "シードラウンド", "プレシリーズ", "プレシード", "エクイティ調達",
    "リード投資", "第三者割当増資", "調達額", "億円を調達", "万円を調達",
    "出資", "増資", "ラウンド",
]
EXIT_KW = ["IPO", "上場", "M&A", "買収", "事業譲渡", "株式公開"]
PARTNERSHIP_KW = ["業務提携", "資本提携", "資本業務提携", "パートナーシップ", "協業"]
ACCELERATOR_KW = ["アクセラレーター", "インキュベーター", "採択", "デモデイ"]


def parse_pub_date(date_str: str) -> str | None:
    """Parse date string to ISO format (handles Japanese dates)."""
    if not date_str:
        return None
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


def classify(title: str, body: str = "") -> tuple[str, bool]:
    """Classify press release. Returns (category, is_funding_related)."""
    text = f"{title} {body}"
    if any(kw in text for kw in FUNDING_KW):
        return "funding", True
    if any(kw in text for kw in EXIT_KW):
        return "exit", True
    if any(kw in text for kw in PARTNERSHIP_KW):
        return "partnership", False
    if any(kw in text for kw in ACCELERATOR_KW):
        return "accelerator", False
    return "other", False


def extract_amount(text: str) -> str | None:
    """Extract funding amount from text."""
    m = re.search(r"([\d,.]+\s*億円)", text)
    if m:
        return m.group(1)
    m = re.search(r"([\d,.]+\s*万円)", text)
    if m:
        return m.group(1)
    return None


# ================================================================
# Web Search via DuckDuckGo (fallback to direct URL extraction)
# ================================================================

def fetch_search_page(url: str) -> list[dict]:
    """Fetch and parse PR TIMES search results page via HTML scraping.

    Same proven approach as sangaku-matcher-v2/fetch_press_releases.py.
    """
    try:
        req = urllib.request.Request(url)
        req.add_header("User-Agent", USER_AGENT)
        with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
            html_data = resp.read().decode("utf-8", errors="replace")

        items = []
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
    except Exception as e:
        print(f"    Fetch error: {e}", file=sys.stderr)
        return []


def search_web(query: str, max_results: int = 30) -> list[dict]:
    """Search via DuckDuckGo and return prtimes.jp results.

    Note: DuckDuckGo's site: filter doesn't work reliably.
    Instead, we search with 'prtimes.jp' as part of the query
    and filter results by domain.
    """
    try:
        from duckduckgo_search import DDGS
        # Add prtimes.jp to query instead of using site: filter
        search_query = f"prtimes.jp {query}"
        with DDGS() as ddgs:
            results = list(ddgs.text(search_query, max_results=max_results))
            prtimes_results = []
            for r in results:
                url = r.get("href", r.get("link", ""))
                if "prtimes.jp" in url:
                    prtimes_results.append({
                        "title": r.get("title", ""),
                        "url": url,
                        "snippet": r.get("body", r.get("snippet", "")),
                    })
            return prtimes_results
    except Exception as e:
        print(f"    DuckDuckGo error: {e}", file=sys.stderr)
        return []


def search_google_via_scrape(query: str) -> list[dict]:
    """Fallback: scrape Google search results for prtimes.jp URLs."""
    encoded = urllib.parse.quote(f"site:prtimes.jp {query}")
    url = f"https://www.google.com/search?q={encoded}&num=20"
    try:
        req = urllib.request.Request(url)
        req.add_header("User-Agent", USER_AGENT)
        with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
            html = resp.read().decode("utf-8", errors="replace")

        # Extract prtimes URLs from Google results
        urls = re.findall(r'href="(https://prtimes\.jp/main/html/rd/p/[^"]+)"', html)
        results = []
        for u in set(urls):
            results.append({"title": "", "url": u, "snippet": ""})
        return results
    except Exception as e:
        print(f"    Google scrape error: {e}", file=sys.stderr)
        return []


# ================================================================
# PR TIMES article detail fetching
# ================================================================

def fetch_article_detail(url: str) -> dict | None:
    """Fetch title, company name, date, and body from a PR TIMES article page."""
    try:
        req = urllib.request.Request(url)
        req.add_header("User-Agent", USER_AGENT)
        with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
            html = resp.read().decode("utf-8", errors="replace")

        result = {"url": url}

        # Title
        title_m = re.search(r"<h1[^>]*>(.*?)</h1>", html, re.DOTALL)
        if title_m:
            result["title"] = re.sub(r"<[^>]+>", "", title_m.group(1)).strip()
            result["title"] = htmlmod.unescape(result["title"])

        # Company name
        company_m = (
            re.search(r'class="[^"]*company-name[^"]*"[^>]*>(.*?)</(?:a|span|div)>', html, re.DOTALL)
            or re.search(r'class="[^"]*companyName[^"]*"[^>]*>(.*?)</(?:a|span|div)>', html, re.DOTALL)
        )
        if company_m:
            result["company_name"] = re.sub(r"<[^>]+>", "", company_m.group(1)).strip()
            result["company_name"] = htmlmod.unescape(result["company_name"])

        # Published date
        date_m = re.search(r"<time[^>]*datetime=\"([^\"]+)\"", html)
        if date_m:
            dt_str = date_m.group(1)[:10]  # YYYY-MM-DD
            result["published_at"] = dt_str
        else:
            date_m2 = re.search(r"(\d{4})年(\d{1,2})月(\d{1,2})日", html[:5000])
            if date_m2:
                result["published_at"] = (
                    f"{date_m2.group(1)}-{int(date_m2.group(2)):02d}-{int(date_m2.group(3)):02d}"
                )

        # Body text (first 3000 chars)
        body_div = re.search(
            r'class="[^"]*(?:rich-text|article-body|press-release-body)[^"]*"[^>]*>(.*?)</div>',
            html, re.DOTALL,
        )
        if body_div:
            body = re.sub(r"<[^>]+>", " ", body_div.group(1))
            body = re.sub(r"\s+", " ", body).strip()
            result["body_text"] = body[:3000]

        return result if "title" in result else None

    except Exception as e:
        print(f"    Article fetch error ({url[:50]}): {e}", file=sys.stderr)
        return None


# ================================================================
# Database operations
# ================================================================

def get_existing_urls(db_path: Path) -> set[str]:
    """Get all existing source_url values from DB."""
    conn = sqlite3.connect(db_path)
    urls = set(r[0] for r in conn.execute("SELECT source_url FROM press_releases").fetchall())
    conn.close()
    return urls


def store_releases(db_path: Path, releases: list[dict]) -> dict:
    """Store collected releases in DB."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    existing = set(r[0] for r in conn.execute("SELECT source_url FROM press_releases").fetchall())
    stored = 0
    skipped = 0

    for pr in releases:
        url = pr.get("url", "")
        if not url or url in existing:
            skipped += 1
            continue

        title = pr.get("title", "")
        body = pr.get("body_text", "")
        category, is_funding = classify(title, body)
        url_hash = hashlib.sha256(url.encode()).hexdigest()[:16]
        amount = extract_amount(f"{title} {body}")

        extracted = {}
        if amount:
            extracted["amount_raw"] = amount
        if pr.get("company_name"):
            extracted["company_name"] = pr["company_name"]
        if pr.get("search_keyword"):
            extracted["search_keyword"] = pr["search_keyword"]

        conn.execute("""
            INSERT INTO press_releases
                (title, body_text, source, source_url, url_hash, published_at,
                 company_name, organization_id, category, is_funding_related,
                 extracted_data, confidence_score, collected_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
        """, (
            title, body[:5000],
            "prtimes_historical", url, url_hash, pr.get("published_at"),
            pr.get("company_name", ""), None, category,
            1 if is_funding else 0,
            json.dumps(extracted, ensure_ascii=False) if extracted else "{}",
            0.85,
        ))
        existing.add(url)
        stored += 1

    conn.commit()

    total = conn.execute("SELECT COUNT(*) FROM press_releases").fetchone()[0]
    funding = conn.execute(
        "SELECT COUNT(*) FROM press_releases WHERE is_funding_related = 1"
    ).fetchone()[0]
    by_source = dict(conn.execute(
        "SELECT source, COUNT(*) FROM press_releases GROUP BY source"
    ).fetchall())
    conn.close()

    return {"stored": stored, "skipped": skipped, "total": total,
            "funding": funding, "by_source": by_source}


# ================================================================
# Main collection pipeline
# ================================================================

def collect_historical(
    years: list[int] | None = None,
    db_path: Path | None = None,
    dry_run: bool = False,
    fetch_details: bool = True,
) -> dict:
    """Collect historical funding press releases via web search.

    Strategy: search "site:prtimes.jp {keyword} {year}" for each combination,
    then fetch article details from discovered URLs.
    """
    target_years = years or DEFAULT_YEARS
    db = db_path or DB_PATH
    start = datetime.now()

    print(f"=== Historical Funding Collector ===")
    print(f"  Years: {target_years}")
    print(f"  Search keywords: {len(ALL_SEARCH_KEYWORDS)}")
    print(f"  Fetch details: {fetch_details}")
    print(f"  Dry run: {dry_run}")
    print()

    existing_urls = get_existing_urls(db) if not dry_run else set()
    print(f"  Existing URLs in DB: {len(existing_urls)}")

    discovered: dict[str, dict] = {}  # url -> metadata
    search_count = 0

    # Phase 1: PR TIMES HTML search (proven pattern from sangaku-matcher-v2)
    print(f"\n--- Phase 1: PR TIMES Search Scraping ---")
    print(f"  Keywords: {len(ALL_SEARCH_KEYWORDS)}, Years: {target_years}")
    total_searches = len(ALL_SEARCH_KEYWORDS) * len(target_years)
    print(f"  Total search combinations: {total_searches}")

    for year in target_years:
        year_count = 0
        for kw in ALL_SEARCH_KEYWORDS:
            search_count += 1
            # Combine keyword with year for temporal filtering
            query = f"{kw} {year}年"
            encoded = urllib.parse.quote(query)
            url = PRTIMES_SEARCH_URL.format(keyword=encoded)

            items = fetch_search_page(url)
            new = 0
            for item in items:
                pr_url = item.get("url", "")
                if not pr_url or pr_url in discovered or pr_url in existing_urls:
                    continue
                discovered[pr_url] = {
                    "url": pr_url,
                    "title": item.get("title", ""),
                    "description": item.get("description", ""),
                    "company_name": item.get("company_name", ""),
                    "published_raw": item.get("published", ""),
                    "published_at": parse_pub_date(item.get("published", "")),
                    "search_keyword": kw,
                    "search_year": year,
                }
                new += 1
                year_count += 1

            if new > 0:
                sys.stdout.write(f"\r  [{search_count}/{total_searches}] {year} '{kw[:20]}': +{new} new (total: {len(discovered)})")
                sys.stdout.flush()
            time.sleep(RATE_LIMIT)

        print(f"\n  Year {year}: +{year_count} items")

    print(f"\n  Total discovered: {len(discovered)} unique URLs")

    if dry_run:
        return {"discovered": len(discovered), "search_count": search_count}

    # Phase 2: Fetch article details for funding-related releases
    releases = list(discovered.values())

    if fetch_details:
        # Only fetch details for items that look funding-related
        funding_candidates = [
            pr for pr in releases
            if classify(pr.get("title", ""), pr.get("description", ""))[1]
            or any(kw in (pr.get("title", "") + " " + pr.get("description", ""))
                   for kw in ["資金調達", "出資", "調達", "シリーズ", "増資", "ラウンド"])
        ]
        print(f"\n--- Phase 2: Fetching Details for {len(funding_candidates)} Funding Candidates ---")

        for i, pr in enumerate(funding_candidates):
            url = pr["url"]
            if "/main/html/rd/p/" not in url:
                continue

            detail = fetch_article_detail(url)
            if detail:
                pr.update({k: v for k, v in detail.items() if v})

            if (i + 1) % 20 == 0:
                print(f"    [{i+1}/{len(funding_candidates)}] fetched")
            time.sleep(RATE_LIMIT)

        print(f"    Done: {len(funding_candidates)} articles fetched")

    # Phase 3: Store in database
    print(f"\n--- Phase 3: Storing in Database ---")
    stats = store_releases(db, releases)

    elapsed = (datetime.now() - start).total_seconds()
    stats["discovered"] = len(discovered)
    stats["search_count"] = search_count
    stats["elapsed_seconds"] = round(elapsed, 1)

    print(f"\n=== Collection Complete ({elapsed:.0f}s) ===")
    print(f"  Searches: {search_count}")
    print(f"  Discovered: {len(discovered)}")
    print(f"  Stored (new): {stats['stored']}")
    print(f"  Skipped (dup): {stats['skipped']}")
    print(f"  Total in DB: {stats['total']}")
    print(f"  Funding in DB: {stats['funding']}")
    print(f"  By source: {stats['by_source']}")

    return stats


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Collect historical VC/funding press releases from PR TIMES"
    )
    parser.add_argument(
        "--years", nargs="*", type=int,
        help="Years to collect (default: 2021-2026)",
    )
    parser.add_argument(
        "--db", type=str, default=str(DB_PATH),
        help="Database path",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Only discover URLs, don't fetch or store",
    )
    parser.add_argument(
        "--no-details", action="store_true",
        help="Skip fetching article details (store search results only)",
    )
    args = parser.parse_args()

    result = collect_historical(
        years=args.years,
        db_path=Path(args.db),
        dry_run=args.dry_run,
        fetch_details=not args.no_details,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
