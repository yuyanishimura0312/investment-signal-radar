#!/usr/bin/env python3
"""Collect additional historical VC/funding press releases - extended keywords.

This script runs NEW keywords not covered by the original collect_historical_funding.py,
targeting 2021-2023 to improve coverage for those years.
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

PRTIMES_SEARCH_URL = "https://prtimes.jp/main/action.php?run=html&page=searchkey&search_word={keyword}"

REQUEST_TIMEOUT = 20
RATE_LIMIT = 2.5  # slightly longer to avoid rate limiting
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

# ================================================================
# NEW keywords not in the original script
# ================================================================

# Additional VC/investor names (not in original)
VC_KEYWORDS_NEW = [
    "Coral Capital 投資",
    "WiL 出資",
    "B Dash Ventures 投資",
    "YJ Capital 投資",
    "伊藤忠テクノロジーベンチャーズ 出資",
    "Sony Innovation Fund 出資",
    "SoftBank Vision Fund",
    "SMBC ベンチャーキャピタル",
    "三菱UFJキャピタル 出資",
    "住友商事 出資 スタートアップ",
    "電通ベンチャーズ 出資",
    "NTTドコモ・ベンチャーズ 出資",
    "トヨタ AI Ventures",
    "モバイル・インターネットキャピタル 出資",
    "日本ベンチャーキャピタル 出資",
    "ジャフコ 投資",
    "Eight Roads Ventures 出資",
    "Headline Asia 出資",
]

# Sector-specific keywords
SECTOR_KEYWORDS = [
    "AIスタートアップ 資金調達",
    "ヘルスケア スタートアップ 資金調達",
    "フィンテック 資金調達",
    "SaaS スタートアップ 資金調達",
    "バイオ スタートアップ 資金調達",
    "EdTech 資金調達",
    "HRTech 資金調達",
    "アグリテック 資金調達",
    "クリーンテック 資金調達",
    "宇宙 スタートアップ 資金調達",
    "量子コンピュータ スタートアップ 資金調達",
    "ブロックチェーン スタートアップ 資金調達",
    "ロボティクス スタートアップ 資金調達",
    "不動産テック 資金調達",
    "フードテック 資金調達",
    "モビリティ スタートアップ 資金調達",
    "医療 スタートアップ 資金調達",
    "介護 スタートアップ 資金調達",
]

# Amount-based keywords (different amounts from original)
AMOUNT_KEYWORDS_NEW = [
    "2億円 資金調達",
    "4億円 資金調達",
    "7億円 資金調達",
    "8億円 資金調達",
    "15億円 資金調達",
    "30億円 資金調達",
    "40億円 資金調達",
    "70億円 資金調達",
    "80億円 資金調達",
    "億円 シリーズA",
    "億円 シリーズB",
    "億円 シリーズC",
]

# Round/stage keywords
ROUND_KEYWORDS_NEW = [
    "シリーズE 資金調達",
    "シリーズF 資金調達",
    "プレIPO 調達",
    "レイターステージ 資金調達",
    "グロースラウンド 資金調達",
    "転換社債 スタートアップ",
    "新株予約権 スタートアップ 調達",
    "J-KISS 調達",
]

# Year-specific search terms (explicitly mentioning the year)
YEAR_KEYWORDS = [
    "2021年 資金調達 スタートアップ",
    "2022年 資金調達 スタートアップ",
    "2023年 資金調達 スタートアップ",
    "2021年 VC投資",
    "2022年 VC投資",
    "2023年 VC投資",
]

# Combine all new keywords
ALL_NEW_KEYWORDS = (
    VC_KEYWORDS_NEW
    + SECTOR_KEYWORDS
    + AMOUNT_KEYWORDS_NEW
    + ROUND_KEYWORDS_NEW
    + YEAR_KEYWORDS
)

# Target years for backfill
TARGET_YEARS = [2021, 2022, 2023]

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
    m = re.search(r"([\d,.]+\s*億円)", text)
    if m:
        return m.group(1)
    m = re.search(r"([\d,.]+\s*万円)", text)
    if m:
        return m.group(1)
    return None


def fetch_search_page(url: str) -> list[dict]:
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


def fetch_article_detail(url: str) -> dict | None:
    try:
        req = urllib.request.Request(url)
        req.add_header("User-Agent", USER_AGENT)
        with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
            html = resp.read().decode("utf-8", errors="replace")

        result = {"url": url}

        title_m = re.search(r"<h1[^>]*>(.*?)</h1>", html, re.DOTALL)
        if title_m:
            result["title"] = re.sub(r"<[^>]+>", "", title_m.group(1)).strip()
            result["title"] = htmlmod.unescape(result["title"])

        company_m = (
            re.search(r'class="[^"]*company-name[^"]*"[^>]*>(.*?)</(?:a|span|div)>', html, re.DOTALL)
            or re.search(r'class="[^"]*companyName[^"]*"[^>]*>(.*?)</(?:a|span|div)>', html, re.DOTALL)
        )
        if company_m:
            result["company_name"] = re.sub(r"<[^>]+>", "", company_m.group(1)).strip()
            result["company_name"] = htmlmod.unescape(result["company_name"])

        date_m = re.search(r"<time[^>]*datetime=\"([^\"]+)\"", html)
        if date_m:
            result["published_at"] = date_m.group(1)[:10]
        else:
            date_m2 = re.search(r"(\d{4})年(\d{1,2})月(\d{1,2})日", html[:5000])
            if date_m2:
                result["published_at"] = (
                    f"{date_m2.group(1)}-{int(date_m2.group(2)):02d}-{int(date_m2.group(3)):02d}"
                )

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


def get_existing_urls(db_path: Path) -> set[str]:
    conn = sqlite3.connect(db_path, timeout=60)
    urls = set(r[0] for r in conn.execute("SELECT source_url FROM press_releases").fetchall())
    conn.close()
    return urls


def store_releases(db_path: Path, releases: list[dict]) -> dict:
    conn = sqlite3.connect(db_path, timeout=60)  # wait up to 60s for DB lock
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
    by_year = dict(conn.execute("""
        SELECT strftime('%Y', published_at), COUNT(*)
        FROM press_releases
        WHERE source='prtimes_historical' AND published_at IS NOT NULL
        GROUP BY 1 ORDER BY 1
    """).fetchall())
    conn.close()

    return {"stored": stored, "skipped": skipped, "total": total,
            "funding": funding, "by_year": by_year}


def collect_extended(
    years: list[int] | None = None,
    db_path: Path | None = None,
    fetch_details: bool = True,
) -> dict:
    target_years = years or TARGET_YEARS
    db = db_path or DB_PATH
    start = datetime.now()

    print(f"=== Extended Historical Funding Collector ===")
    print(f"  Target years: {target_years}")
    print(f"  New keywords: {len(ALL_NEW_KEYWORDS)}")
    print(f"  Fetch details: {fetch_details}")
    print()

    existing_urls = get_existing_urls(db)
    print(f"  Existing URLs in DB: {len(existing_urls)}")

    discovered: dict[str, dict] = {}
    search_count = 0
    total_searches = len(ALL_NEW_KEYWORDS) * len(target_years)
    print(f"  Total search combinations: {total_searches}")

    print(f"\n--- Phase 1: PR TIMES Search Scraping ---")

    for year in target_years:
        year_count = 0
        for kw in ALL_NEW_KEYWORDS:
            search_count += 1
            # For year-specific keywords (already contain the year), don't add year again
            if str(year) in kw:
                query = kw
            else:
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

            sys.stdout.write(
                f"\r  [{search_count}/{total_searches}] {year} '{kw[:25]}': "
                f"+{new} new (total: {len(discovered)})"
            )
            sys.stdout.flush()
            time.sleep(RATE_LIMIT)

        print(f"\n  Year {year}: +{year_count} items")

    print(f"\n  Total discovered: {len(discovered)} unique URLs")

    # Phase 2: Fetch article details
    releases = list(discovered.values())

    if fetch_details and releases:
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
    print(f"  By year (prtimes_historical): {stats['by_year']}")

    return stats


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Collect additional historical VC/funding press releases (extended keywords)"
    )
    parser.add_argument(
        "--years", nargs="*", type=int,
        help="Years to collect (default: 2021-2023)",
    )
    parser.add_argument(
        "--db", type=str, default=str(DB_PATH),
        help="Database path",
    )
    parser.add_argument(
        "--no-details", action="store_true",
        help="Skip fetching article details",
    )
    args = parser.parse_args()

    result = collect_extended(
        years=args.years,
        db_path=Path(args.db),
        fetch_details=not args.no_details,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
