#!/usr/bin/env python3
"""
Backfill funding data via Google News RSS.

Google News provides RSS feeds for any search query, no API key needed.
Much more reliable than DuckDuckGo for bulk collection.

Usage:
    python3 scripts/collect_google_news.py
    python3 scripts/collect_google_news.py --dry-run
    python3 scripts/collect_google_news.py --backfill
    python3 scripts/collect_google_news.py --backfill --start-year 2023 --end-year 2023 --end-month 3
"""
from __future__ import annotations

import calendar
import json
import logging
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from db.models_v2 import (
    get_conn, insert_funding_round, round_exists,
    get_stats, insert_press_release, url_hash,
)
from extractor.claude_extractor import extract_investment_info

try:
    import feedparser
except ImportError:
    print("feedparser required: pip install feedparser", file=sys.stderr)
    sys.exit(1)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# Google News RSS base URL
GNEWS_RSS = "https://news.google.com/rss/search?q={query}&hl=ja&gl=JP&ceid=JP:ja"

# Base search queries (year-agnostic, used with date ranges in backfill mode)
SEARCH_QUERIES = [
    # --- Round stage ---
    "スタートアップ 資金調達 億円",
    "ベンチャー シリーズA 調達",
    "ベンチャー シリーズB 調達",
    "ベンチャー シリーズC 調達",
    "スタートアップ シード 資金調達",
    "プレシリーズA 調達",
    "シリーズD 調達",
    "シリーズE 調達",
    "スタートアップ IPO 上場承認",
    "第三者割当増資 スタートアップ",
    # --- Sector ---
    "AI スタートアップ 調達 億円",
    "ヘルスケア バイオ 資金調達",
    "クリーンテック 資金調達 億円",
    "フィンテック 調達 億円",
    "SaaS 資金調達 億円",
    "ディープテック 調達 億円",
    "ロボティクス 資金調達",
    "宇宙 スタートアップ 調達",
    "モビリティ スタートアップ 調達",
    "EdTech 資金調達",
    "不動産テック 調達",
    "フードテック 資金調達",
    "HRテック 資金調達",
    "アグリテック 調達",
    "量子コンピュータ 資金調達",
    "ブロックチェーン スタートアップ 調達",
    # --- VC names ---
    "グローバル・ブレイン 出資",
    "DCM Ventures 出資",
    "Coral Capital 出資",
    "WiL 出資",
    "ジャフコ 投資",
    "グロービス・キャピタル 出資",
    "East Ventures 出資",
    "ANRI 出資",
    "DNX Ventures 出資",
    "B Dash Ventures 出資",
    "Headline Asia 出資",
    "Eight Roads Ventures 出資",
    "SoftBank Vision Fund 出資",
    "Sony Innovation Fund 出資",
    "トヨタベンチャーズ 出資",
    "NTTドコモ・ベンチャーズ 出資",
    "SMBC ベンチャーキャピタル 出資",
    "三菱UFJキャピタル 出資",
    "みずほキャピタル 出資",
    "住友商事 スタートアップ 出資",
    # --- Regional ---
    "福岡 スタートアップ 資金調達",
    "大阪 スタートアップ 資金調達",
    "名古屋 スタートアップ 資金調達",
    "京都 スタートアップ 調達",
    "札幌 スタートアップ 調達",
    "仙台 スタートアップ 調達",
]

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
)
RATE_LIMIT = 5.0  # seconds between RSS fetches
EXTRACT_RATE_LIMIT = 0.3

# Keyword filter applied before Claude API call
FUNDING_KEYWORDS = [
    "資金調達", "調達", "億円", "万円", "出資", "増資",
    "シリーズ", "シード", "IPO", "上場", "ファンド",
    "投資", "VC", "Series",
]


def generate_date_ranges(
    start_year: int = 2023,
    start_month: int = 1,
    end_year: int = 2026,
    end_month: int = 4,
) -> list[tuple[str, str]]:
    """Generate (after_date, before_date) pairs for each calendar month."""
    result = []
    y, m = start_year, start_month
    while (y, m) <= (end_year, end_month):
        last_day = calendar.monthrange(y, m)[1]
        after = f"{y}-{m:02d}-01"
        before = f"{y}-{m:02d}-{last_day:02d}"
        result.append((after, before))
        m += 1
        if m > 12:
            m = 1
            y += 1
    return result


def count_press_releases_for_month(conn, ym: str, source: str) -> int:
    """Count press_releases for a given YYYY-MM and source."""
    row = conn.execute(
        """SELECT COUNT(*) FROM press_releases
           WHERE source = ?
             AND strftime('%Y-%m', published_at) = ?""",
        (source, ym),
    ).fetchone()
    return row[0] if row else 0


def fetch_google_news_rss(query: str) -> list[dict]:
    """Fetch articles from Google News RSS."""
    encoded = urllib.parse.quote(query)
    url = GNEWS_RSS.format(query=encoded)

    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = resp.read()
    except Exception as e:
        logger.warning(f"RSS fetch failed for '{query[:50]}': {e}")
        return []

    feed = feedparser.parse(data)
    articles = []
    for entry in feed.entries:
        link = entry.get("link", "")
        articles.append({
            "title": entry.get("title", ""),
            "url": link,
            "date": entry.get("published", ""),
            "summary": entry.get("summary", ""),
            "source": entry.get("source", {}).get("title", ""),
        })

    return articles


def _parse_date(date_str: str | None) -> str | None:
    """Parse various date formats."""
    if not date_str:
        return None
    import re
    for fmt in (
        "%a, %d %b %Y %H:%M:%S %Z",
        "%a, %d %b %Y %H:%M:%S %z",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%d",
    ):
        try:
            return datetime.strptime(date_str.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})", date_str)
    if m:
        return m.group(0)
    return None


def process_article(conn, article: dict, dry_run: bool = False) -> bool:
    """Process a single article. Returns True if new round stored."""
    url = article.get("url", "")
    if not url:
        return False

    if round_exists(conn, url):
        return False

    # Check press_releases too
    h = url_hash(url)
    if conn.execute("SELECT 1 FROM press_releases WHERE url_hash = ?", (h,)).fetchone():
        return False

    title = article.get("title", "")
    summary = article.get("summary", "")
    text = f"{title}\n\n{summary}" if summary else title

    # Quick keyword filter before calling Claude API
    if not any(kw in text for kw in FUNDING_KEYWORDS):
        return False

    try:
        data = extract_investment_info(title=title, text=text)
    except Exception as e:
        logger.warning(f"Extraction failed for '{title[:40]}': {e}")
        return False

    time.sleep(EXTRACT_RATE_LIMIT)

    if data is None or data.get("is_funding") is False:
        # Save as non-funding press release
        if not dry_run:
            insert_press_release(conn, {
                "title": title,
                "body_text": summary,
                "source": "google_news",
                "source_url": url,
                "published_at": _parse_date(article.get("date")),
                "company_name": None,
                "category": "other",
                "is_funding_related": 0,
                "confidence_score": 0.3,
                "data_source_name": "claude_extracted",
            })
        return False

    amount_jpy = data.get("amount_jpy")
    if amount_jpy is not None:
        try:
            amount_jpy = int(amount_jpy)
        except (ValueError, TypeError):
            amount_jpy = None

    investors = data.get("investors") or []
    if not isinstance(investors, list):
        investors = []

    announced = data.get("announced_date") or _parse_date(article.get("date"))

    if dry_run:
        logger.info(
            f"  [DRY] {data.get('company_name')}: "
            f"{data.get('amount_raw')} ({data.get('round_type')})"
        )
        return True

    round_id = insert_funding_round(
        conn=conn,
        company_name=data.get("company_name") or "Unknown",
        investors=investors,
        amount_jpy=amount_jpy,
        amount_raw=data.get("amount_raw") or "",
        round_type=data.get("round_type") or "unknown",
        announced_date=announced or "",
        source_url=url,
        source_title=title,
        sector=data.get("sector") or "",
        pestle_category=data.get("pestle_category") or "",
        confidence=data.get("confidence") or "medium",
        description=data.get("company_description") or "",
        data_source_name="claude_extracted",
    )

    if round_id:
        logger.info(
            f"  NEW: {data.get('company_name')}: "
            f"{data.get('amount_raw')} ({data.get('round_type')}) "
            f"[round_id={round_id}]"
        )
        return True
    return False


def run_backfill(args, conn):
    """Run monthly date-range backfill."""
    date_ranges = generate_date_ranges(
        args.start_year, args.start_month,
        args.end_year, args.end_month,
    )
    base_queries = SEARCH_QUERIES
    total_months = len(date_ranges)
    total_queries = len(base_queries)

    logger.info(
        f"BACKFILL START: {total_months} months × {total_queries} queries "
        f"({args.start_year}-{args.start_month:02d} to "
        f"{args.end_year}-{args.end_month:02d})"
    )

    total_new = 0
    total_articles = 0

    for month_idx, (after, before) in enumerate(date_ranges):
        ym = after[:7]  # "YYYY-MM"

        # Resumability: skip months already collected
        already = count_press_releases_for_month(conn, ym, source="google_news")
        if already >= args.min_collected:
            logger.info(
                f"[{month_idx+1}/{total_months}] {ym}: "
                f"skip ({already} already collected)"
            )
            continue

        month_new = 0
        month_articles = 0

        for q_idx, base_query in enumerate(base_queries):
            query = f"{base_query} after:{after} before:{before}"
            logger.info(
                f"[{month_idx+1}/{total_months}] {ym} "
                f"q[{q_idx+1}/{total_queries}]: {base_query[:40]}"
            )

            articles = fetch_google_news_rss(query)
            month_articles += len(articles)

            for article in articles:
                stored = process_article(conn, article, dry_run=args.dry_run)
                if stored:
                    month_new += 1
                    total_new += 1

            if not args.dry_run:
                conn.commit()

            time.sleep(args.rate_limit)

        total_articles += month_articles
        logger.info(
            f"  {ym} done: +{month_new} new, "
            f"{month_articles} articles (running total: {total_new})"
        )

    logger.info(
        f"\nBACKFILL DONE. "
        f"Total articles: {total_articles}, New rounds: {total_new}"
    )
    return total_new


def run_single(args, conn):
    """Run a single sweep (legacy mode)."""
    queries = args.queries or SEARCH_QUERIES
    total_new = 0
    total_articles = 0

    for i, query in enumerate(queries):
        logger.info(f"[{i+1}/{len(queries)}] Searching: {query}")
        articles = fetch_google_news_rss(query)
        logger.info(f"  Found {len(articles)} articles")
        total_articles += len(articles)

        for article in articles:
            stored = process_article(conn, article, dry_run=args.dry_run)
            if stored:
                total_new += 1

        if not args.dry_run:
            conn.commit()

        time.sleep(RATE_LIMIT)

    logger.info(f"\nDone. Total articles: {total_articles}, New rounds: {total_new}")
    return total_new


def main():
    import argparse
    parser = argparse.ArgumentParser(
        description="Collect funding data via Google News RSS"
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Skip DB writes, log what would be inserted")
    parser.add_argument("--queries", nargs="+", default=None,
                        help="Custom search queries (single sweep mode only)")
    # Backfill mode
    parser.add_argument("--backfill", action="store_true",
                        help="Run monthly date-range backfill")
    parser.add_argument("--start-year", type=int, default=2023)
    parser.add_argument("--start-month", type=int, default=1)
    parser.add_argument("--end-year", type=int, default=2026)
    parser.add_argument("--end-month", type=int, default=4)
    parser.add_argument("--min-collected", type=int, default=5,
                        help="Skip months with >= N articles already (default: 5)")
    parser.add_argument("--rate-limit", type=float, default=RATE_LIMIT,
                        help=f"Seconds between RSS fetches (default: {RATE_LIMIT})")
    args = parser.parse_args()

    conn = get_conn()

    try:
        if args.backfill:
            run_backfill(args, conn)
        else:
            run_single(args, conn)

        if not args.dry_run:
            stats = get_stats(conn)
            logger.info(f"DB stats: {json.dumps(stats, indent=2)}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
