#!/usr/bin/env python3
"""
Backfill funding data via DuckDuckGo news search.

Searches for Japanese startup funding news using multiple keywords
and date ranges, then extracts structured data via Claude API.

Usage:
    python3 scripts/collect_ddg_funding.py
    python3 scripts/collect_ddg_funding.py --max-results 50
    python3 scripts/collect_ddg_funding.py --dry-run
    python3 scripts/collect_ddg_funding.py --backfill
    python3 scripts/collect_ddg_funding.py --backfill --start-year 2023 --end-year 2023 --end-month 3
"""
from __future__ import annotations

import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from db.models_v2 import (
    get_conn, insert_funding_round, round_exists,
    get_stats, insert_press_release, url_hash,
)
from extractor.claude_extractor import extract_investment_info

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# Base search queries (year-agnostic, shared with Google News collector)
SEARCH_QUERIES = [
    # --- Round stage ---
    "スタートアップ 資金調達 億円",
    "ベンチャー シリーズA 調達",
    "ベンチャー シリーズB 調達",
    "ベンチャー シリーズC 調達",
    "スタートアップ シード 調達",
    "スタートアップ プレシリーズA 調達",
    "シリーズD 調達",
    "シリーズE 調達",
    "スタートアップ IPO 上場",
    "第三者割当増資 スタートアップ",
    # --- Sector ---
    "AI スタートアップ 資金調達",
    "ヘルスケア スタートアップ 資金調達",
    "クリーンテック 資金調達",
    "フィンテック 資金調達 日本",
    "SaaS スタートアップ 調達",
    "ディープテック 資金調達",
    "宇宙 スタートアップ 資金調達",
    "バイオテック 資金調達 日本",
    "ロボティクス 資金調達",
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

# Rate limits
DDG_RATE_LIMIT = 3.0  # seconds between searches (single sweep)
DDG_BACKFILL_RATE_LIMIT = 8.0  # more conservative for backfill
EXTRACT_RATE_LIMIT = 0.5  # seconds between Claude API calls

# Keyword filter applied before Claude API call
FUNDING_KEYWORDS = [
    "資金調達", "調達", "億円", "万円", "出資", "増資",
    "シリーズ", "シード", "IPO", "上場", "ファンド",
    "投資", "VC", "Series",
]


def search_ddg_news(
    query: str,
    max_results: int = 30,
    timelimit: str | None = None,
) -> list[dict]:
    """Search DuckDuckGo news for funding articles."""
    try:
        from duckduckgo_search import DDGS
    except ImportError:
        logger.error("duckduckgo-search not installed. Run: pip install duckduckgo-search")
        return []

    results = []
    try:
        with DDGS() as ddgs:
            kwargs = {"region": "jp-jp", "max_results": max_results}
            if timelimit:
                kwargs["timelimit"] = timelimit
            for r in ddgs.news(query, **kwargs):
                results.append({
                    "title": r.get("title", ""),
                    "body": r.get("body", ""),
                    "url": r.get("url", ""),
                    "date": r.get("date", ""),
                    "source": r.get("source", ""),
                })
    except Exception as e:
        logger.warning(f"DDG search failed for '{query[:50]}': {e}")

    return results


def process_article(conn, article: dict, dry_run: bool = False) -> bool:
    """Process a single news article. Returns True if stored."""
    url = article.get("url", "")
    if not url:
        return False

    # Skip if already processed (check both tables)
    if round_exists(conn, url):
        return False

    h = url_hash(url)
    if conn.execute("SELECT 1 FROM press_releases WHERE url_hash = ?", (h,)).fetchone():
        return False

    title = article.get("title", "")
    body = article.get("body", "")
    text = f"{title}\n\n{body}" if body else title

    # Quick keyword filter before calling Claude API
    if not any(kw in text for kw in FUNDING_KEYWORDS):
        return False

    # Extract via Claude API
    try:
        data = extract_investment_info(title=title, text=text)
    except Exception as e:
        logger.warning(f"Extraction failed: {e}")
        return False

    time.sleep(EXTRACT_RATE_LIMIT)

    if data is None or data.get("is_funding") is False:
        # Still save as press release for coverage
        if not dry_run:
            insert_press_release(conn, {
                "title": title,
                "body_text": body,
                "source": "ddg_news",
                "source_url": url,
                "published_at": _parse_date(article.get("date")),
                "company_name": None,
                "category": "other",
                "is_funding_related": 0,
                "confidence_score": 0.3,
                "data_source_name": "claude_extracted",
            })
        return False

    # Parse amount
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


def _parse_date(date_str: str | None) -> str | None:
    """Try to parse various date formats to YYYY-MM-DD."""
    if not date_str:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(date_str[:19], fmt[:len(date_str[:19])]).strftime("%Y-%m-%d")
        except (ValueError, IndexError):
            continue
    import re
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})", date_str)
    if m:
        return m.group(0)
    return None


def count_press_releases_for_month(conn, ym: str, source: str) -> int:
    """Count press_releases for a given YYYY-MM and source."""
    row = conn.execute(
        """SELECT COUNT(*) FROM press_releases
           WHERE source = ?
             AND strftime('%Y-%m', published_at) = ?""",
        (source, ym),
    ).fetchone()
    return row[0] if row else 0


def generate_months(
    start_year: int = 2023,
    start_month: int = 1,
    end_year: int = 2026,
    end_month: int = 4,
) -> list[tuple[int, int]]:
    """Generate (year, month) pairs."""
    result = []
    y, m = start_year, start_month
    while (y, m) <= (end_year, end_month):
        result.append((y, m))
        m += 1
        if m > 12:
            m = 1
            y += 1
    return result


def run_backfill(args, conn):
    """Run monthly date-embedded backfill."""
    months = generate_months(
        args.start_year, args.start_month,
        args.end_year, args.end_month,
    )
    base_queries = SEARCH_QUERIES
    total_months = len(months)
    total_queries = len(base_queries)
    rate = args.rate_limit or DDG_BACKFILL_RATE_LIMIT

    logger.info(
        f"BACKFILL START: {total_months} months × {total_queries} queries "
        f"({args.start_year}-{args.start_month:02d} to "
        f"{args.end_year}-{args.end_month:02d}), "
        f"rate_limit={rate}s"
    )

    total_new = 0
    total_articles = 0

    for month_idx, (year, month) in enumerate(months):
        ym = f"{year}-{month:02d}"

        # Resumability: skip months already collected
        already = count_press_releases_for_month(conn, ym, source="ddg_news")
        if already >= args.min_collected:
            logger.info(
                f"[{month_idx+1}/{total_months}] {ym}: "
                f"skip ({already} already collected)"
            )
            continue

        month_new = 0
        month_articles = 0

        for q_idx, base_query in enumerate(base_queries):
            # Embed year-month in query for historical targeting
            query = f"{base_query} {year}年{month}月"
            logger.info(
                f"[{month_idx+1}/{total_months}] {ym} "
                f"q[{q_idx+1}/{total_queries}]: {base_query[:40]}"
            )

            try:
                articles = search_ddg_news(
                    query,
                    max_results=args.max_results,
                )
            except Exception as e:
                logger.warning(f"DDG error for '{query[:40]}': {e}")
                time.sleep(rate * 3)  # Back off on error
                continue

            month_articles += len(articles)

            for article in articles:
                stored = process_article(conn, article, dry_run=args.dry_run)
                if stored:
                    month_new += 1
                    total_new += 1

            if not args.dry_run:
                conn.commit()

            time.sleep(rate)

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
    total_processed = 0

    for i, query in enumerate(queries):
        logger.info(f"[{i+1}/{len(queries)}] Searching: {query}")
        articles = search_ddg_news(query, max_results=args.max_results)
        logger.info(f"  Found {len(articles)} articles")

        for article in articles:
            total_processed += 1
            stored = process_article(conn, article, dry_run=args.dry_run)
            if stored:
                total_new += 1

        if not args.dry_run:
            conn.commit()

        time.sleep(DDG_RATE_LIMIT)

    logger.info(f"\nDone. Processed: {total_processed}, New rounds: {total_new}")
    return total_new


def main():
    import argparse
    parser = argparse.ArgumentParser(
        description="Collect funding data via DuckDuckGo news search"
    )
    parser.add_argument("--max-results", type=int, default=30,
                        help="Max results per query")
    parser.add_argument("--dry-run", action="store_true",
                        help="Skip DB writes, log what would be inserted")
    parser.add_argument("--queries", nargs="+", default=None,
                        help="Custom search queries (single sweep mode only)")
    # Backfill mode
    parser.add_argument("--backfill", action="store_true",
                        help="Run monthly date-embedded backfill")
    parser.add_argument("--start-year", type=int, default=2023)
    parser.add_argument("--start-month", type=int, default=1)
    parser.add_argument("--end-year", type=int, default=2026)
    parser.add_argument("--end-month", type=int, default=4)
    parser.add_argument("--min-collected", type=int, default=5,
                        help="Skip months with >= N articles already (default: 5)")
    parser.add_argument("--rate-limit", type=float, default=None,
                        help=f"Seconds between searches (default: {DDG_BACKFILL_RATE_LIMIT} for backfill, {DDG_RATE_LIMIT} for single)")
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
