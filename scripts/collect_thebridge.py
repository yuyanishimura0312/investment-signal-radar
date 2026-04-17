#!/usr/bin/env python3
"""Collect VC/funding-related articles from The Bridge (thebridge.jp) via RSS.

The Bridge is a major Japanese startup media outlet. This collector fetches
articles from their RSS feeds, classifies them by category (funding, exit,
partnership, accelerator, other), and stores them in the investment signal DB.

Usage:
    # Collect from all feeds (main + fundraising tag)
    python3 scripts/collect_thebridge.py

    # Main feed only
    python3 scripts/collect_thebridge.py --feed main

    # Fundraising tag feed only
    python3 scripts/collect_thebridge.py --feed fundraising

    # Dry run (no DB writes)
    python3 scripts/collect_thebridge.py --dry-run
"""
from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

import feedparser

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "data" / "investment_signal_v2.db"

# The Bridge RSS feeds
FEEDS = {
    "main": "https://thebridge.jp/feed",
    "fundraising": "https://thebridge.jp/tag/fundraising/feed",
}

# Source identifier stored in DB
SOURCE_NAME = "thebridge"

# ================================================================
# Classification keywords
# ================================================================

FUNDING_CLASSIFY_KW = [
    "資金調達", "シリーズA", "シリーズB", "シリーズC", "シリーズD", "シリーズE",
    "シードラウンド", "プレシリーズ", "エクイティ調達",
    "リード投資", "第三者割当増資", "調達額", "億円を調達",
    "万円を調達", "出資", "増資", "ラウンド", "fundrais",
    "seed round", "series a", "series b", "series c",
]

PARTNERSHIP_CLASSIFY_KW = [
    "業務提携", "資本提携", "資本業務提携", "パートナーシップ", "協業",
    "共同開発", "提携", "アライアンス", "合弁", "partnership",
]

EXIT_CLASSIFY_KW = [
    "IPO", "上場", "M&A", "買収", "事業譲渡", "株式公開",
    "acquisition", "ipo", "merger",
]

ACCELERATOR_CLASSIFY_KW = [
    "アクセラレーター", "インキュベーター", "採択", "デモデイ",
    "ピッチ", "スタートアップ支援", "accelerator", "incubator",
]


# ================================================================
# Classification & extraction
# ================================================================

def classify(title: str, body: str = "") -> tuple[str, bool]:
    """Classify an article into category. Returns (category, is_funding_related)."""
    text = f"{title} {body}".lower()

    if any(kw.lower() in text for kw in FUNDING_CLASSIFY_KW):
        return "funding", True

    if any(kw.lower() in text for kw in EXIT_CLASSIFY_KW):
        return "exit", True

    if any(kw.lower() in text for kw in PARTNERSHIP_CLASSIFY_KW):
        return "partnership", False

    if any(kw.lower() in text for kw in ACCELERATOR_CLASSIFY_KW):
        return "accelerator", False

    return "other", False


def extract_amount(text: str) -> str | None:
    """Try to extract funding amount from text."""
    m = re.search(r"([\d,.]+\s*億円)", text)
    if m:
        return m.group(1)
    m = re.search(r"([\d,.]+\s*万円)", text)
    if m:
        return m.group(1)
    # English amounts like "$10M", "$1.5B"
    m = re.search(r"(\$[\d,.]+\s*[MBmb](?:illion)?)", text)
    if m:
        return m.group(1)
    return None


def extract_company_name(title: str) -> str:
    """Try to extract company name from article title.

    The Bridge titles often follow patterns like:
    - "〇〇、シリーズAで5億円を調達"
    - "〇〇が資金調達"
    """
    # Pattern: leading company name before a particle (、が は を の)
    m = re.match(r"^(.+?)[、がはをの]", title)
    if m:
        name = m.group(1).strip()
        # Skip if the extracted name is too generic or too long
        if 2 <= len(name) <= 30:
            return name
    return ""


def strip_html(text: str) -> str:
    """Remove HTML tags from text."""
    return re.sub(r"<[^>]+>", "", text).strip()


def parse_pub_date(entry: dict) -> str | None:
    """Parse published date from feedparser entry to ISO format."""
    # feedparser normalizes to published_parsed (time.struct_time)
    parsed = entry.get("published_parsed")
    if parsed:
        try:
            dt = datetime(*parsed[:6])
            return dt.strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            pass

    # Fallback: try raw string
    raw = entry.get("published", "")
    if raw:
        for fmt in ["%a, %d %b %Y %H:%M:%S %z", "%Y-%m-%dT%H:%M:%S%z"]:
            try:
                dt = datetime.strptime(raw.strip(), fmt)
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue
    return None


# ================================================================
# RSS fetching
# ================================================================

def fetch_feed(feed_url: str) -> list[dict]:
    """Fetch and parse an RSS feed, returning structured article dicts."""
    print(f"  Fetching: {feed_url}")
    parsed = feedparser.parse(feed_url)

    if parsed.bozo and not parsed.entries:
        print(f"  WARNING: Feed parse error: {parsed.bozo_exception}", file=sys.stderr)
        return []

    items = []
    for entry in parsed.entries:
        title = entry.get("title", "").strip()
        link = entry.get("link", "").strip()
        if not title or not link:
            continue

        # Body text: prefer content, fall back to summary
        body = ""
        if entry.get("content"):
            body = entry.content[0].get("value", "")
        elif entry.get("summary"):
            body = entry.summary
        body = strip_html(body)

        published_at = parse_pub_date(entry)
        category, is_funding = classify(title, body)
        amount = extract_amount(f"{title} {body}")
        company = extract_company_name(title)

        items.append({
            "title": title,
            "url": link,
            "body_text": body,
            "published_at": published_at,
            "company_name": company,
            "category": category,
            "is_funding_related": is_funding,
            "amount": amount,
        })

    print(f"  -> {len(items)} articles parsed")
    return items


# ================================================================
# Database operations
# ================================================================

def store_articles(db_path: Path, articles: list[dict]) -> dict:
    """Store articles in press_releases table. Returns stats."""
    conn = sqlite3.connect(db_path)

    # Check existing url_hashes to avoid duplicates
    existing_hashes = set(
        r[0] for r in conn.execute(
            "SELECT url_hash FROM press_releases WHERE source = ?",
            (SOURCE_NAME,)
        ).fetchall()
    )

    stored = 0
    skipped = 0

    for art in articles:
        url = art["url"]
        url_hash = hashlib.sha256(url.encode()).hexdigest()[:16]

        if url_hash in existing_hashes:
            skipped += 1
            continue

        # Build extracted_data JSON
        extracted = {}
        if art.get("amount"):
            extracted["amount_raw"] = art["amount"]
        if art.get("company_name"):
            extracted["company_name"] = art["company_name"]

        # Confidence: higher for fundraising-tagged articles
        confidence = 0.8 if art["is_funding_related"] else 0.5

        conn.execute("""
            INSERT INTO press_releases
                (title, body_text, source, source_url, url_hash, published_at,
                 company_name, organization_id, category, is_funding_related,
                 extracted_data, confidence_score, collected_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
        """, (
            art["title"],
            art.get("body_text", ""),
            SOURCE_NAME,
            url,
            url_hash,
            art.get("published_at"),
            art.get("company_name", ""),
            None,
            art["category"],
            1 if art["is_funding_related"] else 0,
            json.dumps(extracted, ensure_ascii=False) if extracted else "{}",
            confidence,
        ))
        existing_hashes.add(url_hash)
        stored += 1

    conn.commit()

    # Gather stats
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

def collect_thebridge(
    feed_names: list[str] | None = None,
    db_path: Path | None = None,
    dry_run: bool = False,
) -> dict:
    """Full collection pipeline for The Bridge RSS feeds.

    Args:
        feed_names: Which feeds to fetch ("main", "fundraising", or both).
        db_path: Database path (defaults to investment_signal_v2.db).
        dry_run: If True, skip DB writes and just show what would be stored.

    Returns summary stats.
    """
    feeds_to_fetch = feed_names or list(FEEDS.keys())
    db = db_path or DB_PATH
    start = datetime.now()

    print("=== The Bridge RSS Collector ===")
    print(f"  Feeds: {feeds_to_fetch}")
    print(f"  DB: {db}")
    print(f"  Dry run: {dry_run}")
    print()

    all_items: list[dict] = []
    seen_urls: set[str] = set()

    for feed_name in feeds_to_fetch:
        url = FEEDS.get(feed_name)
        if not url:
            print(f"  Unknown feed: {feed_name}, skipping", file=sys.stderr)
            continue

        print(f"[{feed_name}]")
        items = fetch_feed(url)

        # Deduplicate across feeds
        for item in items:
            if item["url"] not in seen_urls:
                seen_urls.add(item["url"])
                all_items.append(item)

    # Articles from fundraising feed are more likely funding-related;
    # boost classification for those that came from that feed
    funding_count = sum(1 for x in all_items if x["is_funding_related"])

    print(f"\nTotal unique articles: {len(all_items)}")
    print(f"Funding-related: {funding_count}")

    # Category breakdown
    cats = {}
    for item in all_items:
        cats[item["category"]] = cats.get(item["category"], 0) + 1
    print(f"By category: {cats}")

    if dry_run:
        print("\n[DRY RUN] Skipping database writes.")
        for item in all_items:
            flag = "*" if item["is_funding_related"] else " "
            print(f"  {flag} [{item['category']:12s}] {item['title'][:60]}")
        return {
            "collected": len(all_items),
            "funding_count": funding_count,
            "categories": cats,
            "dry_run": True,
        }

    # Store in database
    stats = store_articles(db, all_items)

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
        description="Collect VC/funding articles from The Bridge RSS feeds"
    )
    parser.add_argument(
        "--feed", choices=["main", "fundraising"],
        help="Fetch only this feed (default: both)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Parse feeds but don't write to DB",
    )
    parser.add_argument(
        "--db", type=str, default=str(DB_PATH),
        help="Database path",
    )
    args = parser.parse_args()

    feed_list = [args.feed] if args.feed else None

    result = collect_thebridge(
        feed_names=feed_list,
        db_path=Path(args.db),
        dry_run=args.dry_run,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
