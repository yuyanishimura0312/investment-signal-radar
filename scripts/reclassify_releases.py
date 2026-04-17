#!/usr/bin/env python3
"""
Reclassify funding_releases records with category='other' using keyword matching.

Assigns new categories based on title patterns:
  - hiring: 採用/人材/CxO就任
  - product_launch: 新サービス/ローンチ/提供開始
  - expansion: 海外展開/支社開設/事業拡大
  - award: 受賞/表彰/認定
  - event: イベント/カンファレンス/セミナー
  - funding: 資金調達 (missed in original classification)
  - exit: IPO/M&A (missed in original classification)
  - partnership: 提携/協業 (missed in original classification)
  - accelerator: アクセラレーター (missed in original classification)

Usage:
  python3 scripts/reclassify_releases.py --dry-run   # preview changes
  python3 scripts/reclassify_releases.py              # apply changes
"""

import argparse
import re
import sqlite3
import sys
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "funding_database.db"

# Keyword patterns for each category.
# Order matters: first match wins, so more specific patterns come first.
# Each entry is (category, list_of_regex_patterns).
CATEGORY_RULES = [
    # --- Existing categories (catch missed ones) ---
    ("funding", [
        r"資金調達",
        r"調達を[実完]施",
        r"追加調達",
        r"シリーズ[A-FＡ-Ｆa-f]",
        r"Series\s*[A-F]",
        r"シード.*調達",
        r"プレシリーズ",
        r"ファンド.*組成",
        r"ファンド.*設立",
        r"出資",
        r"増資",
        r"億円.*調達",
        r"調達.*億円",
        r"プレシード",
        r"ベンチャーデット",
        r"投資",
        r"ラウンド",
        r"ファンド・オブ・ファンズ",
        r"\d+号ファンド",
        r"VC",
        r"CVC",
        r"ベンチャーキャピタル",
        r"エクイティ",
    ]),
    ("exit", [
        r"IPO",
        r"上場",
        r"M&A",
        r"買収",
        r"子会社化",
        r"経営統合",
        r"合併",
        r"TOB",
        r"株式公開",
        r"グロース市場",
        r"東証",
        r"マザーズ",
    ]),
    ("partnership", [
        r"提携",
        r"協業",
        r"連携を[開始]",
        r"連携.*開始",
        r"パートナーシップ",
        r"合弁",
        r"業務委託",
        r"共同開発",
        r"共同.*事業",
        r"アライアンス",
        r"サービス連携",
    ]),
    ("accelerator", [
        r"アクセラレーター",
        r"アクセラレータ[^ー]",
        r"インキュベート",
        r"インキュベーター",
        r"インキュベーション",
        r"創業支援プログラム",
        r"起業プログラム",
        r"STARTLINE",
        r"Ignition Academy",
        r"アクセラ[プ期]",
        r"バッチ.*プログラム",
    ]),

    # --- New categories ---
    ("hiring", [
        r"採用",
        r"入社",
        r"就任",
        r"CxO",
        r"CEO[にが就]",
        r"COO[にが就]",
        r"CTO[にが就]",
        r"CFO[にが就]",
        r"代表取締役.*就任",
        r"取締役.*就任",
        r"幹事に就任",
        r"新社長",
        r"人材.*募集",
        r"インターン",
        r"リクルート",
        r"エンジニア.*募集",
        r"エンジニア.*採用",
        r"組織.*拡大.*採用",
    ]),
    ("product_launch", [
        r"提供[をを]開始",
        r"提供開始",
        r"ローンチ",
        r"リリース[をしい]",
        r"新サービス",
        r"新機能",
        r"新製品",
        r"サービス開始",
        r"正式リリース",
        r"β版",
        r"ベータ版",
        r"α版",
        r"アルファ版",
        r"プロダクト.*発表",
        r"公式.*リリース",
        r"アプリ.*公開",
        r"公開しました",
        r"対応しました",
        r"開発[をし]",
        r"開発始動",
        r"新作",
        r"新色",
        r"新登場",
        r"配信.*スタート",
        r"スタート[!！。]",
        r"より.*スタート",
        r"販売開始",
        r"受付開始",
        r"導入[をし]",
        r"搭載",
        r"アップデート",
        r"バージョン",
        r"ver\.",
        r"v\d+",
        r"生成機能",
        r"ラインナップ.*公開",
    ]),
    ("expansion", [
        r"海外展開",
        r"海外進出",
        r"グローバル展開",
        r"支社.*開設",
        r"拠点.*開設",
        r"オフィス.*開設",
        r"オフィス移転",
        r"事業拡大",
        r"進出",
        r"新拠点",
        r"支店.*開設",
        r"出店",
        r"出展",
        r"展開.*開始",
    ]),
    ("award", [
        r"受賞",
        r"表彰",
        r"認定",
        r"アワード",
        r"Award",
        r"グランプリ",
        r"最優秀",
        r"入賞",
        r"選出",
        r"グッドデザイン賞",
        r"ランキング",
        r"選定",
    ]),
    ("event", [
        r"セミナー",
        r"ウェビナー",
        r"カンファレンス",
        r"イベント",
        r"開催[のを!！]",
        r"開催[決し]",
        r"開催します",
        r"サミット",
        r"フォーラム",
        r"シンポジウム",
        r"ミートアップ",
        r"Meetup",
        r"読書会",
        r"ピッチ",
        r"デモデイ",
        r"Demo Day",
        r"登壇",
        r"キックオフ",
        r"オンライン開催",
    ]),
]

# Categories where is_funding_related should be True
FUNDING_RELATED_CATEGORIES = {"funding", "exit"}


def classify_title(title: str) -> str | None:
    """Return new category for the title, or None if no match."""
    for category, patterns in CATEGORY_RULES:
        for pat in patterns:
            if re.search(pat, title, re.IGNORECASE):
                return category
    return None


def main():
    parser = argparse.ArgumentParser(description="Reclassify 'other' releases by keyword matching")
    parser.add_argument("--dry-run", action="store_true", help="Preview changes without updating DB")
    args = parser.parse_args()

    if not DB_PATH.exists():
        print(f"Error: DB not found at {DB_PATH}")
        sys.exit(1)

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # Fetch all 'other' records
    cur.execute("SELECT id, title FROM funding_releases WHERE category = 'other'")
    rows = cur.fetchall()
    total_other = len(rows)
    print(f"Total 'other' records: {total_other}")

    # Classify each record
    reclassified = {}  # id -> new_category
    category_counts = {}

    for row in rows:
        new_cat = classify_title(row["title"])
        if new_cat:
            reclassified[row["id"]] = new_cat
            category_counts[new_cat] = category_counts.get(new_cat, 0) + 1

    # Summary
    print(f"\nReclassification summary:")
    print(f"{'Category':<20} {'Count':>6}")
    print("-" * 28)
    for cat in sorted(category_counts, key=lambda c: -category_counts[c]):
        print(f"  {cat:<18} {category_counts[cat]:>6}")
    print("-" * 28)
    reclassified_total = sum(category_counts.values())
    remaining = total_other - reclassified_total
    print(f"  {'reclassified':<18} {reclassified_total:>6}")
    print(f"  {'still other':<18} {remaining:>6}")
    print(f"  reduction: {total_other} -> {remaining} ({reclassified_total / total_other * 100:.1f}% resolved)")

    if args.dry_run:
        print("\n[DRY RUN] No changes applied.")
    else:
        # Apply updates
        for record_id, new_cat in reclassified.items():
            is_funding = 1 if new_cat in FUNDING_RELATED_CATEGORIES else 0
            cur.execute(
                "UPDATE funding_releases SET category = ?, is_funding_related = ? WHERE id = ?",
                (new_cat, is_funding, record_id),
            )
        conn.commit()
        print(f"\nUpdated {reclassified_total} records in {DB_PATH.name}.")

    conn.close()


if __name__ == "__main__":
    main()
