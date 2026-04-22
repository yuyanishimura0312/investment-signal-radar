#!/usr/bin/env python3
"""
Normalize fragmented sectors (220+) to 33 canonical categories.

Maps Japanese/duplicate sector names to the standard Dealroom-based sector list.
Updates organization_sectors to point to canonical sector IDs.
"""

import sqlite3
import sys
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "investment_signal_v2.db"

# Canonical sectors (id 1-33 from init_db_v2.py seed data)
# Map: fragments → canonical sector name
SECTOR_MAP = {
    # AI/Machine Learning (id=1)
    "AI/機械学習、ロボティクス、デジタルツイン": "AI/Machine Learning",
    "ロボティクス/AI": "AI/Machine Learning",
    "ロボティクス・AI": "AI/Machine Learning",
    "IoT/AI": "AI/Machine Learning",
    "ロボティクス/AI/ディープテック": "AI/Machine Learning",
    "ロボティクス・AI・インフラテック": "AI/Machine Learning",
    "ディープテック": "AI/Machine Learning",
    "スポーツテック/AI": "AI/Machine Learning",

    # Fintech (id=2)
    "FintechAI/機械学習": "Fintech",
    "FintechおよびSaaS": "Fintech",
    "FintechInsurTech": "Fintech",
    "FintechおよびFinancial Services": "Fintech",
    "FintTech": "Fintech",
    "FintechFinTech": "Fintech",
    "ファイナンス・アセットマネジメント": "Fintech",
    "金融犯罪対策/コンプライアンス": "Fintech",
    "ベンチャーデット・ファイナンス": "Fintech",

    # Healthcare/Biotech (id=3)
    "バイオテック": "Healthcare/Biotech",
    "ヘルスケア": "Healthcare/Biotech",
    "ビューティ・ヘルスケア": "Healthcare/Biotech",
    "ウェルビーイング・ヘルスケア": "Healthcare/Biotech",
    "ヘルステック": "Healthcare/Biotech",
    "医療機器": "Healthcare/Biotech",
    "FemTech": "Healthcare/Biotech",
    "ウェルビーイングテック": "Healthcare/Biotech",
    "介護テック": "Healthcare/Biotech",
    "AgeTech": "Healthcare/Biotech",

    # Climate/CleanTech (id=4)
    "クリーンテック": "Climate/CleanTech",
    "Climatetech": "Climate/CleanTech",
    "CleanTech": "Climate/CleanTech",
    "サステナビリティ・ESG": "Climate/CleanTech",
    "インパクト投資/ESG": "Climate/CleanTech",

    # Energy (id=5)
    "EV・電動車両": "Energy",

    # Mobility/Transportation (id=6)
    "モビリティ": "Mobility/Transportation",
    "モビリティ・物流・ロボティクス": "Mobility/Transportation",
    "モビリティ・インフラテック": "Mobility/Transportation",
    "モビリティ・ゲーミング": "Mobility/Transportation",

    # Space (id=7)
    "宇宙技術/推進システム": "Space",
    "宇宙産業": "Space",
    "宇宙テック": "Space",
    "宇宙開発/ディープテック": "Space",
    "宇宙開発・ディープテック": "Space",
    "宇宙開発": "Space",
    "宇宙技術": "Space",

    # Robotics (id=8)
    "ロボティクス・ファクトリーオートメーション": "Robotics",
    "ロボティクス/自動化": "Robotics",

    # Quantum Computing (id=9)
    "量子コンピュータ": "Quantum Computing",

    # Enterprise Software/SaaS (id=11)
    "SaaS": "Enterprise Software/SaaS",
    "マーケティング/SaaS": "Enterprise Software/SaaS",
    "クラウドインフラ": "Enterprise Software/SaaS",

    # Consumer/E-commerce (id=12)
    "Eコマース・リテールテック": "Consumer/E-commerce",
    "E-commerce": "Consumer/E-commerce",
    "eコマース/FoodTech": "Consumer/E-commerce",
    "EC/ソーシャルコマース": "Consumer/E-commerce",
    "EC/ショッピング": "Consumer/E-commerce",
    "EC/D2C": "Consumer/E-commerce",
    "EC/DX支援": "Consumer/E-commerce",
    "Eコマース・モノづくり": "Consumer/E-commerce",
    "eコマース": "Consumer/E-commerce",
    "ソーシャルコマース": "Consumer/E-commerce",

    # Real Estate/PropTech (id=13)
    "不動産テック/シェアリングエコノミー": "Real Estate/PropTech",
    "不動産テック / シェアリングエコノミー": "Real Estate/PropTech",
    "不動産テック": "Real Estate/PropTech",
    "シェアリングエコノミー": "Real Estate/PropTech",
    "スマートホーム/不動産テック": "Real Estate/PropTech",

    # Construction Tech (id=14)
    "建設DX": "Construction Tech",
    "建築テック": "Construction Tech",
    "ConstructionTech": "Construction Tech",
    "インフラテック": "Construction Tech",

    # Food/AgTech (id=15)
    "アグリテック": "Food/AgTech",
    "地域産業再生/農業テック": "Food/AgTech",
    "AgriTech": "Food/AgTech",
    "FoodTech": "Food/AgTech",
    "地域活性化、食品開発": "Food/AgTech",

    # Education/EdTech (id=16)
    "EdTech": "Education/EdTech",

    # HR/WorkTech (id=17)
    "HR Tech": "HR/WorkTech",
    "HRTech": "HR/WorkTech",

    # Media/Entertainment (id=18)
    "エンターテイメント/ファンプラットフォーム": "Media/Entertainment",
    "エンタテインメント": "Media/Entertainment",
    "エンターテイメント": "Media/Entertainment",
    "エンターテインメント": "Media/Entertainment",
    "メディア・エンタメ": "Media/Entertainment",
    "メディア": "Media/Entertainment",
    "メディア/エンターテイメント": "Media/Entertainment",
    "メディア・エンターテイメント": "Media/Entertainment",
    "エンタメテック": "Media/Entertainment",
    "エンターテック": "Media/Entertainment",
    "コンテンツ・IP": "Media/Entertainment",
    "映画・映像コンテンツ": "Media/Entertainment",
    "音楽テック": "Media/Entertainment",
    "音楽Tech": "Media/Entertainment",
    "コンテンツ・メディア": "Media/Entertainment",
    "コンテンツ/メディア": "Media/Entertainment",
    "クリエイティブ/メディアテック": "Media/Entertainment",
    "エンタメ/コンテンツ": "Media/Entertainment",
    "スポーツ・エンターテインメント": "Media/Entertainment",
    "エンターテインメント/バーチャルエンターテインメント": "Media/Entertainment",
    "音声配信プラットフォーム": "Media/Entertainment",
    "ゲーム・エンターテインメント": "Media/Entertainment",
    "ゲーム・ブロックチェーン・VR": "Media/Entertainment",

    # Gaming (id=19)
    "ゲーム・メタバース": "Gaming",
    "ゲーム/XR": "Gaming",
    "ゲーム/VR": "Gaming",
    "ゲーム/メタバース": "Gaming",
    "ゲーム/エンターテインメント": "Gaming",
    "ライブ配信・ゲーミング": "Gaming",
    "ゲーミング・eスポーツ": "Gaming",
    "eスポーツ": "Gaming",
    "VRゲーム/エンターテインメント": "Gaming",
    "Web3/ゲーム": "Gaming",
    "Web3/GameFi": "Gaming",
    "GameFi/Web3": "Gaming",

    # Logistics/Supply Chain (id=20)
    "物流": "Logistics/Supply Chain",

    # Manufacturing/Industrial (id=21)
    "製造テック": "Manufacturing/Industrial",
    "製造業/ものづくり": "Manufacturing/Industrial",
    "製造業DX/センサー/ディープテック": "Manufacturing/Industrial",
    "製造技術/バイオものづくり": "Manufacturing/Industrial",

    # Semiconductor (id=23)
    "半導体/チップ設計": "Semiconductor",

    # Hardware/IoT (id=24)
    "IoT/DX": "Hardware/IoT",

    # Blockchain/Web3 (id=25)
    "ブロックチェーン/暗号資産": "Blockchain/Web3",
    "Web3/ブロックチェーン": "Blockchain/Web3",
    "Web3/NFT": "Blockchain/Web3",
    "Web3/NFT/DAO": "Blockchain/Web3",
    "Web3/DAO/ブロックチェーン": "Blockchain/Web3",
    "Web3/クリエイターエコノミー": "Blockchain/Web3",
    "Web3": "Blockchain/Web3",
    "メタバース・Web3": "Blockchain/Web3",
    "メタバース/VR/XR": "Blockchain/Web3",
    "メタバース": "Blockchain/Web3",
    "ブロックチェーン": "Blockchain/Web3",
    "ブロックチェーン/NFT": "Blockchain/Web3",
    "ブロックチェーン/NFT/ゲーム": "Blockchain/Web3",
    "VR/AR": "Blockchain/Web3",

    # Legal Tech (id=26)
    "LegalTech": "Legal Tech",

    # GovTech (id=27)
    "政府・行政DX": "GovTech",

    # InsurTech (id=28)

    # Marketing/AdTech (id=29)
    "マーケティング/クリエイターエコノミー": "Marketing/AdTech",
    "マーケティング/D2C": "Marketing/AdTech",
    "CreatorEconomy": "Marketing/AdTech",
    "SNSマーケティング": "Marketing/AdTech",
    "MarTech": "Marketing/AdTech",
    "マーケティングテック/AdTech": "Marketing/AdTech",
    "マーケティング/ソーシャルメディア": "Marketing/AdTech",
    "広告テック": "Marketing/AdTech",
    "SNS/コミュニティプラットフォーム": "Marketing/AdTech",
    "SNS": "Marketing/AdTech",

    # Travel/Hospitality (id=30)
    "観光・インバウンド": "Travel/Hospitality",
    "観光・トラベルテック": "Travel/Hospitality",
    "トラベルテック": "Travel/Hospitality",
    "Travel Tech": "Travel/Hospitality",
    "OTA/観光テック": "Travel/Hospitality",
    "ツーリズム/トラベルテック": "Travel/Hospitality",

    # Retail Tech (id=31)
    "小売テック": "Retail Tech",

    # Developer Tools (id=32)

    # Other (id=33)
    "M&A": "Other",
    "M&A/Corporate Finance": "Other",
    "M&A仲介・スタートアップ支援": "Other",
    "M&A・コンサルティング": "Other",
    "M&A/ロールアップ": "Other",
    "M&A仲介・事業承継": "Other",
    "M&A・事業投資": "Other",
    "ベンチャーキャピタル": "Other",
    "VC/ベンチャーキャピタル": "Other",
    "VC/PE": "Other",
    "VC/投資ファンド": "Other",
    "ベンチャーキャピタル/ファンド": "Other",
    "ベンチャーキャピタル・ファンド": "Other",
    "ベンチャーキャピタル/マルチセクター": "Other",
    "ファンド・投資": "Other",
    "ファンド・オブ・ファンズ/オープンイノベーション": "Other",
    "その他（非営利スタートアップ支援財団）": "Other",
    "unknown": "Other",
    "地域活性化/スタートアップ支援": "Other",
    "防災DX": "Other",
    "防衛テック": "Other",
    "デジタルアイデンティティ": "Other",
    "RegTech": "Other",
    "RegTech/サイバーセキュリティ": "Other",
    "テレコミュニケーション": "Other",
    "通信・5G": "Other",
    "スポーツDX": "Other",
    "ペットテック": "Other",
    "ファッションテック": "Other",
    "FashionTech": "Other",
    "美容テック": "Other",
    "アート・テック": "Other",
    "フィットネス/ウェルネス": "Other",
    "ウェルネス/FitTech": "Other",
    "FitTech": "Other",
    "ライフテック": "Other",
}


def normalize_sectors(dry_run: bool = False):
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")

    # Get canonical sector IDs
    canonical = {}
    for row in conn.execute("SELECT id, name FROM sectors WHERE id <= 33"):
        canonical[row["name"]] = row["id"]

    # Get all non-canonical sectors
    non_canonical = conn.execute(
        "SELECT id, name, name_ja FROM sectors WHERE id > 33"
    ).fetchall()

    remapped = 0
    unmapped = []

    for sector in non_canonical:
        old_id = sector["id"]
        old_name = sector["name"]

        # Look up mapping
        target_name = SECTOR_MAP.get(old_name)
        if not target_name:
            # Try name_ja
            target_name = SECTOR_MAP.get(sector["name_ja"])

        if not target_name:
            unmapped.append(old_name)
            continue

        new_id = canonical.get(target_name)
        if not new_id:
            print(f"  WARNING: canonical sector '{target_name}' not found")
            continue

        # Count affected organizations
        count = conn.execute(
            "SELECT COUNT(*) as c FROM organization_sectors WHERE sector_id = ?",
            (old_id,)
        ).fetchone()["c"]

        if count == 0:
            continue

        print(f"  {old_name} ({count} orgs) → {target_name}")

        if not dry_run:
            # Update organization_sectors: remap to canonical
            # Handle potential conflicts (org already has canonical sector)
            conn.execute("""
                UPDATE OR IGNORE organization_sectors
                SET sector_id = ?
                WHERE sector_id = ?
            """, (new_id, old_id))

            # Delete remaining rows that conflicted (org already had canonical)
            conn.execute(
                "DELETE FROM organization_sectors WHERE sector_id = ?",
                (old_id,)
            )

            remapped += count

    if not dry_run:
        conn.commit()

    # Summary
    print(f"\nRemapped: {remapped} organization-sector links")
    if unmapped:
        print(f"\nUnmapped sectors ({len(unmapped)}):")
        for name in sorted(set(unmapped)):
            count = conn.execute(
                "SELECT COUNT(*) as c FROM organization_sectors os "
                "JOIN sectors s ON os.sector_id = s.id WHERE s.name = ?",
                (name,)
            ).fetchone()["c"]
            if count > 0:
                print(f"  {name} ({count} orgs)")

    # Final stats
    sector_counts = conn.execute("""
        SELECT s.name, COUNT(*) as c
        FROM organization_sectors os
        JOIN sectors s ON os.sector_id = s.id
        WHERE s.id <= 33
        GROUP BY s.id
        ORDER BY c DESC
    """).fetchall()
    print(f"\nCanonical sector distribution:")
    for row in sector_counts:
        print(f"  {row['name']}: {row['c']}")

    conn.close()


if __name__ == "__main__":
    dry_run = "--dry-run" in sys.argv
    if dry_run:
        print("=== DRY RUN ===")
    normalize_sectors(dry_run=dry_run)
