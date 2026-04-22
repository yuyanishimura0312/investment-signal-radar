#!/usr/bin/env python3
"""
Part 1: Tag Generation
Populates the tags and organization_tags tables by:
1. Defining a curated tag taxonomy
2. Matching company descriptions + press release titles against tag keywords
3. Inserting matched tags via direct SQL
"""

import sys
import sqlite3
import logging
from pathlib import Path

# Allow src imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

DB_PATH = project_root / "data" / "investment_signal_v2.db"

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


# ================================================================
# Tag taxonomy
# ================================================================

TECHNOLOGY_TAGS = {
    "AI": ["ai", "人工知能", "artificial intelligence", "gpt", "llm", "生成ai", "生成系ai"],
    "ML": ["machine learning", "機械学習", "deep learning", "深層学習", "neural network", "ニューラル"],
    "NLP": ["nlp", "natural language", "自然言語", "テキスト解析", "言語モデル", "text analysis"],
    "Computer Vision": ["computer vision", "画像認識", "image recognition", "画像解析", "visual ai", "cv "],
    "Blockchain": ["blockchain", "ブロックチェーン", "web3", "nft", "crypto", "暗号資産", "分散台帳", "dao"],
    "IoT": ["iot", "internet of things", "センサー", "スマートデバイス", "connected", "スマートホーム"],
    "Cloud": ["cloud", "クラウド", "saas", "paas", "iaas", "aws", "azure", "gcp", "kubernetes"],
    "API": ["api", "api-first", "openapi", "rest api", "webhook", "連携api", "データ連携"],
    "Big Data": ["big data", "ビッグデータ", "データ分析", "data analytics", "データプラットフォーム", "データ基盤"],
    "AR/VR": ["ar", "vr", "xr", "augmented reality", "virtual reality", "拡張現実", "仮想現実", "メタバース", "metaverse"],
    "Quantum": ["quantum", "量子", "量子コンピュータ", "量子暗号"],
    "Robotics": ["robot", "ロボット", "自動化", "automation", "無人", "自律", "autonomous"],
    "5G": ["5g", "6g", "通信インフラ", "モバイル通信", "wireless"],
    "Edge Computing": ["edge computing", "エッジコンピューティング", "edge ai", "fog computing"],
    "Cybersecurity": ["セキュリティ", "security", "cybersecurity", "サイバーセキュリティ", "ゼロトラスト", "zero trust", "暗号化"],
}

BUSINESS_MODEL_TAGS = {
    "SaaS": ["saas", "ソフトウェアサービス", "software as a service", "クラウドサービス", "サブスクリプション型ソフト"],
    "Marketplace": ["marketplace", "マーケットプレイス", "プラットフォーム取引", "c2c", "取引プラットフォーム"],
    "D2C": ["d2c", "direct to consumer", "直販", "eコマース", "ec事業", "直接販売"],
    "Platform": ["プラットフォーム", "platform", "エコシステム", "ecosystem"],
    "Subscription": ["サブスクリプション", "subscription", "月額", "定額", "recurring"],
    "Freemium": ["freemium", "フリーミアム", "無料プラン", "premium plan"],
    "B2B": ["b2b", "法人向け", "enterprise", "エンタープライズ", "企業向け"],
    "B2C": ["b2c", "消費者向け", "consumer", "個人向け", "一般ユーザー"],
    "API-first": ["api-first", "api first", "developer platform", "開発者向け", "sdk"],
}

MARKET_TAGS = {
    "SDGs": ["sdg", "sdgs", "持続可能", "sustainable", "サステナブル", "社会課題"],
    "DX": ["dx", "デジタルトランスフォーメーション", "digital transformation", "デジタル化", "業務改革", "dxtransformation"],
    "Remote Work": ["リモートワーク", "remote work", "テレワーク", "在宅勤務", "ハイブリッドワーク"],
    "Gig Economy": ["ギグ", "gig economy", "フリーランス", "freelance", "副業", "シェアエコ"],
    "Creator Economy": ["クリエイター", "creator economy", "コンテンツクリエイター", "インフルエンサー", "creator"],
    "Open Innovation": ["オープンイノベーション", "open innovation", "産学連携", "共創", "産官学"],
    "ESG": ["esg", "環境・社会・ガバナンス", "脱炭素", "カーボン", "co2", "温室効果ガス", "グリーン"],
    "Smart City": ["スマートシティ", "smart city", "スマートタウン", "都市os", "まちづくり"],
    "MaaS": ["maas", "mobility as a service", "モビリティサービス", "交通サービス", "移動サービス"],
    "AgingTech": ["高齢化", "介護", "aging", "シニア", "elderly", "longevity", "ヘルスケア高齢"],
    "FemTech": ["femtech", "フェムテック", "女性向け", "月経", "妊活", "女性健康"],
    "PetTech": ["pettech", "ペットテック", "ペット", "pet tech", "動物"],
}

ALL_TAGS = {
    "technology": TECHNOLOGY_TAGS,
    "business_model": BUSINESS_MODEL_TAGS,
    "market": MARKET_TAGS,
}


def normalize_text(text: str) -> str:
    """Lowercase and normalize text for keyword matching."""
    if not text:
        return ""
    return text.lower()


def match_tags(text: str) -> list[tuple[str, str, float]]:
    """
    Match text against tag keywords.
    Returns list of (category, tag_name, confidence) tuples.
    """
    normalized = normalize_text(text)
    matches = []

    for category, tag_dict in ALL_TAGS.items():
        for tag_name, keywords in tag_dict.items():
            hit_count = 0
            for kw in keywords:
                if kw in normalized:
                    hit_count += 1
            if hit_count > 0:
                # More keyword hits → higher confidence, max 0.95
                confidence = min(0.95, 0.6 + (hit_count - 1) * 0.1)
                matches.append((category, tag_name, confidence))

    return matches


def ensure_tags_exist(conn: sqlite3.Connection) -> dict[tuple, int]:
    """Insert all taxonomy tags and return {(category, name): tag_id} map."""
    tag_map: dict[tuple, int] = {}
    name_ja_map = {
        # technology
        "AI": "AI・人工知能",
        "ML": "機械学習",
        "NLP": "自然言語処理",
        "Computer Vision": "コンピュータビジョン",
        "Blockchain": "ブロックチェーン",
        "IoT": "IoT",
        "Cloud": "クラウド",
        "API": "API",
        "Big Data": "ビッグデータ",
        "AR/VR": "AR/VR",
        "Quantum": "量子コンピューティング",
        "Robotics": "ロボティクス",
        "5G": "5G",
        "Edge Computing": "エッジコンピューティング",
        "Cybersecurity": "サイバーセキュリティ",
        # business_model
        "SaaS": "SaaS",
        "Marketplace": "マーケットプレイス",
        "D2C": "D2C",
        "Platform": "プラットフォーム",
        "Subscription": "サブスクリプション",
        "Freemium": "フリーミアム",
        "B2B": "B2B",
        "B2C": "B2C",
        "API-first": "APIファースト",
        # theme
        "SDGs": "SDGs",
        "DX": "DX・デジタル変革",
        "Remote Work": "リモートワーク",
        "Gig Economy": "ギグエコノミー",
        "Creator Economy": "クリエイターエコノミー",
        "Open Innovation": "オープンイノベーション",
        "ESG": "ESG",
        "Smart City": "スマートシティ",
        "MaaS": "MaaS",
        "AgingTech": "エイジングテック",
        "FemTech": "フェムテック",
        "PetTech": "ペットテック",
    }

    for category, tag_dict in ALL_TAGS.items():
        for tag_name in tag_dict:
            name_ja = name_ja_map.get(tag_name, tag_name)
            row = conn.execute(
                "SELECT id FROM tags WHERE tag_category = ? AND name = ?",
                (category, tag_name),
            ).fetchone()
            if row:
                tag_id = row["id"]
            else:
                cur = conn.execute(
                    "INSERT INTO tags (tag_category, name, name_ja) VALUES (?, ?, ?)",
                    (category, tag_name, name_ja),
                )
                tag_id = cur.lastrowid
            tag_map[(category, tag_name)] = tag_id

    conn.commit()
    return tag_map


def generate_organization_tags(conn: sqlite3.Connection, tag_map: dict[tuple, int]) -> int:
    """
    For each company, match tags from description + press release titles.
    Returns total organization_tag records inserted.
    """
    # Fetch all companies with their descriptions
    companies = conn.execute(
        "SELECT id, name, description FROM organizations WHERE is_company = 1"
    ).fetchall()

    logger.info(f"Processing {len(companies)} companies...")

    # Build a lookup: organization_id → press release titles (concatenated)
    pr_texts: dict[int, str] = {}
    pr_rows = conn.execute(
        "SELECT organization_id, title FROM press_releases WHERE organization_id IS NOT NULL"
    ).fetchall()
    for pr in pr_rows:
        org_id = pr["organization_id"]
        if org_id not in pr_texts:
            pr_texts[org_id] = ""
        pr_texts[org_id] += " " + (pr["title"] or "")

    inserted = 0
    for org in companies:
        org_id = org["id"]
        desc = org["description"] or ""
        pr_text = pr_texts.get(org_id, "")
        full_text = desc + " " + pr_text

        if not full_text.strip():
            continue

        matches = match_tags(full_text)
        for category, tag_name, confidence in matches:
            tag_id = tag_map.get((category, tag_name))
            if tag_id is None:
                continue
            conn.execute(
                """INSERT OR REPLACE INTO organization_tags
                   (organization_id, tag_id, confidence_score, assigned_by)
                   VALUES (?, ?, ?, 'rule')""",
                (org_id, tag_id, round(confidence, 2)),
            )
            inserted += 1

    conn.commit()
    return inserted


def main():
    logger.info(f"Connecting to {DB_PATH}")
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")

    logger.info("Step 1: Inserting tag taxonomy...")
    tag_map = ensure_tags_exist(conn)
    tag_count = conn.execute("SELECT COUNT(*) FROM tags").fetchone()[0]
    logger.info(f"  tags table: {tag_count} records")

    logger.info("Step 2: Matching and tagging companies...")
    org_tag_count = generate_organization_tags(conn, tag_map)
    total_org_tags = conn.execute("SELECT COUNT(*) FROM organization_tags").fetchone()[0]
    logger.info(f"  organization_tags table: {total_org_tags} records ({org_tag_count} inserted)")

    conn.close()
    logger.info("Done.")
    print(f"\n=== Tag Generation Complete ===")
    print(f"tags:              {tag_count}")
    print(f"organization_tags: {total_org_tags}")


if __name__ == "__main__":
    main()
