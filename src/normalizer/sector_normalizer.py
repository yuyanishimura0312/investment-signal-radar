"""
Real-time sector name normalizer.

Maps raw sector strings (from Claude extraction or manual input) to the 33
canonical sector names defined in init_db_v2.py. Used both at ingestion time
(preventing fragmentation) and for batch cleanup.
"""

import re

# Canonical sector names (matching init_db_v2.py SEED_SECTORS, ids 1-33)
CANONICAL_SECTORS = [
    "AI/Machine Learning",
    "Fintech",
    "Healthcare/Biotech",
    "Climate/CleanTech",
    "Energy",
    "Mobility/Transportation",
    "Space",
    "Robotics",
    "Quantum Computing",
    "Cybersecurity",
    "Enterprise Software/SaaS",
    "Consumer/E-commerce",
    "Real Estate/PropTech",
    "Construction Tech",
    "Food/AgTech",
    "Education/EdTech",
    "HR/WorkTech",
    "Media/Entertainment",
    "Gaming",
    "Logistics/Supply Chain",
    "Manufacturing/Industrial",
    "Materials/Chemistry",
    "Semiconductor",
    "Hardware/IoT",
    "Blockchain/Web3",
    "Legal Tech",
    "GovTech",
    "InsurTech",
    "Marketing/AdTech",
    "Travel/Hospitality",
    "Retail Tech",
    "Developer Tools",
    "Other",
]

# Comprehensive mapping: raw name → canonical name
# Covers Japanese variants, typos, compound names, and English variants
_RAW_TO_CANONICAL = {
    # --- AI/Machine Learning ---
    "AI/機械学習、ロボティクス、デジタルツイン": "AI/Machine Learning",
    "ロボティクス/AI": "AI/Machine Learning",
    "ロボティクス・AI": "AI/Machine Learning",
    "IoT/AI": "AI/Machine Learning",
    "ロボティクス/AI/ディープテック": "AI/Machine Learning",
    "ロボティクス・AI・インフラテック": "AI/Machine Learning",
    "ディープテック": "AI/Machine Learning",
    "スポーツテック/AI": "AI/Machine Learning",
    "AI/ML": "AI/Machine Learning",

    # --- Fintech ---
    "FintechAI/機械学習": "Fintech",
    "FintechおよびSaaS": "Fintech",
    "FintechInsurTech": "Fintech",
    "FintechおよびFinancial Services": "Fintech",
    "FintTech": "Fintech",
    "FintechFinTech": "Fintech",
    "ファイナンス・アセットマネジメント": "Fintech",
    "金融犯罪対策/コンプライアンス": "Fintech",
    "ベンチャーデット・ファイナンス": "Fintech",
    "FinTech": "Fintech",
    "フィンテック": "Fintech",

    # --- Healthcare/Biotech ---
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
    "HealthTech": "Healthcare/Biotech",
    "BioTech": "Healthcare/Biotech",
    "美容テック": "Healthcare/Biotech",
    "フィットネス/ウェルネス": "Healthcare/Biotech",
    "ウェルネス/FitTech": "Healthcare/Biotech",
    "FitTech": "Healthcare/Biotech",
    "ライフテック": "Healthcare/Biotech",

    # --- Climate/CleanTech ---
    "クリーンテック": "Climate/CleanTech",
    "Climatetech": "Climate/CleanTech",
    "CleanTech": "Climate/CleanTech",
    "サステナビリティ・ESG": "Climate/CleanTech",
    "インパクト投資/ESG": "Climate/CleanTech",

    # --- Energy ---
    "EV・電動車両": "Energy",
    "エネルギー": "Energy",

    # --- Mobility/Transportation ---
    "モビリティ": "Mobility/Transportation",
    "モビリティ・物流・ロボティクス": "Mobility/Transportation",
    "モビリティ・インフラテック": "Mobility/Transportation",
    "モビリティ・ゲーミング": "Mobility/Transportation",

    # --- Space ---
    "宇宙技術/推進システム": "Space",
    "宇宙産業": "Space",
    "宇宙テック": "Space",
    "宇宙開発/ディープテック": "Space",
    "宇宙開発・ディープテック": "Space",
    "宇宙開発": "Space",
    "宇宙技術": "Space",

    # --- Robotics ---
    "ロボティクス・ファクトリーオートメーション": "Robotics",
    "ロボティクス/自動化": "Robotics",

    # --- Quantum Computing ---
    "量子コンピュータ": "Quantum Computing",

    # --- Cybersecurity ---
    "RegTech/サイバーセキュリティ": "Cybersecurity",

    # --- Enterprise Software/SaaS ---
    "SaaS": "Enterprise Software/SaaS",
    "マーケティング/SaaS": "Enterprise Software/SaaS",
    "クラウドインフラ": "Enterprise Software/SaaS",
    "EC/DX支援": "Enterprise Software/SaaS",

    # --- Consumer/E-commerce ---
    "Eコマース・リテールテック": "Consumer/E-commerce",
    "E-commerce": "Consumer/E-commerce",
    "eコマース/FoodTech": "Consumer/E-commerce",
    "EC/ソーシャルコマース": "Consumer/E-commerce",
    "EC/ショッピング": "Consumer/E-commerce",
    "EC/D2C": "Consumer/E-commerce",
    "Eコマース・モノづくり": "Consumer/E-commerce",
    "eコマース": "Consumer/E-commerce",
    "ソーシャルコマース": "Consumer/E-commerce",
    "マーケティング/D2C": "Consumer/E-commerce",

    # --- Real Estate/PropTech ---
    "不動産テック/シェアリングエコノミー": "Real Estate/PropTech",
    "不動産テック / シェアリングエコノミー": "Real Estate/PropTech",
    "不動産テック": "Real Estate/PropTech",
    "シェアリングエコノミー": "Real Estate/PropTech",
    "スマートホーム/不動産テック": "Real Estate/PropTech",

    # --- Construction Tech ---
    "建設DX": "Construction Tech",
    "建築テック": "Construction Tech",
    "ConstructionTech": "Construction Tech",
    "インフラテック": "Construction Tech",

    # --- Food/AgTech ---
    "アグリテック": "Food/AgTech",
    "地域産業再生/農業テック": "Food/AgTech",
    "AgriTech": "Food/AgTech",
    "FoodTech": "Food/AgTech",
    "地域活性化、食品開発": "Food/AgTech",

    # --- Education/EdTech ---
    "EdTech": "Education/EdTech",

    # --- HR/WorkTech ---
    "HR Tech": "HR/WorkTech",
    "HRTech": "HR/WorkTech",

    # --- Media/Entertainment ---
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
    "アート・テック": "Media/Entertainment",
    "スポーツDX": "Media/Entertainment",
    "SNS": "Media/Entertainment",
    "SNS/コミュニティプラットフォーム": "Media/Entertainment",

    # --- Gaming ---
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

    # --- Logistics/Supply Chain ---
    "物流": "Logistics/Supply Chain",

    # --- Manufacturing/Industrial ---
    "製造テック": "Manufacturing/Industrial",
    "製造業/ものづくり": "Manufacturing/Industrial",
    "製造業DX/センサー/ディープテック": "Manufacturing/Industrial",
    "製造技術/バイオものづくり": "Manufacturing/Industrial",

    # --- Semiconductor ---
    "半導体/チップ設計": "Semiconductor",

    # --- Hardware/IoT ---
    "IoT/DX": "Hardware/IoT",

    # --- Blockchain/Web3 ---
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

    # --- Legal Tech ---
    "LegalTech": "Legal Tech",

    # --- GovTech ---
    "政府・行政DX": "GovTech",

    # --- InsurTech ---
    "InsurTech": "InsurTech",

    # --- Marketing/AdTech ---
    "マーケティング/クリエイターエコノミー": "Marketing/AdTech",
    "CreatorEconomy": "Marketing/AdTech",
    "SNSマーケティング": "Marketing/AdTech",
    "MarTech": "Marketing/AdTech",
    "マーケティングテック/AdTech": "Marketing/AdTech",
    "マーケティング/ソーシャルメディア": "Marketing/AdTech",
    "広告テック": "Marketing/AdTech",

    # --- Travel/Hospitality ---
    "観光・インバウンド": "Travel/Hospitality",
    "観光・トラベルテック": "Travel/Hospitality",
    "トラベルテック": "Travel/Hospitality",
    "Travel Tech": "Travel/Hospitality",
    "OTA/観光テック": "Travel/Hospitality",
    "ツーリズム/トラベルテック": "Travel/Hospitality",

    # --- Retail Tech ---
    "小売テック": "Retail Tech",

    # --- Other ---
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
    "テレコミュニケーション": "Other",
    "通信・5G": "Other",
    "ペットテック": "Other",
    "ファッションテック": "Other",
    "FashionTech": "Other",
}

# Build case-insensitive index
_LOWER_MAP: dict[str, str] = {k.lower(): v for k, v in _RAW_TO_CANONICAL.items()}
_CANONICAL_LOWER: set[str] = {s.lower() for s in CANONICAL_SECTORS}

# Keyword-based fallback patterns (checked in order)
_KEYWORD_PATTERNS: list[tuple[re.Pattern, str]] = [
    (re.compile(r"ai|機械学習|ディープ", re.I), "AI/Machine Learning"),
    (re.compile(r"fintech|フィンテック|金融", re.I), "Fintech"),
    (re.compile(r"health|ヘルス|バイオ|医療|介護|ウェルビーイング", re.I), "Healthcare/Biotech"),
    (re.compile(r"clean|climate|クリーン|気候|esg|サステナ", re.I), "Climate/CleanTech"),
    (re.compile(r"energy|エネルギー|ev|電動", re.I), "Energy"),
    (re.compile(r"mobil|モビリティ|交通|自動車", re.I), "Mobility/Transportation"),
    (re.compile(r"宇宙|space", re.I), "Space"),
    (re.compile(r"ロボティクス|robot", re.I), "Robotics"),
    (re.compile(r"量子|quantum", re.I), "Quantum Computing"),
    (re.compile(r"cyber|サイバー", re.I), "Cybersecurity"),
    (re.compile(r"saas|エンタープライズ|クラウド", re.I), "Enterprise Software/SaaS"),
    (re.compile(r"eコマース|ec/|e-commerce|コマース|d2c", re.I), "Consumer/E-commerce"),
    (re.compile(r"不動産|prop.*tech|シェアリング", re.I), "Real Estate/PropTech"),
    (re.compile(r"建設|construct|建築", re.I), "Construction Tech"),
    (re.compile(r"food|agri|農|アグリ|フード", re.I), "Food/AgTech"),
    (re.compile(r"edtech|教育|ed.*tech", re.I), "Education/EdTech"),
    (re.compile(r"hr|人事|work.*tech", re.I), "HR/WorkTech"),
    (re.compile(r"メディア|エンタ|コンテンツ|映画|音楽|音声|スポーツ", re.I), "Media/Entertainment"),
    (re.compile(r"ゲーム|gaming|eスポーツ|gamefi", re.I), "Gaming"),
    (re.compile(r"物流|logistics|supply", re.I), "Logistics/Supply Chain"),
    (re.compile(r"製造|manufactur|ものづくり", re.I), "Manufacturing/Industrial"),
    (re.compile(r"素材|material|化学|chemist", re.I), "Materials/Chemistry"),
    (re.compile(r"半導体|semicon|チップ", re.I), "Semiconductor"),
    (re.compile(r"iot|ハードウェア|hardware", re.I), "Hardware/IoT"),
    (re.compile(r"web3|ブロックチェーン|blockchain|nft|dao|メタバース|vr/ar", re.I), "Blockchain/Web3"),
    (re.compile(r"legal|リーガル", re.I), "Legal Tech"),
    (re.compile(r"gov.*tech|行政|ガブ", re.I), "GovTech"),
    (re.compile(r"insur|インシュア|保険", re.I), "InsurTech"),
    (re.compile(r"market|adtech|広告|マーケ|sns", re.I), "Marketing/AdTech"),
    (re.compile(r"travel|観光|ツーリズム|ホスピタリティ|インバウンド", re.I), "Travel/Hospitality"),
    (re.compile(r"retail|リテール|小売", re.I), "Retail Tech"),
    (re.compile(r"developer|デベロッパー", re.I), "Developer Tools"),
]


def normalize_sector(raw: str) -> str:
    """Normalize a raw sector string to a canonical sector name.

    Resolution order:
    1. Exact match against canonical names (case-insensitive)
    2. Explicit mapping table lookup
    3. Keyword pattern matching
    4. Fallback to "Other"
    """
    if not raw or not raw.strip():
        return "Other"

    name = raw.strip()

    # 1. Already canonical?
    if name.lower() in _CANONICAL_LOWER:
        # Return the properly-cased canonical name
        for c in CANONICAL_SECTORS:
            if c.lower() == name.lower():
                return c
        return name

    # 2. Explicit mapping
    mapped = _LOWER_MAP.get(name.lower())
    if mapped:
        return mapped

    # 3. Keyword pattern fallback
    for pattern, canonical in _KEYWORD_PATTERNS:
        if pattern.search(name):
            return canonical

    # 4. Default
    return "Other"
