#!/usr/bin/env python3
"""
PESTLE auto-classification for investment data.
Classifies each investment's sector into PESTLE categories.
"""

import sys
import json
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))
from db.models import get_conn
from extractor.claude_extractor import get_api_key

logger = logging.getLogger(__name__)

PESTLE_PROMPT = """以下の投資案件の情報を読み、PESTLE分析のカテゴリを1つ選んでください。

## カテゴリ
- P (Political): 政治・規制・ガバナンスに関連する領域
- E (Economic): 金融・経済インフラ・商取引に関連する領域
- S (Social): 社会・教育・健康・コミュニティに関連する領域
- T (Technological): 技術・AI・ソフトウェア・ハードウェアに関連する領域
- L (Legal): 法律・コンプライアンス・知的財産に関連する領域
- E2 (Environmental): 環境・エネルギー・サステナビリティに関連する領域

## 投資案件
企業名: {company_name}
セクター: {sector}
説明: {description}

JSONで回答してください: {{"category": "P/E/S/T/L/E2", "reason": "簡潔な理由"}}
"""


def classify_investments(batch_size: int = 50):
    """Classify unclassified investments with PESTLE categories."""
    import anthropic

    conn = get_conn()
    rows = conn.execute("""
        SELECT i.id, c.canonical_name, c.description,
               COALESCE(s.name, '') as sector
        FROM investments i
        JOIN companies c ON i.company_id = c.id
        LEFT JOIN sectors s ON c.sector_id = s.id
        WHERE i.pestle_category IS NULL OR i.pestle_category = ''
        LIMIT ?
    """, (batch_size,)).fetchall()

    if not rows:
        logger.info("All investments already classified")
        return 0

    api_key = get_api_key()
    client = anthropic.Anthropic(api_key=api_key)
    classified = 0

    for row in rows:
        prompt = PESTLE_PROMPT.format(
            company_name=row["canonical_name"],
            sector=row["sector"],
            description=row["description"] or "No description",
        )

        try:
            response = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=128,
                messages=[{"role": "user", "content": prompt}],
            )
            content = response.content[0].text.strip()
            # Parse JSON
            import re
            json_match = re.search(r'\{[^}]+\}', content)
            if json_match:
                data = json.loads(json_match.group())
                category = data.get("category", "T")
                conn.execute(
                    "UPDATE investments SET pestle_category = ? WHERE id = ?",
                    (category, row["id"])
                )
                conn.execute(
                    "UPDATE companies SET pestle_category = ? WHERE id = ?",
                    (category, row["id"])
                )
                classified += 1
                logger.info(f"  {row['canonical_name']}: {category}")
        except Exception as e:
            logger.warning(f"Failed to classify {row['canonical_name']}: {e}")

    conn.commit()
    conn.close()
    logger.info(f"Classified {classified}/{len(rows)} investments")
    return classified


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    classify_investments()
