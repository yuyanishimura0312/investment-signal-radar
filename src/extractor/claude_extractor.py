#!/usr/bin/env python3
"""
Claude API-based structured information extractor.
Extracts investment details from press release text.
Uses claude-haiku for cost efficiency, falls back to sonnet for low-confidence results.
"""

import json
import subprocess
import logging
import re
import functools
from typing import Optional
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

# Models: haiku for cost, sonnet for accuracy
MODEL_FAST = "claude-haiku-4-5-20251001"
MODEL_ACCURATE = "claude-sonnet-4-20250514"

EXTRACTION_PROMPT_TEMPLATE = """あなたはVC投資情報の構造化抽出エキスパートです。
以下のプレスリリースから、資金調達に関する情報を正確にJSON形式で抽出してください。

## 抽出項目
- company_name: 資金調達した企業名（正式名称）
- company_description: 企業の簡潔な説明（1-2文）
- investors: 投資家のリスト。各投資家はオブジェクト（name, is_lead, type を含む。typeはvc/cvc/angel/gov/bank/corporate/otherのいずれか）
- amount_raw: 調達金額の原文表記（例: "5億円", "$10M"）
- amount_jpy: 日本円換算の金額（万円単位の整数。不明なら null）
- round_type: ラウンド種別。以下のいずれか: pre-seed, seed, pre-a, a, b, c, d, e, strategic, debt, grant, ipo, unknown
- sector: 事業セクター（例: "AI/機械学習", "ヘルスケア", "フィンテック"）
- announced_date: 発表日（YYYY-MM-DD形式。不明なら null）
- confidence: 抽出の信頼度 (high/medium/low)。情報が明確なら high、推測を含むなら medium、不確実なら low

## ルール
- 資金調達に関する情報がない場合は is_funding: false のみ含むJSONを返してください
- 金額が「数億円」等の曖昧な表現の場合、amount_jpy は null とし confidence を low にしてください
- 複数の投資家がいる場合、リード投資家を is_lead: true としてください
- 1ドル=150円で概算してください（ドル建ての場合）
- JSON以外の出力は不要です

## プレスリリース
タイトル: __TITLE__

本文:
__TEXT__
"""


@functools.lru_cache(maxsize=1)
def get_api_key() -> str:
    """Retrieve Anthropic API key from macOS Keychain. Cached after first call."""
    try:
        result = subprocess.run(
            ["security", "find-generic-password", "-s", "ANTHROPIC_API_KEY", "-w"],
            capture_output=True, text=True, check=True,
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError:
        raise RuntimeError(
            "ANTHROPIC_API_KEY not found in Keychain. "
            "Add it with: security add-generic-password -s ANTHROPIC_API_KEY -a claude -w 'your-key'"
        )


def validate_extracted_data(data: dict) -> dict:
    """Validate and sanitize fields extracted by Claude API."""
    # Truncate overly long strings
    for field in ("company_name", "amount_raw", "sector"):
        if field in data and isinstance(data[field], str):
            data[field] = data[field][:500]

    # Validate announced_date format
    date_val = data.get("announced_date")
    if date_val and not re.match(r"^\d{4}-\d{2}-\d{2}$", str(date_val)):
        data["announced_date"] = None

    # Validate confidence
    if data.get("confidence") not in ("high", "medium", "low"):
        data["confidence"] = "medium"

    # Validate investors list
    investors = data.get("investors")
    if not isinstance(investors, list):
        data["investors"] = []
    else:
        data["investors"] = [
            inv for inv in investors
            if isinstance(inv, dict) and inv.get("name")
        ]

    return data


def validate_url(url: str) -> bool:
    """Check that a URL uses http/https scheme."""
    try:
        parsed = urlparse(url)
        return parsed.scheme in ("http", "https")
    except Exception:
        return False


def extract_investment_info(
    title: str,
    text: str,
    model: str = MODEL_FAST,
    _is_retry: bool = False,
) -> Optional[dict]:
    """
    Extract structured investment information from press release text.
    Returns parsed JSON dict or None if not a funding article.
    """
    import anthropic

    api_key = get_api_key()
    client = anthropic.Anthropic(api_key=api_key)

    prompt = EXTRACTION_PROMPT_TEMPLATE.replace("__TITLE__", title).replace("__TEXT__", text)

    try:
        response = client.messages.create(
            model=model,
            max_tokens=1024,
            messages=[{"role": "user", "content": prompt}],
        )
        content = response.content[0].text.strip()

        # Extract JSON from response (handle markdown code blocks)
        json_match = re.search(r'```(?:json)?\s*([\s\S]*?)```', content)
        if json_match:
            content = json_match.group(1).strip()

        data = json.loads(content)

        # Not a funding article
        if data.get("is_funding") is False:
            return None

        # Validate and sanitize extracted data
        data = validate_extracted_data(data)

        # Re-extract with accurate model if confidence is low (one retry only)
        if data.get("confidence") == "low" and not _is_retry:
            logger.info(f"Low confidence, re-extracting with {MODEL_ACCURATE}")
            return extract_investment_info(
                title, text, model=MODEL_ACCURATE, _is_retry=True
            )

        return data

    except json.JSONDecodeError as e:
        logger.warning(f"Failed to parse JSON from Claude response: {e}")
        return None
    except Exception as e:
        logger.error(f"Claude API error: {e}")
        return None


def estimate_cost(article_count: int) -> dict:
    """Estimate API costs for a batch of articles."""
    # Average tokens per article: ~1500 input, ~300 output
    avg_input_tokens = 1500
    avg_output_tokens = 300

    # Haiku pricing (per 1M tokens): $0.25 input, $1.25 output
    haiku_cost = article_count * (
        avg_input_tokens * 0.25 / 1_000_000
        + avg_output_tokens * 1.25 / 1_000_000
    )

    return {
        "article_count": article_count,
        "estimated_cost_usd": round(haiku_cost, 4),
        "estimated_cost_jpy": round(haiku_cost * 150, 1),
        "model": MODEL_FAST,
    }
