"""
gBizINFO API client for enriching organization data with official corporate info.

gBizINFO (https://info.gbiz.go.jp/) is a Japanese government API that provides
corporate information including capital, employee count, address, and industry.

Authentication:
  - Requires a free API token (register at https://info.gbiz.go.jp/)
  - Token is read from macOS Keychain: `security find-generic-password -s gbizinfo-api-token -w`
  - To store: `security add-generic-password -s gbizinfo-api-token -a gbizinfo -w YOUR_TOKEN`
"""

import logging
import subprocess
import time
from dataclasses import dataclass, asdict
from typing import Optional, List

import requests

logger = logging.getLogger(__name__)

BASE_URL = "https://info.gbiz.go.jp/hojin/v1/hojin"


def get_api_token() -> Optional[str]:
    """Retrieve gBizINFO API token from macOS Keychain."""
    try:
        result = subprocess.run(
            ["security", "find-generic-password", "-s", "gbizinfo-api-token", "-w"],
            capture_output=True, text=True, timeout=5,
        )
        if result.returncode == 0:
            return result.stdout.strip()
    except Exception as e:
        logger.debug(f"Keychain lookup failed: {e}")
    return None


@dataclass
class CorporateInfo:
    """Structured corporate data from gBizINFO."""
    corporate_number: str
    name: str
    name_kana: Optional[str] = None
    address: Optional[str] = None
    capital: Optional[int] = None
    employee_count: Optional[int] = None
    founded_date: Optional[str] = None
    industry: Optional[str] = None
    status: Optional[str] = None

    def to_dict(self) -> dict:
        return asdict(self)


class GBizInfoClient:
    """Client for the gBizINFO REST API.

    Requires a valid API token. Register for free at https://info.gbiz.go.jp/.
    The token is passed via the X-hojinInfo-api-token header.
    """

    def __init__(self, api_token: Optional[str] = None, rate_limit_seconds: float = 1.0):
        self.session = requests.Session()
        # Try keychain if no token provided
        token = api_token or get_api_token()
        if not token:
            raise ValueError(
                "gBizINFO API token not found. "
                "Register at https://info.gbiz.go.jp/ and store the token:\n"
                "  security add-generic-password -s gbizinfo-api-token -a gbizinfo -w YOUR_TOKEN"
            )
        self.session.headers.update({
            "Accept": "application/json",
            "X-hojinInfo-api-token": token,
        })
        self.rate_limit = rate_limit_seconds
        self._last_request: float = 0
        self._available = True

    @property
    def available(self) -> bool:
        return self._available

    def search_by_name(self, company_name: str, limit: int = 5) -> List[CorporateInfo]:
        """Search companies by name.

        gBizINFO response structure (per hojin-infos item):
          - corporate_number: 法人番号
          - name: 法人名
          - kana: フリガナ
          - location: 所在地
          - capital_stock: 資本金 (from business results summary)
          - employee_number: 従業員数
          - date_of_establishment: 設立日
          - business_summary: 事業概要
          - status: ステータス
        """
        self._throttle()
        try:
            resp = self.session.get(
                BASE_URL,
                params={"name": company_name, "page": 1, "limit": limit},
                timeout=15,
            )
            if resp.status_code == 401:
                logger.error("gBizINFO: Invalid or expired API token (401)")
                self._available = False
                return []
            if resp.status_code == 404:
                return []
            resp.raise_for_status()
            data = resp.json()

            results = []
            for item in data.get("hojin-infos", []):
                results.append(self._parse_item(item))
            return results

        except requests.exceptions.Timeout:
            logger.warning(f"gBizINFO: Timeout searching for '{company_name}'")
            return []
        except requests.exceptions.ConnectionError:
            logger.warning("gBizINFO: Connection error")
            self._available = False
            return []
        except Exception as e:
            logger.warning(f"gBizINFO: Search failed for '{company_name}': {e}")
            return []

    def get_by_corporate_number(self, corp_number: str) -> Optional[CorporateInfo]:
        """Fetch a specific company by corporate number (法人番号)."""
        self._throttle()
        try:
            resp = self.session.get(f"{BASE_URL}/{corp_number}", timeout=15)
            if resp.status_code in (401, 404):
                return None
            resp.raise_for_status()
            data = resp.json()
            items = data.get("hojin-infos", [])
            if items:
                return self._parse_item(items[0])
            return None
        except Exception as e:
            logger.warning(f"gBizINFO: Lookup failed for '{corp_number}': {e}")
            return None

    def _parse_item(self, item: dict) -> CorporateInfo:
        """Parse a single hojin-infos item into CorporateInfo."""
        return CorporateInfo(
            corporate_number=item.get("corporate_number", ""),
            name=item.get("name", ""),
            name_kana=item.get("kana"),
            address=item.get("location"),
            capital=self._parse_int(item.get("capital_stock")),
            employee_count=self._parse_int(item.get("employee_number")),
            founded_date=item.get("date_of_establishment"),
            industry=item.get("business_summary"),
            status=item.get("status"),
        )

    def _throttle(self):
        """Rate-limit requests to be polite to the government API."""
        elapsed = time.time() - self._last_request
        if elapsed < self.rate_limit:
            time.sleep(self.rate_limit - elapsed)
        self._last_request = time.time()

    @staticmethod
    def _parse_int(value) -> Optional[int]:
        if value is None:
            return None
        try:
            return int(str(value).replace(",", ""))
        except (ValueError, TypeError):
            return None
