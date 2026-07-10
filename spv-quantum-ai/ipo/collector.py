import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import httpx

from sqlalchemy import select
from database.connection import async_session
from database.models import IPOIssueModel, IPOSubscriptionSnapshotModel
from core.logging import get_logger

logger = get_logger("ipo_collector")

NSE_BASE = "https://www.nseindia.com"
NSE_API = f"{NSE_BASE}/api"

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
}

_PRICE_RANGE_RE = re.compile(r"Rs\.?\s*([\d,.]+)\s*to\s*Rs\.?\s*([\d,.]+)", re.IGNORECASE)


def _parse_price_band(price_str: Optional[str]) -> tuple[Optional[float], Optional[float]]:
    """'Rs.203 to Rs.214' -> (203.0, 214.0). A single flat price ('99') has
    no band — both bounds come back equal. Anything unparseable returns
    (None, None) rather than a guessed number."""
    if not price_str:
        return None, None
    price_str = price_str.strip()
    m = _PRICE_RANGE_RE.search(price_str)
    if m:
        try:
            return float(m.group(1).replace(",", "")), float(m.group(2).replace(",", ""))
        except ValueError:
            return None, None
    # Flat single price, e.g. "Rs.1000" or "99" — strip the currency prefix
    # first so its own "." isn't misread as the number's decimal point.
    cleaned = re.sub(r"^Rs\.?\s*", "", price_str, flags=re.IGNORECASE)
    m2 = re.search(r"[\d,]+\.?\d*", cleaned)
    if not m2:
        return None, None
    try:
        val = float(m2.group(0).replace(",", ""))
        return val, val
    except ValueError:
        return None, None


def _parse_nse_date(date_str: Optional[str]) -> Optional[datetime]:
    """NSE dates come as 'dd-Mon-yyyy' (mixed case, e.g. '09-Jul-2026' or
    '07-JUL-2026'). Returns None (never a guessed date) if unparseable."""
    if not date_str:
        return None
    for fmt in ("%d-%b-%Y", "%d-%B-%Y"):
        try:
            return datetime.strptime(date_str.strip(), fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def _to_float(val: Any) -> Optional[float]:
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


class IPOCollector:
    """
    Fetches real IPO data from NSE's public JSON API (verified working
    endpoints, not scraped from rendered HTML). NSE blocks direct API
    requests without a warmed-up browser-like session, so every fetch
    first visits the NSE homepage to acquire cookies before calling the
    API with them. There is no synthetic/fabricated fallback — if NSE is
    unreachable or a field is missing, that field stays null rather than
    being invented.
    """

    def __init__(self) -> None:
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(headers=_HEADERS, timeout=15.0, follow_redirects=True)
        # Warm up cookies — NSE returns 401/403 on the API without a prior
        # visit to a normal page establishing session cookies.
        try:
            await self._client.get(NSE_BASE, headers={**_HEADERS, "Accept": "text/html"})
        except httpx.HTTPError as e:
            logger.warning("NSE session warmup failed", error=str(e))
        return self._client

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def _fetch_json(self, path: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        client = await self._get_client()
        try:
            resp = await client.get(f"{NSE_API}{path}", params=params, headers={**_HEADERS, "Referer": NSE_BASE})
            resp.raise_for_status()
            data = resp.json()
            return data if isinstance(data, list) else []
        except httpx.HTTPError as e:
            logger.error(f"NSE fetch failed for {path}", error=str(e))
            return []
        except ValueError as e:
            logger.error(f"NSE response for {path} was not valid JSON", error=str(e))
            return []

    async def fetch_current(self) -> List[Dict[str, Any]]:
        """Currently open IPOs, with live subscription figures."""
        return await self._fetch_json("/ipo-current-issue")

    async def fetch_upcoming(self) -> List[Dict[str, Any]]:
        """Mix of Active (open) and Forthcoming (truly upcoming) issues —
        the caller distinguishes via the `status` field."""
        return await self._fetch_json("/all-upcoming-issues", params={"category": "ipo"})

    async def fetch_past(self, from_date: Optional[str] = None, to_date: Optional[str] = None) -> List[Dict[str, Any]]:
        """Listed IPOs. from_date/to_date as 'dd-mm-yyyy'; NSE defaults to
        the last 90 days when omitted."""
        params = {}
        if from_date:
            params["fromDate"] = from_date
        if to_date:
            params["toDate"] = to_date
        return await self._fetch_json("/public-past-issues", params=params or None)

    # ── Normalization ──────────────────────────────────────────────────────

    @staticmethod
    def _normalize_current_or_upcoming(item: Dict[str, Any], force_status: Optional[str] = None) -> Optional[Dict[str, Any]]:
        symbol = item.get("symbol")
        if not symbol:
            return None
        if force_status:
            status = force_status
        else:
            status = "OPEN" if (item.get("status") or "").upper() == "ACTIVE" else "UPCOMING"
        low, high = _parse_price_band(item.get("issuePrice"))
        return {
            "symbol": symbol,
            "company_name": item.get("companyName", symbol),
            "status": status,
            "fields": {
                "security_type": item.get("series"),
                "price_band_low": low,
                "price_band_high": high,
                "issue_size": _to_float(item.get("issueSize")),
                "issue_start_date": _parse_nse_date(item.get("issueStartDate")),
                "issue_end_date": _parse_nse_date(item.get("issueEndDate")),
            },
            "raw": item,
        }

    @staticmethod
    def _normalize_past(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        symbol = item.get("symbol")
        if not symbol:
            return None
        low, high = _parse_price_band(item.get("priceRange"))
        return {
            "symbol": symbol,
            "company_name": item.get("companyName") or item.get("company") or symbol,
            "status": "LISTED",
            "fields": {
                "security_type": item.get("securityType"),
                "price_band_low": low,
                "price_band_high": high,
                "issue_start_date": _parse_nse_date(item.get("ipoStartDate")),
                "issue_end_date": _parse_nse_date(item.get("ipoEndDate")),
                "listing_date": _parse_nse_date(item.get("listingDate")),
                "listing_price": _to_float(item.get("issuePrice")),
            },
            "raw": item,
        }

    # ── Persistence ────────────────────────────────────────────────────────
    # Bulk per collection pass: dedupe by symbol in Python first (NSE's own
    # feeds contain repeat symbols — e.g. multiple bond/NCD tranches sharing
    # a base symbol), one bulk SELECT to find existing rows, then a single
    # commit. Avoids both a commit-per-row pattern (public-past-issues alone
    # can return 1000+ rows) and the UNIQUE-constraint crash a naive
    # select-then-insert loop hits on in-batch duplicates under this
    # session's autoflush=False.

    async def _bulk_upsert(self, normalized: List[Dict[str, Any]]) -> int:
        if not normalized:
            return 0
        by_symbol: Dict[str, Dict[str, Any]] = {n["symbol"]: n for n in normalized}  # last one wins

        async with async_session() as session:
            result = await session.execute(
                select(IPOIssueModel).where(IPOIssueModel.symbol.in_(by_symbol.keys()))
            )
            existing = {row.symbol: row for row in result.scalars().all()}

            for symbol, n in by_symbol.items():
                row = existing.get(symbol)
                if row is None:
                    row = IPOIssueModel(symbol=symbol, company_name=n["company_name"], status=n["status"])
                    session.add(row)
                row.company_name = n["company_name"] or row.company_name
                row.status = n["status"]
                for key, value in n["fields"].items():
                    if value is not None:
                        setattr(row, key, value)
                row.raw_data = n["raw"]

            await session.commit()
        return len(by_symbol)

    async def _record_subscriptions(self, items: List[Dict[str, Any]]) -> None:
        async with async_session() as session:
            for item in items:
                symbol = item.get("symbol")
                subscription_times = _to_float(item.get("noOfTime"))
                if not symbol or subscription_times is None:
                    continue  # no real figure reported — don't fabricate a row
                session.add(IPOSubscriptionSnapshotModel(
                    ipo_symbol=symbol,
                    category=item.get("category") or "Total",
                    shares_offered=_to_float(item.get("noOfSharesOffered")),
                    shares_bid=_to_float(item.get("noOfsharesBid")),
                    subscription_times=subscription_times,
                ))
            await session.commit()

    async def collect_all(self) -> Dict[str, int]:
        """Runs a full collection pass across all three real NSE sources.
        Returns counts collected per category for observability."""
        current_items = await self.fetch_current()
        current_normalized = [
            n for n in (self._normalize_current_or_upcoming(i, force_status="OPEN") for i in current_items) if n
        ]
        current_count = await self._bulk_upsert(current_normalized)
        await self._record_subscriptions(current_items)

        upcoming_items = await self.fetch_upcoming()
        upcoming_normalized = [
            n for n in (self._normalize_current_or_upcoming(i) for i in upcoming_items) if n
        ]
        upcoming_count = await self._bulk_upsert(upcoming_normalized)

        past_items = await self.fetch_past()
        past_normalized = [n for n in (self._normalize_past(i) for i in past_items) if n]
        past_count = await self._bulk_upsert(past_normalized)

        counts = {"current": current_count, "upcoming": upcoming_count, "past": past_count}
        logger.info("IPO collection pass complete", **counts)
        return counts


# Singleton
ipo_collector = IPOCollector()
