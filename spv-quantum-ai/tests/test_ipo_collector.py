import pytest
from datetime import datetime, timezone
from sqlalchemy import delete, select
from database.connection import async_session
from database.models import IPOIssueModel, IPOSubscriptionSnapshotModel
from ipo.collector import (
    IPOCollector, _parse_price_band, _parse_nse_date, _to_float,
)


# ── Pure parsing functions ──────────────────────────────────────────────────

def test_parse_price_band_range():
    assert _parse_price_band("Rs.203 to Rs.214") == (203.0, 214.0)

def test_parse_price_band_flat_price():
    assert _parse_price_band("Rs.1000") == (1000.0, 1000.0)

def test_parse_price_band_missing():
    assert _parse_price_band(None) == (None, None)
    assert _parse_price_band("") == (None, None)

def test_parse_price_band_garbage_returns_none_not_a_guess():
    assert _parse_price_band("TBA") == (None, None)

def test_parse_nse_date_standard_format():
    d = _parse_nse_date("09-Jul-2026")
    assert d == datetime(2026, 7, 9, tzinfo=timezone.utc)

def test_parse_nse_date_uppercase_month():
    d = _parse_nse_date("07-JUL-2026")
    assert d == datetime(2026, 7, 7, tzinfo=timezone.utc)

def test_parse_nse_date_missing_returns_none():
    assert _parse_nse_date(None) is None
    assert _parse_nse_date("") is None

def test_parse_nse_date_unparseable_returns_none_not_a_guess():
    assert _parse_nse_date("not a date") is None

def test_to_float_valid_and_invalid():
    assert _to_float("123.45") == 123.45
    assert _to_float(None) is None
    assert _to_float("not a number") is None


# ── Normalization ────────────────────────────────────────────────────────────

def test_normalize_current_forces_open_status():
    collector = IPOCollector()
    item = {"symbol": "TEST1", "companyName": "Test Co", "status": "Forthcoming", "issuePrice": "Rs.100 to Rs.110"}
    result = collector._normalize_current_or_upcoming(item, force_status="OPEN")
    assert result["status"] == "OPEN"
    assert result["fields"]["price_band_low"] == 100.0

def test_normalize_upcoming_maps_active_to_open_and_forthcoming_to_upcoming():
    collector = IPOCollector()
    active = collector._normalize_current_or_upcoming({"symbol": "A", "status": "Active"})
    forthcoming = collector._normalize_current_or_upcoming({"symbol": "B", "status": "Forthcoming"})
    assert active["status"] == "OPEN"
    assert forthcoming["status"] == "UPCOMING"

def test_normalize_skips_items_with_no_symbol():
    collector = IPOCollector()
    assert collector._normalize_current_or_upcoming({"companyName": "No Symbol Co"}) is None
    assert collector._normalize_past({"companyName": "No Symbol Co"}) is None

def test_normalize_past_uses_price_range_field_and_listing_data():
    collector = IPOCollector()
    item = {
        "symbol": "LISTED1", "companyName": "Listed Co", "priceRange": "Rs.94 to Rs.99",
        "listingDate": "10-JUL-2026", "issuePrice": "99",
    }
    result = collector._normalize_past(item)
    assert result["status"] == "LISTED"
    assert result["fields"]["price_band_low"] == 94.0
    assert result["fields"]["listing_price"] == 99.0


# ── Bulk upsert: dedup + real persistence ───────────────────────────────────

async def _clean(*symbols: str) -> None:
    async with async_session() as session:
        await session.execute(delete(IPOIssueModel).where(IPOIssueModel.symbol.in_(symbols)))
        await session.execute(delete(IPOSubscriptionSnapshotModel).where(IPOSubscriptionSnapshotModel.ipo_symbol.in_(symbols)))
        await session.commit()


@pytest.mark.asyncio
async def test_bulk_upsert_creates_new_rows():
    await _clean("BULKTEST1", "BULKTEST2")
    collector = IPOCollector()
    normalized = [
        {"symbol": "BULKTEST1", "company_name": "Co 1", "status": "OPEN", "fields": {"price_band_low": 100.0}, "raw": {}},
        {"symbol": "BULKTEST2", "company_name": "Co 2", "status": "UPCOMING", "fields": {"price_band_low": 200.0}, "raw": {}},
    ]
    count = await collector._bulk_upsert(normalized)
    assert count == 2

    async with async_session() as session:
        result = await session.execute(select(IPOIssueModel).where(IPOIssueModel.symbol == "BULKTEST1"))
        row = result.scalars().first()
        assert row is not None
        assert row.status == "OPEN"
        assert row.price_band_low == 100.0
    await _clean("BULKTEST1", "BULKTEST2")


@pytest.mark.asyncio
async def test_bulk_upsert_handles_in_batch_duplicate_symbols_without_crashing():
    """This is the exact scenario that crashed before the fix: NSE's own
    feed contains the same symbol more than once in a single fetch."""
    await _clean("DUPTEST")
    collector = IPOCollector()
    normalized = [
        {"symbol": "DUPTEST", "company_name": "Dup Co", "status": "LISTED", "fields": {"listing_price": 100.0}, "raw": {}},
        {"symbol": "DUPTEST", "company_name": "Dup Co", "status": "LISTED", "fields": {"listing_price": 150.0}, "raw": {}},
    ]
    count = await collector._bulk_upsert(normalized)
    assert count == 1  # deduped

    async with async_session() as session:
        result = await session.execute(select(IPOIssueModel).where(IPOIssueModel.symbol == "DUPTEST"))
        rows = result.scalars().all()
        assert len(rows) == 1
        assert rows[0].listing_price == 150.0  # last one wins
    await _clean("DUPTEST")


@pytest.mark.asyncio
async def test_bulk_upsert_updates_existing_row_not_duplicate():
    await _clean("UPDATETEST")
    collector = IPOCollector()
    await collector._bulk_upsert([
        {"symbol": "UPDATETEST", "company_name": "V1", "status": "OPEN", "fields": {"price_band_low": 100.0}, "raw": {}},
    ])
    await collector._bulk_upsert([
        {"symbol": "UPDATETEST", "company_name": "V1", "status": "LISTED", "fields": {"listing_price": 120.0}, "raw": {}},
    ])
    async with async_session() as session:
        result = await session.execute(select(IPOIssueModel).where(IPOIssueModel.symbol == "UPDATETEST"))
        rows = result.scalars().all()
        assert len(rows) == 1
        assert rows[0].status == "LISTED"
        assert rows[0].price_band_low == 100.0  # preserved, not overwritten by a None
        assert rows[0].listing_price == 120.0
    await _clean("UPDATETEST")


@pytest.mark.asyncio
async def test_record_subscriptions_skips_items_without_real_figures():
    await _clean("NOSUBTEST")
    collector = IPOCollector()
    await collector._record_subscriptions([
        {"symbol": "NOSUBTEST", "noOfTime": None},
        {"symbol": "", "noOfTime": "5.0"},
    ])
    async with async_session() as session:
        result = await session.execute(
            select(IPOSubscriptionSnapshotModel).where(IPOSubscriptionSnapshotModel.ipo_symbol == "NOSUBTEST")
        )
        assert result.scalars().all() == []
    await _clean("NOSUBTEST")
