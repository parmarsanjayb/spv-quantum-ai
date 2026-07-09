import pytest
import asyncio
import json
import unittest.mock as mock
from unittest.mock import AsyncMock, MagicMock
from market.models import MarketData, Timeframe, MarketSession, FeedStatus
from market.cache import DataCacheManager
from market.registry import SymbolRegistry
from market.instrument import InstrumentManager
from market.tick import TickDataManager
from market.candle import CandleManager
from market.health import FeedHealthMonitor
from market.status import MarketStatusManager
from market.manager import MarketDataManager
from core.bus import event_bus, EventModel


# ── DataCacheManager ──────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_cache_tick_and_session_boundaries() -> None:
    cache = DataCacheManager()
    tick = MarketData(symbol="BTCUSD", ltp=65000.0, volume=10.0, vwap=65000.0, bid=64990.0, ask=65010.0)
    await cache.update_tick(tick)

    stored = await cache.get_tick("BTCUSD")
    assert stored is not None
    assert stored.ltp == 65000.0
    assert await cache.get_session_high("BTCUSD") == 65000.0
    assert await cache.get_session_low("BTCUSD") == 65000.0

    # Higher tick raises session high
    tick2 = MarketData(symbol="BTCUSD", ltp=66000.0, volume=5.0, vwap=65333.0, bid=65990.0, ask=66010.0)
    await cache.update_tick(tick2)
    assert await cache.get_session_high("BTCUSD") == 66000.0
    assert await cache.get_session_low("BTCUSD") == 65000.0


@pytest.mark.asyncio
async def test_cache_reset_session() -> None:
    cache = DataCacheManager()
    tick = MarketData(symbol="NIFTY50", ltp=24200.0, volume=100.0, vwap=24200.0, bid=24199.0, ask=24201.0)
    await cache.update_tick(tick)
    await cache.reset_session("NIFTY50")
    assert await cache.get_volume("NIFTY50") == 0.0
    assert await cache.get_session_high("NIFTY50") == 0.0


# ── SymbolRegistry ────────────────────────────────────────────────────────────

def test_symbol_registry_operations() -> None:
    reg = SymbolRegistry()
    assert reg.is_registered("NIFTY50")
    reg.register("RELIANCE", {"exchange": "NSE"})
    assert reg.is_registered("RELIANCE")
    assert reg.get_meta("RELIANCE")["exchange"] == "NSE"
    reg.unregister("RELIANCE")
    assert not reg.is_registered("RELIANCE")


# ── InstrumentManager ─────────────────────────────────────────────────────────

def test_instrument_manager_lookup() -> None:
    mgr = InstrumentManager()
    inst = mgr.get("NIFTY50")
    assert inst is not None
    assert inst["lot_size"] == 65  # real NSE NIFTY50 lot size
    assert mgr.get_token("NIFTY50") == "26000"
    assert mgr.get_by_token("26000") == "NIFTY50"


# ── TickDataManager ───────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_tick_data_manager_vwap() -> None:
    cache = DataCacheManager()
    tm    = TickDataManager(cache)
    tick1 = await tm.process({"symbol": "ETHUSD", "price": 3500.0, "volume": 10.0})
    assert tick1.ltp == 3500.0
    assert tick1.vwap == 3500.0

    tick2 = await tm.process({"symbol": "ETHUSD", "price": 3600.0, "volume": 10.0})
    # VWAP = (3500*10 + 3600*10) / 20 = 3550
    assert tick2.vwap == 3550.0


# ── CandleManager ─────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_candle_manager_bar_close() -> None:
    closed_candles = []
    cache = DataCacheManager()

    async def on_close(candle):
        closed_candles.append(candle)

    cm   = CandleManager(cache, on_close)
    from datetime import datetime, timezone

    # Tick at start of minute 0
    t0 = datetime(2026, 7, 4, 9, 0, 0, tzinfo=timezone.utc)
    tick0 = MarketData(symbol="BTCUSD", ltp=65000.0, volume=5.0, timestamp=t0,
                       bid=64990.0, ask=65010.0, vwap=65000.0)
    await cm.process_tick(tick0)

    # Tick at minute 1 — should close the minute-0 bar
    t1 = datetime(2026, 7, 4, 9, 1, 5, tzinfo=timezone.utc)
    tick1 = MarketData(symbol="BTCUSD", ltp=65100.0, volume=8.0, timestamp=t1,
                       bid=65090.0, ask=65110.0, vwap=65050.0)
    await cm.process_tick(tick1)

    await asyncio.sleep(0.05)  # Allow create_task to fire
    assert len(closed_candles) >= 1
    c = closed_candles[0]
    assert c.symbol == "BTCUSD"
    assert c.timeframe == Timeframe.M1
    assert c.complete is True
    assert c.open == 65000.0


# ── FeedHealthMonitor ─────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_feed_health_monitor_stale_detection() -> None:
    event_bus.start()
    received = []

    async def cb(evt: EventModel):
        if evt.event_type == "feed_disconnected":
            received.append(evt)

    await event_bus.subscribe("feed_disconnected", cb)
    monitor = FeedHealthMonitor(stale_threshold_sec=0.1, check_interval_sec=0.1)
    monitor.signal_connected()
    await monitor.start()

    # Don't call record_tick → monitor detects stale after 0.1s.
    # DEGRADED (connection alive, ticks momentarily sparse) intentionally does
    # NOT fire feed_disconnected anymore — only a genuine DISCONNECTED does,
    # so the UI doesn't wipe live prices during a normal quiet moment.
    for _ in range(20):
        if monitor.get_status() == FeedStatus.DEGRADED:
            break
        await asyncio.sleep(0.05)

    assert monitor.get_status() == FeedStatus.DEGRADED
    assert len(received) == 0

    monitor.signal_disconnected("test forced disconnect")
    for _ in range(20):
        if len(received) >= 1:
            break
        await asyncio.sleep(0.05)
    assert len(received) >= 1

    await monitor.stop()
    await event_bus.unsubscribe("feed_disconnected", cb)
    await event_bus.stop()


# ── MarketStatusManager ───────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_market_status_transitions() -> None:
    event_bus.start()
    events_received = []

    async def cb(evt: EventModel):
        events_received.append(evt.event_type)

    await event_bus.subscribe("market_open", cb)
    await event_bus.subscribe("market_close", cb)
    await event_bus.subscribe("market_status_changed", cb)

    sm = MarketStatusManager()
    await sm.set_status(MarketSession.OPEN)
    assert sm.get_status() == MarketSession.OPEN

    await sm.set_status(MarketSession.CLOSED)
    assert sm.get_status() == MarketSession.CLOSED

    for _ in range(20):
        if "market_open" in events_received and "market_close" in events_received:
            break
        await asyncio.sleep(0.05)

    assert "market_open" in events_received
    assert "market_close" in events_received

    await event_bus.unsubscribe("market_open", cb)
    await event_bus.unsubscribe("market_close", cb)
    await event_bus.unsubscribe("market_status_changed", cb)
    await event_bus.stop()


# ── Full MarketDataManager integration ────────────────────────────────────────

@pytest.mark.asyncio
async def test_market_data_manager_has_no_price_source_without_kotak() -> None:
    """Without a reachable Kotak Neo feed, the manager must not fabricate any prices."""
    event_bus.start()
    mgr = MarketDataManager()
    await mgr.start()
    assert mgr.stream.is_connected() is False
    # Session status now reflects real NSE hours (9:15-15:30 IST, Mon-Fri), not
    # a hardcoded OPEN — compare against the same real-time computation rather
    # than assuming a fixed value that would only be true during market hours.
    assert mgr.status.get_status() == mgr.status.compute_real_session()

    await asyncio.sleep(0.2)
    for sym in mgr.registry.get_symbols():
        t = await mgr.cache.get_tick(sym)
        assert t is None

    await mgr.stop()
    assert mgr.status.get_status() == MarketSession.CLOSED
    await event_bus.stop()


@pytest.mark.asyncio
async def test_market_data_manager_integration_with_live_kotak_feed() -> None:
    """With a (mocked) authenticated Kotak Neo session, a real feed message flows
    through the full pipeline: WebSocketStreamManager -> MarketDataManager -> cache."""
    event_bus.start()
    mgr = MarketDataManager()

    mock_client = MagicMock()
    mock_config = mock_client.api_client.configuration

    def fake_totp_login(mobile_number=None, ucc=None, totp=None):
        mock_config.view_token = "view-token"
        mock_config.sid = "sid"
        return {"data": {"token": "view-token", "sid": "sid"}}

    def fake_totp_validate(mpin=None):
        mock_config.edit_token = "edit-token"
        mock_config.edit_sid = "edit-sid"
        return {"data": {"token": "edit-token", "sid": "edit-sid"}}

    mock_totp_api = MagicMock()
    mock_totp_api.totp_login.side_effect = fake_totp_login
    mock_totp_api.totp_validate.side_effect = fake_totp_validate

    with mock.patch("brokers.kotak_neo.settings") as mock_settings:
        mock_settings.KOTAK_NEO_CONSUMER_KEY = "test-key"
        mock_settings.KOTAK_NEO_MOBILE_NUMBER = "+919999999999"
        mock_settings.KOTAK_NEO_UCC = "TESTUCC"
        mock_settings.KOTAK_NEO_MPIN = "123456"
        mock_settings.KOTAK_NEO_TOTP_SECRET = "JBSWY3DPEHPK3PXP"
        mock_settings.KOTAK_NEO_ENVIRONMENT = "prod"

        with mock.patch("brokers.kotak_neo.NeoAPI", return_value=mock_client):
            with mock.patch("brokers.kotak_neo.TotpAPI", return_value=mock_totp_api):
                await mgr.start()
                await asyncio.sleep(0.2)
                assert mgr.stream.is_connected() is False  # on_open not fired by the mock yet

                # Simulate the SDK's background thread delivering a live tick.
                mock_client.on_message({
                    "type": "stock_feed",
                    "data": [{"tk": "26000", "e": "nse_cm", "iv": "24300.5", "ic": "24200.0"}],
                })
                await asyncio.sleep(0.1)

                tick = await mgr.cache.get_tick("NIFTY50")
                assert tick is not None
                assert tick.ltp == 24300.5

                await mgr.stop()

    assert mgr.status.get_status() == MarketSession.CLOSED
    await event_bus.stop()


# ── WebSocket reconnect (deterministic) ───────────────────────────────────────

@pytest.mark.asyncio
async def test_websocket_reconnect_failure_event() -> None:
    event_bus.start()
    received = []

    async def cb(evt: EventModel):
        if evt.event_type == "feed_disconnected":
            received.append(evt)

    await event_bus.subscribe("feed_disconnected", cb)

    from market.websocket import WebSocketStreamManager
    monitor = FeedHealthMonitor()
    stream = WebSocketStreamManager(
        on_raw_tick=lambda x: None,
        health_monitor=monitor,
        instruments=InstrumentManager(),
        registry=SymbolRegistry(),
    )
    stream._max_reconnects = 2

    with mock.patch("asyncio.sleep", new=AsyncMock()):
        await stream._backoff()   # attempt 1
        await stream._backoff()   # attempt 2 → fires event
        assert stream.is_connected() is False

    for _ in range(20):
        if len(received) >= 1:
            break
        await asyncio.sleep(0.05)

    assert len(received) >= 1

    await event_bus.unsubscribe("feed_disconnected", cb)
    await event_bus.stop()
