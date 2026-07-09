import pytest
import asyncio
import unittest.mock as mock
from unittest.mock import AsyncMock, MagicMock
from market.websocket import WebSocketStreamManager
from market.health import FeedHealthMonitor
from market.instrument import InstrumentManager
from market.registry import SymbolRegistry


def _make_manager():
    monitor = FeedHealthMonitor()
    instruments = InstrumentManager()
    registry = SymbolRegistry()
    received = []

    async def on_tick(raw):
        received.append(raw)

    manager = WebSocketStreamManager(
        on_raw_tick=on_tick,
        health_monitor=monitor,
        instruments=instruments,
        registry=registry,
    )
    return manager, received


def test_build_subscription_lists_splits_index_scrip_and_skips_crypto():
    manager, _ = _make_manager()
    index_tokens, scrip_tokens = manager._build_subscription_lists()

    index_symbols = {
        manager._token_by_key[(t["exchange_segment"], str(t["instrument_token"]))]
        for t in index_tokens
    }
    scrip_symbols = {
        manager._token_by_key[(t["exchange_segment"], str(t["instrument_token"]))]
        for t in scrip_tokens
    }

    assert {"NIFTY50", "BANKNIFTY", "FINNIFTY"} <= index_symbols
    assert {"RELIANCE", "GOLD", "USDINR"} <= scrip_symbols
    assert "BTCUSD" not in index_symbols and "BTCUSD" not in scrip_symbols
    assert "ETHUSD" not in index_symbols and "ETHUSD" not in scrip_symbols


def test_parse_feed_item_index():
    manager, _ = _make_manager()
    item = {
        "tk": "26000", "e": "nse_cm",
        "iv": "24300.5", "ic": "24200.0",
        "openingPrice": "24250.0", "highPrice": "24350.0", "lowPrice": "24150.0",
    }
    raw = manager._parse_feed_item("NIFTY50", item)
    assert raw["symbol"] == "NIFTY50"
    assert raw["ltp"] == 24300.5
    assert raw["prev_close"] == 24200.0
    assert raw["high"] == 24350.0
    assert raw["low"] == 24150.0


def test_parse_feed_item_scrip():
    manager, _ = _make_manager()
    item = {
        "tk": "2885", "e": "nse_cm", "ltp": "2950.25", "v": "1000", "oi": "0",
        "op": "2940.0", "h": "2960.0", "lo": "2930.0", "c": "2945.0", "ap": "2948.0",
    }
    raw = manager._parse_feed_item("RELIANCE", item)
    assert raw["symbol"] == "RELIANCE"
    assert raw["ltp"] == 2950.25
    assert raw["volume"] == 1000.0
    assert raw["vwap"] == 2948.0


@pytest.mark.asyncio
async def test_handle_feed_message_routes_to_on_raw_tick():
    manager, received = _make_manager()
    manager._build_subscription_lists()  # populate _token_by_key
    loop = asyncio.get_running_loop()

    message = {"type": "stock_feed", "data": [{"tk": "26000", "e": "nse_cm", "iv": "24300.5", "ic": "24200.0"}]}
    manager._handle_feed_message(message, loop)
    await asyncio.sleep(0.05)

    assert len(received) == 1
    assert received[0]["symbol"] == "NIFTY50"
    assert received[0]["ltp"] == 24300.5


@pytest.mark.asyncio
async def test_handle_feed_message_ignores_unknown_token():
    manager, received = _make_manager()
    manager._build_subscription_lists()
    loop = asyncio.get_running_loop()

    message = {"type": "stock_feed", "data": [{"tk": "999999", "e": "nse_cm", "ltp": "1.0"}]}
    manager._handle_feed_message(message, loop)
    await asyncio.sleep(0.05)

    assert received == []


@pytest.mark.asyncio
async def test_kotak_loop_stays_disconnected_when_auth_fails():
    manager, _ = _make_manager()
    manager._running = True
    manager._max_reconnects = 1

    mock_auth = AsyncMock()
    mock_auth.authenticate = AsyncMock(return_value=False)

    with mock.patch("brokers.kotak_neo.KotakAuthenticationManager", return_value=mock_auth):
        async def stop_after_backoff(*_args, **_kwargs):
            manager._running = False
        with mock.patch.object(manager, "_backoff", side_effect=stop_after_backoff):
            await manager._kotak_loop()

    assert not manager.is_connected()


@pytest.mark.asyncio
async def test_kotak_loop_subscribes_index_and_scrip_batches_when_authenticated():
    manager, _ = _make_manager()
    manager._running = True

    mock_client = MagicMock()
    mock_auth = AsyncMock()
    mock_auth.client = mock_client
    # First check (right after subscribing) reports expired so the loop falls
    # through immediately instead of sleeping in a real 5s wait loop; stop the
    # outer loop after this single iteration so the test doesn't run forever.
    mock_auth.is_token_valid = MagicMock(return_value=False)

    async def fake_authenticate():
        manager._running = False
        return True
    mock_auth.authenticate = AsyncMock(side_effect=fake_authenticate)

    with mock.patch("brokers.kotak_neo.KotakAuthenticationManager", return_value=mock_auth):
        with mock.patch("asyncio.sleep", new=AsyncMock()):
            await manager._kotak_loop()

    assert mock_client.subscribe.call_count == 2
    calls = mock_client.subscribe.call_args_list
    assert calls[0].kwargs["isIndex"] is True
    assert calls[1].kwargs["isIndex"] is False
    assert callable(mock_client.on_message)
    assert callable(mock_client.on_open)
    assert callable(mock_client.on_close)
    assert callable(mock_client.on_error)
