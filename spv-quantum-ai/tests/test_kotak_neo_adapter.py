import pytest
import asyncio
import time
import unittest.mock as mock
from unittest.mock import AsyncMock, MagicMock
from datetime import datetime, timezone
from core.bus import event_bus, EventModel
from brokers.models import BrokerResponse, Funds, Order, OrderStatus, OrderSide, OrderType
from brokers.kotak_neo import KotakNeoAdapter, KotakAuthenticationManager


def _mock_successful_authenticate(auth_mgr: KotakAuthenticationManager):
    """Order-execution tests care about session state transitions, not the real
    TOTP+MPIN handshake — that's covered separately below. Patch authenticate()
    to succeed without hitting Kotak's real API."""
    async def fake_authenticate() -> bool:
        auth_mgr.client = MagicMock()
        auth_mgr.session_token = "fake-edit-token-for-tests"
        auth_mgr.token_expiry = time.time() + 480.0
        return True
    return mock.patch.object(auth_mgr, "authenticate", side_effect=fake_authenticate)


@pytest.mark.asyncio
async def test_kotak_neo_adapter_connect_disconnect():
    adapter = KotakNeoAdapter()
    assert adapter.is_connected() is False

    with _mock_successful_authenticate(adapter.auth_mgr):
        resp = await adapter.connect()
        assert resp.success is True
        assert adapter.is_connected() is True
        assert adapter.session_mgr.session_status == "CONNECTED"

        resp = await adapter.disconnect()
        assert resp.success is True
        assert adapter.is_connected() is False
        assert adapter.session_mgr.session_status == "DISCONNECTED"


@pytest.mark.asyncio
async def test_kotak_neo_adapter_reconnect_and_events():
    event_bus.start()

    events = []
    async def capture_event(evt: EventModel):
        events.append(evt)

    await event_bus.subscribe("kotak_connected", capture_event)
    await event_bus.subscribe("kotak_order_placed", capture_event)
    await event_bus.subscribe("kotak_order_filled", capture_event)
    await event_bus.subscribe("kotak_disconnected", capture_event)
    await event_bus.subscribe("kotak_session_expired", capture_event)

    try:
        adapter = KotakNeoAdapter()
        with _mock_successful_authenticate(adapter.auth_mgr):
            await adapter.connect()

            await asyncio.sleep(0.05)
            assert any(e.event_type == "kotak_connected" for e in events)

            # Test place order triggers placement and fill events
            resp = await adapter.place_order(
                symbol="RELIANCE",
                side=OrderSide.BUY,
                quantity=10.0,
                order_type=OrderType.MARKET,
                price=2500.0
            )
            assert resp.success is True

            await asyncio.sleep(0.05)
            assert any(e.event_type == "kotak_order_placed" for e in events)
            assert any(e.event_type == "kotak_order_filled" for e in events)

            # Force reconnect simulation
            await adapter.session_mgr.reconnect()
            assert adapter.is_connected() is True

            # Test disconnect
            await adapter.disconnect()
            await asyncio.sleep(0.05)
            assert any(e.event_type == "kotak_disconnected" for e in events)

    finally:
        await event_bus.stop()


def test_kotak_order_manager_status_mapping():
    adapter = KotakNeoAdapter()
    mgr = adapter.order_mgr

    assert mgr.map_status("Complete") == OrderStatus.FILLED
    assert mgr.map_status("Open") == OrderStatus.OPEN
    assert mgr.map_status("Trg Pending") == OrderStatus.TRIGGER_PENDING
    assert mgr.map_status("Partially Filled") == OrderStatus.PARTIAL
    assert mgr.map_status("Cancelled") == OrderStatus.CANCELLED
    assert mgr.map_status("Rejected") == OrderStatus.REJECTED
    assert mgr.map_status("InvalidStatus") == OrderStatus.NEW


@pytest.mark.asyncio
async def test_kotak_funds_manager():
    adapter = KotakNeoAdapter()
    with _mock_successful_authenticate(adapter.auth_mgr):
        await adapter.connect()

        resp = await adapter.get_funds()
        assert resp.success is True
        assert resp.data["equity"] == 150000.0

        margin_resp = await adapter.get_margin()
        assert margin_resp.success is True
        assert margin_resp.data["available_margin"] == 150000.0


# ── Real TOTP+MPIN login flow ─────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_authenticate_fails_fast_when_credentials_missing():
    auth_mgr = KotakAuthenticationManager()
    with mock.patch("brokers.kotak_neo.settings") as mock_settings:
        mock_settings.KOTAK_NEO_CONSUMER_KEY = None
        mock_settings.KOTAK_NEO_MOBILE_NUMBER = None
        mock_settings.KOTAK_NEO_UCC = None
        mock_settings.KOTAK_NEO_MPIN = None
        mock_settings.KOTAK_NEO_TOTP_SECRET = None
        mock_settings.KOTAK_NEO_ENVIRONMENT = "prod"

        result = await auth_mgr.authenticate()

    assert result is False
    assert auth_mgr.client is None
    assert auth_mgr.is_token_valid() is False


@pytest.mark.asyncio
async def test_authenticate_totp_login_and_validate_success_sets_edit_token():
    auth_mgr = KotakAuthenticationManager()

    with mock.patch("brokers.kotak_neo.settings") as mock_settings:
        mock_settings.KOTAK_NEO_CONSUMER_KEY = "test-consumer-key"
        mock_settings.KOTAK_NEO_MOBILE_NUMBER = "+919999999999"
        mock_settings.KOTAK_NEO_UCC = "TESTUCC"
        mock_settings.KOTAK_NEO_MPIN = "123456"
        mock_settings.KOTAK_NEO_TOTP_SECRET = "JBSWY3DPEHPK3PXP"  # arbitrary valid base32 seed
        mock_settings.KOTAK_NEO_ENVIRONMENT = "prod"

        mock_client = MagicMock()
        mock_config = mock_client.api_client.configuration

        def fake_totp_login(mobile_number=None, ucc=None, totp=None):
            mock_config.view_token = "view-token-123"
            mock_config.sid = "sid-123"
            return {"data": {"token": "view-token-123", "sid": "sid-123"}}

        def fake_totp_validate(mpin=None):
            mock_config.edit_token = "edit-token-456"
            mock_config.edit_sid = "edit-sid-456"
            return {"data": {"token": "edit-token-456", "sid": "edit-sid-456"}}

        mock_totp_api = MagicMock()
        mock_totp_api.totp_login.side_effect = fake_totp_login
        mock_totp_api.totp_validate.side_effect = fake_totp_validate

        with mock.patch("brokers.kotak_neo.NeoAPI", return_value=mock_client):
            with mock.patch("brokers.kotak_neo.TotpAPI", return_value=mock_totp_api):
                result = await auth_mgr.authenticate()

    assert result is True
    assert auth_mgr.session_token == "edit-token-456"
    assert auth_mgr.is_token_valid() is True
    mock_totp_api.totp_validate.assert_called_once_with(mpin="123456")


@pytest.mark.asyncio
async def test_authenticate_returns_false_when_totp_validate_fails():
    auth_mgr = KotakAuthenticationManager()

    with mock.patch("brokers.kotak_neo.settings") as mock_settings:
        mock_settings.KOTAK_NEO_CONSUMER_KEY = "test-consumer-key"
        mock_settings.KOTAK_NEO_MOBILE_NUMBER = "+919999999999"
        mock_settings.KOTAK_NEO_UCC = "TESTUCC"
        mock_settings.KOTAK_NEO_MPIN = "wrong-mpin"
        mock_settings.KOTAK_NEO_TOTP_SECRET = "JBSWY3DPEHPK3PXP"
        mock_settings.KOTAK_NEO_ENVIRONMENT = "prod"

        mock_client = MagicMock()
        mock_config = mock_client.api_client.configuration

        def fake_totp_login(mobile_number=None, ucc=None, totp=None):
            mock_config.view_token = "view-token-123"
            mock_config.sid = "sid-123"
            return {"data": {"token": "view-token-123", "sid": "sid-123"}}

        def fake_totp_validate(mpin=None):
            # Simulate a real failure response: edit_token/edit_sid never get set.
            mock_config.edit_token = None
            mock_config.edit_sid = None
            return {"error": "Invalid MPIN"}

        mock_totp_api = MagicMock()
        mock_totp_api.totp_login.side_effect = fake_totp_login
        mock_totp_api.totp_validate.side_effect = fake_totp_validate

        with mock.patch("brokers.kotak_neo.NeoAPI", return_value=mock_client):
            with mock.patch("brokers.kotak_neo.TotpAPI", return_value=mock_totp_api):
                result = await auth_mgr.authenticate()

    assert result is False
    assert auth_mgr.is_token_valid() is False
