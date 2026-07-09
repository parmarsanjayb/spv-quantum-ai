import pytest
import asyncio
import time
import unittest.mock as mock
from unittest.mock import MagicMock
from datetime import datetime, timezone
from core.bus import event_bus, EventModel
from brokers.models import BrokerResponse, BrokerState, Order, OrderSide, OrderType, OrderStatus
from brokers.resolver import BrokerResolver
from brokers.factory import BrokerFactory
from brokers.registry import BrokerRegistry
from brokers.kotak_neo import KotakAuthenticationManager
from brokers import broker_engine


def _mock_kotak_authentication():
    """Patches the class method so broker_manager's internally-created
    KotakNeoAdapter authenticates without hitting Kotak's real API."""
    async def fake_authenticate(self) -> bool:
        self.client = MagicMock()
        self.session_token = "fake-edit-token-for-tests"
        self.token_expiry = time.time() + 480.0
        return True
    return mock.patch.object(KotakAuthenticationManager, "authenticate", fake_authenticate)

@pytest.mark.asyncio
async def test_broker_resolver_and_registry():
    # Verify resolver returns configured broker name
    active_name = BrokerResolver.resolve_active_name()
    assert active_name in ["paper_broker", "kotak_neo"]  # standard defaults

    # Verify registry mappings
    path = BrokerRegistry.get_class_path("kotak_neo")
    assert path == "brokers.kotak_neo.KotakNeoAdapter"
    
    registered = BrokerRegistry.get_registered_brokers()
    assert "kotak_neo" in registered
    assert "zerodha" in registered

@pytest.mark.asyncio
async def test_broker_factory():
    # Test Kotak Neo adapter instantiation
    kotak = BrokerFactory.create_broker("kotak_neo")
    assert kotak.name == "kotak_neo"
    assert kotak.is_connected() is False

    # Test Paper Broker instantiation
    paper = BrokerFactory.create_broker("paper_broker")
    assert paper.name == "paper_broker"

@pytest.mark.asyncio
async def test_broker_engine_lifecycle_and_events():
    event_bus.start()
    
    events = []
    async def capture_event(evt: EventModel):
        events.append(evt)
        
    await event_bus.subscribe("broker_connected", capture_event)
    await event_bus.subscribe("broker_disconnected", capture_event)
    await event_bus.subscribe("broker_order_placed", capture_event)

    try:
        with _mock_kotak_authentication():
            # Test connection
            resp = await broker_engine.connect("kotak_neo")
            assert resp.success is True
            assert broker_engine.get_broker_state("kotak_neo") == BrokerState.CONNECTED

            await asyncio.sleep(0.05)
            assert any(e.event_type == "broker_connected" for e in events)

            # Test place order
            order_resp = await broker_engine.place_order(
                symbol="INFY",
                side=OrderSide.BUY,
                quantity=5.0,
                order_type=OrderType.MARKET,
                price=1400.0
            )
            assert order_resp.success is True

            await asyncio.sleep(0.05)
            assert any(e.event_type == "broker_order_placed" for e in events)

            # Test disconnect
            disc_resp = await broker_engine.disconnect("kotak_neo")
            assert disc_resp.success is True
            assert broker_engine.get_broker_state("kotak_neo") == BrokerState.DISCONNECTED

            await asyncio.sleep(0.05)
            assert any(e.event_type == "broker_disconnected" for e in events)

    finally:
        await event_bus.stop()
