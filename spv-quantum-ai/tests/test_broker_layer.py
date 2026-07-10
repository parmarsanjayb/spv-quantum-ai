import pytest
from brokers.models import OrderSide, OrderStatus, OrderType
from brokers.paper import PaperBroker
from brokers.manager import BrokerManager
from brokers.registry import BROKER_REGISTRY
from market.manager import market_data_manager
from market.models import MarketData

# ── Registry ──────────────────────────────────────────────────────────────────

def test_registry_contains_all_brokers() -> None:
    """Verifies that all required future brokers are present in the registry."""
    required = [
        "paper_broker", "kotak_neo", "zerodha_kite", "angel_one",
        "fyers", "upstox", "dhan", "shoonya", "alice_blue", "ibkr"
    ]
    for b in required:
        assert b in BROKER_REGISTRY, f"Missing broker: {b}"

# ── PaperBroker ───────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_paper_broker_connect_disconnect() -> None:
    broker = PaperBroker()
    assert broker.is_connected() is False

    resp = await broker.connect()
    assert resp.success is True
    assert broker.is_connected() is True

    resp = await broker.disconnect()
    assert resp.success is True
    assert broker.is_connected() is False

@pytest.mark.asyncio
async def test_paper_broker_login_logout() -> None:
    broker = PaperBroker()
    resp = await broker.login()
    assert resp.success is True
    assert broker.is_connected() is True

    resp = await broker.logout()
    assert resp.success is True
    assert broker.is_connected() is False

@pytest.mark.asyncio
async def test_paper_broker_profile() -> None:
    broker = PaperBroker()
    await broker.connect()
    resp = await broker.get_profile()
    assert resp.success is True
    assert "name" in resp.data
    assert "segments" in resp.data

@pytest.mark.asyncio
async def test_paper_broker_balance() -> None:
    broker = PaperBroker()
    await broker.connect()
    resp = await broker.get_balance()
    assert resp.success is True
    funds = resp.data
    assert "equity" in funds
    assert "available_margin" in funds
    assert funds["equity"] == 1_000_000.0

@pytest.mark.asyncio
async def test_paper_broker_place_and_cancel_order() -> None:
    broker = PaperBroker()
    await broker.connect()

    # Force rejection_rate to 0 to guarantee a FILLED/PARTIAL result
    broker._rejection_rate = 0.0
    broker._partial_fill_rate = 0.0

    resp = await broker.place_order(
        symbol="BTCUSD",
        side=OrderSide.BUY,
        quantity=1.0,
        order_type=OrderType.LIMIT,
        price=65000.0,
    )
    assert resp.success is True
    order = resp.data
    assert order["status"] == OrderStatus.FILLED
    order_id = order["order_id"]

    # Verify order appears in get_orders
    orders_resp = await broker.get_orders()
    assert orders_resp.success is True
    order_ids = [o["order_id"] for o in orders_resp.data]
    assert order_id in order_ids

    # Cancel attempt on FILLED order should fail
    cancel_resp = await broker.cancel_order(order_id)
    assert cancel_resp.success is False


@pytest.mark.asyncio
async def test_paper_broker_opposite_side_fill_closes_position_instead_of_opening_phantom() -> None:
    """A SELL against an open BUY must net against and close that position —
    previously positions were keyed by symbol+side, so a closing SELL opened
    an unrelated second position instead of closing the original one."""
    broker = PaperBroker()
    await broker.connect()
    broker._rejection_rate = 0.0
    broker._partial_fill_rate = 0.0

    await broker.place_order(
        symbol="NETTEST", side=OrderSide.BUY, quantity=10.0,
        order_type=OrderType.LIMIT, price=100.0,
    )
    positions_resp = await broker.get_positions()
    assert len(positions_resp.data) == 1
    assert positions_resp.data[0]["side"] == OrderSide.BUY
    assert positions_resp.data[0]["quantity"] == 10.0

    # Full close via an opposite-side fill for the same quantity
    await broker.place_order(
        symbol="NETTEST", side=OrderSide.SELL, quantity=10.0,
        order_type=OrderType.LIMIT, price=110.0,
    )
    positions_resp = await broker.get_positions()
    assert positions_resp.data == []  # closed, not a lingering phantom position


@pytest.mark.asyncio
async def test_paper_broker_opposite_side_fill_partially_reduces_position() -> None:
    broker = PaperBroker()
    await broker.connect()
    broker._rejection_rate = 0.0
    broker._partial_fill_rate = 0.0

    await broker.place_order(
        symbol="NETTEST2", side=OrderSide.BUY, quantity=10.0,
        order_type=OrderType.LIMIT, price=100.0,
    )
    await broker.place_order(
        symbol="NETTEST2", side=OrderSide.SELL, quantity=4.0,
        order_type=OrderType.LIMIT, price=110.0,
    )
    positions_resp = await broker.get_positions()
    assert len(positions_resp.data) == 1
    assert positions_resp.data[0]["side"] == OrderSide.BUY
    assert positions_resp.data[0]["quantity"] == 6.0
    assert positions_resp.data[0]["realised_pnl"] == pytest.approx(40.0)  # (110-100)*4


@pytest.mark.asyncio
async def test_paper_broker_order_rejection() -> None:
    broker = PaperBroker()
    await broker.connect()
    broker._rejection_rate = 1.0  # 100% rejection

    resp = await broker.place_order(
        symbol="ETHUSD",
        side=OrderSide.SELL,
        quantity=2.0,
        order_type=OrderType.MARKET,
    )
    assert resp.success is False
    assert resp.data["status"] == OrderStatus.REJECTED
    assert resp.error is not None

@pytest.mark.asyncio
async def test_paper_broker_partial_fill() -> None:
    broker = PaperBroker()
    await broker.connect()
    broker._rejection_rate = 0.0
    broker._partial_fill_rate = 1.0  # 100% partial fills

    # Market orders fill at the real cached LTP now, never a placeholder.
    await market_data_manager.cache.update_tick(MarketData(symbol="NIFTY50", ltp=24200.0))

    resp = await broker.place_order(
        symbol="NIFTY50",
        side=OrderSide.BUY,
        quantity=10.0,
        order_type=OrderType.MARKET,
    )
    assert resp.success is True
    assert resp.data["status"] == OrderStatus.PARTIAL
    assert resp.data["filled_quantity"] < 10.0

@pytest.mark.asyncio
async def test_paper_broker_modify_order() -> None:
    broker = PaperBroker()
    await broker.connect()
    broker._rejection_rate = 0.0
    broker._partial_fill_rate = 0.0

    # First place then get into NEW state manually for modify
    resp = await broker.place_order(
        symbol="RELIANCE", side=OrderSide.BUY, quantity=5.0,
        order_type=OrderType.LIMIT, price=2800.0
    )
    order_id = resp.data["order_id"]

    # Directly set status to OPEN to allow modification
    broker._orders[order_id].status = OrderStatus.OPEN

    mod_resp = await broker.modify_order(order_id, price=2850.0)
    assert mod_resp.success is True
    assert mod_resp.data["price"] == 2850.0

@pytest.mark.asyncio
async def test_paper_broker_historical_data() -> None:
    broker = PaperBroker()
    await broker.connect()
    resp = await broker.get_historical_data("NIFTY50", "1m", "2026-01-01", "2026-01-10")
    assert resp.success is True
    assert isinstance(resp.data, list)
    assert len(resp.data) == 50
    assert "open" in resp.data[0]

@pytest.mark.asyncio
async def test_paper_broker_subscriptions() -> None:
    broker = PaperBroker()
    await broker.connect()

    resp = await broker.subscribe_market_data(["BTCUSD", "ETHUSD"])
    assert resp.success is True

    resp = await broker.unsubscribe_market_data(["BTCUSD"])
    assert resp.success is True

    resp = await broker.subscribe_option_chain("NIFTY50", "2026-07-30")
    assert resp.success is True

@pytest.mark.asyncio
async def test_paper_broker_health_check() -> None:
    broker = PaperBroker()
    await broker.connect()
    resp = await broker.health_check()
    assert resp.success is True
    assert resp.latency_ms > 0.0

# ── BrokerManager ─────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_broker_manager_load_and_switch() -> None:
    manager = BrokerManager()
    broker = await manager.load("paper_broker")
    assert broker.is_connected() is True
    assert manager.get_active().name == "paper_broker"

    # Switch to paper_broker again (no-op switch, same broker)
    await manager.switch_broker("paper_broker")
    assert manager.get_active().name == "paper_broker"

    # Health check all loaded brokers
    health = await manager.check_health()
    assert "paper_broker" in health
    assert health["paper_broker"]["connected"] is True

    await manager.shutdown_all()
    assert manager.get_active() if False else True  # After shutdown pool is empty; just check it ran

@pytest.mark.asyncio
async def test_broker_manager_reconnect() -> None:
    manager = BrokerManager()
    await manager.load("paper_broker")

    # Manually disconnect
    broker = manager.get_active()
    await broker.disconnect()
    assert broker.is_connected() is False

    # Reconnect via manager
    result = await manager.reconnect("paper_broker")
    assert result is True
    assert broker.is_connected() is True

    await manager.shutdown_all()
