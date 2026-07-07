import pytest
import asyncio
from datetime import datetime, timezone
from core.bus import event_bus, EventModel
from brokers.models import OrderSide, OrderType
from brokers.manager import broker_manager
from portfolio.engine import portfolio_engine
from safety.models import SafetyStatus
from safety import safety_engine

@pytest.mark.asyncio
async def test_safety_engine_lifecycle_and_guards():
    await safety_engine.start()
    assert safety_engine._running is True
    
    # Disable session/holiday guards for tests
    safety_engine.config["trading_session_guard"] = False
    safety_engine.config["holiday_guard"] = False
    safety_engine.config["market_closing_guard"] = False
    safety_engine.config["broker_disconnect_guard"] = False
    safety_engine.config["cooldown_between_trades_sec"] = 0.0

    # 1. Base allowance check
    order_data = {"symbol": "INFY", "side": "BUY", "quantity": 10.0, "price": 1400.0, "type": "MARKET"}
    resp = await safety_engine.check_order(order_data)
    assert resp.allowed is True
    assert resp.status == SafetyStatus.PASSED

    # 2. Daily Loss Guard Block simulation
    original_loss_guard = safety_engine.config["daily_loss_guard_usd"]
    safety_engine.config["daily_loss_guard_usd"] = 100.0
    
    # Mock portfolio realized pnl to trigger daily loss guard
    original_realized_pnl = portfolio_engine.summary.realized_pnl
    portfolio_engine.summary.realized_pnl = -200.0

    resp = await safety_engine.check_order(order_data)
    assert resp.allowed is False
    assert resp.status == SafetyStatus.BLOCKED
    assert "Daily loss guard" in resp.reason

    # Restore
    portfolio_engine.summary.realized_pnl = original_realized_pnl
    safety_engine.config["daily_loss_guard_usd"] = original_loss_guard
    await safety_engine.stop()

@pytest.mark.asyncio
async def test_emergency_manager_functions():
    await safety_engine.start()
    safety_engine.config["trading_session_guard"] = False
    safety_engine.config["holiday_guard"] = False
    safety_engine.config["market_closing_guard"] = False
    safety_engine.config["broker_disconnect_guard"] = False
    safety_engine.config["cooldown_between_trades_sec"] = 0.0
    
    try:
        # Pause trading
        await safety_engine.manager.emergency.pause_trading()
        assert safety_engine.manager.emergency.trading_paused is True
        
        order_data = {"symbol": "INFY", "side": "BUY", "quantity": 10.0, "price": 1400.0, "type": "MARKET"}
        resp = await safety_engine.check_order(order_data)
        assert resp.allowed is False
        assert "paused" in resp.reason.lower()

        # Resume trading
        await safety_engine.manager.emergency.resume_trading()
        assert safety_engine.manager.emergency.trading_paused is False
        
        # Trigger kill switch
        await safety_engine.manager.emergency.trigger_kill_switch("Test switch")
        assert safety_engine.manager.emergency.kill_switch_active is True
        
        resp = await safety_engine.check_order(order_data)
        assert resp.allowed is False
        assert "kill switch" in resp.reason.lower()

        # Reset kill switch
        await safety_engine.manager.emergency.reset_kill_switch()
        assert safety_engine.manager.emergency.kill_switch_active is False

    finally:
        await safety_engine.stop()

@pytest.mark.asyncio
async def test_hidden_stop_loss_and_trailing():
    await safety_engine.start()
    await broker_manager.load("paper_broker")
    broker_manager._active_broker_name = "paper_broker"
    
    # Configure custom parameters
    safety_engine.config["hidden_sl_pct"] = 2.0
    safety_engine.config["trailing_stop_pct"] = 1.0
    safety_engine.config["break_even_shift_pct"] = 1.5
    safety_engine.config["profit_lock_pct"] = 3.0

    pm = safety_engine.manager.protection
    
    # Register long position: entry 100, SL at 98
    pm.register_position("TCS", "BUY", 10.0, 100.0)
    assert "TCS" in pm.active_sls
    assert pm.active_sls["TCS"]["sl_price"] == 98.0

    # 1. Price goes up to 101 - no trailing update because highest remains close
    await pm._evaluate_price_update("TCS", 101.0)
    # Highest update to 101.0, trailing SL trailing 1% behind highest => 101 * 0.99 = 99.99 > 98.0
    assert pm.active_sls["TCS"]["sl_price"] == 99.99
    
    # 2. Break-even Shift: Profit reaches 1.5% at 101.5
    await pm._evaluate_price_update("TCS", 101.5)
    # Highest update to 101.5, trailing SL: 101.5 * 0.99 = 100.485
    assert pm.active_sls["TCS"]["sl_price"] == 100.485
    assert pm.active_sls["TCS"]["sl_shifted_to_be"] is True

    # 3. Trigger Hidden stop-loss: price falls below SL (100.485) to 100.0
    event_bus.start()
    hidden_events = []
    async def capture_hidden(evt: EventModel):
        hidden_events.append(evt)
    await event_bus.subscribe("hidden_stop_triggered", capture_hidden)
    
    try:
        await pm._evaluate_price_update("TCS", 100.0)
        assert "TCS" not in pm.active_sls  # Position should be closed out and removed
        
        await asyncio.sleep(0.05)
        assert len(hidden_events) == 1
        assert hidden_events[0].payload["symbol"] == "TCS"
    finally:
        await event_bus.unsubscribe("hidden_stop_triggered", capture_hidden)
        await event_bus.stop()
        await safety_engine.stop()


@pytest.mark.asyncio
async def test_trading_guard_session_check_uses_ist_not_server_local_time():
    """NSE market hours (09:15-15:30) are always in IST. TradingGuard.check_session
    used to call .astimezone() with no timezone argument, which resolves to the
    server/container's local timezone (UTC in this environment, or anything else
    depending on deployment) instead of India Standard Time - so on a UTC server,
    trades were spuriously allowed/blocked based on the wrong clock entirely.
    Real-exchange-hours guards only apply to non-paper (live) brokers - the paper
    broker's mock feed runs continuously and is exempt (see next test)."""
    from datetime import datetime, timezone as tz, time as dt_time
    from unittest.mock import patch, MagicMock
    from zoneinfo import ZoneInfo
    from safety.guard import TradingGuard

    guard = TradingGuard({"trading_session_guard": True})

    fake_broker = MagicMock()
    fake_broker.name = "kotak_neo"
    with patch("safety.guard.broker_manager.get_active", return_value=fake_broker):
        allowed, reason = await guard.check_session({"symbol": "INFY"})

    ist_now = datetime.now(tz.utc).astimezone(ZoneInfo("Asia/Kolkata")).time()
    expected_allowed = dt_time(9, 15) <= ist_now <= dt_time(15, 30)

    assert allowed == expected_allowed
    if not expected_allowed:
        assert "outside allowed window" in reason


@pytest.mark.asyncio
async def test_trading_guard_session_check_bypassed_for_paper_broker():
    """Real-exchange-hours guards (session/holiday/market-closing) model NSE's
    actual trading calendar, which doesn't apply to the paper broker's mock feed -
    paper trading must be exercisable anytime, not just during real NSE hours."""
    from unittest.mock import patch, MagicMock
    from safety.guard import TradingGuard

    guard = TradingGuard({
        "trading_session_guard": True,
        "holiday_guard": True,
        "market_closing_guard": True,
    })

    fake_broker = MagicMock()
    fake_broker.name = "paper_broker"
    with patch("safety.guard.broker_manager.get_active", return_value=fake_broker):
        allowed, _ = await guard.check_session({"symbol": "INFY"})
        assert allowed is True
        allowed, _ = await guard.check_holiday({"symbol": "INFY"})
        assert allowed is True
        allowed, _ = await guard.check_market_closing({"symbol": "INFY"})
        assert allowed is True
