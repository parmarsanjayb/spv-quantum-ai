import pytest
import asyncio
from datetime import datetime, timezone
from execution.models import ExecutionOrder, OrderLifecycleStatus, OrderProductType
from execution.validator import OrderValidator
from execution.tracker import OrderTracker
from execution.queue import ExecutionQueue
from execution.engine import ExecutionEngine
from agents.execution_agent import ExecutionAgent
from core.bus import event_bus, EventModel
from brokers.manager import broker_manager
from market.manager import market_data_manager
from market.models import MarketData

# ── Validator Tests ───────────────────────────────────────────────────────────

def test_order_validator():
    val = OrderValidator()
    
    # 1. Valid MARKET order
    o1 = ExecutionOrder(symbol="BTCUSD", side="BUY", order_type="MARKET", quantity=1.0)
    ok, msg = val.validate(o1)
    assert ok is True
    
    # 2. Invalid side
    o2 = ExecutionOrder(symbol="BTCUSD", side="HOLD", order_type="MARKET", quantity=1.0)
    ok, msg = val.validate(o2)
    assert ok is False
    
    # 3. Invalid quantity
    o3 = ExecutionOrder(symbol="BTCUSD", side="SELL", order_type="MARKET", quantity=-0.5)
    ok, msg = val.validate(o3)
    assert ok is False
    
    # 4. LIMIT missing price
    o4 = ExecutionOrder(symbol="BTCUSD", side="BUY", order_type="LIMIT", quantity=1.0)
    ok, msg = val.validate(o4)
    assert ok is False
    
    # 5. SL-M missing stop price
    o5 = ExecutionOrder(symbol="BTCUSD", side="BUY", order_type="SL-M", quantity=1.0)
    ok, msg = val.validate(o5)
    assert ok is False


# ── Tracker & Queue Tests ─────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_order_tracker():
    tracker = OrderTracker()
    order = ExecutionOrder(symbol="BTCUSD", side="BUY", order_type="MARKET", quantity=1.0)
    
    await tracker.add_order(order)
    await tracker.update_status(order.order_id, OrderLifecycleStatus.SENT, "Dispatched")
    
    o = await tracker.get_order(order.order_id)
    assert o is not None
    assert o.status == OrderLifecycleStatus.SENT
    
    trail = await tracker.get_audit_trail(order.order_id)
    assert len(trail) == 2
    assert trail[1][1] == OrderLifecycleStatus.SENT


@pytest.mark.asyncio
async def test_execution_queue():
    queue = ExecutionQueue()
    processed = []
    
    async def cb(order: ExecutionOrder):
        processed.append(order)
        
    queue.set_callback(cb)
    await queue.start()
    
    order = ExecutionOrder(symbol="BTCUSD", side="BUY", order_type="MARKET", quantity=1.0)
    await queue.enqueue(order)
    
    # Wait for execution
    await asyncio.sleep(0.05)
    assert len(processed) == 1
    assert processed[0].order_id == order.order_id
    
    await queue.stop()


# ── Engine End-to-End Tests ───────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_execution_engine_pipeline():
    event_bus.start()
    await broker_manager.load("paper_broker")
    broker_manager._active_broker_name = "paper_broker"
    from safety import safety_engine
    safety_engine.config["trading_session_guard"] = False
    safety_engine.config["holiday_guard"] = False
    safety_engine.config["market_closing_guard"] = False
    safety_engine.config["broker_disconnect_guard"] = False
    safety_engine.config["cooldown_between_trades_sec"] = 0.0
    
    # Disable random partial fills and rejections for tests
    broker = broker_manager.get_active()
    broker._partial_fill_rate = 0.0
    broker._rejection_rate = 0.0
    
    engine = ExecutionEngine(max_retries=1, retry_delay_sec=0.01)
    await engine.start()
    
    # Track filled order events
    filled_events = []
    async def cb(evt: EventModel):
        filled_events.append(evt)
        
    await event_bus.subscribe("order_filled", cb)

    # Market orders now fill at the real cached LTP, never a placeholder —
    # seed one so this MARKET order has a real price to fill against.
    await market_data_manager.cache.update_tick(MarketData(symbol="ETHUSD", ltp=3500.0))

    # Submit valid market order
    req = {"symbol": "ETHUSD", "side": "BUY", "quantity": 2.0, "type": "MARKET"}
    order = await engine.submit_order_request(req)
    
    # Await engine loop processing
    for _ in range(20):
        if order.status == OrderLifecycleStatus.FILLED:
            break
        await asyncio.sleep(0.05)
        
    assert order.status == OrderLifecycleStatus.FILLED
    assert order.filled_quantity == 2.0
    assert order.broker_order_id is not None
    
    # Verify event bus notification
    for _ in range(20):
        if len(filled_events) >= 1:
            break
        await asyncio.sleep(0.05)
        
    assert len(filled_events) == 1
    assert filled_events[0].payload["order"]["order_id"] == order.order_id
    
    await engine.stop()
    await event_bus.unsubscribe("order_filled", cb)
    await event_bus.stop()


@pytest.mark.asyncio
async def test_execution_engine_retries():
    # Test retry handler by simulating place_order exception
    await broker_manager.load("paper_broker")
    broker_manager._active_broker_name = "paper_broker"
    from safety import safety_engine
    safety_engine.config["trading_session_guard"] = False
    safety_engine.config["holiday_guard"] = False
    safety_engine.config["market_closing_guard"] = False
    safety_engine.config["broker_disconnect_guard"] = False
    safety_engine.config["cooldown_between_trades_sec"] = 0.0
    engine = ExecutionEngine(max_retries=2, retry_delay_sec=0.01)
    await engine.start()
    
    # Mock broker failure
    broker = broker_manager.get_active()
    original_place = broker.place_order
    
    calls = []
    async def mock_place(*args, **kwargs):
        calls.append(args)
        raise ConnectionError("Broker connection reset")
        
    broker.place_order = mock_place
    
    req = {"symbol": "ETHUSD", "side": "SELL", "quantity": 1.0, "type": "MARKET"}
    order = await engine.submit_order_request(req)
    
    # Wait for retries to exhaust
    for _ in range(20):
        if order.status == OrderLifecycleStatus.FAILED:
            break
        await asyncio.sleep(0.05)
        
    assert order.status == OrderLifecycleStatus.FAILED
    assert order.retry_count == 2
    assert len(calls) == 3 # 1 original + 2 retries
    
    # Restore mock
    broker.place_order = original_place
    await engine.stop()


# ── Execution Agent Integration ───────────────────────────────────────────────

@pytest.mark.asyncio
async def test_execution_agent_integration():
    event_bus.start()
    await broker_manager.load("paper_broker")
    broker_manager._active_broker_name = "paper_broker"
    from safety import safety_engine
    safety_engine.config["trading_session_guard"] = False
    safety_engine.config["holiday_guard"] = False
    safety_engine.config["market_closing_guard"] = False
    safety_engine.config["broker_disconnect_guard"] = False
    safety_engine.config["cooldown_between_trades_sec"] = 0.0
    safety_engine.config["max_exposure_usd"] = 5000000.0
    safety_engine.config["max_position_size_usd"] = 5000000.0
    
    broker = broker_manager.get_active()
    broker._partial_fill_rate = 0.0
    broker._rejection_rate = 0.0
    
    agent = ExecutionAgent()
    await agent.initialize()
    agent.status = "RUNNING"
    
    # Simulate approved order request event
    event = EventModel(
        source_agent="risk_agent",
        event_type="order_approved",
        payload={"symbol": "NIFTY50", "side": "BUY", "quantity": 50.0, "price": 24000.0, "type": "LIMIT"}
    )
    
    result = await agent.analyze(event)
    assert result is not None
    assert result.signal == "BUY"
    assert "order_id" in result.metadata
    assert result.metadata["status"] == OrderLifecycleStatus.FILLED.value
    
    await agent.shutdown()
    await event_bus.stop()
