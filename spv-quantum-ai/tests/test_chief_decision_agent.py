import pytest
import asyncio
from datetime import datetime, timezone
from agents.chief_decision_agent import ChiefDecisionAgent, ApprovalManager, ConflictResolver
from core.bus import event_bus, EventModel
from portfolio.engine import portfolio_engine
from portfolio.models import Position

# ── Component Validation Tests ────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_approval_manager_checks():
    mgr = ApprovalManager()
    mgr.reset_daily_trades()
    
    # Clean portfolio state
    async with portfolio_engine.positions._lock:
        portfolio_engine.positions._positions.clear()
    portfolio_engine.summary.available_capital = 50000.0
    
    # 1. Successful check
    payload = {
        "symbol": "TCS",
        "overall_confidence": 75.0,
        "risk_status": "ALLOW"
    }
    state, code, desc = await mgr.validate_checks(payload)
    assert state == "APPROVED"
    assert code == "SUCCESS"
    
    # 2. Risk block
    payload_risk = {
        "symbol": "TCS",
        "overall_confidence": 75.0,
        "risk_status": "BLOCK"
    }
    state, code, desc = await mgr.validate_checks(payload_risk)
    assert state == "BLOCKED"
    assert code == "RISK_REJECTION"
    
    # 3. Low confidence
    payload_conf = {
        "symbol": "TCS",
        "overall_confidence": 30.0,
        "risk_status": "ALLOW"
    }
    state, code, desc = await mgr.validate_checks(payload_conf)
    assert state == "REJECTED"
    assert code == "CONFIDENCE_TOO_LOW"


@pytest.mark.asyncio
async def test_approval_manager_rejects_sell_with_no_open_position():
    """A SIGNAL_SELL exit with nothing open to close must be rejected —
    this system doesn't support naked shorting."""
    mgr = ApprovalManager()
    mgr.reset_daily_trades()
    async with portfolio_engine.positions._lock:
        portfolio_engine.positions._positions.clear()

    state, code, desc = await mgr.validate_checks({
        "symbol": "TCS", "overall_confidence": 75.0, "risk_status": "ALLOW", "side": "SELL",
    })
    assert state == "REJECTED"
    assert code == "NO_POSITION_TO_CLOSE"


@pytest.mark.asyncio
async def test_approval_manager_allows_sell_close_bypassing_position_and_capital_limits():
    """A SELL that closes an existing position must be approved even when
    the position-count/capital checks would otherwise block a new BUY —
    closing a position reduces risk, it shouldn't be blocked by the same
    limits meant to cap new exposure."""
    mgr = ApprovalManager()
    mgr.reset_daily_trades()
    mgr.max_open_positions = 1
    async with portfolio_engine.positions._lock:
        portfolio_engine.positions._positions.clear()
        portfolio_engine.positions._positions["TCS"] = Position(
            symbol="TCS", segment="Equity", side="BUY", quantity=7.0, avg_price=3500.0,
        )
    portfolio_engine.summary.available_capital = 0.0  # would block a BUY

    state, code, desc = await mgr.validate_checks({
        "symbol": "TCS", "overall_confidence": 75.0, "risk_status": "ALLOW", "side": "SELL",
    })
    assert state == "APPROVED"
    assert code == "SUCCESS"

    async with portfolio_engine.positions._lock:
        portfolio_engine.positions._positions.clear()


def test_conflict_resolver():
    resolver = ConflictResolver()
    assert resolver.resolve(85.0, "ALLOW") == "APPROVED"
    assert resolver.resolve(90.0, "BLOCK") == "REJECTED"
    assert resolver.resolve(40.0, "ALLOW") == "REJECTED"
    assert resolver.resolve(60.0, "ALLOW") == "MANUAL_REVIEW"


# ── Integration Tests ─────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_chief_decision_agent_integration():
    event_bus.start()
    
    agent = ChiefDecisionAgent()
    await agent.start()
    
    # Clear queues
    agent.approved_queue.clear()
    agent.rejected_queue.clear()
    agent.blocked_queue.clear()
    agent.coordinator.reset_daily_trades()
    
    # Setup mock margin
    portfolio_engine.summary.available_capital = 100000.0
    async with portfolio_engine.positions._lock:
        portfolio_engine.positions._positions.clear()

    # Track events
    approved_events = []
    async def cb(evt: EventModel):
        approved_events.append(evt)
        
    await event_bus.subscribe("trade_approved", cb)

    # Publish decision score event
    await event_bus.publish(EventModel(
        source_agent="decision_scoring_engine",
        event_type="decision_score",
        payload={
            "symbol": "RELIANCE",
            "overall_confidence": 85.0,
            "risk_status": "ALLOW",
            "recommended_strategy": "ema_crossover",
            "side": "BUY",
            "quantity": 10.0,
            "price": 2500.0
        }
    ))

    # Wait for processing
    for _ in range(20):
        if len(agent.approved_queue) >= 1:
            break
        await asyncio.sleep(0.05)

    assert len(agent.approved_queue) == 1
    assert agent.approved_queue[0]["symbol"] == "RELIANCE"
    assert agent.approved_queue[0]["status"] == "APPROVED"
    
    # Verify published event on bus
    for _ in range(20):
        if len(approved_events) >= 1:
            break
        await asyncio.sleep(0.05)
        
    assert len(approved_events) == 1
    assert approved_events[0].payload["symbol"] == "RELIANCE"
    assert approved_events[0].payload["status"] == "APPROVED"
    
    await agent.stop()
    await event_bus.unsubscribe("trade_approved", cb)
    await event_bus.stop()


@pytest.mark.asyncio
async def test_chief_decision_agent_routes_through_risk_before_execution():
    """Approved decisions must be routed via order_request (Risk Agent) rather than
    being sent straight to order_approved (Execution Agent), so the Risk step in the
    Market Feed -> Scanner -> Decision Score -> Chief Decision -> Risk -> Execution
    pipeline is not bypassed."""
    event_bus.start()

    agent = ChiefDecisionAgent()
    await agent.start()

    agent.approved_queue.clear()
    agent.coordinator.reset_daily_trades()

    portfolio_engine.summary.available_capital = 100000.0
    async with portfolio_engine.positions._lock:
        portfolio_engine.positions._positions.clear()

    order_requests = []
    order_approved_events = []

    async def on_order_request(evt: EventModel):
        order_requests.append(evt)

    async def on_order_approved(evt: EventModel):
        order_approved_events.append(evt)

    await event_bus.subscribe("order_request", on_order_request)
    await event_bus.subscribe("order_approved", on_order_approved)

    await event_bus.publish(EventModel(
        source_agent="decision_scoring_engine",
        event_type="decision_score",
        payload={
            "symbol": "HDFCBANK",
            "overall_confidence": 90.0,
            "risk_status": "ALLOW",
            "recommended_strategy": "ema_crossover",
            "side": "BUY",
            "quantity": 5.0,
            "price": 1600.0
        }
    ))

    for _ in range(20):
        if len(order_requests) >= 1:
            break
        await asyncio.sleep(0.05)

    assert len(order_requests) == 1
    assert order_requests[0].payload["symbol"] == "HDFCBANK"
    # The Chief Decision Agent must not publish order_approved directly - only the
    # Risk Agent is allowed to do that after validating the order_request.
    assert len(order_approved_events) == 0

    # The agent must not crash while building its AgentResultModel.
    assert agent.status == "RUNNING"

    await agent.stop()
    await event_bus.unsubscribe("order_request", on_order_request)
    await event_bus.unsubscribe("order_approved", on_order_approved)
    await event_bus.stop()
