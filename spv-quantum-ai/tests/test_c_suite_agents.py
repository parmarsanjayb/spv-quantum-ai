import pytest
import asyncio
from datetime import datetime, timezone
from core.bus import event_bus, EventModel
from agents.chief_executive_officer_agent import ChiefExecutiveOfficerAgent
from agents.chief_investment_officer_agent import ChiefInvestmentOfficerAgent
from agents.chief_operating_officer_agent import ChiefOperatingOfficerAgent
from agents.chief_financial_officer_agent import ChiefFinancialOfficerAgent
from agents.chief_technology_officer_agent import ChiefTechnologyOfficerAgent

@pytest.mark.asyncio
async def test_ceo_agent_consensus():
    event_bus.start()
    
    ceo = ChiefExecutiveOfficerAgent()
    await ceo.start()

    decision_received = asyncio.Event()
    received_payload = {}

    async def on_chief_decision(event):
        nonlocal received_payload
        received_payload.update(event.payload)
        decision_received.set()

    await event_bus.subscribe("chief_decision", on_chief_decision)

    # Publish some mock employee decisions
    await event_bus.publish(EventModel(
        event_type="employee_decision",
        source_agent="momentum_employee",
        payload={
            "symbol": "NIFTY 50",
            "employee_code": "EMP-MOM",
            "decision": "BUY",
            "confidence": 85.0
        }
    ))

    await event_bus.publish(EventModel(
        event_type="employee_decision",
        source_agent="vwap_employee",
        payload={
            "symbol": "NIFTY 50",
            "employee_code": "EMP-VWP",
            "decision": "BUY",
            "confidence": 75.0
        }
    ))

    # Give event bus a tick to distribute
    await asyncio.sleep(0.1)

    # Publish decision_score
    await event_bus.publish(EventModel(
        event_type="decision_score",
        source_agent="strategy_agent",
        payload={
            "symbol": "NIFTY 50",
            "recommended_strategy": "trend_strategy",
            "side": "BUY",
            "price": 24200.0,
            "quantity": 10.0,
            "overall_confidence": 70.0
        }
    ))

    try:
        await asyncio.wait_for(decision_received.wait(), timeout=1.0)
    except asyncio.TimeoutError:
        pytest.fail("CEO chief_decision event was not received within timeout.")

    assert received_payload.get("final_decision") == "BUY"
    assert received_payload.get("confidence") == 80.0  # Avg of 85 and 75
    assert len(ceo.approved_queue) == 1
    
    await ceo.stop()
    await event_bus.stop()


@pytest.mark.asyncio
async def test_cio_allocation_rules():
    cio = ChiefInvestmentOfficerAgent()
    await cio.initialize()

    # Evaluate approval
    result = await cio._evaluate_allocation({
        "decision_id": "CEO-test-123",
        "symbol": "NIFTY 50",
        "side": "BUY",
        "quantity": 5.0,
        "price": 24200.0
    })

    assert result.signal in ("APPROVE", "REDUCE QUANTITY", "REJECT")
    assert len(cio.approved_queue) + len(cio.rejected_queue) == 1

    await cio.shutdown()


@pytest.mark.asyncio
async def test_coo_execution_tracking():
    coo = ChiefOperatingOfficerAgent()
    await coo.initialize()

    # Evaluate placement initiation
    result = await coo._initiate_execution({
        "symbol": "NIFTY 50",
        "final_quantity": 5.0,
        "price": 24200.0,
        "side": "BUY",
        "strategy_name": "trend_strategy"
    })

    assert result.signal == "EXECUTING"
    assert coo.total_orders == 1

    # Simulate fill
    await coo._record_success({
        "order": {
            "order_id": list(coo.pending_executions.keys())[0],
            "commission": 15.0,
            "charges": 2.5
        }
    })

    assert coo.successful_orders == 1
    assert len(coo.pending_executions) == 0

    await coo.shutdown()


@pytest.mark.asyncio
async def test_cfo_pnl_accounting():
    cfo = ChiefFinancialOfficerAgent()
    await cfo.initialize()

    # Simulate trade closed with positive realized PNL
    await cfo._on_trade_closed({
        "trade": {
            "realized_pnl": 5000.0,
            "net_pnl": 4900.0
        }
    })

    assert cfo.gross_profit == 5000.0
    assert cfo.net_profit == 5000.0
    assert len(cfo.equity_curve) == 2

    await cfo.shutdown()


@pytest.mark.asyncio
async def test_cto_telemetry_gathering():
    cto = ChiefTechnologyOfficerAgent()
    await cto.initialize()

    # Trigger single monitor round
    await cto._perform_telemetry_check()
    assert cto.recovery_count == 0

    await cto.shutdown()
