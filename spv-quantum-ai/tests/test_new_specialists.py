import pytest
import asyncio
from datetime import datetime, timezone
from core.bus import event_bus, EventModel
from market.models import Timeframe, Candle
from employees.models import EmployeeState, EmployeeType
from employees import employee_engine

@pytest.fixture(autouse=True)
def reset_employee_engine():
    employee_engine.manager.active_code = None
    employee_engine.manager.profiles.clear()
    yield
    employee_engine.manager.active_code = None
    employee_engine.manager.profiles.clear()

@pytest.mark.asyncio
async def test_new_specialists_initialization_and_lifecycle():
    await employee_engine.start()
    
    try:
        # Give heartbeat loops a fraction of a second to run
        await asyncio.sleep(0.2)
        
        # Verify that all new employees are loaded and registered
        expected_codes = [
            "EMP-MOM", "EMP-VWP", "EMP-RGM", "EMP-OIE", "EMP-PCR", "EMP-GRK", "EMP-MPN",
            "EMP-SME", "EMP-LQD", "EMP-OFL", "EMP-DEL", "EMP-RSK", "EMP-PZS", "EMP-CPT",
            "EMP-EXP", "EMP-NWS", "EMP-CAL", "EMP-EVR", "EMP-EXE", "EMP-PTF", "EMP-PPR",
            "EMP-OPT", "EMP-EQI", "EMP-EQS", "EMP-COM", "EMP-CUR", "EMP-PM"
        ]
        
        for code in expected_codes:
            profile = employee_engine.manager.get_profile(code)
            assert profile is not None, f"Employee {code} not registered"
            assert profile.is_active is True, f"Employee {code} is not active"
            assert profile.health_status == "HEALTHY", f"Employee {code} is not healthy"
            
    finally:
        await employee_engine.stop()

@pytest.mark.asyncio
async def test_momentum_employee_analysis():
    event_bus.start()
    # Verify MomentumEmployee calculates RSI and reacts
    await employee_engine.start()
    try:
        mom_emp = employee_engine.momentum
        
        # Push 20 candles
        for i in range(20):
            candle = Candle(
                symbol="NIFTY50",
                timeframe=Timeframe.M1,
                open=100.0,
                high=102.0,
                low=98.0,
                close=100.0 + (i * 2.0), # Rising RSI
                volume=1000.0,
                complete=True,
                timestamp=datetime.now(timezone.utc)
            )
            await event_bus.publish(EventModel(
                event_type="candle",
                source_agent="market_data_engine",
                payload=candle.model_dump()
            ))
            
        await asyncio.sleep(0.05)
        res = mom_emp.latest_results.get("NIFTY50")
        assert res is not None
        assert res["rsi"] > 50.0
    finally:
        await employee_engine.stop()
        await event_bus.stop()

@pytest.mark.asyncio
async def test_vwap_employee_analysis():
    event_bus.start()
    await employee_engine.start()
    try:
        vwp_emp = employee_engine.vwap_emp
        
        # Publish some candles
        for i in range(5):
            candle = Candle(
                symbol="NIFTY50",
                timeframe=Timeframe.M1,
                open=100.0,
                high=105.0,
                low=95.0,
                close=102.0,
                volume=1000.0,
                complete=True,
                timestamp=datetime.now(timezone.utc)
            )
            await event_bus.publish(EventModel(
                event_type="candle",
                source_agent="market_data_engine",
                payload=candle.model_dump()
            ))
            
        await asyncio.sleep(0.05)
        res = vwp_emp.latest_results.get("NIFTY50")
        assert res is not None
        assert res["vwap"] > 0.0
    finally:
        await employee_engine.stop()
        await event_bus.stop()


@pytest.mark.asyncio
async def test_risk_employee_reacts_to_real_safety_events():
    """RiskEmployee used to subscribe to 'safety_status', an event nothing ever
    publishes (SafetyEngine emits 'safety_blocked' / 'safety_check_passed' per
    order instead), so it was permanently stuck on its default 'WAIT' decision."""
    event_bus.start()
    await employee_engine.start()
    try:
        risk_emp = employee_engine.risk_emp
        assert risk_emp.latest_results == {}

        await event_bus.publish(EventModel(
            event_type="safety_check_passed",
            source_agent="safety_engine",
            payload={"order_details": {"symbol": "RELIANCE"}, "response": {"allowed": True}}
        ))
        await asyncio.sleep(0.05)
        res = risk_emp.latest_results.get("RELIANCE")
        assert res is not None
        assert res["recommendation"] == "BUY"

        await event_bus.publish(EventModel(
            event_type="safety_blocked",
            source_agent="safety_engine",
            payload={"order_details": {"symbol": "RELIANCE"}, "response": {"allowed": False}}
        ))
        await asyncio.sleep(0.05)
        res = risk_emp.latest_results.get("RELIANCE")
        assert res["recommendation"] == "WAIT"
    finally:
        await employee_engine.stop()
        await event_bus.stop()


@pytest.mark.asyncio
async def test_market_regime_employee_reacts_to_real_regime_events():
    """MarketRegimeEmployee used to subscribe to 'regime_changed' (never published)
    and read payload['regime'] (wrong key); RegimeEngine actually publishes
    'market_regime' with the value under 'market_regime'."""
    event_bus.start()
    await employee_engine.start()
    try:
        regime_emp = employee_engine.market_regime
        assert regime_emp.latest_results == {}

        await event_bus.publish(EventModel(
            event_type="market_regime",
            source_agent="regime_engine",
            payload={"symbol": "NIFTY50", "market_regime": "TRENDING_BULLISH", "confidence": 80.0}
        ))
        await asyncio.sleep(0.05)
        res = regime_emp.latest_results.get("NIFTY50")
        assert res is not None
        assert res["recommendation"] == "TRENDING_BULLISH"
        assert res["confidence"] == 80.0
    finally:
        await employee_engine.stop()
        await event_bus.stop()


@pytest.mark.asyncio
async def test_capital_protection_employee_reacts_to_real_pnl_events():
    """CapitalProtectionEmployee used to subscribe to 'pnl_update' (never published);
    PortfolioEngine actually publishes 'pnl_updated'."""
    event_bus.start()
    await employee_engine.start()
    try:
        cap_emp = employee_engine.cap_protection
        assert cap_emp.latest_results == {}

        await event_bus.publish(EventModel(
            event_type="pnl_updated",
            source_agent="portfolio_engine",
            payload={"realized_pnl": 100.0, "unrealized_pnl": 50.0, "mtm": 150.0}
        ))
        await asyncio.sleep(0.05)
        res = cap_emp.latest_results.get("SYSTEM")
        assert res is not None
        assert res["recommendation"] == "BUY"
    finally:
        await employee_engine.stop()
        await event_bus.stop()


@pytest.mark.asyncio
async def test_paper_trading_employee_reacts_to_real_session_events():
    """PaperTradingEmployee used to subscribe to 'paper_status_changed' (never
    published); PaperTradingEngine actually publishes 'paper_trade_started' and
    'paper_trade_stopped'."""
    event_bus.start()
    await employee_engine.start()
    try:
        paper_emp = employee_engine.paper_trading
        assert paper_emp.latest_results == {}

        await event_bus.publish(EventModel(
            event_type="paper_trade_started",
            source_agent="paper_trading_engine",
            payload={"session_id": "PPS-test"}
        ))
        await asyncio.sleep(0.05)
        res = paper_emp.latest_results.get("SYSTEM")
        assert res is not None
        assert res["recommendation"] == "BUY"

        await event_bus.publish(EventModel(
            event_type="paper_trade_stopped",
            source_agent="paper_trading_engine",
            payload={"session_id": "PPS-test"}
        ))
        await asyncio.sleep(0.05)
        res = paper_emp.latest_results.get("SYSTEM")
        assert res["recommendation"] == "WAIT"
    finally:
        await employee_engine.stop()
        await event_bus.stop()
