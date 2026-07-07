import pytest
import asyncio
from datetime import datetime, timezone
from market.models import Timeframe
from analysis.models import MarketAnalysisReport
from scanner.models import ScanResult
from scoring.models import DecisionScoreResult, DecisionQuality, DecisionScoreEvent
from scoring.weights import WeightManager
from scoring.calculator import ConfidenceCalculator
from scoring.publisher import DecisionPublisher
from scoring.engine import DecisionScoringEngine
from core.bus import event_bus, EventModel

# ── WeightManager & Calculator Tests ──────────────────────────────────────────

def test_weight_manager_values():
    wm = WeightManager()
    assert wm.min_confidence_threshold == 60.0
    assert wm.get_weight("market_analysis") == 0.30
    assert wm.get_weight("market_regime") == 0.20
    assert wm.get_weight("strategy_match") == 0.25
    assert wm.get_weight("risk_status") == 0.25


def test_confidence_calculator_alignment_bonus():
    wm = WeightManager()
    calc = ConfidenceCalculator(wm)

    report = MarketAnalysisReport(
        symbol="BTCUSD", timeframe="1m", market_bias="BULLISH",
        trend_strength="STRONG", momentum="BULLISH", volatility="NORMAL",
        market_structure="TRENDING_BULLISH", support=60000.0, resistance=61000.0,
        recommended_strategy="sample", confidence=90.0, reasoning="Bullish context"
    )

    inputs = {
        "market_analysis": report,
        "market_regime": "TRENDING_BULLISH",
        "risk_status": "ALLOW",
        "strategy_matched": True,
        "strategy_action": "SIGNAL_BUY"
    }

    conf, comp_scores, quality, missing, conflicts = calc.calculate(inputs)
    
    # 0.3*90 + 0.2*100 + 0.25*100 + 0.25*100 = 27 + 20 + 25 + 25 = 97
    # +5 bonus for alignment (BULLISH bias, TRENDING_BULLISH regime, BULLISH momentum) -> capped at 100
    assert conf == 100.0
    assert quality == DecisionQuality.VERY_STRONG
    assert len(conflicts) == 0


def test_confidence_calculator_conflict_penalty():
    wm = WeightManager()
    calc = ConfidenceCalculator(wm)

    report = MarketAnalysisReport(
        symbol="BTCUSD", timeframe="1m", market_bias="BULLISH",
        trend_strength="STRONG", momentum="BEARISH", volatility="NORMAL", # Conflict: momentum bearish
        market_structure="TRENDING_BEARISH", support=60000.0, resistance=61000.0, # Conflict: regime bearish
        recommended_strategy="sample", confidence=80.0, reasoning="Mixed context"
    )

    inputs = {
        "market_analysis": report,
        "market_regime": "TRENDING_BEARISH",
        "risk_status": "ALLOW",
        "strategy_matched": True,
        "strategy_action": "SIGNAL_BUY" # Conflict: buy strategy with bearish regime/momentum?
    }

    conf, comp_scores, quality, missing, conflicts = calc.calculate(inputs)
    # Weighted base: 0.3*80 + 0.2*100 + 0.25*100 + 0.25*100 = 24 + 20 + 25 + 25 = 94
    # Conflicts:
    # 1. bullish_bias_in_bearish_regime
    # 2. bullish_bias_with_bearish_momentum
    # Total 2 conflicts. Penalty = 2 * 15 = 30.
    # Expected final score = 94 - 30 = 64
    assert conf == 64.0
    assert quality == DecisionQuality.MODERATE
    assert "bullish_bias_in_bearish_regime" in conflicts
    assert "bullish_bias_with_bearish_momentum" in conflicts


def test_confidence_calculator_risk_block():
    wm = WeightManager()
    calc = ConfidenceCalculator(wm)

    report = MarketAnalysisReport(
        symbol="BTCUSD", timeframe="1m", market_bias="BULLISH",
        trend_strength="STRONG", momentum="BULLISH", volatility="NORMAL",
        market_structure="TRENDING_BULLISH", support=60000.0, resistance=61000.0,
        recommended_strategy="sample", confidence=90.0, reasoning=""
    )

    inputs = {
        "market_analysis": report,
        "market_regime": "TRENDING_BULLISH",
        "risk_status": "BLOCK", # Enforce BLOCK
        "strategy_matched": True,
        "strategy_action": "SIGNAL_BUY"
    }

    conf, comp_scores, quality, missing, conflicts = calc.calculate(inputs)
    # Risk status BLOCK forces Decision Quality to INVALID
    assert quality == DecisionQuality.INVALID


# ── Engine & Publisher Tests ──────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_decision_scoring_engine_integration():
    event_bus.start()
    
    engine = DecisionScoringEngine()
    await engine.start()
    
    # Subscribe to decision scores published on Event Bus
    published_scores = []
    async def cb(evt: EventModel):
        published_scores.append(evt)
        
    await event_bus.subscribe("decision_score", cb)
    
    report = MarketAnalysisReport(
        symbol="BTCUSD",
        timeframe="1m",
        market_bias="BULLISH",
        trend_strength="STRONG",
        momentum="BULLISH",
        volatility="NORMAL",
        market_structure="TRENDING_BULLISH",
        support=60000.0,
        resistance=61000.0,
        recommended_strategy="sample_golden_cross",
        confidence=90.0,
        reasoning="Bullish analysis"
    )
    
    # Mock analysis update event to trigger engine evaluation automatically
    await event_bus.publish(EventModel(
        event_type="market_analysis",
        source_agent="market_analyst_agent",
        payload={"report": report.model_dump()}
    ))
    
    # Wait for Event Bus processing
    for _ in range(20):
        if len(published_scores) >= 1:
            break
        await asyncio.sleep(0.05)
    
    # Check result
    assert len(published_scores) == 1
    score = published_scores[0].payload["decision_score"]
    assert score["symbol"] == "BTCUSD"
    assert score["overall_confidence"] > 0.0
    
    cached = await engine.get_latest("BTCUSD", "1m")
    assert cached is not None
    assert cached.overall_confidence == score["overall_confidence"]
    
    await engine.stop()
    await event_bus.unsubscribe("decision_score", cb)
    await event_bus.stop()


@pytest.mark.asyncio
async def test_decision_scoring_engine_scanner_match_integration():
    """Scanner opportunities must also trigger an automatic decision score evaluation."""
    event_bus.start()

    engine = DecisionScoringEngine()
    await engine.start()

    published_scores = []
    async def cb(evt: EventModel):
        published_scores.append(evt)

    await event_bus.subscribe("decision_score", cb)

    scan_result = ScanResult(
        symbol="INFY",
        exchange="NSE",
        segment="Equity",
        scanner_name="volume_spike",
        priority=1,
        confidence=80.0,
        matched_conditions=["Volume spike: 5000.0 > average 2000.0 * 2.0"]
    )

    await event_bus.publish(EventModel(
        event_type="scanner_match",
        source_agent="market_scanner_engine",
        payload={"scan_result": scan_result.model_dump()}
    ))

    for _ in range(20):
        if len(published_scores) >= 1:
            break
        await asyncio.sleep(0.05)

    assert len(published_scores) == 1
    score = published_scores[0].payload["decision_score"]
    assert score["symbol"] == "INFY"
    assert score["timeframe"] == Timeframe.M1.value

    cached = await engine.get_latest("INFY", Timeframe.M1.value)
    assert cached is not None

    await engine.stop()
    await event_bus.unsubscribe("decision_score", cb)
    await event_bus.stop()


@pytest.mark.asyncio
async def test_evaluate_decision_translates_risk_engine_vocabulary():
    """RiskEngine.get_dashboard_metrics() reports OPERATIONAL/RESTRICTED (system-wide
    health), not the ALLOW/BLOCK/REDUCE_POSITION vocabulary used elsewhere in risk
    decisions. evaluate_decision() must translate it and expose the translated value
    on DecisionScoreResult.risk_status so downstream consumers (ChiefDecisionAgent)
    never see the raw dashboard vocabulary."""
    engine = DecisionScoringEngine()

    result = await engine.evaluate_decision("WIPRO", Timeframe.M1)

    assert result.risk_status in ("ALLOW", "BLOCK")
    assert result.component_scores["risk_status"] in (0.0, 60.0, 100.0)
    if result.risk_status == "ALLOW":
        assert result.component_scores["risk_status"] == 100.0
    else:
        assert result.component_scores["risk_status"] == 0.0

