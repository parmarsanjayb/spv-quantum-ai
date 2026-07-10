import pytest
import asyncio
from datetime import datetime, timezone
import os
import yaml
from market.models import Timeframe, Candle
from strategies.models import Strategy, StrategyResponse, Condition, RuleGroup
from strategies.evaluator import ConditionEvaluator, RuleEngine
from strategies.loader import StrategyRegistry, StrategyLoader
from strategies.engine import StrategyEngine
from core.bus import event_bus, EventModel

# ── Evaluator & RuleEngine Tests ──────────────────────────────────────────────

def test_condition_evaluator_operators():
    evaluator = ConditionEvaluator()
    
    # Context representation: current and previous states
    context = {
        "current": {
            "indicators": {
                "RSI": 65.0,
                "EMA_9": 105.0,
                "EMA_20": 100.0,
                "MACD": {"macd_line": 2.5, "signal_line": 1.0}
            },
            "market_regime": "TRENDING_BULLISH",
            "market_data": {
                "ltp": 100.0,
                "volume": 5000.0
            }
        },
        "prev": {
            "indicators": {
                "EMA_9": 98.0,
                "EMA_20": 101.0
            }
        }
    }

    # 1. Greater than: RSI > 50
    cond1 = Condition(source="indicator", key="RSI", operator=">", value=50.0)
    assert evaluator.evaluate_condition(cond1, context) is True

    # 2. Less than: ltp < 150
    cond2 = Condition(source="market_data", key="ltp", operator="<", value=150.0)
    assert evaluator.evaluate_condition(cond2, context) is True

    # 3. Equals: market_regime == TRENDING_BULLISH
    cond3 = Condition(source="market_regime", operator="==", value="TRENDING_BULLISH")
    assert evaluator.evaluate_condition(cond3, context) is True

    # 4. Between / Inside Range: RSI between 60 and 70
    cond4 = Condition(source="indicator", key="RSI", operator="between", value=[60.0, 70.0])
    assert evaluator.evaluate_condition(cond4, context) is True

    # 5. Outside Range: RSI outside 10 and 50
    cond5 = Condition(source="indicator", key="RSI", operator="outside_range", value=[10.0, 50.0])
    assert evaluator.evaluate_condition(cond5, context) is True

    # 6. Crossover: EMA_9 crosses_above EMA_20
    # prev: EMA_9 (98) <= EMA_20 (101). current: EMA_9 (105) > EMA_20 (100)
    cond6 = Condition(source="indicator", key="EMA_9", operator="crosses_above", target="EMA_20")
    assert evaluator.evaluate_condition(cond6, context) is True

    # 7. Nested indicator values: MACD.macd_line > 2.0
    cond7 = Condition(source="indicator", key="MACD.macd_line", operator=">", value=2.0)
    assert evaluator.evaluate_condition(cond7, context) is True


def test_rule_engine_boolean_groups():
    engine = RuleEngine()
    
    context = {
        "current": {
            "indicators": {"RSI": 65.0},
            "market_regime": "TRENDING_BULLISH"
        }
    }

    cond_rsi = Condition(source="indicator", key="RSI", operator=">", value=50.0)
    cond_regime = Condition(source="market_regime", operator="==", value="TRENDING_BULLISH")
    cond_rsi_fail = Condition(source="indicator", key="RSI", operator="<", value=40.0)

    # 1. AND Group
    grp_and = RuleGroup(operator="AND", conditions=[cond_rsi, cond_regime])
    assert engine.evaluate_group(grp_and, context) is True

    # 2. OR Group
    grp_or = RuleGroup(operator="OR", conditions=[cond_rsi_fail, cond_regime])
    assert engine.evaluate_group(grp_or, context) is True

    # 3. NOT Group
    grp_not = RuleGroup(operator="NOT", conditions=[cond_rsi_fail])
    assert engine.evaluate_group(grp_not, context) is True


# ── Registry & Loader Tests ───────────────────────────────────────────────────

def test_strategy_loader_and_registry(tmp_path):
    # Setup temporary directory for strategy configurations
    dir_path = tmp_path / "strategies"
    dir_path.mkdir()
    
    registry = StrategyRegistry()
    loader = StrategyLoader(registry, str(dir_path))
    
    # Write sample strategy
    sample_strategy = {
        "name": "sample_golden_cross",
        "version": "1.2.0",
        "description": "Golden cross validation",
        "enabled": True,
        "rules": {
            "operator": "AND",
            "conditions": [
                {
                    "source": "indicator",
                    "key": "EMA_9",
                    "operator": ">",
                    "value": 100.0
                }
            ]
        },
        "actions": {
            "matched": {
                "action": "SIGNAL_BUY",
                "confidence": 90.0,
                "reason": "EMA is over 100"
            }
        }
    }
    
    file_path = dir_path / "golden_cross.yaml"
    with open(file_path, "w") as f:
        yaml.safe_dump(sample_strategy, f)
        
    # Load all
    loader.load_all()
    
    # Verify registration
    assert len(registry.get_all()) == 1
    strategy = registry.get_strategy("sample_golden_cross")
    assert strategy is not None
    assert strategy.version == "1.2.0"
    assert strategy.enabled is True
    
    # Verify active listing
    assert len(registry.get_active()) == 1
    
    # Disable
    registry.set_enabled("sample_golden_cross", False)
    assert len(registry.get_active()) == 0
    
    # Hot-reload after deleting the file
    os.remove(file_path)
    loader.hot_reload()
    assert len(registry.get_all()) == 0


# ── Strategy Engine End-to-End Tests ──────────────────────────────────────────

@pytest.mark.asyncio
async def test_strategy_engine_integration(tmp_path):
    event_bus.start()
    
    dir_path = tmp_path / "strategies"
    dir_path.mkdir()
    
    # Setup a strategy rules file
    sample_strategy = {
        "name": "test_rsi_rule",
        "version": "1.0.0",
        "enabled": True,
        "rules": {
            "operator": "AND",
            "conditions": [
                {
                    "source": "indicator",
                    "key": "RSI",
                    "operator": ">",
                    "value": 50.0
                }
            ]
        },
        "actions": {
            "matched": {
                "action": "BUY_SIGNAL",
                "confidence": 85.0,
                "reason": "RSI is in bullish range"
            }
        }
    }
    
    file_path = dir_path / "rsi_rule.yaml"
    with open(file_path, "w") as f:
        yaml.safe_dump(sample_strategy, f)
        
    engine = StrategyEngine(directory=str(dir_path))
    await engine.start()
    
    # Mock indicator_engine cache
    from indicators.engine import indicator_engine
    from indicators.models import IndicatorResult
    
    await indicator_engine.cache.store(IndicatorResult(
        indicator_name="RSI",
        symbol="BTCUSD",
        timeframe=Timeframe.M1,
        value=62.5
    ))
    
    # Subscribe to strategy events on the bus
    matched_events = []
    async def cb(evt: EventModel):
        matched_events.append(evt)
        
    await event_bus.subscribe("strategy_matched", cb)
    
    # Run manual evaluation
    responses = await engine.evaluate_all("BTCUSD", Timeframe.M1)
    
    assert len(responses) == 1
    resp = responses[0]
    assert resp.strategy_name == "test_rsi_rule"
    assert resp.matched is True
    assert resp.required_action == "BUY_SIGNAL"
    assert resp.confidence == 85.0

    # Wait for Event Bus dispatch
    for _ in range(20):
        if len(matched_events) >= 1:
            break
        await asyncio.sleep(0.05)
        
    assert len(matched_events) == 1
    assert matched_events[0].payload["strategy_response"]["strategy_name"] == "test_rsi_rule"
    
    # Clean up
    await engine.stop()
    await event_bus.unsubscribe("strategy_matched", cb)
    await event_bus.stop()


@pytest.mark.asyncio
async def test_strategy_engine_exit_rules_fire_when_entry_does_not_match(tmp_path):
    """exit_rules must be evaluated (and produce actions.exit) once the entry
    condition stops matching — this is what turns an exit signal into a real
    SELL, not just an entry-only strategy that never closes its own trades."""
    event_bus.start()

    dir_path = tmp_path / "strategies"
    dir_path.mkdir()

    sample_strategy = {
        "name": "test_exit_rule",
        "version": "1.0.0",
        "enabled": True,
        "rules": {
            "operator": "AND",
            "conditions": [
                {"source": "indicator", "key": "RSI", "operator": ">", "value": 70.0}
            ]
        },
        "exit_rules": {
            "operator": "AND",
            "conditions": [
                {"source": "indicator", "key": "RSI", "operator": "<", "value": 30.0}
            ]
        },
        "actions": {
            "matched": {"action": "SIGNAL_BUY", "confidence": 85.0, "reason": "RSI overbought entry"},
            "exit": {"action": "SIGNAL_SELL", "confidence": 80.0, "reason": "RSI oversold exit"},
        },
    }
    file_path = dir_path / "exit_rule.yaml"
    with open(file_path, "w") as f:
        yaml.safe_dump(sample_strategy, f)

    engine = StrategyEngine(directory=str(dir_path))
    await engine.start()

    from indicators.engine import indicator_engine
    from indicators.models import IndicatorResult

    await indicator_engine.cache.store(IndicatorResult(
        indicator_name="RSI", symbol="EXITTEST", timeframe=Timeframe.M1, value=20.0,
    ))

    responses = await engine.evaluate_all("EXITTEST", Timeframe.M1)

    assert len(responses) == 1
    resp = responses[0]
    assert resp.matched is True
    assert resp.required_action == "SIGNAL_SELL"
    assert resp.confidence == 80.0

    await engine.stop()
    await event_bus.stop()
