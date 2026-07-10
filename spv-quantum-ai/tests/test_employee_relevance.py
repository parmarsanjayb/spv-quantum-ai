from strategies.models import Strategy, RuleGroup, Condition
from employees.relevance import get_relevant_employee_codes, ALWAYS_RELEVANT


def _sample_strategy() -> Strategy:
    return Strategy(
        name="sample_trend_strategy",
        version="1.1.0",
        rules=RuleGroup(operator="AND", conditions=[
            Condition(source="indicator", key="EMA_9", operator=">", target="EMA_20"),
            Condition(source="indicator", key="RSI", operator=">", value=50.0),
            Condition(source="market_regime", operator="==", value="TRENDING_BULLISH"),
        ]),
        exit_rules=RuleGroup(operator="AND", conditions=[
            Condition(source="indicator", key="EMA_9", operator="<", target="EMA_20"),
        ]),
        actions={"matched": {"action": "SIGNAL_BUY"}, "exit": {"action": "SIGNAL_SELL"}},
    )


def test_always_relevant_employees_included():
    codes = get_relevant_employee_codes(_sample_strategy())
    for code in ALWAYS_RELEVANT:
        assert code in codes


def test_trend_strategy_includes_trend_and_momentum_employees():
    codes = get_relevant_employee_codes(_sample_strategy())
    assert "EMP-TRD" in codes   # EMA_9/EMA_20 keys
    assert "EMP-MOM" in codes   # RSI key


def test_trend_strategy_excludes_unrelated_options_employees():
    codes = get_relevant_employee_codes(_sample_strategy(), segment="EQUITY")
    assert "EMP-GRK" not in codes
    assert "EMP-PCR" not in codes
    assert "EMP-OPT" not in codes


def test_commodity_segment_adds_commodity_employee():
    codes = get_relevant_employee_codes(_sample_strategy(), segment="COMMODITY")
    assert "EMP-COM" in codes
