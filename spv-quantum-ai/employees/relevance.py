from typing import Any, Dict, List, Set

# Every trade goes through risk and execution regardless of strategy —
# these departments are always relevant.
ALWAYS_RELEVANT: Set[str] = {
    "EMP-RSK", "EMP-PZS", "EMP-CPT", "EMP-EXP",   # Risk
    "EMP-EXE", "EMP-PTF", "EMP-PPR", "EMP-PM",     # Execution/Portfolio
}

# Keyword found in a rule condition's indicator key -> employee code.
# Deliberately simple and explainable (not ML): if the strategy's own YAML
# references an indicator, the specialist that produces/reads it is relevant.
INDICATOR_KEYWORD_MAP: Dict[str, str] = {
    "EMA": "EMP-TRD", "SMA": "EMP-TRD", "MACD": "EMP-TRD",
    "RSI": "EMP-MOM", "MOMENTUM": "EMP-MOM", "ROC": "EMP-MOM",
    "VWAP": "EMP-VWP",
    "OI": "EMP-OIE",
    "PCR": "EMP-PCR",
    "DELTA": "EMP-GRK", "GAMMA": "EMP-GRK", "THETA": "EMP-GRK", "VEGA": "EMP-GRK",
    "VOLUME": "EMP-VOL", "VOL": "EMP-VOL",
    "MAXPAIN": "EMP-MPN",
}

SEGMENT_EMPLOYEE_MAP: Dict[str, str] = {
    "COMMODITY": "EMP-COM",
    "CURRENCY": "EMP-CUR",
    "OPTIONS": "EMP-OPT",
}


def _collect_condition_keys(rule_group: Any, keys: List[str]) -> None:
    """Walks a strategy RuleGroup (dict or model) and collects every
    indicator `key`/`target` referenced, so relevance can be derived from
    what the strategy actually reads — not guessed."""
    if rule_group is None:
        return
    conditions = rule_group.conditions if hasattr(rule_group, "conditions") else rule_group.get("conditions", [])
    for cond in conditions:
        if hasattr(cond, "conditions") or (isinstance(cond, dict) and "conditions" in cond):
            _collect_condition_keys(cond, keys)
            continue
        key = cond.key if hasattr(cond, "key") else cond.get("key")
        target = cond.target if hasattr(cond, "target") else cond.get("target")
        if key:
            keys.append(str(key).upper())
        if target:
            keys.append(str(target).upper())


def get_relevant_employee_codes(strategy: Any, segment: str = "EQUITY") -> List[str]:
    """
    Returns the subset of the 30 employees actually relevant to this
    strategy + instrument segment, instead of showing all of them.
    """
    relevant: Set[str] = set(ALWAYS_RELEVANT)

    keys: List[str] = []
    _collect_condition_keys(getattr(strategy, "rules", None), keys)
    _collect_condition_keys(getattr(strategy, "exit_rules", None), keys)

    for key in keys:
        for keyword, code in INDICATOR_KEYWORD_MAP.items():
            if keyword in key:
                relevant.add(code)

    seg_code = SEGMENT_EMPLOYEE_MAP.get(segment.upper())
    if seg_code:
        relevant.add(seg_code)

    return sorted(relevant)
