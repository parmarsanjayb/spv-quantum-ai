from typing import Any, Dict, List, Union, Optional
from strategies.models import RuleGroup, Condition
from core.logging import get_logger

logger = get_logger("strategy_evaluator")

class ConditionEvaluator:
    """
    Evaluates individual conditions against the current market, indicator, risk, and session context.
    """

    def evaluate_condition(self, condition: Condition, context: Dict[str, Any]) -> bool:
        # 1. Resolve current and previous values from context based on source
        curr_val = self._resolve_value(condition.source, condition.key, context, is_previous=False)
        prev_val = self._resolve_value(condition.source, condition.key, context, is_previous=True)
        
        # 2. Resolve target value (could be a fixed value or key target)
        target_val = None
        prev_target_val = None
        if condition.target:
            target_val = self._resolve_value(condition.source, condition.target, context, is_previous=False)
            prev_target_val = self._resolve_value(condition.source, condition.target, context, is_previous=True)
        else:
            target_val = condition.value
            prev_target_val = condition.value

        if curr_val is None:
            return False

        op = condition.operator.lower()

        try:
            if op == "==":
                return curr_val == target_val
            elif op == "!=":
                return curr_val != target_val
            elif op == ">":
                return float(curr_val) > float(target_val)
            elif op == "<":
                return float(curr_val) < float(target_val)
            elif op == ">=":
                return float(curr_val) >= float(target_val)
            elif op == "<=":
                return float(curr_val) <= float(target_val)
            elif op == "between" or op == "inside_range":
                if isinstance(target_val, (list, tuple)) and len(target_val) == 2:
                    return float(target_val[0]) <= float(curr_val) <= float(target_val[1])
                return False
            elif op == "outside_range":
                if isinstance(target_val, (list, tuple)) and len(target_val) == 2:
                    return float(curr_val) < float(target_val[0]) or float(curr_val) > float(target_val[1])
                return False
            elif op == "crosses_above":
                if prev_val is None or prev_target_val is None or target_val is None:
                    return False
                return float(prev_val) <= float(prev_target_val) and float(curr_val) > float(target_val)
            elif op == "crosses_below":
                if prev_val is None or prev_target_val is None or target_val is None:
                    return False
                return float(prev_val) >= float(prev_target_val) and float(curr_val) < float(target_val)
        except (ValueError, TypeError) as e:
            logger.debug(f"Error evaluating condition: {e} | operator: {op}, curr: {curr_val}, target: {target_val}")
            return False

        return False

    def _resolve_value(self, source: str, key: Optional[str], context: Dict[str, Any], is_previous: bool = False) -> Any:
        src_dict = context.get("prev" if is_previous else "current", {})
        
        # Source resolution
        if source == "indicator":
            indicators = src_dict.get("indicators", {})
            if key and "." in key:
                parts = key.split(".", 1)
                base_val = indicators.get(parts[0])
                if isinstance(base_val, dict):
                    return base_val.get(parts[1])
                return None
            return indicators.get(key) if key else None
        
        elif source == "market_regime":
            return src_dict.get("market_regime")
            
        elif source == "risk_status":
            return src_dict.get("risk_status")
            
        elif source == "market_data":
            mkt = src_dict.get("market_data", {})
            return mkt.get(key) if key else None
            
        elif source == "time":
            return src_dict.get("time")

        elif source == "session":
            return src_dict.get("session")

        elif source == "symbol":
            return src_dict.get("symbol")

        return None


class RuleEngine:
    """
    Evaluates rule groups (AND, OR, NOT) recursively.
    """
    def __init__(self) -> None:
        self.evaluator = ConditionEvaluator()

    def evaluate_group(self, group: RuleGroup, context: Dict[str, Any]) -> bool:
        if not group.conditions:
            return False

        op = group.operator.upper()

        if op == "AND":
            for cond in group.conditions:
                res = self._eval_nested(cond, context)
                if not res:
                    return False
            return True

        elif op == "OR":
            for cond in group.conditions:
                res = self._eval_nested(cond, context)
                if res:
                    return True
            return False

        elif op == "NOT":
            # Usually negation applies to a single nested group or condition
            if len(group.conditions) == 1:
                return not self._eval_nested(group.conditions[0], context)
            return False

        return False

    def _eval_nested(self, element: Union[Condition, RuleGroup], context: Dict[str, Any]) -> bool:
        if isinstance(element, RuleGroup):
            return self.evaluate_group(element, context)
        elif isinstance(element, Condition):
            return self.evaluator.evaluate_condition(element, context)
        return False
