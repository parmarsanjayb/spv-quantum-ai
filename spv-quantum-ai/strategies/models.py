from pydantic import BaseModel, Field
from typing import Any, Dict, List, Optional, Union
from datetime import datetime, timezone
import uuid

class Condition(BaseModel):
    # What to query: indicator, market_regime, risk_status, market_data (ltp, vwap, volume, oi, etc.), time, session
    source: str  # e.g., 'indicator', 'market_regime', 'risk_status', 'market_data', 'time', 'session'
    key: Optional[str] = None  # e.g., 'EMA_9', 'RSI', 'ltp', 'todays_pnl'
    operator: str  # >, <, >=, <=, ==, !=, between, crosses_above, crosses_below, inside_range, outside_range
    value: Optional[Any] = None  # single value or list/tuple for ranges
    target: Optional[str] = None  # target key to compare against (e.g. comparing EMA_9 crosses_above EMA_20)

class RuleGroup(BaseModel):
    operator: str  # 'AND', 'OR', 'NOT'
    conditions: List[Union[Condition, 'RuleGroup']] = Field(default_factory=list)

class Strategy(BaseModel):
    name: str
    version: str
    description: str = ""
    enabled: bool = True
    rules: RuleGroup                          # entry condition (actions.matched)
    exit_rules: Optional[RuleGroup] = None     # exit condition (actions.exit), evaluated when rules didn't match
    actions: Dict[str, Any] = Field(default_factory=dict)

# Rebuilding forward refs for nested RuleGroup
RuleGroup.model_rebuild()

class StrategyResponse(BaseModel):
    strategy_name: str
    version: str
    status: str  # ACTIVE / DISABLED
    matched: bool
    confidence: float
    reason: str
    required_action: Optional[str] = None

class StrategyMatchedEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    strategy_response: StrategyResponse
    context: Dict[str, Any] = Field(default_factory=dict)

class StrategyRejectedEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    strategy_response: StrategyResponse
    context: Dict[str, Any] = Field(default_factory=dict)
