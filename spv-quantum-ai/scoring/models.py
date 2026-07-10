from enum import Enum
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field
from datetime import datetime, timezone
import uuid

class DecisionQuality(str, Enum):
    VERY_STRONG = "VERY_STRONG"
    STRONG      = "STRONG"
    MODERATE    = "MODERATE"
    WEAK        = "WEAK"
    INVALID     = "INVALID"

class DecisionScoreResult(BaseModel):
    symbol: str
    timeframe: str
    overall_confidence: float               # 0.0 - 100.0
    component_scores: Dict[str, float]      # name -> raw or weighted score
    risk_status: str                        # ALLOW, BLOCK, REDUCE_POSITION
    recommended_strategy: Optional[str]     = None
    decision_quality: DecisionQuality
    missing_requirements: List[str]         = Field(default_factory=list)
    conflicting_signals: List[str]          = Field(default_factory=list)
    reasoning_summary: str
    strategy_action: str                    = "SIGNAL_NONE"  # SIGNAL_BUY, SIGNAL_SELL, SIGNAL_NONE
    timestamp: datetime                     = Field(default_factory=lambda: datetime.now(timezone.utc))

class DecisionScoreEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    decision_score: DecisionScoreResult
