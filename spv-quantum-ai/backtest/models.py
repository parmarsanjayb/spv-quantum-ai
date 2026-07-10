from pydantic import BaseModel, Field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import uuid

class BacktestConfig(BaseModel):
    symbols: List[str]
    timeframe: str = "1m"  # 1m, 3m, 5m, 10m, 15m, 30m, 1H, 1D
    start_date: datetime
    end_date: datetime
    initial_capital: float = 100000.0
    slippage_pct: float = 0.0005  # 0.05%
    brokerage_per_order: float = 20.0  # Flat flat per order
    spread_pct: float = 0.0002
    # Isolates the run to exactly this strategy (by name — Strategy Studio or
    # YAML, the backtest doesn't care which). None = whatever's currently
    # enabled in the registry, unchanged from prior behavior.
    strategy_name: Optional[str] = None

class BacktestProgress(BaseModel):
    backtest_id: str
    status: str = "PENDING"  # PENDING, RUNNING, COMPLETED, FAILED
    progress_pct: float = 0.0
    current_symbol: Optional[str] = None
    current_date: Optional[datetime] = None
    trades_executed: int = 0
    total_pnl: float = 0.0

class BacktestStartedEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    backtest_id: str
    config: BacktestConfig

class BacktestProgressEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    progress: BacktestProgress

class BacktestCompletedEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    backtest_id: str
    progress: BacktestProgress
    metrics: Dict[str, Any] = Field(default_factory=dict)
