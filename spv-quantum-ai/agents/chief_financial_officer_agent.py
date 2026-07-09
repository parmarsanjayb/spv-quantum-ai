import asyncio
import math
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

from core.agent import BaseAgent, AgentResultModel
from core.bus import event_bus, EventModel
from core.logging import get_logger
from database.connection import async_session
from database.models import PerformanceModel, AgentReportModel
from portfolio.engine import portfolio_engine

logger = get_logger("chief_financial_officer_agent")

class ChiefFinancialOfficerAgent(BaseAgent):
    """
    Chief Financial Officer (CFO) Agent.
    Monitors, calculates, and records financial statistics, profits, taxes, brokerage, and risk-return ratios.
    """
    def __init__(self) -> None:
        super().__init__(
            name="chief_financial_officer_agent",
            description="Manages financial metrics, accounting logs, and performance analytics"
        )
        self.gross_profit = 0.0
        self.net_profit = 0.0
        self.brokerage = 0.0
        self.taxes = 0.0
        
        self.pnl_history: List[float] = []
        self.equity_curve: List[float] = [1000000.0]  # Initial capital seed

    @property
    def input_event_types(self) -> List[str]:
        return ["order_filled", "trade_closed"]

    @property
    def output_event_types(self) -> List[str]:
        return ["finance_metrics"]

    async def initialize(self) -> None:
        self.log_info("ChiefFinancialOfficerAgent initialized.")

    async def shutdown(self) -> None:
        self.log_info("ChiefFinancialOfficerAgent stopped.")

    async def analyze(self, event: EventModel) -> Optional[AgentResultModel]:
        if event.event_type == "order_filled":
            await self._on_fill(event.payload)
        elif event.event_type == "trade_closed":
            await self._on_trade_closed(event.payload)
            
        return None

    async def _on_fill(self, payload: Dict[str, Any]) -> None:
        order_data = payload.get("order", payload)
        commission = float(payload.get("commission", order_data.get("commission", 0.0)))
        charges = float(payload.get("charges", order_data.get("charges", 0.0)))
        
        self.brokerage += commission
        self.taxes += charges
        
        await self._recalculate_and_broadcast()

    async def _on_trade_closed(self, payload: Dict[str, Any]) -> None:
        trade_data = payload.get("trade", payload)
        pnl = float(trade_data.get("realized_pnl") or trade_data.get("net_pnl", 0.0))
        
        self.gross_profit += pnl if pnl > 0 else 0.0
        self.net_profit += pnl
        
        # Track P&L and equity
        self.pnl_history.append(pnl)
        
        summary = await portfolio_engine.recalculate_summary()
        current_equity = summary.available_capital + summary.mtm
        self.equity_curve.append(current_equity)
        
        await self._recalculate_and_broadcast()
        await self._save_performance_snapshot(current_equity)

    async def _recalculate_and_broadcast(self) -> None:
        # Calculate Sharpe Ratio (Mock calculation over P&L history)
        sharpe = 0.0
        if len(self.pnl_history) > 1:
            mean_pnl = sum(self.pnl_history) / len(self.pnl_history)
            variance = sum((x - mean_pnl) ** 2 for x in self.pnl_history) / (len(self.pnl_history) - 1)
            std_dev = math.sqrt(variance)
            sharpe = (mean_pnl / std_dev * math.sqrt(252)) if std_dev > 0 else 0.0

        # Calculate Expectancy & Profit Factor
        wins = [x for x in self.pnl_history if x > 0]
        losses = [x for x in self.pnl_history if x < 0]
        
        profit_factor = 1.0
        if len(losses) > 0:
            profit_factor = sum(wins) / abs(sum(losses)) if sum(losses) != 0 else 1.0
            
        win_rate = (len(wins) / len(self.pnl_history) * 100.0) if len(self.pnl_history) > 0 else 0.0
        
        # Calculate Drawdown
        max_eq = max(self.equity_curve) if self.equity_curve else 1000000.0
        current_eq = self.equity_curve[-1] if self.equity_curve else 1000000.0
        drawdown = ((max_eq - current_eq) / max_eq * 100.0) if max_eq > 0 else 0.0

        metrics_payload = {
            "gross_profit": self.gross_profit,
            "net_profit": self.net_profit,
            "brokerage": self.brokerage,
            "taxes": self.taxes,
            "drawdown_pct": drawdown,
            "sharpe_ratio": sharpe,
            "profit_factor": profit_factor,
            "win_rate": win_rate,
            "equity_curve": self.equity_curve,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

        # Broadcast CFO Metrics
        await event_bus.publish(EventModel(
            event_type="finance_metrics",
            source_agent=self.agent_name,
            payload=metrics_payload
        ))

        # Save report
        await self._save_cfo_report(metrics_payload)

    async def _save_performance_snapshot(self, equity: float) -> None:
        try:
            async with async_session() as session:
                db_entry = PerformanceModel(
                    equity=equity,
                    pnl=self.net_profit,
                    drawdown_percent=0.0,
                    sharpe_ratio=0.0,
                    metrics={"gross_profit": self.gross_profit, "brokerage": self.brokerage, "taxes": self.taxes}
                )
                session.add(db_entry)
                await session.commit()
        except Exception as e:
            self.log_error(f"Failed to save performance snapshot: {e}")

    async def _save_cfo_report(self, record: Dict[str, Any]) -> None:
        try:
            async with async_session() as session:
                db_entry = AgentReportModel(
                    agent_name=self.agent_name,
                    report_type="cfo_finance",
                    data=record
                )
                session.add(db_entry)
                await session.commit()
        except Exception as e:
            self.log_error(f"Failed to save CFO report: {e}")
