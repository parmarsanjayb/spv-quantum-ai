import asyncio
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from core.agent import BaseAgent, AgentResultModel
from core.bus import event_bus, EventModel
from core.logging import get_logger
from database.connection import async_session
from database.models import AgentReportModel
from portfolio.engine import portfolio_engine

logger = get_logger("chief_investment_officer_agent")

class ChiefInvestmentOfficerAgent(BaseAgent):
    """
    Chief Investment Officer (CIO) Agent.
    Manages portfolio-level allocations, risk concentrations, and sector exposures.
    Evaluates CEO trade proposals and outputs APPROVED, REDUCED_QTY, POSTPONED, or REJECTED.
    """
    def __init__(self) -> None:
        super().__init__(
            name="chief_investment_officer_agent",
            description="Manages portfolio risk profiles and capital allocation"
        )
        self.approved_queue: List[Dict[str, Any]] = []
        self.rejected_queue: List[Dict[str, Any]] = []

    @property
    def input_event_types(self) -> List[str]:
        return ["chief_decision"]

    @property
    def output_event_types(self) -> List[str]:
        return ["investment_decision"]

    async def initialize(self) -> None:
        self.log_info("ChiefInvestmentOfficerAgent initialized.")

    async def shutdown(self) -> None:
        self.log_info("ChiefInvestmentOfficerAgent stopped.")

    async def analyze(self, event: EventModel) -> Optional[AgentResultModel]:
        if event.event_type == "chief_decision":
            record = event.payload
            if record.get("final_decision") in ("BUY", "SELL"):
                return await self._evaluate_allocation(record)
        return None

    async def _evaluate_allocation(self, ceo_record: Dict[str, Any]) -> AgentResultModel:
        symbol = ceo_record["symbol"]
        qty = float(ceo_record["quantity"])
        price = float(ceo_record["price"])
        side = ceo_record["side"]
        
        # 1. Fetch current portfolio status
        open_positions = await portfolio_engine.positions.get_open_positions()
        summary = await portfolio_engine.recalculate_summary()
        
        capital = summary.available_capital + summary.mtm
        utilized_margin = summary.utilized_margin
        
        # 2. Calculations
        portfolio_heat = (utilized_margin / capital * 100.0) if capital > 0 else 0.0
        open_position_count = len(open_positions)
        margin_usage = utilized_margin
        
        # Total exposure of this trade
        proposed_exposure = qty * price
        current_exposure = sum(p.quantity * p.avg_price for p in open_positions)
        total_exposure = current_exposure + proposed_exposure
        
        exposure_score = (total_exposure / capital * 100.0) if capital > 0 else 0.0
        
        # Sector / Correlation Check (Mock)
        correlation_risk = 10.0
        for p in open_positions:
            if p.symbol == symbol:
                correlation_risk += 40.0
                
        # 3. Decision Rules
        decision = "APPROVE"
        final_qty = qty
        reason = "Trade satisfies all capital allocation criteria."
        
        if open_position_count >= 5:
            decision = "REJECT"
            reason = "Maximum open positions limit (5) reached."
        elif portfolio_heat > 85.0:
            decision = "REJECT"
            reason = f"Portfolio heat {portfolio_heat:.1f}% exceeds safe limit."
        elif total_exposure > 500000.0:
            decision = "REDUCE QUANTITY"
            final_qty = max(1.0, qty * 0.5)
            reason = f"Proposed exposure exceeds limits. Quantity reduced from {qty} to {final_qty}."
        elif correlation_risk > 70.0:
            decision = "REJECT"
            reason = "High correlation/exposure risk to this asset."

        record = {
            "decision_id": f"CIO-{uuid.uuid4().hex[:8]}",
            "ceo_decision_id": ceo_record.get("decision_id"),
            "symbol": symbol,
            "side": side,
            "original_quantity": qty,
            "final_quantity": final_qty,
            "price": price,
            "portfolio_heat": portfolio_heat,
            "exposure_score": exposure_score,
            "open_position_count": open_position_count,
            "margin_usage": margin_usage,
            "correlation_risk": correlation_risk,
            "decision": decision,
            "reason": reason,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

        if decision in ("APPROVE", "REDUCE QUANTITY"):
            self.approved_queue.append(record)
        else:
            self.rejected_queue.append(record)

        if len(self.approved_queue) > 100: self.approved_queue.pop(0)
        if len(self.rejected_queue) > 100: self.rejected_queue.pop(0)

        # Publish Event
        await event_bus.publish(EventModel(
            event_type="investment_decision",
            source_agent=self.agent_name,
            payload=record
        ))

        # Save to database
        await self._save_decision_to_db(record)

        self.log_info(f"CIO Allocation Decision: {decision} for symbol {symbol} | Heat: {portfolio_heat:.1f}%")

        return AgentResultModel(
            agent_name=self.agent_name,
            signal=decision,
            confidence=100.0 - correlation_risk,
            reason=reason,
            processing_time=0.0,
            metadata=record
        )

    async def _save_decision_to_db(self, record: Dict[str, Any]) -> None:
        try:
            async with async_session() as session:
                db_entry = AgentReportModel(
                    agent_name=self.agent_name,
                    report_type="cio_decision",
                    data=record
                )
                session.add(db_entry)
                await session.commit()
        except Exception as e:
            self.log_error(f"Failed to save CIO decision to database: {e}")
