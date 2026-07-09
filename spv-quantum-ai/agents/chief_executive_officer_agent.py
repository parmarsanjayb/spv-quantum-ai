import asyncio
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from core.agent import BaseAgent, AgentResultModel
from core.bus import event_bus, EventModel
from core.logging import get_logger
from database.connection import async_session
from database.models import AgentReportModel

logger = get_logger("chief_executive_officer_agent")

class ChiefExecutiveOfficerAgent(BaseAgent):
    """
    Chief Executive Officer (CEO) Agent.
    Aggregates inputs from all departmental specialists, compiles votes, 
    calculates trade quality and expected reward/risk, and issues a final trade decision.
    """
    def __init__(self) -> None:
        super().__init__(
            name="chief_executive_officer_agent",
            description="Aggregates employee signals, resolves conflicts, and issues final chief trade decisions"
        )
        self.employee_decisions: Dict[str, Dict[str, Dict[str, Any]]] = {}  # symbol -> { employee_code -> decision_dict }
        self._lock = asyncio.Lock()
        self.approved_queue: List[Dict[str, Any]] = []
        self.rejected_queue: List[Dict[str, Any]] = []
        self.blocked_queue: List[Dict[str, Any]] = []

    @property
    def input_event_types(self) -> List[str]:
        return ["employee_decision", "decision_score"]

    @property
    def output_event_types(self) -> List[str]:
        return ["chief_decision"]

    async def initialize(self) -> None:
        self.log_info("ChiefExecutiveOfficerAgent initialized.")

    async def shutdown(self) -> None:
        self.log_info("ChiefExecutiveOfficerAgent stopped.")

    async def analyze(self, event: EventModel) -> Optional[AgentResultModel]:
        if event.event_type == "employee_decision":
            await self._record_employee_decision(event.payload)
            return None

        if event.event_type == "decision_score":
            score_data = event.payload.get("decision_score", event.payload)
            return await self._evaluate_chief_decision(score_data)

        return None

    async def _record_employee_decision(self, payload: Dict[str, Any]) -> None:
        async with self._lock:
            symbol = payload.get("symbol", "NIFTY50")
            code = payload.get("employee_code", "UNKNOWN")
            
            if symbol not in self.employee_decisions:
                self.employee_decisions[symbol] = {}
                
            self.employee_decisions[symbol][code] = {
                "decision": payload.get("decision", "Neutral"),
                "confidence": float(payload.get("confidence", 50.0)),
                "timestamp": payload.get("timestamp", datetime.now(timezone.utc).isoformat())
            }

    async def _evaluate_chief_decision(self, score_data: Dict[str, Any]) -> AgentResultModel:
        symbol = score_data.get("symbol", "NIFTY50")
        strategy_name = score_data.get("recommended_strategy", "trend_strategy")
        side = score_data.get("side", "BUY")
        price = float(score_data.get("price") or 0.0)
        if not price:
            from market.manager import market_data_manager
            price = await market_data_manager.get_ltp(symbol)
        quantity = float(score_data.get("quantity", 10.0))
        
        async with self._lock:
            votes = self.employee_decisions.get(symbol, {})
            
            bullish_votes = 0
            bearish_votes = 0
            neutral_votes = 0
            total_confidence = 0.0
            vote_details = {}
            
            for code, data in votes.items():
                decision = str(data["decision"]).upper()
                conf = data["confidence"]
                vote_details[code] = {"decision": decision, "confidence": conf}
                
                if "BUY" in decision or "BULLISH" in decision:
                    bullish_votes += 1
                    total_confidence += conf
                elif "SELL" in decision or "BEARISH" in decision:
                    bearish_votes += 1
                    total_confidence += conf
                else:
                    neutral_votes += 1
                    
            avg_confidence = float(score_data.get("overall_confidence", 50.0))
            if (bullish_votes + bearish_votes) > 0:
                avg_confidence = total_confidence / (bullish_votes + bearish_votes)
                
            bullish_score = float(bullish_votes * 10.0)
            bearish_score = float(bearish_votes * 10.0)
            
            risk_decision = votes.get("EMP-RSK", {}).get("decision", "ALLOW").upper()
            risk_score = 90.0 if "BLOCK" in risk_decision or "REJECT" in risk_decision else 20.0
            
            expected_reward = 2.0
            expected_risk = 1.0
            
            final_decision = "HOLD"
            reason = "Awaiting stronger confirmation."
            
            if risk_score >= 80.0:
                final_decision = "REJECT"
                reason = "Blocked by Risk Department."
            elif avg_confidence >= 60.0:
                if side == "BUY" and bullish_votes >= bearish_votes:
                    final_decision = "BUY"
                    reason = "Strong bullish consensus across departments."
                elif side == "SELL" and bearish_votes >= bullish_votes:
                    final_decision = "SELL"
                    reason = "Strong bearish consensus across departments."
                else:
                    final_decision = "HOLD"
                    reason = "Vote mismatch with strategy direction."
            else:
                final_decision = "REJECT"
                reason = f"Avg confidence {avg_confidence:.1f}% below minimum 60% threshold."

            trade_quality = "Low"
            if avg_confidence >= 80.0 and final_decision in ("BUY", "SELL"):
                trade_quality = "High"
            elif avg_confidence >= 60.0 and final_decision in ("BUY", "SELL"):
                trade_quality = "Medium"

            decision_id = f"CEO-{uuid.uuid4().hex[:8]}"
            record = {
                "decision_id": decision_id,
                "symbol": symbol,
                "strategy_name": strategy_name,
                "side": side,
                "price": price,
                "quantity": quantity,
                "confidence": avg_confidence,
                "bullish_score": bullish_score,
                "bearish_score": bearish_score,
                "risk_score": risk_score,
                "trade_quality": trade_quality,
                "expected_reward": expected_reward,
                "expected_risk": expected_risk,
                "final_decision": final_decision,
                "reason": reason,
                "employee_votes": vote_details,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }

            if final_decision in ("BUY", "SELL"):
                self.approved_queue.append(record)
            elif final_decision == "REJECT":
                self.rejected_queue.append(record)
            else:
                self.blocked_queue.append(record)

            if len(self.approved_queue) > 100: self.approved_queue.pop(0)
            if len(self.rejected_queue) > 100: self.rejected_queue.pop(0)
            if len(self.blocked_queue) > 100: self.blocked_queue.pop(0)

            await event_bus.publish(EventModel(
                event_type="chief_decision",
                source_agent=self.agent_name,
                payload=record
            ))

            await self._save_decision_to_db(record)

            self.log_info(f"CEO Decision: {final_decision} for symbol {symbol} | Confidence: {avg_confidence:.1f}%")

            return AgentResultModel(
                agent_name=self.agent_name,
                signal=final_decision,
                confidence=avg_confidence,
                reason=reason,
                processing_time=0.0,
                metadata=record
            )

    async def _save_decision_to_db(self, record: Dict[str, Any]) -> None:
        try:
            async with async_session() as session:
                db_entry = AgentReportModel(
                    agent_name=self.agent_name,
                    report_type="ceo_decision",
                    data=record
                )
                session.add(db_entry)
                await session.commit()
        except Exception as e:
            self.log_error(f"Failed to save CEO decision to database: {e}")
