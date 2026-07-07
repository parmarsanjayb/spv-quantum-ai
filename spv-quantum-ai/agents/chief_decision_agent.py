import asyncio
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import uuid

from core.agent import BaseAgent, AgentResultModel
from core.bus import event_bus, EventModel
from core.logging import get_logger

from portfolio.engine import portfolio_engine
from risk.engine import risk_engine
from scoring.engine import decision_scoring_engine

logger = get_logger("chief_decision_agent")

class ConflictResolver:
    """
    Resolves scoring, regime, and risk conflicts.
    """
    def resolve(self, decision_score: float, risk_status: str) -> str:
        if risk_status == "BLOCK":
            return "REJECTED"
        if decision_score < 50.0:
            return "REJECTED"
        if decision_score >= 80.0 and risk_status == "ALLOW":
            return "APPROVED"
        return "MANUAL_REVIEW"


class ApprovalManager:
    """
    Applies mandatory criteria validation checks.
    """
    def __init__(self) -> None:
        self.max_daily_trades = 10
        self.max_open_positions = 5
        self.min_confidence = 50.0
        self._daily_trade_count = 0

    async def validate_checks(self, payload: Dict[str, Any]) -> tuple[str, str, str]:
        symbol = payload.get("symbol", "UNKNOWN")
        confidence = float(payload.get("overall_confidence") or payload.get("confidence") or 0.0)
        risk_status = payload.get("risk_status", "BLOCK")

        # 1. Decision Confidence Check
        if confidence < self.min_confidence:
            return "REJECTED", "CONFIDENCE_TOO_LOW", f"Confidence {confidence}% is below threshold {self.min_confidence}%"

        # 2. Risk Engine Approval
        if risk_status != "ALLOW":
            return "BLOCKED", "RISK_REJECTION", f"Risk engine returned status: {risk_status}"

        # 3. Open Positions Limit
        open_pos = len(await portfolio_engine.positions.get_open_positions())
        if open_pos >= self.max_open_positions:
            return "REJECTED", "POSITION_LIMIT_EXCEEDED", f"Active positions {open_pos} exceed limit {self.max_open_positions}"

        # 4. Capital Availability Check
        summary = await portfolio_engine.recalculate_summary()
        available_capital = summary.available_capital
        if available_capital <= 0.0:
            return "REJECTED", "CAPITAL_UNAVAILABLE", "Available margin is zero or negative."

        # 5. Duplicate Trade Check
        open_pos_list = await portfolio_engine.positions.get_open_positions()
        is_open = any(p.symbol == symbol for p in open_pos_list)
        if is_open:
            return "REJECTED", "DUPLICATE_TRADE", f"An active position already exists for {symbol}."

        # 6. Max Daily Trades Limit
        if self._daily_trade_count >= self.max_daily_trades:
            return "REJECTED", "DAILY_LIMIT_EXCEEDED", f"Daily trade limit {self.max_daily_trades} reached."

        return "APPROVED", "SUCCESS", "All checks passed successfully."

    def increment_daily_trades(self) -> None:
        self._daily_trade_count += 1

    def reset_daily_trades(self) -> None:
        self._daily_trade_count = 0


class DecisionPublisher:
    """
    Publishes chief decision outcomes to the event bus.
    """
    async def publish_approved(self, payload: Dict[str, Any]) -> None:
        await event_bus.publish(EventModel(
            event_type="trade_approved",
            source_agent="chief_decision_agent",
            payload=payload
        ))
        # Route through the Risk Agent for final order-level validation before execution
        await event_bus.publish(EventModel(
            event_type="order_request",
            source_agent="chief_decision_agent",
            payload={
                "symbol": payload["symbol"],
                "side": payload.get("side", "BUY"),
                "quantity": payload.get("quantity", 10.0),
                "price": payload.get("price", 100.0),
                "type": "LIMIT",
                "strategy_name": payload.get("strategy_name")
            }
        ))

    async def publish_rejected(self, payload: Dict[str, Any]) -> None:
        await event_bus.publish(EventModel(
            event_type="trade_rejected",
            source_agent="chief_decision_agent",
            payload=payload
        ))

    async def publish_blocked(self, payload: Dict[str, Any]) -> None:
        await event_bus.publish(EventModel(
            event_type="trade_blocked",
            source_agent="chief_decision_agent",
            payload=payload
        ))


class ChiefDecisionAgent(BaseAgent):
    """
    Final Decision Authority for the Trading OS.
    Evaluates scoring and risk metrics, applies mandatory checks, and issues APPROVED or REJECTED statuses.
    """
    def __init__(self) -> None:
        super().__init__(
            name="chief_decision_agent",
            description="Final trade decision manager of the SPV Quantum AI system"
        )
        self.coordinator = ApprovalManager()
        self.resolver = ConflictResolver()
        self.publisher = DecisionPublisher()
        
        self.approved_queue: List[Dict[str, Any]] = []
        self.rejected_queue: List[Dict[str, Any]] = []
        self.blocked_queue: List[Dict[str, Any]] = []
        self._lock = asyncio.Lock()

    @property
    def input_event_types(self) -> List[str]:
        return ["decision_score"]

    @property
    def output_event_types(self) -> List[str]:
        return ["trade_approved", "trade_rejected", "trade_blocked", "order_request"]

    async def initialize(self) -> None:
        self.log_info("ChiefDecisionAgent initialized.")

    async def shutdown(self) -> None:
        self.log_info("ChiefDecisionAgent stopped.")

    async def analyze(self, event: EventModel) -> Optional[AgentResultModel]:
        if event.event_type != "decision_score":
            return None

        start_time = time.perf_counter()
        score_data = event.payload.get("decision_score", event.payload)
        symbol = score_data.get("symbol", "UNKNOWN")
        confidence = float(score_data.get("overall_confidence", 0.0))
        risk_status = score_data.get("risk_status", "ALLOW")
        strategy_name = score_data.get("recommended_strategy", "trend_strategy")

        async with self._lock:
            # 1. Resolve primary conflicts
            state = self.resolver.resolve(confidence, risk_status)
            
            # 2. Run detailed mandatory checks
            if state != "REJECTED":
                chk_state, code, explanation = await self.coordinator.validate_checks(score_data)
                if chk_state != "APPROVED":
                    state = chk_state
                else:
                    code = "APPROVED"
                    explanation = "All checks passed successfully."
            else:
                code = "CONFIDENCE_OR_RISK_FAILURE"
                explanation = f"Confidence {confidence}% or risk status {risk_status} rejected."

            # Build record payload
            record = {
                "decision_id": f"DEC-{uuid.uuid4().hex[:8]}",
                "symbol": symbol,
                "strategy_name": strategy_name,
                "confidence": confidence,
                "status": state,
                "reason_code": code,
                "explanation": explanation,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "side": score_data.get("side", "BUY"),
                "quantity": score_data.get("quantity", 10.0),
                "price": score_data.get("price", 100.0)
            }

            # 3. Publish and Queue
            if state == "APPROVED":
                self.coordinator.increment_daily_trades()
                self.approved_queue.append(record)
                await self.publisher.publish_approved(record)
            elif state == "BLOCKED":
                self.blocked_queue.append(record)
                await self.publisher.publish_blocked(record)
            else:
                self.rejected_queue.append(record)
                await self.publisher.publish_rejected(record)

            self.log_info(f"Chief Decision: {state} for symbol {symbol} | Reason: {explanation}")
            
            return AgentResultModel(
                agent_name=self.agent_name,
                signal=state,
                confidence=confidence,
                reason=explanation,
                processing_time=(time.perf_counter() - start_time) * 1000.0,
                metadata=record
            )
