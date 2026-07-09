import asyncio
import uuid
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from core.agent import BaseAgent, AgentResultModel
from core.bus import event_bus, EventModel
from core.logging import get_logger
from database.connection import async_session
from database.models import AgentReportModel

logger = get_logger("chief_operating_officer_agent")

class ChiefOperatingOfficerAgent(BaseAgent):
    """
    Chief Operating Officer (COO) Agent.
    Monitors trade execution quality, latency, duplicate orders, and auto-retries failed executions.
    """
    def __init__(self) -> None:
        super().__init__(
            name="chief_operating_officer_agent",
            description="Coordinates order execution pipelines, tracks latency, and handles retries"
        )
        self.retry_limit = 3
        self.pending_executions: Dict[str, Dict[str, Any]] = {}  # order_id -> execution_details
        self.execution_logs: List[Dict[str, Any]] = []
        
        # Stats
        self.total_orders = 0
        self.successful_orders = 0
        self.failed_orders = 0
        self.retry_count = 0
        self.average_latency_ms = 0.0

    @property
    def input_event_types(self) -> List[str]:
        return ["investment_decision", "order_filled", "execution_failed"]

    @property
    def output_event_types(self) -> List[str]:
        return ["order_approved", "coo_telemetry"]

    async def initialize(self) -> None:
        self.log_info("ChiefOperatingOfficerAgent initialized.")

    async def shutdown(self) -> None:
        self.log_info("ChiefOperatingOfficerAgent stopped.")

    async def analyze(self, event: EventModel) -> Optional[AgentResultModel]:
        if event.event_type == "investment_decision":
            record = event.payload
            if record.get("decision") in ("APPROVE", "REDUCE QUANTITY"):
                return await self._initiate_execution(record)
        elif event.event_type == "order_filled":
            await self._record_success(event.payload)
        elif event.event_type == "execution_failed":
            await self._handle_failure(event.payload)
            
        return None

    async def _initiate_execution(self, record: Dict[str, Any]) -> AgentResultModel:
        symbol = record["symbol"]
        qty = float(record["final_quantity"])
        price = float(record["price"])
        side = record["side"]
        
        # Prevent duplicate orders within 5 seconds
        duplicate = False
        now_ts = time.time()
        for pending_id, data in list(self.pending_executions.items()):
            if (
                data["symbol"] == symbol and 
                data["side"] == side and 
                (now_ts - data["start_time"]) < 5.0
            ):
                duplicate = True
                break
                
        if duplicate:
            reason = f"Duplicate execution blocked for {symbol} {side}."
            self.log_warn(reason)
            return AgentResultModel(
                agent_name=self.agent_name,
                signal="REJECTED",
                confidence=0.0,
                reason=reason,
                processing_time=0.0
            )

        order_id = f"ORD-{uuid.uuid4().hex[:8]}"
        self.pending_executions[order_id] = {
            "order_id": order_id,
            "symbol": symbol,
            "side": side,
            "quantity": qty,
            "price": price,
            "start_time": now_ts,
            "retries": 0,
            "record": record
        }
        
        self.total_orders += 1

        # Route order to the ExecutionAgent
        await event_bus.publish(EventModel(
            event_type="order_approved",
            source_agent=self.agent_name,
            payload={
                "order_id": order_id,
                "symbol": symbol,
                "side": side,
                "quantity": qty,
                "price": price,
                "type": "LIMIT",
                "strategy_name": record.get("strategy_name")
            }
        ))

        self.log_info(f"COO: Order {order_id} approved and dispatched to Execution Agent.")
        await self._broadcast_telemetry()
        
        return AgentResultModel(
            agent_name=self.agent_name,
            signal="EXECUTING",
            confidence=100.0,
            reason=f"Order {order_id} submitted.",
            processing_time=0.0
        )

    async def _record_success(self, payload: Dict[str, Any]) -> None:
        order_data = payload.get("order", payload)
        order_id = order_data.get("order_id")
        
        if order_id in self.pending_executions:
            data = self.pending_executions.pop(order_id)
            latency = (time.time() - data["start_time"]) * 1000.0
            
            # Recalculate average latency
            self.successful_orders += 1
            n = self.successful_orders
            self.average_latency_ms = ((self.average_latency_ms * (n - 1)) + latency) / n
            
            log_item = {
                "order_id": order_id,
                "symbol": data["symbol"],
                "side": data["side"],
                "quantity": data["quantity"],
                "latency_ms": latency,
                "status": "SUCCESS",
                "retries": data["retries"],
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            
            self.execution_logs.append(log_item)
            if len(self.execution_logs) > 100: self.execution_logs.pop(0)
            
            await self._save_log_to_db(log_item)
            self.log_info(f"COO: Order {order_id} successfully filled. Latency: {latency:.1f}ms")
            await self._broadcast_telemetry()

    async def _handle_failure(self, payload: Dict[str, Any]) -> None:
        order_data = payload.get("order", payload)
        order_id = order_data.get("order_id")
        
        if order_id in self.pending_executions:
            data = self.pending_executions[order_id]
            retries = data["retries"]
            
            if retries < self.retry_limit:
                data["retries"] += 1
                self.retry_count += 1
                new_order_id = f"ORD-{uuid.uuid4().hex[:8]}"
                
                # Update map with new ID
                self.pending_executions[new_order_id] = {
                    "order_id": new_order_id,
                    "symbol": data["symbol"],
                    "side": data["side"],
                    "quantity": data["quantity"],
                    "price": data["price"],
                    "start_time": data["start_time"],
                    "retries": data["retries"],
                    "record": data["record"]
                }
                
                self.pending_executions.pop(order_id)
                
                # Retry order
                self.log_warn(f"COO: Retrying order placement (Attempt {data['retries']}/{self.retry_limit}) for {data['symbol']}")
                await event_bus.publish(EventModel(
                    event_type="order_approved",
                    source_agent=self.name,
                    payload={
                        "order_id": new_order_id,
                        "symbol": data["symbol"],
                        "side": data["side"],
                        "quantity": data["quantity"],
                        "price": data["price"],
                        "type": "LIMIT",
                        "strategy_name": data["record"].get("strategy_name")
                    }
                ))
            else:
                self.pending_executions.pop(order_id)
                self.failed_orders += 1
                
                log_item = {
                    "order_id": order_id,
                    "symbol": data["symbol"],
                    "side": data["side"],
                    "quantity": data["quantity"],
                    "latency_ms": (time.time() - data["start_time"]) * 1000.0,
                    "status": "FAILED",
                    "retries": retries,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
                self.execution_logs.append(log_item)
                await self._save_log_to_db(log_item)
                self.log_error(f"COO: Order {order_id} failed after {retries} retries.")
                await self._broadcast_telemetry()

    async def _broadcast_telemetry(self) -> None:
        success_rate = (self.successful_orders / self.total_orders * 100.0) if self.total_orders > 0 else 100.0
        await event_bus.publish(EventModel(
            event_type="coo_telemetry",
            source_agent=self.agent_name,
            payload={
                "total_orders": self.total_orders,
                "successful_orders": self.successful_orders,
                "failed_orders": self.failed_orders,
                "retry_count": self.retry_count,
                "success_rate": success_rate,
                "average_latency_ms": self.average_latency_ms,
                "execution_logs": self.execution_logs[-10:]
            }
        ))

    async def _save_log_to_db(self, log_item: Dict[str, Any]) -> None:
        try:
            async with async_session() as session:
                db_entry = AgentReportModel(
                    agent_name=self.agent_name,
                    report_type="coo_execution",
                    data=log_item
                )
                session.add(db_entry)
                await session.commit()
        except Exception as e:
            self.log_error(f"Failed to save COO log: {e}")
