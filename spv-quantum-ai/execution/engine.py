import asyncio
import time
import hashlib
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone

from brokers import broker_engine
from brokers.models import OrderSide, OrderType

from execution.models import ExecutionOrder, OrderLifecycleStatus, OrderProductType
from execution.queue import ExecutionQueue
from execution.validator import OrderValidator
from execution.tracker import OrderTracker
from execution.publisher import ExecutionPublisher
from core.logging import get_logger
from core.exceptions import DuplicateOrderError

logger = get_logger("execution_engine")

class ExecutionEngine:
    """
    Enterprise Execution Engine.
    The ONLY module allowed to directly place orders with the active broker.
    Handles queueing, validation, status tracking, latency checks, and automatic retries.
    """
    def __init__(self, max_retries: int = 3, retry_delay_sec: float = 1.0) -> None:
        self.queue = ExecutionQueue()
        self.validator = OrderValidator()
        self.tracker = OrderTracker()
        self.publisher = ExecutionPublisher()
        
        self.max_retries = max_retries
        self.retry_delay_sec = retry_delay_sec
        self._running = False
        
        # Duplicate order prevention: hash → timestamp of last submission
        self._order_dedup: Dict[str, float] = {}
        self._dedup_window_sec: float = 5.0  # Reject identical order within 5 seconds
        
        # Link queue processing callback
        self.queue.set_callback(self._process_queued_order)

    async def start(self) -> None:
        try:
            loop = asyncio.get_running_loop()
            if getattr(self, "_current_loop", None) != loop:
                self._current_loop = loop
                self._running = False
        except RuntimeError:
            pass

        if self._running:
            return
        self._running = True
        await self.queue.start()
        logger.info("ExecutionEngine started.")

    async def stop(self) -> None:
        self._running = False
        await self.queue.stop()
        logger.info("ExecutionEngine stopped.")

    async def submit_order_request(self, order_data: Dict[str, Any]) -> ExecutionOrder:
        """
        Receives order data, creates an ExecutionOrder, validates it, and enqueues it.
        Rejects duplicate orders submitted within the dedup window.
        """
        # Parse Pydantic ExecutionOrder model
        symbol = order_data.get("symbol", "UNKNOWN")
        side = order_data.get("side", "BUY").upper()
        quantity = float(order_data.get("quantity", 0.0))
        price = float(order_data["price"]) if order_data.get("price") is not None else None
        stop_price = float(order_data["stop_price"]) if order_data.get("stop_price") is not None else None
        
        # ── Duplicate Order Prevention ────────────────────────────────────────
        dedup_key = hashlib.md5(
            f"{symbol}:{side}:{quantity}:{price}".encode()
        ).hexdigest()
        now = time.monotonic()
        last_ts = self._order_dedup.get(dedup_key, 0.0)
        if now - last_ts < self._dedup_window_sec:
            logger.warning(
                "Duplicate order rejected",
                symbol=symbol, side=side, quantity=quantity,
                dedup_key=dedup_key, window_sec=self._dedup_window_sec
            )
            # Build a FAILED ExecutionOrder to return a consistent type
            dup_order = ExecutionOrder(
                symbol=symbol, side=side, order_type="MARKET",
                quantity=quantity, price=price,
            )
            dup_order.status = OrderLifecycleStatus.FAILED
            dup_order.rejection_reason = "Duplicate order: identical order submitted within dedup window"
            return dup_order
        self._order_dedup[dedup_key] = now
        # Evict expired entries to prevent memory growth
        self._order_dedup = {
            k: v for k, v in self._order_dedup.items()
            if now - v < self._dedup_window_sec * 2
        }
        # ─────────────────────────────────────────────────────────────────────
        
        raw_product = order_data.get("product_type", "MIS").upper()
        product_type = OrderProductType(raw_product) if raw_product in ("MIS", "CNC", "NRML") else OrderProductType.MIS

        order = ExecutionOrder(
            symbol=symbol,
            side=side,
            order_type=order_data.get("type", "MARKET").upper(),
            quantity=quantity,
            price=price,
            stop_price=stop_price,
            product_type=product_type,
            client_tag=order_data.get("tag", "execution_engine")
        )

        # 1. Add to tracker
        await self.tracker.add_order(order)

        # 2. Validate
        is_valid, validation_msg = self.validator.validate(order)
        if not is_valid:
            await self.tracker.update_status(order.order_id, OrderLifecycleStatus.FAILED, f"Validation failed: {validation_msg}")
            order.status = OrderLifecycleStatus.FAILED
            order.rejection_reason = validation_msg
            await self.publisher.publish_rejected(order, validation_msg)
            return order

        # 3. Enqueue
        await self.tracker.update_status(order.order_id, OrderLifecycleStatus.QUEUED)
        await self.queue.enqueue(order)
        return order

    async def _process_queued_order(self, order: ExecutionOrder) -> None:
        """Worker callback that interacts directly with active broker."""
        await self.tracker.update_status(order.order_id, OrderLifecycleStatus.SENT)
        
        # Determine parameters
        side = OrderSide.BUY if order.side.upper() == "BUY" else OrderSide.SELL
        raw_type = order.order_type.upper()
        if raw_type == "MARKET":
            order_type = OrderType.MARKET
        elif raw_type == "LIMIT":
            order_type = OrderType.LIMIT
        elif raw_type == "SL":
            order_type = OrderType.STOP_LIMIT
        elif raw_type == "SL-M":
            order_type = OrderType.STOP_MARKET
        else:
            order_type = OrderType.MARKET

        # Capital Safety check — resolve the real current price for market
        # orders (order.price is legitimately None for those) rather than
        # assuming a constant. A hardcoded fallback here would let the safety
        # engine size/validate the order against a fictional price.
        safety_price = order.price
        if not safety_price:
            from market.manager import market_data_manager
            safety_price = await market_data_manager.get_ltp(order.symbol)

        from safety import safety_engine
        safety_resp = await safety_engine.check_order({
            "symbol": order.symbol,
            "side": order.side,
            "quantity": order.quantity,
            "price": safety_price or 0.0,
            "order_type": order.order_type
        })
        if not safety_resp.allowed:
            order.status = OrderLifecycleStatus.FAILED
            order.rejection_reason = f"Safety block: {safety_resp.reason}"
            await self.tracker.update_status(order.order_id, OrderLifecycleStatus.FAILED, order.rejection_reason)
            await self.publisher.publish_rejected(order, order.rejection_reason)
            return

        start_time = time.perf_counter()
        try:
            resp = await broker_engine.place_order(
                symbol=order.symbol,
                side=side,
                quantity=order.quantity,
                order_type=order_type,
                price=order.price,
                tag=order.client_tag or "execution_engine"
            )
            
            # Latency check
            latency_ms = (time.perf_counter() - start_time) * 1000.0
            await self.tracker.record_latency(order.order_id, latency_ms)

            if resp.success and resp.data:
                broker_order = resp.data
                order.broker_order_id = broker_order.get("order_id")
                # The broker's Order model reports the real fill price under
                # "avg_price" — "price" is only the originally requested price
                # (None for market orders). Reading the wrong key here always
                # produced avg_fill_price=0.0, which then made PortfolioEngine
                # silently drop the position update (it guards on price > 0).
                order.avg_fill_price = float(broker_order.get("avg_price") or order.price or 0.0)
                
                # Check status return from broker place_order (often FILLED immediately on simulator)
                broker_status = broker_order.get("status", "FILLED").upper()
                if broker_status == "FILLED":
                    order.filled_quantity = order.quantity
                    await self.tracker.update_status(order.order_id, OrderLifecycleStatus.FILLED, f"Filled at {order.avg_fill_price}")
                    
                    from charges import charges_engine
                    await charges_engine.calculate_charges(
                        order_id=order.order_id,
                        symbol=order.symbol,
                        side=order.side,
                        qty=order.filled_quantity,
                        price=order.avg_fill_price
                    )
                    
                    await self.publisher.publish_filled(order)
                elif broker_status == "REJECTED":
                    order.rejection_reason = broker_order.get("rejection_reason", "Broker rejected")
                    await self.tracker.update_status(order.order_id, OrderLifecycleStatus.REJECTED, order.rejection_reason)
                    await self.publisher.publish_rejected(order, order.rejection_reason)
                else:
                    await self.tracker.update_status(order.order_id, OrderLifecycleStatus.ACKNOWLEDGED, f"Broker ID: {order.broker_order_id}")
                    await self.publisher.publish_submitted(order)

            else:
                # Rejected/Failed by broker response
                error_msg = resp.error or "Broker rejected order placement request."
                await self._handle_retry_or_fail(order, error_msg)

        except Exception as e:
            latency_ms = (time.perf_counter() - start_time) * 1000.0
            await self.tracker.record_latency(order.order_id, latency_ms)
            await self._handle_retry_or_fail(order, str(e))

    async def _handle_retry_or_fail(self, order: ExecutionOrder, error_msg: str) -> None:
        """Applies order retry mechanism or fails after max attempts."""
        if order.retry_count < self.max_retries:
            order.retry_count += 1
            await self.tracker.update_status(
                order.order_id, 
                OrderLifecycleStatus.QUEUED, 
                f"Retrying order. Attempt {order.retry_count} / {self.max_retries}. Error: {error_msg}"
            )
            await asyncio.sleep(self.retry_delay_sec)
            await self.queue.enqueue(order)
        else:
            order.rejection_reason = f"Max retries reached: {error_msg}"
            await self.tracker.update_status(order.order_id, OrderLifecycleStatus.FAILED, order.rejection_reason)
            await self.publisher.publish_failed(order, order.rejection_reason)

    # ── Status and dashboard lookups ──────────────────────────────────────────

    async def get_dashboard_metrics(self) -> Dict[str, Any]:
        avg_latency = await self.tracker.get_average_latency_ms()
        queue_size = await self.queue.get_size()
        
        pending = [o.model_dump() for o in await self.tracker.get_pending_orders()]
        open_orders = [o.model_dump() for o in await self.tracker.get_open_orders()]
        completed = [o.model_dump() for o in await self.tracker.get_completed_orders()]
        rejected = [o.model_dump() for o in await self.tracker.get_rejected_orders()]
        
        return {
            "broker_response_time_ms": round(avg_latency, 2),
            "execution_queue_size": queue_size,
            "pending_orders": pending,
            "open_orders": open_orders,
            "completed_orders": completed,
            "rejected_orders": rejected
        }

# Singleton
execution_engine = ExecutionEngine()
