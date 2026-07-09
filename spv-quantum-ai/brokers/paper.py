import time
import uuid
import random
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from brokers.base import BaseBroker
from brokers.models import (
    BrokerResponse, Funds, Order, OrderSide, OrderStatus, OrderType,
    Position, Holding, Trade
)
from core.config import settings
from core.logging import get_logger

logger = get_logger("paper_broker")


class PaperBroker(BaseBroker):
    """
    Full-featured in-memory paper trading simulator.
    Simulates: orders, positions, P/L, margin, partial fills, rejections.
    No real money. No broker SDK dependency.
    """

    def __init__(self) -> None:
        super().__init__("paper_broker")
        cfg = settings.yaml_config.get("brokers", {}).get("paper_broker", {})
        self._initial_balance: float = float(cfg.get("initial_balance", 1_000_000.0))
        self._commission_rate: float = float(cfg.get("commission_rate", 0.0003))
        self._rejection_rate:  float = float(cfg.get("rejection_rate", 0.02))   # 2 %
        self._partial_fill_rate: float = float(cfg.get("partial_fill_rate", 0.05))  # 5 %
        self._margin_multiplier: float = float(cfg.get("margin_multiplier", 5.0))

        self._balance:     float = self._initial_balance
        self._used_margin: float = 0.0

        self._orders:    Dict[str, Order]    = {}
        self._positions: Dict[str, Position] = {}
        self._holdings:  Dict[str, Holding]  = {}
        self._trades:    List[Trade]         = []

    # ── Connection lifecycle ─────────────────────────────────────────────────

    async def connect(self) -> BrokerResponse:
        t0 = time.perf_counter()
        self._connected = True
        latency = (time.perf_counter() - t0) * 1000
        logger.info("PaperBroker connected.", balance=self._balance)
        return BrokerResponse(success=True, broker=self.name, latency_ms=latency)

    async def disconnect(self) -> BrokerResponse:
        self._connected = False
        logger.info("PaperBroker disconnected.")
        return BrokerResponse(success=True, broker=self.name)

    def is_connected(self) -> bool:
        return self._connected

    # ── Authentication ───────────────────────────────────────────────────────

    async def login(self, **credentials: Any) -> BrokerResponse:
        self._connected = True
        logger.info("PaperBroker login (no-op).")
        return BrokerResponse(success=True, broker=self.name, data={"session": "paper-session"})

    async def logout(self) -> BrokerResponse:
        self._connected = False
        return BrokerResponse(success=True, broker=self.name)

    # ── Account information ──────────────────────────────────────────────────

    async def get_profile(self) -> BrokerResponse:
        profile = {
            "name":     "Paper Trader",
            "broker":   self.name,
            "segments": ["EQ", "FO", "CDS", "MCX"],
            "pnl":      round(self._balance - self._initial_balance, 2),
        }
        return BrokerResponse(success=True, broker=self.name, data=profile)

    async def get_balance(self) -> BrokerResponse:
        funds = Funds(
            equity=round(self._balance, 2),
            available_margin=round(self._balance - self._used_margin, 2),
            used_margin=round(self._used_margin, 2),
            broker=self.name,
        )
        return BrokerResponse(success=True, broker=self.name, data=funds.model_dump())

    async def get_funds(self) -> BrokerResponse:
        funds = Funds(
            equity=round(self._balance, 2),
            available_margin=round(self._balance - self._used_margin, 2),
            used_margin=round(self._used_margin, 2),
            broker=self.name,
        )
        return BrokerResponse(success=True, broker=self.name, data=funds.model_dump())

    async def get_margin(self) -> BrokerResponse:
        margin_details = {
            "available_margin": round(self._balance - self._used_margin, 2),
            "used_margin": round(self._used_margin, 2),
            "total_margin": round(self._balance, 2)
        }
        return BrokerResponse(success=True, broker=self.name, data=margin_details)

    async def get_positions(self) -> BrokerResponse:
        # Refresh unrealised P/L using the real Kotak Neo LTP for each symbol.
        # Falls back to the position's own avg_price (flat, zero P&L) only if
        # the feed genuinely has no data for that symbol yet — never a random walk.
        from market.manager import market_data_manager
        positions = []
        for pos in self._positions.values():
            real_ltp = await market_data_manager.get_ltp(pos.symbol)
            ltp = real_ltp if real_ltp else pos.avg_price
            pos.ltp = round(ltp, 2)
            if pos.side == OrderSide.BUY:
                pos.unrealised_pnl = round((ltp - pos.avg_price) * pos.quantity, 2)
            else:
                pos.unrealised_pnl = round((pos.avg_price - ltp) * pos.quantity, 2)
            positions.append(pos.model_dump())
        return BrokerResponse(success=True, broker=self.name, data=positions)

    async def get_holdings(self) -> BrokerResponse:
        return BrokerResponse(
            success=True, broker=self.name,
            data=[h.model_dump() for h in self._holdings.values()]
        )

    async def get_orders(self) -> BrokerResponse:
        return BrokerResponse(
            success=True, broker=self.name,
            data=[o.model_dump() for o in self._orders.values()]
        )

    # ── Order management ─────────────────────────────────────────────────────

    async def place_order(
        self,
        symbol:        str,
        side:          OrderSide,
        quantity:      float,
        order_type:    OrderType = OrderType.MARKET,
        price:         Optional[float] = None,
        trigger_price: Optional[float] = None,
        tag:           Optional[str] = None,
    ) -> BrokerResponse:
        t0 = time.perf_counter()
        order_id = f"paper-{uuid.uuid4().hex[:10]}"

        order = Order(
            order_id=order_id,
            broker_order_id=f"pb-{uuid.uuid4().hex[:8]}",
            symbol=symbol,
            side=side,
            order_type=order_type,
            quantity=quantity,
            price=price,
            trigger_price=trigger_price,
            tag=tag,
            broker=self.name,
        )

        # ── Simulate rejection ──
        if random.random() < self._rejection_rate:
            order.status = OrderStatus.REJECTED
            order.reject_reason = "Simulated rejection (insufficient margin / risk check)"
            self._orders[order_id] = order
            logger.warning("PaperBroker: order REJECTED", order_id=order_id, symbol=symbol)
            latency = (time.perf_counter() - t0) * 1000
            return BrokerResponse(
                success=False, broker=self.name,
                data=order.model_dump(), error=order.reject_reason, latency_ms=latency
            )

        # ── Execution price ──
        # A LIMIT order carries its own price. A MARKET order legitimately has
        # price=None from the caller — that must fill at the real current LTP,
        # never a random number standing in for one.
        exec_price = price
        if not exec_price:
            from market.manager import market_data_manager
            exec_price = await market_data_manager.get_ltp(symbol)
        if not exec_price:
            order.status = OrderStatus.REJECTED
            order.reject_reason = f"No live Kotak Neo price available for {symbol}; refusing to fill blind."
            self._orders[order_id] = order
            logger.warning("PaperBroker: order REJECTED — no live price", order_id=order_id, symbol=symbol)
            latency = (time.perf_counter() - t0) * 1000
            return BrokerResponse(
                success=False, broker=self.name,
                data=order.model_dump(), error=order.reject_reason, latency_ms=latency
            )
        exec_price = round(exec_price, 2)

        # ── Simulate partial fill (5 % chance) ──
        if random.random() < self._partial_fill_rate:
            filled_qty = round(quantity * random.uniform(0.3, 0.8), 4)
            order.filled_quantity = filled_qty
            order.status = OrderStatus.PARTIAL
        else:
            filled_qty = quantity
            order.filled_quantity = quantity
            order.status = OrderStatus.FILLED

        order.avg_price = exec_price
        order.updated_at = datetime.now(timezone.utc)

        # ── Update cash balance ──
        cost       = exec_price * filled_qty
        commission = round(cost * self._commission_rate, 4)
        margin_req = round(cost / self._margin_multiplier, 4)

        if side == OrderSide.BUY:
            self._balance     -= (commission)
            self._used_margin += margin_req
        else:
            self._balance     += (cost - commission)
            self._used_margin  = max(0.0, self._used_margin - margin_req)

        # ── Update position ──
        pos_key = f"{symbol}_{side.value}"
        if pos_key in self._positions:
            existing = self._positions[pos_key]
            total_qty   = existing.quantity + filled_qty
            existing.avg_price = round(
                (existing.avg_price * existing.quantity + exec_price * filled_qty) / total_qty, 4
            )
            existing.quantity = total_qty
        else:
            self._positions[pos_key] = Position(
                symbol=symbol, side=side,
                quantity=filled_qty, avg_price=exec_price, broker=self.name
            )

        # ── Record trade ──
        trade = Trade(
            trade_id=f"paper-trd-{uuid.uuid4().hex[:10]}",
            order_id=order_id, symbol=symbol, side=side,
            quantity=filled_qty, price=exec_price,
            commission=commission, broker=self.name,
        )
        self._trades.append(trade)
        self._orders[order_id] = order

        latency = (time.perf_counter() - t0) * 1000
        logger.info(
            "PaperBroker: order placed",
            order_id=order_id, symbol=symbol,
            status=order.status, exec_price=exec_price, commission=commission
        )
        return BrokerResponse(
            success=True, broker=self.name,
            data=order.model_dump(), latency_ms=latency
        )

    async def modify_order(
        self,
        order_id:   str,
        quantity:   Optional[float] = None,
        price:      Optional[float] = None,
        order_type: Optional[OrderType] = None,
    ) -> BrokerResponse:
        order = self._orders.get(order_id)
        if not order or order.status not in (OrderStatus.NEW, OrderStatus.OPEN, OrderStatus.PARTIAL):
            return BrokerResponse(
                success=False, broker=self.name,
                error=f"Order {order_id} cannot be modified (status={order.status if order else 'NOT_FOUND'})"
            )
        if quantity:    order.quantity   = quantity
        if price:       order.price      = price
        if order_type:  order.order_type = order_type
        order.updated_at = datetime.now(timezone.utc)
        logger.info("PaperBroker: order modified", order_id=order_id)
        return BrokerResponse(success=True, broker=self.name, data=order.model_dump())

    async def cancel_order(self, order_id: str) -> BrokerResponse:
        order = self._orders.get(order_id)
        if not order or order.status in (OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED):
            return BrokerResponse(
                success=False, broker=self.name,
                error=f"Order {order_id} cannot be cancelled"
            )
        order.status     = OrderStatus.CANCELLED
        order.updated_at = datetime.now(timezone.utc)
        logger.info("PaperBroker: order cancelled", order_id=order_id)
        return BrokerResponse(success=True, broker=self.name, data=order.model_dump())

    async def get_order_status(self, order_id: str) -> BrokerResponse:
        order = self._orders.get(order_id)
        if not order:
            return BrokerResponse(success=False, broker=self.name, error="Order not found")
        return BrokerResponse(success=True, broker=self.name, data={"status": order.status})

    # ── Market data subscriptions ────────────────────────────────────────────

    async def subscribe_market_data(self, symbols: List[str]) -> BrokerResponse:
        logger.info("PaperBroker: subscribe_market_data (no-op)", symbols=symbols)
        return BrokerResponse(success=True, broker=self.name, data={"subscribed": symbols})

    async def unsubscribe_market_data(self, symbols: List[str]) -> BrokerResponse:
        logger.info("PaperBroker: unsubscribe_market_data (no-op)", symbols=symbols)
        return BrokerResponse(success=True, broker=self.name, data={"unsubscribed": symbols})

    async def subscribe_option_chain(self, underlying: str, expiry: str) -> BrokerResponse:
        logger.info("PaperBroker: subscribe_option_chain (no-op)", underlying=underlying, expiry=expiry)
        return BrokerResponse(success=True, broker=self.name, data={"underlying": underlying, "expiry": expiry})

    # ── Historical data ──────────────────────────────────────────────────────

    async def get_historical_data(
        self,
        symbol:    str,
        interval:  str,
        from_date: str,
        to_date:   str,
    ) -> BrokerResponse:
        """Returns synthetic OHLCV candles for back-test seeding."""
        candles = []
        price = random.uniform(100, 5000)
        for i in range(50):
            o = round(price, 2)
            h = round(price * random.uniform(1.0, 1.01), 2)
            l = round(price * random.uniform(0.99, 1.0), 2)
            c = round(random.uniform(l, h), 2)
            v = round(random.uniform(1000, 50000), 0)
            candles.append({"open": o, "high": h, "low": l, "close": c, "volume": v, "bar": i})
            price = c
        return BrokerResponse(success=True, broker=self.name, data=candles)

    # ── Health check ─────────────────────────────────────────────────────────

    async def health_check(self) -> BrokerResponse:
        t0 = time.perf_counter()
        latency = (time.perf_counter() - t0) * 1000 + random.uniform(0.5, 3.0)
        self._latency_ms = latency
        return BrokerResponse(
            success=self._connected, broker=self.name,
            data={"connected": self._connected},
            latency_ms=round(latency, 3)
        )
