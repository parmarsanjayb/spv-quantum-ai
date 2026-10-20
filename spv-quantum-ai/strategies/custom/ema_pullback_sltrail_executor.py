"""
Self-contained executor for the ema_9_27_pullback_sltrail_ce / _pe strategies.

Deliberately isolated from the shared chief-decision-agent / risk-agent / execution-
agent order pipeline so it cannot change behavior for any other strategy: it listens
only for "strategy_matched" events naming these two strategies, still honors
safety_engine's pre-trade checks before placing an order, and manages its own fixed
stop-loss + trailing-stop exit (points-based, not the global percentage-based
protection_manager, which stays untouched and keeps applying to everything else).
"""
import asyncio
from datetime import datetime, time as dt_time
from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo

from core.bus import event_bus, EventModel
from core.logging import get_logger
from brokers import broker_engine
from brokers.models import OrderSide, OrderType
from market.manager import market_data_manager
from market.models import Timeframe
from indicators.engine import indicator_engine
from safety import safety_engine

logger = get_logger("ema_pullback_sltrail_executor")

IST = ZoneInfo("Asia/Kolkata")

STRATEGY_NAMES = {"ema_9_27_pullback_sltrail_ce", "ema_9_27_pullback_sltrail_pe"}
SL_POINTS = 25.0
TRAIL_POINTS = 20.0
QTY = 10.0


class EmaPullbackSlTrailExecutor:
    def __init__(self) -> None:
        self._position: Optional[Dict[str, Any]] = None
        self._running = False
        self._monitor_task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        await event_bus.subscribe("strategy_matched", self._on_strategy_matched)
        self._monitor_task = asyncio.create_task(self._monitor_loop())
        logger.info("EmaPullbackSlTrailExecutor started.")

    async def stop(self) -> None:
        self._running = False
        await event_bus.unsubscribe("strategy_matched", self._on_strategy_matched)
        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
            self._monitor_task = None
        logger.info("EmaPullbackSlTrailExecutor stopped.")

    async def _on_strategy_matched(self, event: EventModel) -> None:
        try:
            payload = event.payload
            resp = payload.get("strategy_response", {})
            name = resp.get("strategy_name")
            if name not in STRATEGY_NAMES:
                return
            if self._position is not None:
                return  # one position at a time; ignore further signals until it exits

            context = payload.get("context", {})
            symbol = context.get("symbol")
            if not symbol:
                logger.error("strategy_matched event missing symbol in context; cannot trade.")
                return

            side = OrderSide.BUY if name.endswith("_ce") else OrderSide.SELL

            safety_resp = await safety_engine.check_order({
                "symbol": symbol, "side": side.value, "quantity": QTY,
                "price": None, "order_type": "MARKET",
            })
            if not safety_resp.allowed:
                logger.info(f"EMA pullback entry blocked by safety ({symbol}): {safety_resp.reason}")
                return

            ltp = await market_data_manager.get_ltp(symbol)
            order_resp = await broker_engine.place_order(
                symbol=symbol, side=side, quantity=QTY,
                order_type=OrderType.MARKET, price=ltp if ltp > 0 else None,
                tag=name,
            )
            if not order_resp.success or not order_resp.data:
                logger.error(f"EMA pullback entry order failed ({symbol}): {order_resp.error}")
                return

            entry_price = float(order_resp.data.get("avg_price") or 0.0)
            if entry_price <= 0:
                entry_price = ltp

            self._position = {
                "symbol": symbol, "side": side, "entry_price": entry_price,
                "peak_favorable": 0.0, "strategy_name": name,
            }
            logger.info(f"EMA pullback ENTRY: {symbol} {side.value} @ {entry_price} (strategy={name})")
        except Exception as e:
            logger.error(f"Error handling strategy_matched in EmaPullbackSlTrailExecutor: {e}")

    async def _monitor_loop(self) -> None:
        while self._running:
            try:
                await asyncio.sleep(0.5)
                if self._position is not None:
                    await self._check_exit()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in EmaPullbackSlTrailExecutor monitor loop: {e}")

    async def _check_exit(self) -> None:
        pos = self._position
        symbol = pos["symbol"]
        side = pos["side"]
        entry = pos["entry_price"]

        ltp = await market_data_manager.get_ltp(symbol)
        if ltp <= 0:
            return

        if side == OrderSide.BUY:
            favorable = ltp - entry
            adverse = entry - ltp
        else:
            favorable = entry - ltp
            adverse = ltp - entry

        pos["peak_favorable"] = max(pos["peak_favorable"], favorable)
        reason: Optional[str] = None

        if adverse >= SL_POINTS:
            reason = "Stop_Loss"
        elif pos["peak_favorable"] >= TRAIL_POINTS and (pos["peak_favorable"] - favorable) >= TRAIL_POINTS:
            reason = "Trail_SL"
        else:
            ema9 = await indicator_engine.cache.get_latest(symbol, Timeframe.M3, "EMA_9")
            ema27 = await indicator_engine.cache.get_latest(symbol, Timeframe.M3, "EMA_27")
            if ema9 and ema27:
                if side == OrderSide.BUY and ema9.value < ema27.value:
                    reason = "EMA_Cross_Exit"
                elif side == OrderSide.SELL and ema9.value > ema27.value:
                    reason = "EMA_Cross_Exit"
            if reason is None and datetime.now(IST).time() >= dt_time(15, 15):
                reason = "EOD_Squareoff"

        if reason:
            await self._exit_position(reason, ltp)

    async def _exit_position(self, reason: str, ltp: float) -> None:
        pos = self._position
        exit_side = OrderSide.SELL if pos["side"] == OrderSide.BUY else OrderSide.BUY
        resp = await broker_engine.place_order(
            symbol=pos["symbol"], side=exit_side, quantity=QTY,
            order_type=OrderType.MARKET, price=ltp if ltp > 0 else None,
            tag=f"{pos['strategy_name']}_exit",
        )
        if resp.success:
            logger.info(f"EMA pullback EXIT ({reason}): {pos['symbol']} {exit_side.value} @ {ltp}")
        else:
            logger.error(f"EMA pullback exit order failed ({pos['symbol']}): {resp.error}")
        self._position = None


ema_pullback_sltrail_executor = EmaPullbackSlTrailExecutor()
