import asyncio
from datetime import datetime, time as dtime
from typing import Optional
from zoneinfo import ZoneInfo
from market.models import (
    MarketSession, MarketStatusChangedEvent,
    MarketOpenEvent, MarketCloseEvent
)
from core.bus import event_bus, EventModel
from core.logging import get_logger

logger = get_logger("market_status_manager")

_IST = ZoneInfo("Asia/Kolkata")
_NSE_OPEN  = dtime(9, 15)
_NSE_CLOSE = dtime(15, 30)

class MarketStatusManager:
    """
    Tracks exchange session state and publishes typed events on every transition.
    Transitions: CLOSED → PRE_OPEN → OPEN → CLOSED | HALTED.

    Status reflects the real NSE/BSE cash-market session window (9:15-15:30 IST,
    Mon-Fri), computed live — not just "OPEN while the app happens to be running".
    A background loop re-checks this every 30s so the dashboard can distinguish
    "Market Closed" (no real ticks expected right now) from "Feed Disconnected"
    (should be ticking but isn't).
    """

    def __init__(self) -> None:
        self._status: MarketSession = MarketSession.CLOSED
        self._auto_task: Optional[asyncio.Task] = None

    def get_status(self) -> MarketSession:
        return self._status

    @staticmethod
    def compute_real_session(now: Optional[datetime] = None) -> MarketSession:
        """NSE/BSE cash-market hours: 9:15-15:30 IST, Monday-Friday."""
        now = (now or datetime.now(_IST)).astimezone(_IST)
        if now.weekday() >= 5:  # Sat/Sun
            return MarketSession.CLOSED
        t = now.time()
        if _NSE_OPEN <= t < _NSE_CLOSE:
            return MarketSession.OPEN
        return MarketSession.CLOSED

    async def start_auto_tracking(self) -> None:
        """Sets status from real market hours immediately, then keeps it in sync."""
        await self.set_status(self.compute_real_session())
        self._auto_task = asyncio.create_task(self._auto_loop())

    async def stop_auto_tracking(self) -> None:
        if self._auto_task:
            self._auto_task.cancel()
            try:
                await self._auto_task
            except asyncio.CancelledError:
                pass
            self._auto_task = None

    async def _auto_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(30)
                await self.set_status(self.compute_real_session())
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Error in market status auto-tracking loop", error=str(e))

    async def set_status(self, new_status: MarketSession) -> None:
        if self._status == new_status:
            return
        old = self._status
        self._status = new_status
        logger.info("Market session changed", old=old.value, new=new_status.value)

        # Publish generic status change
        changed_evt = MarketStatusChangedEvent(old_status=old, new_status=new_status)
        await event_bus.publish(EventModel(
            event_type   = "market_status_changed",
            source_agent = "market_status_manager",
            payload      = changed_evt.model_dump(),
            priority     = 1,
        ))

        # Publish specific open / close events
        if new_status == MarketSession.OPEN:
            await event_bus.publish(EventModel(
                event_type   = "market_open",
                source_agent = "market_status_manager",
                payload      = MarketOpenEvent().model_dump(),
                priority     = 1,
            ))
        elif new_status == MarketSession.CLOSED:
            await event_bus.publish(EventModel(
                event_type   = "market_close",
                source_agent = "market_status_manager",
                payload      = MarketCloseEvent().model_dump(),
                priority     = 1,
            ))
