import asyncio
import time
from datetime import datetime, timezone
from typing import Optional
from market.models import FeedStatus, FeedDisconnectedEvent, FeedConnectedEvent
from core.bus import event_bus, EventModel
from core.logging import get_logger

logger = get_logger("feed_health_monitor")

class FeedHealthMonitor:
    """
    Monitors live tick feed health.
    Detects: feed delay, missing ticks, data corruption, reconnect need.
    Publishes FeedDisconnectedEvent / FeedConnectedEvent on state changes.
    """

    def __init__(
        self,
        stale_threshold_sec: float = 5.0,
        check_interval_sec:  float = 2.0,
    ) -> None:
        self._status:              FeedStatus = FeedStatus.DISCONNECTED
        self._last_tick_time:      float      = 0.0
        self._stale_threshold:     float      = stale_threshold_sec
        self._check_interval:      float      = check_interval_sec
        self._total_ticks:         int        = 0
        self._corrupted_ticks:     int        = 0
        self._reconnect_requested: bool       = False
        self._monitor_task: Optional[asyncio.Task] = None

    # ── Public API ────────────────────────────────────────────────────────────

    def record_tick(self) -> None:
        """Call every time a valid tick is received from the feed."""
        self._last_tick_time = time.monotonic()
        self._total_ticks   += 1
        if self._status != FeedStatus.CONNECTED:
            asyncio.create_task(self._transition(FeedStatus.CONNECTED))

    def record_corrupted_tick(self) -> None:
        self._corrupted_ticks += 1

    def signal_connected(self) -> None:
        self._last_tick_time = time.monotonic()
        asyncio.create_task(self._transition(FeedStatus.CONNECTED))

    def signal_disconnected(self, reason: str = "Unknown") -> None:
        asyncio.create_task(self._transition(FeedStatus.DISCONNECTED, reason=reason))

    def get_status(self) -> FeedStatus:
        return self._status

    def get_stats(self) -> dict:
        elapsed = time.monotonic() - self._last_tick_time if self._last_tick_time else -1
        return {
            "status":              self._status.value,
            "last_tick_ago_sec":   round(elapsed, 2),
            "total_ticks":         self._total_ticks,
            "corrupted_ticks":     self._corrupted_ticks,
            "reconnect_requested": self._reconnect_requested,
        }

    # ── Monitor loop ──────────────────────────────────────────────────────────

    async def start(self) -> None:
        self._monitor_task = asyncio.create_task(self._monitor_loop())
        logger.info("FeedHealthMonitor started.")

    async def stop(self) -> None:
        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
        logger.info("FeedHealthMonitor stopped.")

    async def _monitor_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(self._check_interval)
                if self._status == FeedStatus.CONNECTED and self._last_tick_time > 0:
                    age = time.monotonic() - self._last_tick_time
                    if age > self._stale_threshold:
                        logger.warning("Feed is stale", age_sec=round(age, 1))
                        self._reconnect_requested = True
                        await self._transition(FeedStatus.DEGRADED, reason=f"No tick for {age:.1f}s")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Error in feed health monitor", error=str(e))

    async def _transition(self, new_status: FeedStatus, reason: str = "") -> None:
        if self._status == new_status:
            return
        old = self._status
        self._status = new_status
        logger.info("Feed status changed", old=old.value, new=new_status.value)

        if new_status == FeedStatus.DISCONNECTED:
            # DEGRADED means the connection is still alive but ticks are momentarily
            # sparse (e.g. after market hours, or a quiet symbol) — that's normal and
            # shouldn't make the UI wipe out the last known prices. Only a genuine
            # DISCONNECTED (socket actually dropped) should do that.
            evt = FeedDisconnectedEvent(reason=reason or new_status.value)
            await event_bus.publish(EventModel(
                event_type   = "feed_disconnected",
                source_agent = "feed_health_monitor",
                payload      = evt.model_dump(),
                priority     = 0,
            ))
        elif new_status == FeedStatus.CONNECTED:
            self._reconnect_requested = False
            evt = FeedConnectedEvent()
            await event_bus.publish(EventModel(
                event_type   = "feed_connected",
                source_agent = "feed_health_monitor",
                payload      = evt.model_dump(),
                priority     = 1,
            ))
