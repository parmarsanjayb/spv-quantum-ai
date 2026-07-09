import asyncio
from typing import Any, Dict, Optional
from market.models import (
    MarketData, Candle, OptionChain, Timeframe, MarketSession,
    TickEvent, CandleEvent
)
from market.cache import DataCacheManager
from market.registry import SymbolRegistry
from market.instrument import InstrumentManager
from market.tick import TickDataManager
from market.candle import CandleManager
from market.options import OptionChainManager
from market.status import MarketStatusManager
from market.history import HistoricalDataManager
from market.health import FeedHealthMonitor
from market.websocket import WebSocketStreamManager
from core.bus import event_bus, EventModel
from core.logging import get_logger

logger = get_logger("market_data_manager")

class MarketDataManager:
    """
    Central orchestrator of the Market Data Engine.
    The ONLY authoritative source of market data for the entire Trading OS.
    All agents, strategies, and execution modules must consume data exclusively
    from this manager or from the Event Bus events it publishes.
    """

    def __init__(self) -> None:
        self.cache       = DataCacheManager()
        self.registry    = SymbolRegistry()
        self.instruments = InstrumentManager()
        self.status      = MarketStatusManager()
        self.history     = HistoricalDataManager()
        self.health      = FeedHealthMonitor(stale_threshold_sec=60.0)

        self.candles     = CandleManager(self.cache, self._on_candle_close)
        self.ticks       = TickDataManager(self.cache)
        self.options     = OptionChainManager(self.cache)
        self.stream      = WebSocketStreamManager(self._on_raw_tick, self.health, self.instruments, self.registry)

        self._running: bool = False
        self._options_task: Optional[asyncio.Task] = None

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        logger.info("Starting Market Data Engine...")

        await self.status.start_auto_tracking()
        await self.health.start()
        await self.stream.start()

        self._options_task = asyncio.create_task(self._options_loop())
        logger.info("Market Data Engine running.")

    async def stop(self) -> None:
        self._running = False
        if self._options_task:
            self._options_task.cancel()
            try:
                await self._options_task
            except asyncio.CancelledError:
                pass
        await self.stream.stop()
        await self.health.stop()
        await self.status.stop_auto_tracking()
        await self.status.set_status(MarketSession.CLOSED)
        logger.info("Market Data Engine stopped.")

    # ── Feed callbacks ────────────────────────────────────────────────────────

    async def _on_raw_tick(self, raw: Dict[str, Any]) -> None:
        """Invoked by WebSocketStreamManager for every raw tick packet."""
        tick: MarketData = await self.ticks.process(raw)
        await self.candles.process_tick(tick)

        # Publish TickEvent
        await event_bus.publish(EventModel(
            event_type   = "tick",
            source_agent = "market_data_manager",
            payload      = TickEvent(tick=tick).model_dump(),
        ))

    async def _on_candle_close(self, candle: Candle) -> None:
        """Invoked by CandleManager when a timeframe bar completes."""
        await event_bus.publish(EventModel(
            event_type   = "candle",
            source_agent = "market_data_manager",
            payload      = CandleEvent(candle=candle).model_dump(),
        ))

    # ── Background tasks ──────────────────────────────────────────────────────

    async def _options_loop(self) -> None:
        """Rebuilds option chain matrix for index underlyings every 10 s."""
        while self._running:
            try:
                for underlying in ("NIFTY50", "BANKNIFTY"):
                    t = await self.cache.get_tick(underlying)
                    spot = t.ltp if t else (24000.0 if underlying == "NIFTY50" else 52000.0)
                    await self.options.build_chain(underlying, spot, "2026-07-31")
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Options loop error", error=str(e))
                await asyncio.sleep(2.0)

    # ── Convenience getters (public API for agents) ───────────────────────────

    async def get_ltp(self, symbol: str) -> float:
        t = await self.cache.get_tick(symbol)
        return t.ltp if t else 0.0

    async def get_candle(self, symbol: str, timeframe: Timeframe) -> Optional[Candle]:
        return await self.cache.get_candle(symbol, timeframe)

    async def get_option_chain(self, symbol: str) -> Optional[OptionChain]:
        return await self.cache.get_option_chain(symbol)

    async def get_feed_health(self) -> dict:
        return self.health.get_stats()

    async def get_session_summary(self, symbol: str) -> dict:
        return {
            "symbol":       symbol,
            "ltp":          (await self.cache.get_tick(symbol) or MarketData(symbol=symbol)).ltp,
            "session_high": await self.cache.get_session_high(symbol),
            "session_low":  await self.cache.get_session_low(symbol),
            "volume":       await self.cache.get_volume(symbol),
            "vwap":         await self.cache.get_vwap(symbol),
            "oi":           await self.cache.get_oi(symbol),
            "prev_close":   await self.cache.get_prev_close(symbol),
            "status":       self.status.get_status().value,
        }


# Module-level singleton — the ONLY market data source in the OS
market_data_manager = MarketDataManager()
