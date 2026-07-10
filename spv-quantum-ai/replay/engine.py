import asyncio
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import uuid

from core.bus import event_bus, EventModel
from core.logging import get_logger
from brokers.manager import broker_manager
from backtest.loader import HistoricalDataLoader

from replay.models import ReplayConfig, ReplayState
from replay.publisher import ReplayPublisher

logger = get_logger("replay_engine")

class ReplayEngine:
    """
    Enterprise Market Replay Engine.
    Simulates historical markets candle-by-candle with dynamic controls:
    Play, Pause, Resume, Stop, Next Candle, Previous Candle, and Speed factor adjustments.
    """
    def __init__(self) -> None:
        self.loader = HistoricalDataLoader()
        self.publisher = ReplayPublisher()
        
        self.state = ReplayState(replay_id="", status="PENDING")
        self._running = False
        
        self._candles: List[Any] = []
        self._current_index = 0
        self._replay_task: Optional[asyncio.Task] = None
        self._original_broker_name: Optional[str] = None
        self._resume_event = asyncio.Event()
        self._resume_event.set()  # Playing by default
        self._speed_delays = {
            "1x": 1.0,
            "2x": 0.5,
            "5x": 0.2,
            "10x": 0.1,
            "25x": 0.04,
            "50x": 0.02,
            "100x": 0.01,
            "Unlimited": 0.001
        }

    async def start(self) -> None:
        self._running = True
        logger.info("ReplayEngine initialized.")

    async def stop_engine(self) -> None:
        self._running = False
        await self.stop()

    async def setup_replay(self, config: ReplayConfig) -> str:
        """Loads historical candles and prepares playback state."""
        replay_id = f"RPL-{uuid.uuid4().hex[:8]}"
        
        # Stop any active replay
        await self.stop()
        
        # Fetch candles
        self._candles.clear()
        for symbol in config.symbols:
            candles = await self.loader.load_candles(symbol, config.timeframe, config.start_date, config.end_date)
            self._candles.extend(candles)
            
        self._candles.sort(key=lambda c: c.timestamp)
        total = len(self._candles)
        
        self.state = ReplayState(
            replay_id=replay_id,
            status="PENDING",
            speed=config.speed,
            mode=config.mode,
            current_index=0,
            total_candles=total
        )
        self._current_index = 0
        self._resume_event.set()
        
        # Reset trading system if full trading is configured. SAFETY: pin the
        # active broker to paper_broker for the duration of this replay — the
        # same real-money isolation guarantee as BacktestingEngine (see its
        # _execute_simulation docstring). broker_manager.load("paper_broker")
        # alone does not make it active, so without this a replay run while
        # kotak_neo is selected elsewhere in the app would place real orders.
        if config.mode == "Full Trading System":
            self._original_broker_name = broker_manager._active_broker_name
            await broker_manager.load("paper_broker")
            broker_manager._active_broker_name = "paper_broker"
            await self._reset_trading_state(config.initial_capital)
            
        logger.info(f"Replay {replay_id} setup complete. Total candles: {total}")
        return replay_id

    async def play(self) -> None:
        """Starts the sequential playback task."""
        if self.state.status in ("PLAYING", "COMPLETED"):
            return
            
        self.state.status = "PLAYING"
        self._resume_event.set()
        await self.publisher.publish_started(self.state.replay_id, ReplayConfig(
            symbols=[], start_date=datetime.now(), end_date=datetime.now(), speed=self.state.speed, mode=self.state.mode
        ))
        
        self._replay_task = asyncio.create_task(self._playback_loop())

    async def pause(self) -> None:
        """Pauses the playback loop."""
        if self.state.status != "PLAYING":
            return
        self.state.status = "PAUSED"
        self._resume_event.clear()
        await self.publisher.publish_paused(self.state.replay_id, self._current_index)
        logger.info(f"Replay {self.state.replay_id} paused at index {self._current_index}.")

    async def resume(self) -> None:
        """Resumes the playback loop."""
        if self.state.status != "PAUSED":
            return
        self.state.status = "PLAYING"
        self._resume_event.set()
        await self.publisher.publish_resumed(self.state.replay_id)
        logger.info(f"Replay {self.state.replay_id} resumed.")

    async def stop(self) -> None:
        """Stops the playback loop completely."""
        self.state.status = "STOPPED"
        self._resume_event.set()

        if self._replay_task:
            self._replay_task.cancel()
            try:
                await self._replay_task
            except asyncio.CancelledError:
                pass
            self._replay_task = None

        if self._original_broker_name is not None:
            broker_manager._active_broker_name = self._original_broker_name
            self._original_broker_name = None

        await self.publisher.publish_stopped(self.state.replay_id)
        logger.info(f"Replay {self.state.replay_id} stopped.")

    async def set_speed(self, speed: str) -> None:
        """Changes playback speed factor dynamically."""
        if speed in self._speed_delays:
            self.state.speed = speed
            logger.info(f"Replay speed set to {speed}.")

    async def next_candle(self) -> None:
        """Stepping forward: plays a single candle manually when paused."""
        if self.state.status != "PAUSED":
            return
        if self._current_index < len(self._candles):
            await self._dispatch_candle(self._candles[self._current_index])
            self._current_index += 1
            self.state.current_index = self._current_index
            self.state.progress_pct = round(self._current_index / len(self._candles) * 100.0, 2)

    async def previous_candle(self) -> None:
        """Stepping backward: retreats the playback index by 1 when paused."""
        if self.state.status != "PAUSED":
            return
        if self._current_index > 0:
            self._current_index -= 1
            self.state.current_index = self._current_index
            self.state.progress_pct = round(self._current_index / len(self._candles) * 100.0, 2)
            await self._dispatch_candle(self._candles[self._current_index])

    # ── Playback Loop ──────────────────────────────────────────────────────────

    async def _playback_loop(self) -> None:
        try:
            while self._current_index < len(self._candles):
                # Check for pause event blocker
                await self._resume_event.wait()
                
                candle = self._candles[self._current_index]
                self.state.current_symbol = candle.symbol
                self.state.current_time = candle.timestamp
                self.state.current_index = self._current_index
                self.state.progress_pct = round((self._current_index + 1) / len(self._candles) * 100.0, 2)
                
                await self._dispatch_candle(candle)
                
                self._current_index += 1
                
                # Fetch speed factor delay
                delay = self._speed_delays.get(self.state.speed, 0.1)
                await asyncio.sleep(delay)
                
            # Completed
            self.state.status = "COMPLETED"
            self.state.progress_pct = 100.0
            await self.publisher.publish_completed(self.state.replay_id, self.state)
            logger.info(f"Replay {self.state.replay_id} finished successfully.")
            
        except asyncio.CancelledError:
            pass

    async def _dispatch_candle(self, candle: Any) -> None:
        """Broadcasts candles and tick updates onto the Event Bus."""
        # 1. Update MarketDataCache
        from market.manager import market_data_manager
        from market.models import MarketData
        
        tick = MarketData(
            symbol=candle.symbol,
            ltp=candle.close,
            prev_close=candle.open,
            volume=candle.volume,
            timestamp=candle.timestamp
        )
        await market_data_manager.cache.update_tick(tick)
        
        # 2. Publish tick
        await event_bus.publish(EventModel(
            event_type="tick",
            source_agent="market_data_engine",
            payload=tick.model_dump(mode="json")
        ))
        
        # 3. Publish candle
        await event_bus.publish(EventModel(
            event_type="candle",
            source_agent="market_data_engine",
            payload={"candle": candle.model_dump(mode="json")}
        ))

    async def _reset_trading_state(self, initial_capital: float) -> None:
        # Reset PaperBroker
        await broker_manager.load("paper_broker")
        broker = broker_manager.get_active()
        if hasattr(broker, "_positions"):
            broker._positions.clear()
            broker._orders.clear()
            broker._trades.clear()
            broker._balance = initial_capital
            broker._used_margin = 0.0
            broker._partial_fill_rate = 0.0
            broker._rejection_rate = 0.0

        # Reset PortfolioEngine positions
        from portfolio.engine import portfolio_engine
        async with portfolio_engine.positions._lock:
            portfolio_engine.positions._positions.clear()
        portfolio_engine.summary = portfolio_engine.summary.__class__()

        # Reset TradeJournalEngine lists
        from journal.engine import trade_journal_engine
        async with trade_journal_engine._lock:
            trade_journal_engine._active_trades.clear()
        from journal.repository import TradeHistoryRepository
        TradeHistoryRepository._in_memory_journal.clear()

        # Ensure execution engine is active
        from execution.engine import execution_engine
        await execution_engine.start()

    async def get_dashboard_status(self) -> Dict[str, Any]:
        return {
            "replay_id": self.state.replay_id,
            "status": self.state.status,
            "speed": self.state.speed,
            "mode": self.state.mode,
            "current_index": self.state.current_index,
            "total_candles": self.state.total_candles,
            "current_symbol": self.state.current_symbol,
            "current_date": self.state.current_time.isoformat() if self.state.current_time else None,
            "progress_pct": self.state.progress_pct
        }

# Singleton
replay_engine = ReplayEngine()
