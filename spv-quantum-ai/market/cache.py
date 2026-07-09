import asyncio
from datetime import datetime, timezone
from typing import Dict, List, Optional
from market.models import MarketData, Candle, OptionChain, Timeframe

class DataCacheManager:
    """
    High-performance in-memory store for all real-time market state.
    Thread-safe via asyncio.Lock. O(1) lookup for every field.
    """

    def __init__(self) -> None:
        self._ticks:         Dict[str, MarketData]                  = {}
        self._candles:       Dict[str, Dict[Timeframe, Candle]]     = {}
        self._option_chains: Dict[str, OptionChain]                 = {}
        self._prev_close:    Dict[str, float]                       = {}
        self._session_high:  Dict[str, float]                       = {}
        self._session_low:   Dict[str, float]                       = {}
        self._volume:        Dict[str, float]                       = {}
        self._oi:            Dict[str, float]                       = {}
        self._vwap:          Dict[str, float]                       = {}
        self._lock = asyncio.Lock()

    # ── Tick ──────────────────────────────────────────────────────────────────

    async def update_tick(self, tick: MarketData) -> None:
        async with self._lock:
            sym = tick.symbol
            self._ticks[sym] = tick

            # Session boundaries
            if sym not in self._session_high or tick.ltp > self._session_high[sym]:
                self._session_high[sym] = tick.ltp
            if sym not in self._session_low or tick.ltp < self._session_low[sym]:
                self._session_low[sym] = tick.ltp

            # Running totals
            self._volume[sym] = self._volume.get(sym, 0.0) + tick.volume
            self._oi[sym]     = tick.open_interest
            self._vwap[sym]   = tick.vwap

    async def get_tick(self, symbol: str) -> Optional[MarketData]:
        async with self._lock:
            return self._ticks.get(symbol)

    async def get_all_ticks(self) -> Dict[str, MarketData]:
        async with self._lock:
            return self._ticks.copy()

    # ── Candle ────────────────────────────────────────────────────────────────

    async def update_candle(self, candle: Candle) -> None:
        async with self._lock:
            if candle.symbol not in self._candles:
                self._candles[candle.symbol] = {}
            self._candles[candle.symbol][candle.timeframe] = candle

    async def get_candle(self, symbol: str, timeframe: Timeframe) -> Optional[Candle]:
        async with self._lock:
            return self._candles.get(symbol, {}).get(timeframe)

    # ── Option chain ──────────────────────────────────────────────────────────

    async def update_option_chain(self, chain: OptionChain) -> None:
        async with self._lock:
            self._option_chains[chain.underlying] = chain

    async def get_option_chain(self, symbol: str) -> Optional[OptionChain]:
        async with self._lock:
            return self._option_chains.get(symbol)

    # ── Session metrics ───────────────────────────────────────────────────────

    async def set_prev_close(self, symbol: str, price: float) -> None:
        async with self._lock:
            self._prev_close[symbol] = price

    async def get_prev_close(self, symbol: str) -> float:
        async with self._lock:
            return self._prev_close.get(symbol, 0.0)

    async def get_session_high(self, symbol: str) -> float:
        async with self._lock:
            return self._session_high.get(symbol, 0.0)

    async def get_session_low(self, symbol: str) -> float:
        async with self._lock:
            return self._session_low.get(symbol, 0.0)

    async def get_volume(self, symbol: str) -> float:
        async with self._lock:
            return self._volume.get(symbol, 0.0)

    async def get_oi(self, symbol: str) -> float:
        async with self._lock:
            return self._oi.get(symbol, 0.0)

    async def get_vwap(self, symbol: str) -> float:
        async with self._lock:
            return self._vwap.get(symbol, 0.0)

    async def reset_session(self, symbol: str) -> None:
        """Reset all intraday accumulators at market open."""
        async with self._lock:
            self._session_high.pop(symbol, None)
            self._session_low.pop(symbol, None)
            self._volume[symbol] = 0.0
            self._oi[symbol]     = 0.0
            self._vwap[symbol]   = 0.0
