import time
import asyncio
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from market.models import Timeframe, Candle
from indicators.models import (
    IndicatorResult, BollingerBands, MACDResult, ADXResult,
    StochRSIResult, PivotPoints
)
from indicators.math import (
    calc_sma, calc_ema, calc_rsi, calc_macd, calc_atr, calc_adx,
    calc_vwap, calc_supertrend, calc_bollinger, calc_stoch_rsi,
    calc_cci, calc_roc, calc_momentum, calc_pivot_points,
    calc_support_resistance, calc_bars_since_cross
)
from indicators.publisher import IndicatorPublisher
from indicators.cache import IndicatorCache
from core.logging import get_logger

logger = get_logger("indicator_manager")

# OHLCV history per symbol+timeframe
_MAX_BARS = 250

class IndicatorManager:
    """
    Manages per-symbol/timeframe OHLCV history and runs all indicator
    calculations on every new closed candle.
    Never produces BUY/SELL. Never accesses brokers or DB.
    """

    def __init__(self, publisher: IndicatorPublisher) -> None:
        self._pub = publisher
        # (symbol, timeframe) → deque of bar dicts
        self._bars: Dict[tuple, List[Dict[str, float]]] = defaultdict(list)

    async def on_candle(self, candle: Candle) -> None:
        """Entry point: called for every closed candle from the Market Data Engine."""
        key = (candle.symbol, candle.timeframe)
        bars = self._bars[key]
        bars.append({
            "o": candle.open, "h": candle.high,
            "l": candle.low,  "c": candle.close,
            "v": candle.volume,
        })
        if len(bars) > _MAX_BARS:
            bars.pop(0)

        # Run all calculations concurrently
        await asyncio.gather(
            self._calc_all(candle.symbol, candle.timeframe, bars),
            return_exceptions=True
        )

    async def _calc_all(
        self, symbol: str, tf: Timeframe, bars: List[Dict[str, float]]
    ) -> None:
        closes  = [b["c"] for b in bars]
        highs   = [b["h"] for b in bars]
        lows    = [b["l"] for b in bars]
        volumes = [b["v"] for b in bars]

        tasks = [
            self._pub_ema(symbol, tf, closes, 9,   "EMA_9"),
            self._pub_ema(symbol, tf, closes, 20,  "EMA_20"),
            self._pub_ema(symbol, tf, closes, 27,  "EMA_27"),
            self._pub_ema(symbol, tf, closes, 50,  "EMA_50"),
            self._pub_ema(symbol, tf, closes, 100, "EMA_100"),
            self._pub_ema(symbol, tf, closes, 200, "EMA_200"),
            self._pub_sma(symbol, tf, closes, 20,  "SMA_20"),
            self._pub_sma(symbol, tf, closes, 50,  "SMA_50"),
            self._pub_rsi(symbol, tf, closes),
            self._pub_macd(symbol, tf, closes),
            self._pub_atr(symbol, tf, highs, lows, closes),
            self._pub_adx(symbol, tf, highs, lows, closes),
            self._pub_vwap(symbol, tf, highs, lows, closes, volumes),
            self._pub_supertrend(symbol, tf, highs, lows, closes),
            self._pub_bollinger(symbol, tf, closes),
            self._pub_stoch_rsi(symbol, tf, closes),
            self._pub_cci(symbol, tf, highs, lows, closes),
            self._pub_roc(symbol, tf, closes),
            self._pub_momentum(symbol, tf, closes),
            self._pub_pivot(symbol, tf, highs, lows, closes),
            self._pub_sr(symbol, tf, highs, lows),
            self._pub_bars_since_cross(symbol, tf, closes),
        ]
        await asyncio.gather(*tasks, return_exceptions=True)

    # ── Individual publishing helpers ─────────────────────────────────────────

    async def _emit(
        self, name: str, symbol: str, tf: Timeframe,
        value: Any, meta: dict, t0: float
    ) -> None:
        calc_ms = (time.perf_counter() - t0) * 1000
        result  = IndicatorResult(
            indicator_name = name, symbol = symbol, timeframe = tf,
            value          = value, calc_time_ms = round(calc_ms, 4),
            metadata       = meta,
        )
        await self._pub.publish(result)

    async def _pub_ema(self, sym, tf, closes, period, name):
        t0 = time.perf_counter()
        v  = calc_ema(closes, period)
        await self._emit(name, sym, tf, round(v, 4), {"period": period}, t0)

    async def _pub_sma(self, sym, tf, closes, period, name):
        t0 = time.perf_counter()
        v  = calc_sma(closes, period)
        await self._emit(name, sym, tf, round(v, 4), {"period": period}, t0)

    async def _pub_bars_since_cross(self, sym, tf, closes):
        t0 = time.perf_counter()
        v  = calc_bars_since_cross(closes, 9, 27)
        await self._emit("BARS_SINCE_EMA_9_27_CROSS", sym, tf, v, {"fast": 9, "slow": 27}, t0)

    async def _pub_rsi(self, sym, tf, closes):
        t0 = time.perf_counter()
        v  = calc_rsi(closes, 14)
        await self._emit("RSI", sym, tf, v, {"period": 14}, t0)

    async def _pub_macd(self, sym, tf, closes):
        t0 = time.perf_counter()
        ml, sl, h = calc_macd(closes)
        v  = MACDResult(macd_line=ml, signal_line=sl, histogram=h).model_dump()
        await self._emit("MACD", sym, tf, v, {"fast": 12, "slow": 26, "signal": 9}, t0)

    async def _pub_atr(self, sym, tf, highs, lows, closes):
        t0 = time.perf_counter()
        v  = calc_atr(highs, lows, closes, 14)
        await self._emit("ATR", sym, tf, v, {"period": 14}, t0)

    async def _pub_adx(self, sym, tf, highs, lows, closes):
        t0 = time.perf_counter()
        adx, di_p, di_n = calc_adx(highs, lows, closes, 14)
        v = ADXResult(adx=adx, di_pos=di_p, di_neg=di_n).model_dump()
        await self._emit("ADX", sym, tf, v, {"period": 14}, t0)

    async def _pub_vwap(self, sym, tf, highs, lows, closes, volumes):
        t0 = time.perf_counter()
        v  = calc_vwap(highs, lows, closes, volumes)
        await self._emit("VWAP", sym, tf, v, {}, t0)

    async def _pub_supertrend(self, sym, tf, highs, lows, closes):
        t0 = time.perf_counter()
        st_val, direction = calc_supertrend(highs, lows, closes)
        v = {"value": st_val, "direction": direction}
        await self._emit("SUPERTREND", sym, tf, v, {"period": 10, "multiplier": 3.0}, t0)

    async def _pub_bollinger(self, sym, tf, closes):
        t0 = time.perf_counter()
        u, m, l, bw = calc_bollinger(closes, 20, 2.0)
        v = BollingerBands(upper=u, middle=m, lower=l, bandwidth=bw).model_dump()
        await self._emit("BOLLINGER", sym, tf, v, {"period": 20, "std_dev": 2.0}, t0)

    async def _pub_stoch_rsi(self, sym, tf, closes):
        t0 = time.perf_counter()
        k, d = calc_stoch_rsi(closes)
        v = StochRSIResult(k=k, d=d).model_dump()
        await self._emit("STOCH_RSI", sym, tf, v, {}, t0)

    async def _pub_cci(self, sym, tf, highs, lows, closes):
        t0 = time.perf_counter()
        v  = calc_cci(highs, lows, closes, 20)
        await self._emit("CCI", sym, tf, v, {"period": 20}, t0)

    async def _pub_roc(self, sym, tf, closes):
        t0 = time.perf_counter()
        v  = calc_roc(closes, 12)
        await self._emit("ROC", sym, tf, v, {"period": 12}, t0)

    async def _pub_momentum(self, sym, tf, closes):
        t0 = time.perf_counter()
        v  = calc_momentum(closes, 10)
        await self._emit("MOMENTUM", sym, tf, v, {"period": 10}, t0)

    async def _pub_pivot(self, sym, tf, highs, lows, closes):
        t0 = time.perf_counter()
        p, r1, r2, r3, s1, s2, s3 = calc_pivot_points(highs[-1], lows[-1], closes[-1])
        v = PivotPoints(pivot=p, r1=r1, r2=r2, r3=r3, s1=s1, s2=s2, s3=s3).model_dump()
        await self._emit("PIVOT_POINTS", sym, tf, v, {}, t0)

    async def _pub_sr(self, sym, tf, highs, lows):
        t0 = time.perf_counter()
        resistance, support = calc_support_resistance(highs, lows, 20)
        v = {"resistance": resistance, "support": support}
        await self._emit("S_R", sym, tf, v, {"window": 20}, t0)
