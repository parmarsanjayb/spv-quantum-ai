"""
Pure mathematical indicator calculation functions.
All functions are stateless. They accept price lists and return values.
No broker calls. No DB access. No signals.
"""
from typing import List, Tuple, Optional


# ── Moving Averages ──────────────────────────────────────────────────────────

def calc_sma(prices: List[float], period: int) -> float:
    if len(prices) < period:
        return prices[-1] if prices else 0.0
    return sum(prices[-period:]) / period

def calc_ema_series(prices: List[float], period: int) -> List[float]:
    n = len(prices)
    if n == 0:
        return []
    ema = [0.0] * n
    ema[0] = prices[0]
    k = 2.0 / (period + 1)
    for i in range(1, n):
        ema[i] = prices[i] * k + ema[i - 1] * (1.0 - k)
    return ema

def calc_bars_since_cross(prices: List[float], fast_period: int, slow_period: int) -> float:
    """Number of closed bars since the fast/slow EMA last crossed (1 on the bar the
    cross happened on, incrementing each bar after). Matches the convention used by
    externally-supplied BARS_SINCE_CROSS columns (e.g. broker-exported backtest CSVs)."""
    n = len(prices)
    if n == 0:
        return 0.0
    fast = calc_ema_series(prices, fast_period)
    slow = calc_ema_series(prices, slow_period)
    bars = 1
    for i in range(n - 1, 0, -1):
        if (fast[i] > slow[i]) != (fast[i - 1] > slow[i - 1]):
            break
        bars += 1
    return float(bars)

def calc_ema(prices: List[float], period: int) -> float:
    if not prices:
        return 0.0
    series = calc_ema_series(prices, period)
    return series[-1] if series else 0.0


# ── RSI ──────────────────────────────────────────────────────────────────────

def calc_rsi(prices: List[float], period: int = 14) -> float:
    if len(prices) <= period:
        return 50.0
    gains, losses = [], []
    for i in range(1, len(prices)):
        diff = prices[i] - prices[i - 1]
        gains.append(max(0.0, diff))
        losses.append(max(0.0, -diff))
    ag = sum(gains[:period]) / period
    al = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        ag = (ag * (period - 1) + gains[i]) / period
        al = (al * (period - 1) + losses[i]) / period
    if al == 0:
        return 100.0
    return round(100.0 - (100.0 / (1.0 + ag / al)), 4)


# ── MACD ─────────────────────────────────────────────────────────────────────

def calc_macd(
    prices: List[float],
    fast: int = 12, slow: int = 26, signal: int = 9
) -> Tuple[float, float, float]:
    if len(prices) < slow:
        return 0.0, 0.0, 0.0
    fast_s  = calc_ema_series(prices, fast)
    slow_s  = calc_ema_series(prices, slow)
    macd_s  = [fast_s[i] - slow_s[i] for i in range(len(prices))]
    sig_s   = calc_ema_series(macd_s, signal)
    macd_v  = macd_s[-1]
    sig_v   = sig_s[-1]
    hist_v  = macd_v - sig_v
    return round(macd_v, 6), round(sig_v, 6), round(hist_v, 6)


# ── ATR ──────────────────────────────────────────────────────────────────────

def calc_atr(
    highs: List[float], lows: List[float], closes: List[float], period: int = 14
) -> float:
    n = len(closes)
    if n < period + 1:
        return 0.0
    trs = [highs[0] - lows[0]]
    for i in range(1, n):
        tr = max(highs[i] - lows[i], abs(highs[i] - closes[i-1]), abs(lows[i] - closes[i-1]))
        trs.append(tr)
    atr = sum(trs[:period]) / period
    for i in range(period, n):
        atr = (atr * (period - 1) + trs[i]) / period
    return round(atr, 6)


# ── ADX / DI +/- ─────────────────────────────────────────────────────────────

def calc_adx(
    highs: List[float], lows: List[float], closes: List[float], period: int = 14
) -> Tuple[float, float, float]:
    """Returns (ADX, DI+, DI-)."""
    n = len(closes)
    if n < period * 2:
        return 0.0, 0.0, 0.0

    dm_pos, dm_neg, trs = [], [], []
    for i in range(1, n):
        up   = highs[i]  - highs[i-1]
        down = lows[i-1] - lows[i]
        dm_pos.append(up   if up > down and up > 0 else 0.0)
        dm_neg.append(down if down > up and down > 0 else 0.0)
        tr = max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1]))
        trs.append(tr)

    def smooth(series: List[float]) -> List[float]:
        s = [sum(series[:period])]
        for i in range(period, len(series)):
            s.append(s[-1] - s[-1] / period + series[i])
        return s

    sm_tr  = smooth(trs)
    sm_pos = smooth(dm_pos)
    sm_neg = smooth(dm_neg)

    di_pos = [100 * sm_pos[i] / sm_tr[i] if sm_tr[i] else 0 for i in range(len(sm_tr))]
    di_neg = [100 * sm_neg[i] / sm_tr[i] if sm_tr[i] else 0 for i in range(len(sm_tr))]

    dx = [
        100 * abs(di_pos[i] - di_neg[i]) / (di_pos[i] + di_neg[i])
        if (di_pos[i] + di_neg[i]) else 0
        for i in range(len(di_pos))
    ]
    adx = sum(dx[-period:]) / period if len(dx) >= period else 0.0
    return round(adx, 4), round(di_pos[-1], 4), round(di_neg[-1], 4)


# ── VWAP ─────────────────────────────────────────────────────────────────────

def calc_vwap(
    highs: List[float], lows: List[float], closes: List[float], volumes: List[float]
) -> float:
    tp_vol = sum(((h + l + c) / 3) * v for h, l, c, v in zip(highs, lows, closes, volumes))
    total_v = sum(volumes)
    return round(tp_vol / total_v, 4) if total_v else 0.0


# ── SuperTrend ────────────────────────────────────────────────────────────────

def calc_supertrend(
    highs: List[float], lows: List[float], closes: List[float],
    period: int = 10, multiplier: float = 3.0
) -> Tuple[float, int]:
    """Returns (supertrend_value, direction) where direction: 1=bullish, -1=bearish."""
    n = len(closes)
    if n < period:
        return closes[-1] if closes else 0.0, 1

    atr_val = calc_atr(highs, lows, closes, period)
    hl2 = [(highs[i] + lows[i]) / 2 for i in range(n)]

    basic_ub = hl2[-1] + multiplier * atr_val
    basic_lb = hl2[-1] - multiplier * atr_val

    # Simplified single-pass final value
    direction = 1 if closes[-1] > basic_lb else -1
    st_value  = basic_lb if direction == 1 else basic_ub
    return round(st_value, 4), direction


# ── Bollinger Bands ───────────────────────────────────────────────────────────

def calc_bollinger(prices: List[float], period: int = 20, std_dev: float = 2.0):
    if len(prices) < period:
        p = prices[-1] if prices else 0.0
        return p, p, p, 0.0
    window = prices[-period:]
    mid    = sum(window) / period
    var    = sum((x - mid) ** 2 for x in window) / period
    sd     = var ** 0.5
    upper  = round(mid + std_dev * sd, 4)
    lower  = round(mid - std_dev * sd, 4)
    bw     = round((upper - lower) / mid * 100, 4) if mid else 0.0
    return round(upper, 4), round(mid, 4), round(lower, 4), bw


# ── Stochastic RSI ────────────────────────────────────────────────────────────

def calc_stoch_rsi(prices: List[float], rsi_period: int = 14, stoch_period: int = 14,
                   k_period: int = 3, d_period: int = 3) -> Tuple[float, float]:
    if len(prices) < rsi_period + stoch_period:
        return 50.0, 50.0
    # Build RSI series
    rsi_series = []
    for i in range(rsi_period, len(prices) + 1):
        rsi_series.append(calc_rsi(prices[:i], rsi_period))
    if len(rsi_series) < stoch_period:
        return 50.0, 50.0
    window = rsi_series[-stoch_period:]
    lo, hi = min(window), max(window)
    k = ((rsi_series[-1] - lo) / (hi - lo) * 100) if (hi - lo) else 50.0
    k_series = [k]
    d = sum(k_series[-d_period:]) / len(k_series[-d_period:])
    return round(k, 4), round(d, 4)


# ── CCI ───────────────────────────────────────────────────────────────────────

def calc_cci(
    highs: List[float], lows: List[float], closes: List[float], period: int = 20
) -> float:
    if len(closes) < period:
        return 0.0
    tp     = [(highs[i] + lows[i] + closes[i]) / 3 for i in range(len(closes))]
    tp_w   = tp[-period:]
    mean   = sum(tp_w) / period
    mad    = sum(abs(x - mean) for x in tp_w) / period
    return round((tp[-1] - mean) / (0.015 * mad), 4) if mad else 0.0


# ── ROC ───────────────────────────────────────────────────────────────────────

def calc_roc(prices: List[float], period: int = 12) -> float:
    if len(prices) <= period:
        return 0.0
    prev = prices[-period - 1]
    return round(((prices[-1] - prev) / prev) * 100, 4) if prev else 0.0


# ── Momentum ──────────────────────────────────────────────────────────────────

def calc_momentum(prices: List[float], period: int = 10) -> float:
    if len(prices) <= period:
        return 0.0
    return round(prices[-1] - prices[-period - 1], 4)


# ── Pivot Points ──────────────────────────────────────────────────────────────

def calc_pivot_points(high: float, low: float, close: float):
    """Classic floor pivot points."""
    p  = (high + low + close) / 3
    r1 = 2 * p - low
    r2 = p + (high - low)
    r3 = high + 2 * (p - low)
    s1 = 2 * p - high
    s2 = p - (high - low)
    s3 = low - 2 * (high - p)
    return (round(x, 4) for x in (p, r1, r2, r3, s1, s2, s3))


# ── Support & Resistance ──────────────────────────────────────────────────────

def calc_support_resistance(
    highs: List[float], lows: List[float], window: int = 20
) -> Tuple[float, float]:
    """Basic: highest high and lowest low over window."""
    if not highs or not lows:
        return 0.0, 0.0
    w_h = highs[-window:]
    w_l = lows[-window:]
    return round(max(w_h), 4), round(min(w_l), 4)
