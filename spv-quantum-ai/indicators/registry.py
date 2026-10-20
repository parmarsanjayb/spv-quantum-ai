"""
IndicatorRegistry — catalogue of all supported indicator names.
Add new indicators here. Do NOT put calculation logic in this file.
"""

INDICATOR_REGISTRY = {
    # Moving Averages
    "EMA_9":   {"family": "MA",        "period": 9,   "description": "Exponential Moving Average 9"},
    "EMA_20":  {"family": "MA",        "period": 20,  "description": "Exponential Moving Average 20"},
    "EMA_27":  {"family": "MA",        "period": 27,  "description": "Exponential Moving Average 27"},
    "EMA_50":  {"family": "MA",        "period": 50,  "description": "Exponential Moving Average 50"},
    "EMA_100": {"family": "MA",        "period": 100, "description": "Exponential Moving Average 100"},
    "EMA_200": {"family": "MA",        "period": 200, "description": "Exponential Moving Average 200"},
    "SMA_20":  {"family": "MA",        "period": 20,  "description": "Simple Moving Average 20"},
    "SMA_50":  {"family": "MA",        "period": 50,  "description": "Simple Moving Average 50"},

    # Oscillators
    "RSI":          {"family": "OSCILLATOR", "period": 14, "description": "Relative Strength Index"},
    "STOCH_RSI":    {"family": "OSCILLATOR", "description": "Stochastic RSI (K, D)"},
    "CCI":          {"family": "OSCILLATOR", "period": 20,  "description": "Commodity Channel Index"},
    "ROC":          {"family": "OSCILLATOR", "period": 12,  "description": "Rate of Change"},
    "MOMENTUM":     {"family": "OSCILLATOR", "period": 10,  "description": "Price Momentum"},

    # Trend
    "MACD":         {"family": "TREND", "description": "MACD (12,26,9)"},
    "ADX":          {"family": "TREND", "description": "ADX + DI+/DI-"},
    "SUPERTREND":   {"family": "TREND", "description": "SuperTrend (10, 3)"},
    "VWAP":         {"family": "TREND", "description": "Volume Weighted Average Price"},

    # Volatility
    "ATR":          {"family": "VOLATILITY", "period": 14, "description": "Average True Range"},
    "BOLLINGER":    {"family": "VOLATILITY", "description": "Bollinger Bands (20, 2)"},

    # Support / Resistance
    "PIVOT_POINTS": {"family": "S_R", "description": "Classic Pivot Points (P, R1-R3, S1-S3)"},
    "S_R":          {"family": "S_R", "description": "Support & Resistance (20-bar high/low)"},

    # Crossover tracking
    "BARS_SINCE_EMA_9_27_CROSS": {"family": "TREND", "description": "Closed bars since EMA_9/EMA_27 last crossed"},
}

def is_registered(name: str) -> bool:
    return name in INDICATOR_REGISTRY

def get_all() -> dict:
    return INDICATOR_REGISTRY.copy()
