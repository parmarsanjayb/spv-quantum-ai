import math
from typing import Dict, Any, Optional

class PositionSizingEngine:
    """
    PositionSizingEngine calculates the recommended position size based on
    different mathematical models:
      - Fixed Quantity
      - Fixed Capital
      - Percentage Risk
      - ATR Based
      - Volatility Based
      - Config Driven

    All division-based strategies floor to a whole number before returning —
    real exchanges don't fill fractional shares/lots, so a raw
    capital / price division (e.g. "5.599626691553897") is never a valid
    order quantity on its own.
    """

    def __init__(self, default_strategy: str = "fixed_quantity", default_params: Optional[Dict[str, Any]] = None) -> None:
        self.default_strategy = default_strategy
        self.default_params = default_params or {}

    def calculate_size(
        self,
        strategy: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
        capital_available: float = 100000.0,
        entry_price: float = 100.0,
        atr: Optional[float] = None,
        volatility: Optional[float] = None,
        lot_size: float = 1.0,
    ) -> float:
        strat = strategy or self.default_strategy
        p = {**self.default_params, **(params or {})}
        lot_size = lot_size if lot_size and lot_size > 0 else 1.0

        def _floor_to_lot(raw_qty: float) -> float:
            lots = math.floor(raw_qty / lot_size)
            return float(lots * lot_size)

        if strat == "fixed_quantity":
            return float(p.get("quantity", 1.0))

        elif strat == "fixed_capital":
            allocated_capital = float(p.get("capital", 10000.0))
            if entry_price <= 0:
                return 0.0
            return _floor_to_lot(allocated_capital / entry_price)

        elif strat == "percentage_risk":
            risk_pct = float(p.get("risk_pct", 1.0)) / 100.0  # e.g. 1%
            stop_loss_dist = float(p.get("stop_loss_distance", 5.0))
            if stop_loss_dist <= 0:
                return 0.0
            risk_amount = capital_available * risk_pct
            return _floor_to_lot(risk_amount / stop_loss_dist)

        elif strat == "atr_based":
            risk_pct = float(p.get("risk_pct", 1.0)) / 100.0
            multiplier = float(p.get("atr_multiplier", 2.0))
            atr_val = atr if atr is not None else float(p.get("atr", 1.5))
            if atr_val <= 0:
                return 0.0
            risk_amount = capital_available * risk_pct
            return _floor_to_lot(risk_amount / (atr_val * multiplier))

        elif strat == "volatility_based":
            risk_pct = float(p.get("risk_pct", 1.0)) / 100.0
            vol_val = volatility if volatility is not None else float(p.get("volatility", 0.02))
            if vol_val <= 0:
                return 0.0
            # Volatility based sizing: Risk Amount / (Entry Price * Volatility Factor)
            risk_amount = capital_available * risk_pct
            return _floor_to_lot(risk_amount / (entry_price * vol_val))

        elif strat == "config_driven":
            # Uses config dict directly
            return float(p.get("configured_size", 1.0))

        else:
            # Fallback to fixed quantity
            return float(p.get("quantity", 1.0))
