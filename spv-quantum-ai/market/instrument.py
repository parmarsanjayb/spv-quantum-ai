from typing import Any, Dict, List, Optional

class InstrumentManager:
    """
    Manages instrument specifications: tokens, segments, lot sizes, tick sizes.
    Every broker adapter must map its internal tokens to canonical symbol names here.
    """

    def __init__(self) -> None:
        self._instruments: Dict[str, Dict[str, Any]] = {
            # Index
            "NIFTY50":    {"token": "26000", "exchange": "NSE",    "segment": "nse_cm",  "lot_size": 50,   "tick_size": 0.05,   "precision": 2},
            "BANKNIFTY":  {"token": "26009", "exchange": "NSE",    "segment": "nse_cm",  "lot_size": 15,   "tick_size": 0.05,   "precision": 2},
            "FINNIFTY":   {"token": "26037", "exchange": "NSE",    "segment": "nse_cm",  "lot_size": 40,   "tick_size": 0.05,   "precision": 2},
            # Equity
            "RELIANCE":   {"token": "2885",  "exchange": "NSE",    "segment": "nse_cm",  "lot_size": 1,    "tick_size": 0.05,   "precision": 2},
            "TCS":        {"token": "11536", "exchange": "NSE",    "segment": "nse_cm",  "lot_size": 1,    "tick_size": 0.05,   "precision": 2},
            "HDFCBANK":   {"token": "1333",  "exchange": "NSE",    "segment": "nse_cm",  "lot_size": 1,    "tick_size": 0.05,   "precision": 2},
            "INFY":       {"token": "1594",  "exchange": "NSE",    "segment": "nse_cm",  "lot_size": 1,    "tick_size": 0.05,   "precision": 2},
            "ICICIBANK":  {"token": "4963",  "exchange": "NSE",    "segment": "nse_cm",  "lot_size": 1,    "tick_size": 0.05,   "precision": 2},
            "SBIN":       {"token": "3045",  "exchange": "NSE",    "segment": "nse_cm",  "lot_size": 1,    "tick_size": 0.05,   "precision": 2},
            "ITC":        {"token": "1660",  "exchange": "NSE",    "segment": "nse_cm",  "lot_size": 1,    "tick_size": 0.05,   "precision": 2},
            # Currency
            "USDINR":     {"token": "usdinr","exchange": "CDS",    "segment": "cd_fo",   "lot_size": 1000, "tick_size": 0.0025, "precision": 4},
            # Commodity
            "CRUDEOIL":   {"token": "crudeoil", "exchange": "MCX", "segment": "mcx_fo",  "lot_size": 100,  "tick_size": 1.0,    "precision": 2},
            "GOLD":       {"token": "gold",  "exchange": "MCX",    "segment": "mcx_fo",  "lot_size": 100,  "tick_size": 1.0,    "precision": 2},
            "SILVER":     {"token": "silver","exchange": "MCX",    "segment": "mcx_fo",  "lot_size": 30,   "tick_size": 1.0,    "precision": 2},
            "NATURALGAS": {"token": "natgas","exchange": "MCX",    "segment": "mcx_fo",  "lot_size": 1250, "tick_size": 0.1,    "precision": 2},
            # Crypto
            "BTCUSD":     {"token": "btc",   "exchange": "CRYPTO", "segment": "spot",    "lot_size": 1,    "tick_size": 0.01,   "precision": 2},
            "ETHUSD":     {"token": "eth",   "exchange": "CRYPTO", "segment": "spot",    "lot_size": 1,    "tick_size": 0.01,   "precision": 2},
        }

    def register(
        self,
        symbol:    str,
        token:     str,
        exchange:  str,
        segment:   str,
        lot_size:  int   = 1,
        tick_size: float = 0.01,
        precision: int   = 2,
    ) -> None:
        self._instruments[symbol] = {
            "token": token, "exchange": exchange, "segment": segment,
            "lot_size": lot_size, "tick_size": tick_size, "precision": precision,
        }

    def get(self, symbol: str) -> Optional[Dict[str, Any]]:
        return self._instruments.get(symbol)

    def get_token(self, symbol: str) -> Optional[str]:
        inst = self._instruments.get(symbol)
        return inst["token"] if inst else None

    def get_all(self) -> Dict[str, Dict[str, Any]]:
        return self._instruments.copy()

    def get_by_token(self, token: str) -> Optional[str]:
        """Reverse lookup: broker token → canonical symbol name."""
        for sym, meta in self._instruments.items():
            if meta["token"] == token:
                return sym
        return None
