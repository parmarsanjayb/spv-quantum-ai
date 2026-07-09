from typing import Any, Dict, Optional, Set

class SymbolRegistry:
    """
    Central catalogue of all symbols that the Market Data Engine is tracking.
    No broker or agent may subscribe to data without first registering here.
    """

    def __init__(self) -> None:
        # Default universe — spans Index, Equity, Currency, Commodity, and Crypto segments
        self._meta: Dict[str, Dict[str, Any]] = {
            # Index
            "NIFTY50":      {"exchange": "NSE",  "segment": "INDEX",     "lot_size": 65,   "tick_size": 0.05},
            "BANKNIFTY":    {"exchange": "NSE",  "segment": "INDEX",     "lot_size": 30,   "tick_size": 0.05},
            "FINNIFTY":     {"exchange": "NSE",  "segment": "INDEX",     "lot_size": 60,   "tick_size": 0.05},
            "MIDCPNIFTY":   {"exchange": "NSE",  "segment": "INDEX",     "lot_size": 120,  "tick_size": 0.05},
            "SENSEX":       {"exchange": "BSE",  "segment": "INDEX",     "lot_size": 20,   "tick_size": 0.05},
            # Equity
            "RELIANCE":     {"exchange": "NSE",  "segment": "EQUITY",    "lot_size": 1,    "tick_size": 0.05},
            "TCS":          {"exchange": "NSE",  "segment": "EQUITY",    "lot_size": 1,    "tick_size": 0.05},
            "HDFCBANK":     {"exchange": "NSE",  "segment": "EQUITY",    "lot_size": 1,    "tick_size": 0.05},
            "INFY":         {"exchange": "NSE",  "segment": "EQUITY",    "lot_size": 1,    "tick_size": 0.05},
            "ICICIBANK":    {"exchange": "NSE",  "segment": "EQUITY",    "lot_size": 1,    "tick_size": 0.05},
            "SBIN":         {"exchange": "NSE",  "segment": "EQUITY",    "lot_size": 1,    "tick_size": 0.05},
            "ITC":          {"exchange": "NSE",  "segment": "EQUITY",    "lot_size": 1,    "tick_size": 0.05},
            "LT":           {"exchange": "NSE",  "segment": "EQUITY",    "lot_size": 1,    "tick_size": 0.05},
            "AXISBANK":     {"exchange": "NSE",  "segment": "EQUITY",    "lot_size": 1,    "tick_size": 0.05},
            "KOTAKBANK":    {"exchange": "NSE",  "segment": "EQUITY",    "lot_size": 1,    "tick_size": 0.05},
            "HINDUNILVR":   {"exchange": "NSE",  "segment": "EQUITY",    "lot_size": 1,    "tick_size": 0.05},
            "BHARTIARTL":   {"exchange": "NSE",  "segment": "EQUITY",    "lot_size": 1,    "tick_size": 0.05},
            "BAJFINANCE":   {"exchange": "NSE",  "segment": "EQUITY",    "lot_size": 1,    "tick_size": 0.05},
            "MARUTI":       {"exchange": "NSE",  "segment": "EQUITY",    "lot_size": 1,    "tick_size": 0.05},
            "WIPRO":        {"exchange": "NSE",  "segment": "EQUITY",    "lot_size": 1,    "tick_size": 0.05},
            # Currency
            "USDINR":       {"exchange": "CDS",  "segment": "CURRENCY",  "lot_size": 1000, "tick_size": 0.0025},
            # Commodity
            "CRUDEOIL":     {"exchange": "MCX",  "segment": "COMMODITY", "lot_size": 100,  "tick_size": 1.0},
            "NATURALGAS":   {"exchange": "MCX",  "segment": "COMMODITY", "lot_size": 1250, "tick_size": 0.1},
            "GOLD":         {"exchange": "MCX",  "segment": "COMMODITY", "lot_size": 1,    "tick_size": 1.0},
            "SILVER":       {"exchange": "MCX",  "segment": "COMMODITY", "lot_size": 30,   "tick_size": 1.0},
            "COPPER":       {"exchange": "MCX",  "segment": "COMMODITY", "lot_size": 2500, "tick_size": 0.05},
            "ZINC":         {"exchange": "MCX",  "segment": "COMMODITY", "lot_size": 5,    "tick_size": 0.05},
            "ALUMINIUM":    {"exchange": "MCX",  "segment": "COMMODITY", "lot_size": 5,    "tick_size": 0.05},
            "LEAD":         {"exchange": "MCX",  "segment": "COMMODITY", "lot_size": 5,    "tick_size": 0.05},
            "NICKEL":       {"exchange": "MCX",  "segment": "COMMODITY", "lot_size": 250,  "tick_size": 0.1},
            # Crypto
            "BTCUSD":       {"exchange": "CRYPTO", "segment": "SPOT",    "lot_size": 1,    "tick_size": 0.01},
            "ETHUSD":       {"exchange": "CRYPTO", "segment": "SPOT",    "lot_size": 1,    "tick_size": 0.01},
        }
        self._symbols: Set[str] = set(self._meta.keys())

    def register(self, symbol: str, meta: Optional[Dict[str, Any]] = None) -> None:
        self._symbols.add(symbol)
        if meta:
            self._meta[symbol] = meta

    def unregister(self, symbol: str) -> None:
        self._symbols.discard(symbol)
        self._meta.pop(symbol, None)

    def is_registered(self, symbol: str) -> bool:
        return symbol in self._symbols

    def get_symbols(self) -> Set[str]:
        return self._symbols.copy()

    def get_meta(self, symbol: str) -> Optional[Dict[str, Any]]:
        return self._meta.get(symbol)

    def get_all_meta(self) -> Dict[str, Dict[str, Any]]:
        return self._meta.copy()
