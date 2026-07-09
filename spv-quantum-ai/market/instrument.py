from typing import Any, Dict, List, Optional

class InstrumentManager:
    """
    Manages instrument specifications: tokens, segments, lot sizes, tick sizes.
    Every broker adapter must map its internal tokens to canonical symbol names here.
    """

    def __init__(self) -> None:
        self._instruments: Dict[str, Dict[str, Any]] = {
            # Index — token is the underlying index's pAssetCode from Kotak's scrip master
            "NIFTY50":      {"token": "26000",  "exchange": "NSE", "segment": "nse_cm", "lot_size": 65,   "tick_size": 0.05,   "precision": 2},
            "BANKNIFTY":    {"token": "26009",  "exchange": "NSE", "segment": "nse_cm", "lot_size": 30,   "tick_size": 0.05,   "precision": 2},
            "FINNIFTY":     {"token": "26037",  "exchange": "NSE", "segment": "nse_cm", "lot_size": 60,   "tick_size": 0.05,   "precision": 2},
            "MIDCPNIFTY":   {"token": "26074",  "exchange": "NSE", "segment": "nse_cm", "lot_size": 120,  "tick_size": 0.05,   "precision": 2},
            "SENSEX":       {"token": "1",      "exchange": "BSE", "segment": "bse_cm", "lot_size": 20,   "tick_size": 0.05,   "precision": 2},
            # Equity — verified against Kotak's real scrip master (nse_cm, EQ series)
            "RELIANCE":     {"token": "2885",   "exchange": "NSE", "segment": "nse_cm", "lot_size": 1,    "tick_size": 0.05,   "precision": 2},
            "TCS":          {"token": "11536",  "exchange": "NSE", "segment": "nse_cm", "lot_size": 1,    "tick_size": 0.05,   "precision": 2},
            "HDFCBANK":     {"token": "1333",   "exchange": "NSE", "segment": "nse_cm", "lot_size": 1,    "tick_size": 0.05,   "precision": 2},
            "INFY":         {"token": "1594",   "exchange": "NSE", "segment": "nse_cm", "lot_size": 1,    "tick_size": 0.05,   "precision": 2},
            "ICICIBANK":    {"token": "4963",   "exchange": "NSE", "segment": "nse_cm", "lot_size": 1,    "tick_size": 0.05,   "precision": 2},
            "SBIN":         {"token": "3045",   "exchange": "NSE", "segment": "nse_cm", "lot_size": 1,    "tick_size": 0.05,   "precision": 2},
            "ITC":          {"token": "1660",   "exchange": "NSE", "segment": "nse_cm", "lot_size": 1,    "tick_size": 0.05,   "precision": 2},
            "LT":           {"token": "11483",  "exchange": "NSE", "segment": "nse_cm", "lot_size": 1,    "tick_size": 0.05,   "precision": 2},
            "AXISBANK":     {"token": "5900",   "exchange": "NSE", "segment": "nse_cm", "lot_size": 1,    "tick_size": 0.05,   "precision": 2},
            "KOTAKBANK":    {"token": "1922",   "exchange": "NSE", "segment": "nse_cm", "lot_size": 1,    "tick_size": 0.05,   "precision": 2},
            "HINDUNILVR":   {"token": "1394",   "exchange": "NSE", "segment": "nse_cm", "lot_size": 1,    "tick_size": 0.05,   "precision": 2},
            "BHARTIARTL":   {"token": "10604",  "exchange": "NSE", "segment": "nse_cm", "lot_size": 1,    "tick_size": 0.05,   "precision": 2},
            "BAJFINANCE":   {"token": "317",    "exchange": "NSE", "segment": "nse_cm", "lot_size": 1,    "tick_size": 0.05,   "precision": 2},
            "MARUTI":       {"token": "10999",  "exchange": "NSE", "segment": "nse_cm", "lot_size": 1,    "tick_size": 0.05,   "precision": 2},
            "WIPRO":        {"token": "3787",   "exchange": "NSE", "segment": "nse_cm", "lot_size": 1,    "tick_size": 0.05,   "precision": 2},
            # Currency
            "USDINR":       {"token": "usdinr", "exchange": "CDS", "segment": "cd_fo",  "lot_size": 1000, "tick_size": 0.0025, "precision": 4},
            # Commodity — nearest-expiry MCX futures contract, verified against Kotak's
            # real scrip master (the earlier placeholder string tokens were never real).
            "CRUDEOIL":     {"token": "520702", "exchange": "MCX", "segment": "mcx_fo", "lot_size": 100,  "tick_size": 1.0,    "precision": 2},
            "NATURALGAS":   {"token": "538685", "exchange": "MCX", "segment": "mcx_fo", "lot_size": 1250, "tick_size": 0.1,    "precision": 2},
            "GOLD":         {"token": "466583", "exchange": "MCX", "segment": "mcx_fo", "lot_size": 1,    "tick_size": 1.0,    "precision": 2},
            "SILVER":       {"token": "471725", "exchange": "MCX", "segment": "mcx_fo", "lot_size": 30,   "tick_size": 1.0,    "precision": 2},
            "COPPER":       {"token": "562048", "exchange": "MCX", "segment": "mcx_fo", "lot_size": 2500, "tick_size": 0.05,   "precision": 2},
            "ZINC":         {"token": "562053", "exchange": "MCX", "segment": "mcx_fo", "lot_size": 5,    "tick_size": 0.05,   "precision": 2},
            "ALUMINIUM":    {"token": "562047", "exchange": "MCX", "segment": "mcx_fo", "lot_size": 5,    "tick_size": 0.05,   "precision": 2},
            "LEAD":         {"token": "562049", "exchange": "MCX", "segment": "mcx_fo", "lot_size": 5,    "tick_size": 0.05,   "precision": 2},
            "NICKEL":       {"token": "562051", "exchange": "MCX", "segment": "mcx_fo", "lot_size": 250,  "tick_size": 0.1,    "precision": 2},
            # Crypto — not carried by Kotak Neo; no live feed for these.
            "BTCUSD":       {"token": "btc",    "exchange": "CRYPTO", "segment": "spot", "lot_size": 1,   "tick_size": 0.01,   "precision": 2},
            "ETHUSD":       {"token": "eth",    "exchange": "CRYPTO", "segment": "spot", "lot_size": 1,   "tick_size": 0.01,   "precision": 2},
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
