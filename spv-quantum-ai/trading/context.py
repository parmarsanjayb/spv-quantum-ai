from typing import Optional
from core.logging import get_logger

logger = get_logger("trading_context")


class TradingContextManager:
    """
    Tracks the single instrument the user is actively working with on the
    simplified Trade tab. Not a subscription filter — the market data engine
    still tracks its full registry — this is purely "what should the home
    screen show me right now."
    """

    def __init__(self) -> None:
        self._active_symbol: Optional[str] = None

    def get_active_symbol(self) -> Optional[str]:
        return self._active_symbol

    def set_active_symbol(self, symbol: str) -> None:
        self._active_symbol = symbol.upper()
        logger.info(f"Active symbol set to {self._active_symbol}")


# Singleton
trading_context_manager = TradingContextManager()
