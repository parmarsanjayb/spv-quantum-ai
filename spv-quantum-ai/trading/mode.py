from typing import Any, Dict, List, Optional
from core.logging import get_logger

logger = get_logger("trading_mode")


class TradingModeManager:
    """
    Gates whether an APPROVED decision auto-executes (AUTO, the existing
    default behavior) or waits for the user to explicitly confirm/reject it
    (MANUAL) before an order_request is ever published. Defaults to AUTO so
    existing behavior is unchanged unless a user turns MANUAL on.
    """

    def __init__(self) -> None:
        self._mode: str = "AUTO"
        self._pending: Dict[str, Dict[str, Any]] = {}

    def get_mode(self) -> str:
        return self._mode

    def set_mode(self, mode: str) -> None:
        mode = mode.upper()
        if mode not in ("AUTO", "MANUAL"):
            raise ValueError(f"Invalid trading mode: {mode}")
        self._mode = mode
        logger.info(f"Trading mode set to {mode}")

    def hold_for_confirmation(self, record: Dict[str, Any]) -> str:
        decision_id = record["decision_id"]
        self._pending[decision_id] = record
        return decision_id

    def get_pending(self) -> List[Dict[str, Any]]:
        return list(self._pending.values())

    def pop_pending(self, decision_id: str) -> Optional[Dict[str, Any]]:
        return self._pending.pop(decision_id, None)


# Singleton
trading_mode_manager = TradingModeManager()
