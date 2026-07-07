import asyncio
from typing import Dict, Any, Optional
from core.config import settings
from core.bus import event_bus, EventModel
from core.logging import get_logger
from safety.models import SafetyResponse, SafetyStatus
from safety.manager import SafetyManager

logger = get_logger("safety_engine")

class SafetyEngine:
    """Enterprise Safety & Capital Protection Engine."""
    def __init__(self) -> None:
        self.config = settings.yaml_config.get("safety_limits", {})
        # If safety_limits block is empty in config, load sensible defaults
        if not self.config:
            self.config = {
                "trading_session_guard": True,
                "holiday_guard": True,
                "market_closing_guard": True,
                "broker_disconnect_guard": True,
                "cooldown_between_trades_sec": 10.0,
                "duplicate_symbol_protection": True,
                "daily_loss_guard_usd": 500.0,
                "daily_profit_lock_usd": 2000.0,
                "max_consecutive_losses": 4,
                "max_consecutive_wins": 8,
                "max_open_positions_guard": 5,
                "max_exposure_usd": 50000.0,
                "hidden_sl_pct": 2.0,
                "trailing_stop_pct": 1.0,
                "break_even_shift_pct": 1.5,
                "profit_lock_pct": 3.0
            }
        self.manager = SafetyManager(self.config)
        self._running = False

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        await self.manager.start()
        # Subscribe to order filled event for streak updates and hidden stop loss registration
        await event_bus.subscribe("order_filled", self._handle_order_filled)
        logger.info("SafetyEngine started and subscribed to events.")

    async def stop(self) -> None:
        self._running = False
        await self.manager.stop()
        await event_bus.unsubscribe("order_filled", self._handle_order_filled)
        logger.info("SafetyEngine stopped.")

    async def check_order(self, order_data: Dict[str, Any]) -> SafetyResponse:
        """Core safety evaluation method called BEFORE order execution."""
        return await self.manager.evaluate_order(order_data)

    async def _handle_order_filled(self, event: EventModel) -> None:
        try:
            payload = event.payload
            symbol = payload.get("symbol")
            side = payload.get("side", "BUY")
            qty = float(payload.get("filled_quantity", payload.get("quantity", 0.0)))
            avg_price = float(payload.get("avg_price", payload.get("price", 0.0)))
            pnl = float(payload.get("pnl", 0.0))

            # Record execution in trading guard
            self.manager.guard.record_execution(symbol, side, qty, pnl)

            # Query portfolio position to update hidden stop loss registration
            from portfolio.engine import portfolio_engine
            positions = await portfolio_engine.positions.get_all_positions()
            pos = next((p for p in positions if p.symbol == symbol), None)
            if pos and pos.quantity != 0:
                pos_side = "BUY" if pos.quantity > 0 else "SELL"
                self.manager.protection.register_position(symbol, pos_side, pos.quantity, pos.avg_price)
            else:
                self.manager.protection.register_position(symbol, side, 0, avg_price)
        except Exception as e:
            logger.error("Failed to handle order filled in SafetyEngine", error=str(e))

    async def get_dashboard_metrics(self) -> Dict[str, Any]:
        """Provides status details to APIs."""
        from portfolio.engine import portfolio_engine
        daily_pnl = portfolio_engine.summary.realized_pnl
        active_positions = [p for p in await portfolio_engine.positions.get_all_positions() if p.quantity != 0]
        total_exposure = sum(abs(p.quantity * p.avg_price) for p in active_positions)

        daily_loss_limit = float(self.config.get("daily_loss_guard_usd", 500.0))
        remaining_loss = max(0.0, daily_loss_limit - abs(daily_pnl)) if daily_pnl < 0 else daily_loss_limit

        return {
            "safety_status": "RESTRICTED" if (
                self.manager.emergency.kill_switch_active or
                self.manager.emergency.trading_paused or
                daily_pnl <= -daily_loss_limit
            ) else "OPERATIONAL",
            "today_risk": round(100.0 - (remaining_loss / daily_loss_limit * 100.0) if daily_loss_limit > 0 else 0.0, 2),
            "current_exposure": round(total_exposure, 2),
            "emergency_status": {
                "kill_switch_active": self.manager.emergency.kill_switch_active,
                "trading_paused": self.manager.emergency.trading_paused,
                "new_entries_disabled": self.manager.emergency.new_entries_disabled
            },
            "hidden_sl_status": [
                {
                    "symbol": sym,
                    "side": val["side"],
                    "entry_price": val["entry_price"],
                    "stop_loss_price": val["sl_price"]
                }
                for sym, val in self.manager.protection.active_sls.items()
            ],
            "trailing_status": {
                "trailing_stop_pct": self.config.get("trailing_stop_pct", 1.0),
                "break_even_shift_pct": self.config.get("break_even_shift_pct", 1.5),
                "profit_lock_pct": self.config.get("profit_lock_pct", 3.0)
            },
            "daily_limits": {
                "daily_loss_limit_usd": daily_loss_limit,
                "daily_profit_lock_usd": self.config.get("daily_profit_lock_usd", 2000.0),
                "max_exposure_usd": float(self.config.get("max_exposure_usd", 50000.0))
            }
        }

# Singleton instance
safety_engine = SafetyEngine()
