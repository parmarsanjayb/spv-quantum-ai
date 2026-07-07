import time
from datetime import datetime, timezone, time as dt_time
from zoneinfo import ZoneInfo
from typing import Dict, Any, Tuple, Optional
from core.logging import get_logger
from brokers import broker_engine
from brokers.manager import broker_manager

logger = get_logger("trading_guard")

# NSE market hours (09:15-15:30) are always defined in India Standard Time,
# regardless of what timezone the server/container happens to run in.
IST = ZoneInfo("Asia/Kolkata")

class TradingGuard:
    """Evaluates various pre-trade risk controls and safety gates."""
    def __init__(self, config: Dict[str, Any]) -> None:
        self.config = config
        self.last_trade_time: Dict[str, float] = {}  # symbol -> epoch time
        self.last_global_trade_time: float = 0.0
        self.recent_trades: list = []  # List of (timestamp, symbol, side, qty)

        # streak trackers
        self.consecutive_losses = 0
        self.consecutive_wins = 0

    def _is_paper_broker_active(self) -> bool:
        """Real-exchange-hours guards (session/holiday/market-closing) model NSE's
        actual trading calendar, which is meaningless for the paper broker's mock
        feed - paper trading is meant to be exercised anytime, not just 09:15-15:30
        IST on NSE trading days. Live/real brokers remain fully gated."""
        try:
            return broker_manager.get_active().name == "paper_broker"
        except Exception:
            return False

    async def check_all(self, order_data: Dict[str, Any]) -> Tuple[bool, str]:
        """Runs all safety check functions in sequence."""
        checks = [
            self.check_employee_permission,
            self.check_session,
            self.check_holiday,
            self.check_market_closing,
            self.check_broker_connection,
            self.check_cooldown,
            self.check_duplicate_trade,
            self.check_duplicate_symbol,
            self.check_daily_limits,
            self.check_streaks,
            self.check_exposure_limits,
        ]

        for check_fn in checks:
            allowed, reason = await check_fn(order_data)
            if not allowed:
                return False, reason

        return True, "Passed all safety checks."

    async def check_session(self, order_data: Dict[str, Any]) -> Tuple[bool, str]:
        if not self.config.get("trading_session_guard", True) or self._is_paper_broker_active():
            return True, ""
        now = datetime.now(timezone.utc).astimezone(IST).time()
        start = dt_time(9, 15)
        end = dt_time(15, 30)
        if now < start or now > end:
            return False, f"Trading session guard: current time {now.strftime('%H:%M:%S')} is outside allowed window (09:15 - 15:30)."
        return True, ""

    async def check_holiday(self, order_data: Dict[str, Any]) -> Tuple[bool, str]:
        if not self.config.get("holiday_guard", True) or self._is_paper_broker_active():
            return True, ""
        # Simulate simple holiday schedule (Saturday, Sunday)
        today = datetime.now(timezone.utc).astimezone(IST).weekday()
        if today >= 5:  # 5 is Saturday, 6 is Sunday
            return False, "Holiday guard: trading is closed on weekends."
        return True, ""

    async def check_market_closing(self, order_data: Dict[str, Any]) -> Tuple[bool, str]:
        if not self.config.get("market_closing_guard", True) or self._is_paper_broker_active():
            return True, ""
        now = datetime.now(timezone.utc).astimezone(IST)
        # Market closes at 15:30. Check if we are within 15 minutes of close
        if now.hour == 15 and now.minute >= 15:
            return False, "Market closing guard: new entry blocked within 15 minutes of market close."
        return True, ""

    async def check_broker_connection(self, order_data: Dict[str, Any]) -> Tuple[bool, str]:
        if not self.config.get("broker_disconnect_guard", True):
            return True, ""
        if not broker_engine.is_connected():
            return False, "Broker disconnect guard: active broker session is offline."
        return True, ""

    async def check_cooldown(self, order_data: Dict[str, Any]) -> Tuple[bool, str]:
        cooldown_sec = float(self.config.get("cooldown_between_trades_sec", 10.0))
        now = time.time()
        elapsed = now - self.last_global_trade_time
        if elapsed < cooldown_sec:
            return False, f"Cooldown guard: must wait {cooldown_sec - elapsed:.1f}s before next execution."
        return True, ""

    async def check_duplicate_trade(self, order_data: Dict[str, Any]) -> Tuple[bool, str]:
        symbol = order_data.get("symbol")
        side = order_data.get("side")
        qty = float(order_data.get("quantity", 0))
        now = time.time()

        # Check for same symbol/side/qty in the last 5 seconds
        for ts, sym, sd, q in list(self.recent_trades):
            if now - ts > 5.0:
                self.recent_trades.remove((ts, sym, sd, q))
                continue
            if sym == symbol and sd == side and q == qty:
                return False, f"Duplicate trade protection: identical order for {symbol} placed within 5 seconds."
        return True, ""

    async def check_duplicate_symbol(self, order_data: Dict[str, Any]) -> Tuple[bool, str]:
        if not self.config.get("duplicate_symbol_protection", True):
            return True, ""
        from portfolio.engine import portfolio_engine
        symbol = order_data.get("symbol")
        positions = await portfolio_engine.positions.get_all_positions()
        pos = next((p for p in positions if p.symbol == symbol), None)
        if pos and pos.quantity != 0:
            return False, f"Duplicate symbol protection: active position in {symbol} already exists."
        return True, ""

    async def check_daily_limits(self, order_data: Dict[str, Any]) -> Tuple[bool, str]:
        from portfolio.engine import portfolio_engine
        # 1. Daily Loss Guard
        daily_loss_limit = float(self.config.get("daily_loss_guard_usd", 500.0))
        daily_pnl = portfolio_engine.summary.realized_pnl
        if daily_pnl <= -daily_loss_limit:
            return False, f"Daily loss guard: realized loss ${daily_pnl:.2f} meets/exceeds limit ${daily_loss_limit:.2f}."

        # 2. Daily Profit Lock
        daily_profit_lock = float(self.config.get("daily_profit_lock_usd", 2000.0))
        if daily_pnl >= daily_profit_lock:
            return False, f"Daily profit lock: realized profit ${daily_pnl:.2f} meets/exceeds lock target ${daily_profit_lock:.2f}."
        return True, ""

    async def check_streaks(self, order_data: Dict[str, Any]) -> Tuple[bool, str]:
        max_losses = int(self.config.get("max_consecutive_losses", 4))
        if self.consecutive_losses >= max_losses:
            return False, f"Streak guard: blocked after {self.consecutive_losses} consecutive losses."

        max_wins = int(self.config.get("max_consecutive_wins", 8))
        if self.consecutive_wins >= max_wins:
            return False, f"Streak guard: optional pause after {self.consecutive_wins} consecutive wins."
        return True, ""

    async def check_exposure_limits(self, order_data: Dict[str, Any]) -> Tuple[bool, str]:
        from portfolio.engine import portfolio_engine
        # 1. Max open positions
        max_pos = int(self.config.get("max_open_positions_guard", 5))
        active_positions = [p for p in await portfolio_engine.positions.get_all_positions() if p.quantity != 0]
        if len(active_positions) >= max_pos:
            return False, f"Exposure guard: max open positions count limit {max_pos} reached."

        # 2. Max exposure value
        symbol = order_data.get("symbol")
        qty = float(order_data.get("quantity", 0))
        price = float(order_data.get("price") or 100.0)
        order_exposure = qty * price

        total_exposure = sum(abs(p.quantity * p.avg_price) for p in active_positions)
        max_exposure = float(self.config.get("max_exposure_usd", 50000.0))
        if total_exposure + order_exposure > max_exposure:
            return False, f"Exposure guard: order would push total exposure (${total_exposure + order_exposure:.2f}) beyond limit (${max_exposure:.2f})."
        return True, ""

    def record_execution(self, symbol: str, side: str, qty: float, pnl: float) -> None:
        now = time.time()
        self.last_global_trade_time = now
        self.last_trade_time[symbol] = now
        self.recent_trades.append((now, symbol, side, qty))

        if pnl < 0:
            self.consecutive_losses += 1
            self.consecutive_wins = 0
        elif pnl > 0:
            self.consecutive_wins += 1
            self.consecutive_losses = 0

    async def check_employee_permission(self, order_data: Dict[str, Any]) -> Tuple[bool, str]:
        from employees import employee_engine
        return await employee_engine.check_allowed_order(order_data)
