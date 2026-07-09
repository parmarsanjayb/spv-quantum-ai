import asyncio
import math
import time
from typing import Any, Dict, Optional
from datetime import datetime, timezone
from core.config import settings
from core.bus import event_bus, EventModel
from core.logging import get_logger
from risk.models import (
    RiskStatus, RiskResponse, RiskApprovedEvent, RiskRejectedEvent,
    DrawdownAlertEvent, DailyLossLimitEvent, PositionSizeAdjustedEvent
)
from risk.sizing import PositionSizingEngine
from risk.managers import (
    CapitalManager, DrawdownManager, ExposureManager,
    DailyLossManager, TradeLimitManager, PortfolioRiskManager
)

logger = get_logger("risk_engine")

class RiskEngine:
    """
    Enterprise Risk Management Engine.
    Has absolute authority to ALLOW, BLOCK, or REDUCE_POSITION sizes for any trade.
    Does not decide BUY or SELL.
    """
    def __init__(self) -> None:
        self.config = settings.yaml_config.get("risk_limits", {})
        
        # Load sub-managers
        self.capital_mgr = CapitalManager(self.config)
        self.drawdown_mgr = DrawdownManager(self.config)
        self.exposure_mgr = ExposureManager(self.config)
        self.daily_loss_mgr = DailyLossManager(self.config)
        self.limit_mgr = TradeLimitManager(self.config)
        self.portfolio_mgr = PortfolioRiskManager(self.config)
        self.sizer = PositionSizingEngine(
            default_strategy=self.config.get("position_sizing_strategy", "fixed_quantity"),
            default_params=self.config.get("position_sizing_params", {})
        )
        
        self.max_position_size_usd = float(self.config.get("max_position_size_usd", 10000.0))
        self.max_risk_per_trade_usd = float(self.config.get("max_risk_per_trade_usd", 1000.0))
        self._running = False

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        # Subscribe to portfolio updates and trade fills to update managers
        await event_bus.subscribe("portfolio_update", self._handle_portfolio_update)
        await event_bus.subscribe("order_filled", self._handle_order_filled)
        logger.info("RiskEngine started and subscribed to events.")

    async def stop(self) -> None:
        self._running = False
        await event_bus.unsubscribe("portfolio_update", self._handle_portfolio_update)
        await event_bus.unsubscribe("order_filled", self._handle_order_filled)
        logger.info("RiskEngine stopped.")

    async def validate_order(self, order_data: Dict[str, Any]) -> RiskResponse:
        """
        Validates if an order is ALLOWED, BLOCKED, or needs to be REDUCE_POSITION.
        """
        symbol = order_data.get("symbol", "UNKNOWN")
        quantity = float(order_data.get("quantity", 0.0))
        price = float(order_data.get("price") or order_data.get("ltp") or 1.0)
        estimated_cost = quantity * price
        
        # Initial sizing recommendation
        sizing_strategy = order_data.get("sizing_strategy") or self.config.get("position_sizing_strategy", "fixed_quantity")
        sizing_params = order_data.get("sizing_params") or self.config.get("position_sizing_params", {})
        
        capital_info = await self.capital_mgr.get_capital_info()
        rec_size = self.sizer.calculate_size(
            strategy=sizing_strategy,
            params=sizing_params,
            capital_available=capital_info["equity"],
            entry_price=price,
            atr=order_data.get("atr"),
            volatility=order_data.get("volatility")
        )
        
        # 1. Evaluate Trade Limits & Cooldowns
        limit_allowed, limit_reason = await self.limit_mgr.validate_trade_limits()
        if not limit_allowed:
            return await self._block_response(order_data, limit_reason, rec_size)

        # 2. Evaluate Drawdown limits
        dd_allowed, dd_percent = await self.drawdown_mgr.check_drawdown(capital_info["equity"])
        if not dd_allowed:
            # Emit Drawdown alert
            alert = DrawdownAlertEvent(
                current_drawdown=dd_percent,
                max_drawdown_limit=self.drawdown_mgr.max_drawdown_pct,
                message=f"Drawdown {dd_percent:.2f}% meets/exceeds limit {self.drawdown_mgr.max_drawdown_pct}%"
            )
            await event_bus.publish(EventModel(
                event_type="drawdown_alert",
                source_agent="risk_engine",
                payload=alert.model_dump()
            ))
            return await self._block_response(order_data, alert.message, rec_size)

        # 3. Evaluate Daily/Weekly Loss limits
        loss_allowed, loss_reason, daily_pnl, weekly_pnl = await self.daily_loss_mgr.validate_limits()
        if not loss_allowed:
            # Emit Loss limit alert
            alert = DailyLossLimitEvent(
                daily_loss=daily_pnl,
                daily_loss_limit=self.daily_loss_mgr.daily_loss_limit,
                message=loss_reason
            )
            await event_bus.publish(EventModel(
                event_type="daily_loss_limit_reached",
                source_agent="risk_engine",
                payload=alert.model_dump()
            ))
            return await self._block_response(order_data, loss_reason, rec_size)

        # 4. Evaluate Portfolio Risk limits
        port_allowed, port_reason = await self.portfolio_mgr.validate_portfolio_risk()
        if not port_allowed:
            return await self._block_response(order_data, port_reason, rec_size)

        # 5. Check Margin availability
        margin_allowed = await self.capital_mgr.validate_margin(estimated_cost)
        if not margin_allowed:
            return await self._block_response(
                order_data, 
                f"Insufficient available margin for trade cost {estimated_cost:.2f}. Available: {capital_info['available_margin']:.2f}", 
                rec_size
            )

        # 6. Evaluate position/exposure limits
        exp_allowed, exp_reason = await self.exposure_mgr.evaluate_exposure(symbol, estimated_cost)
        if not exp_allowed:
            return await self._block_response(order_data, exp_reason, rec_size)

        # 7. Check Max Risk Per Trade & Max Position Size limits
        # If requested size exceeds max allowed size, reduce position size
        final_size = quantity
        status = RiskStatus.ALLOW
        reason = "Passes all risk parameters."
        
        # Max cost check
        max_cost_limit = self.max_position_size_usd
        if estimated_cost > max_cost_limit:
            # Equity trades on real exchanges only in whole shares — floor, never
            # leave a fractional remainder like "5.599626691553897" in an order.
            max_qty_allowed = math.floor(max_cost_limit / price)
            if max_qty_allowed < 1:
                return await self._block_response(
                    order_data,
                    f"Even 1 share of {symbol} at {price} exceeds max position size limit ${max_cost_limit}",
                    rec_size,
                )
            if self.config.get("allow_partial_size_adjustment", True):
                final_size = max_qty_allowed
                status = RiskStatus.REDUCE_POSITION
                reason = f"Reduced position size to stay within max position size cost limit ${max_cost_limit}"
                # Emit adjustment event
                adj = PositionSizeAdjustedEvent(
                    original_size=quantity,
                    adjusted_size=final_size,
                    reason=reason
                )
                await event_bus.publish(EventModel(
                    event_type="position_size_adjusted",
                    source_agent="risk_engine",
                    payload=adj.model_dump()
                ))
            else:
                return await self._block_response(order_data, f"Order cost ${estimated_cost:.2f} exceeds position size limit ${max_cost_limit}", rec_size)

        # Compute dynamic risk score
        risk_score = self._calculate_risk_score(dd_percent, capital_info, estimated_cost)

        response = RiskResponse(
            risk_status=status,
            allowed=True,
            reason=reason,
            risk_score=risk_score,
            recommended_position_size=final_size,
            recommended_max_loss=self.max_risk_per_trade_usd
        )

        # Emit Approved Event
        approved_evt = RiskApprovedEvent(
            order_details=order_data,
            risk_response=response
        )
        await event_bus.publish(EventModel(
            event_type="risk_approved",
            source_agent="risk_engine",
            payload=approved_evt.model_dump()
        ))

        return response

    async def _block_response(self, order_data: Dict[str, Any], reason: str, rec_size: float) -> RiskResponse:
        response = RiskResponse(
            risk_status=RiskStatus.BLOCK,
            allowed=False,
            reason=reason,
            risk_score=100.0,
            recommended_position_size=0.0,
            recommended_max_loss=0.0
        )
        rejected_evt = RiskRejectedEvent(
            order_details=order_data,
            risk_response=response
        )
        await event_bus.publish(EventModel(
            event_type="risk_rejected",
            source_agent="risk_engine",
            payload=rejected_evt.model_dump()
        ))
        return response

    def _calculate_risk_score(self, drawdown_pct: float, capital_info: Dict[str, float], trade_cost: float) -> float:
        # Scale risk score based on current drawdown proximity to max, and trade cost
        dd_factor = (drawdown_pct / self.drawdown_mgr.max_drawdown_pct) * 50.0 if self.drawdown_mgr.max_drawdown_pct > 0 else 0.0
        cost_factor = (trade_cost / self.max_position_size_usd) * 50.0 if self.max_position_size_usd > 0 else 0.0
        return min(100.0, max(0.0, dd_factor + cost_factor))

    # ── Event handlers ────────────────────────────────────────────────────────

    async def _handle_portfolio_update(self, event: EventModel) -> None:
        """Handles external balance/performance metric updates to keep Drawdown and DailyLoss up to date."""
        try:
            payload = event.payload
            equity = float(payload.get("equity", 0.0))
            realized_pnl = float(payload.get("realized_pnl", 0.0))
            if equity > 0:
                await self.drawdown_mgr.check_drawdown(equity)
            if realized_pnl != 0.0:
                await self.daily_loss_mgr.update_pnl(realized_pnl)
        except Exception as e:
            logger.error("Failed to handle portfolio update in risk engine", error=str(e))

    async def _handle_order_filled(self, event: EventModel) -> None:
        """Called when an order fills. Used to increment trade count & check loss streak."""
        try:
            order = event.payload
            # We can calculate simulated P&L from fill or wait for trade executions
            # Let's check if the filled order is Buy or Sell
            # For simplicity, if we get trade updates we increment daily counts
            pnl = float(order.get("pnl", 0.0))
            await self.limit_mgr.record_trade_execution(pnl)
        except Exception as e:
            logger.error("Failed to handle order filled in risk engine", error=str(e))

    async def get_dashboard_metrics(self) -> Dict[str, Any]:
        capital = await self.capital_mgr.get_capital_info()
        allowed_dd, dd_pct = await self.drawdown_mgr.check_drawdown(capital["equity"])
        allowed_loss, loss_reason, daily_pnl, weekly_pnl = await self.daily_loss_mgr.validate_limits()
        
        remaining_loss = max(0.0, self.daily_loss_mgr.daily_loss_limit - abs(daily_pnl)) if daily_pnl < 0 else self.daily_loss_mgr.daily_loss_limit

        return {
            "risk_status": "OPERATIONAL" if (allowed_dd and allowed_loss) else "RESTRICTED",
            "current_drawdown": round(dd_pct, 2),
            "current_exposure": 0.0,  # Dynamically queried by agents/exposure if needed
            "current_capital": round(capital["equity"], 2),
            "todays_pnl": round(daily_pnl, 2),
            "todays_risk": round(100.0 - (remaining_loss / self.daily_loss_mgr.daily_loss_limit * 100.0) if self.daily_loss_mgr.daily_loss_limit > 0 else 0.0, 2),
            "remaining_risk": round(remaining_loss, 2),
            "limit_cooldown": self.limit_mgr.cooldown_until.isoformat() if self.limit_mgr.cooldown_until else None
        }

# Singleton instance
risk_engine = RiskEngine()
