import asyncio
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
from core.bus import event_bus, EventModel
from core.logging import get_logger
from market.models import Candle

# Import stateless math helpers
from indicators.math import calc_rsi, calc_vwap, calc_adx

logger = get_logger("new_specialists")

class BaseNewSpecialist:
    """Base class for new specialists to inherit standard heartbeat and lifecycle control."""
    def __init__(self, employee_code: str, default_decision: str = "Neutral") -> None:
        self.employee_code = employee_code
        self.default_decision = default_decision
        self.latest_results: Dict[str, Dict[str, Any]] = {}
        self._running = False
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        logger.info(f"{self.__class__.__name__} ({self.employee_code}) started.")

    async def stop(self) -> None:
        self._running = False
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
            self._heartbeat_task = None
        logger.info(f"{self.__class__.__name__} ({self.employee_code}) stopped.")

    async def _heartbeat_loop(self) -> None:
        while self._running:
            try:
                from employees.engine import employee_engine
                decision_str = self.default_decision
                score = 50.0
                if self.latest_results:
                    first_res = next(iter(self.latest_results.values()))
                    decision_str = first_res.get('recommendation', self.default_decision)
                    score = first_res.get("confidence", 50.0)
                
                await employee_engine.manager.record_activity(
                    employee_code=self.employee_code,
                    decision=decision_str,
                    confidence=score,
                    execution_time_ms=0.0
                )
            except Exception as e:
                logger.error(f"Heartbeat fail in {self.__class__.__name__}", error=str(e))
            await asyncio.sleep(5)


# ── Market Intelligence Department ──────────────────────────────────────────

class MomentumEmployee(BaseNewSpecialist):
    def __init__(self) -> None:
        super().__init__("EMP-MOM", "WAIT")
        self.candles_history: Dict[str, List[float]] = {}

    async def start(self) -> None:
        await super().start()
        await event_bus.subscribe("candle", self._on_candle)

    async def stop(self) -> None:
        await super().stop()
        await event_bus.unsubscribe("candle", self._on_candle)

    async def _on_candle(self, event: EventModel) -> None:
        try:
            payload = event.payload
            raw_candle = payload.get("candle", payload)
            symbol = raw_candle.get("symbol", "NIFTY50")
            close = float(raw_candle.get("close", 0.0))
            complete = raw_candle.get("complete", False)
            if not complete:
                return

            if symbol not in self.candles_history:
                self.candles_history[symbol] = []
            self.candles_history[symbol].append(close)
            if len(self.candles_history[symbol]) > 50:
                self.candles_history[symbol].pop(0)

            closes = self.candles_history[symbol]
            rsi = 50.0
            if len(closes) >= 15:
                rsi = calc_rsi(closes, 14)

            rec = "WAIT"
            if rsi > 65:
                rec = "BUY"
            elif rsi < 35:
                rec = "SELL"

            async with self._lock:
                self.latest_results[symbol] = {
                    "recommendation": rec,
                    "confidence": float(50.0 + abs(rsi - 50.0) * 2.0),
                    "rsi": rsi,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
        except Exception as e:
            logger.error("Error in MomentumEmployee _on_candle", error=str(e))


class VWAPEmployee(BaseNewSpecialist):
    def __init__(self) -> None:
        super().__init__("EMP-VWP", "WAIT")
        self.candles_history: Dict[str, Dict[str, List[float]]] = {}

    async def start(self) -> None:
        await super().start()
        await event_bus.subscribe("candle", self._on_candle)

    async def stop(self) -> None:
        await super().stop()
        await event_bus.unsubscribe("candle", self._on_candle)

    async def _on_candle(self, event: EventModel) -> None:
        try:
            payload = event.payload
            raw_candle = payload.get("candle", payload)
            symbol = raw_candle.get("symbol", "NIFTY50")
            high = float(raw_candle.get("high", 0.0))
            low = float(raw_candle.get("low", 0.0))
            close = float(raw_candle.get("close", 0.0))
            volume = float(raw_candle.get("volume", 0.0))
            complete = raw_candle.get("complete", False)
            if not complete:
                return

            if symbol not in self.candles_history:
                self.candles_history[symbol] = {"highs": [], "lows": [], "closes": [], "volumes": []}

            hist = self.candles_history[symbol]
            hist["highs"].append(high)
            hist["lows"].append(low)
            hist["closes"].append(close)
            hist["volumes"].append(volume)

            if len(hist["closes"]) > 100:
                hist["highs"].pop(0)
                hist["lows"].pop(0)
                hist["closes"].pop(0)
                hist["volumes"].pop(0)

            vwap = calc_vwap(hist["highs"], hist["lows"], hist["closes"], hist["volumes"])
            rec = "WAIT"
            if close > vwap:
                rec = "BUY"
            elif close < vwap:
                rec = "SELL"

            async with self._lock:
                self.latest_results[symbol] = {
                    "recommendation": rec,
                    "confidence": 60.0 if close != vwap else 50.0,
                    "vwap": vwap,
                    "close": close,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
        except Exception as e:
            logger.error("Error in VWAPEmployee _on_candle", error=str(e))


class MarketRegimeEmployee(BaseNewSpecialist):
    def __init__(self) -> None:
        super().__init__("EMP-RGM", "Sideways")

    async def start(self) -> None:
        await super().start()
        # RegimeEngine publishes "market_regime", not "regime_changed".
        await event_bus.subscribe("market_regime", self._on_regime)

    async def stop(self) -> None:
        await super().stop()
        await event_bus.unsubscribe("market_regime", self._on_regime)

    async def _on_regime(self, event: EventModel) -> None:
        try:
            payload = event.payload
            regime = payload.get("market_regime", "Sideways")
            confidence = payload.get("confidence", 50.0)
            symbol = payload.get("symbol", "NIFTY50")
            
            async with self._lock:
                self.latest_results[symbol] = {
                    "recommendation": regime,
                    "confidence": confidence,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
        except Exception as e:
            logger.error("Error in MarketRegimeEmployee _on_regime", error=str(e))


# ── Options Intelligence Department ─────────────────────────────────────────

class OIEmployee(BaseNewSpecialist):
    def __init__(self) -> None:
        super().__init__("EMP-OIE", "WAIT")

    async def start(self) -> None:
        await super().start()
        await event_bus.subscribe("option_chain_updated", self._on_chain)

    async def stop(self) -> None:
        await super().stop()
        await event_bus.unsubscribe("option_chain_updated", self._on_chain)

    async def _on_chain(self, event: EventModel) -> None:
        try:
            payload = event.payload
            symbol = payload.get("underlying_symbol", "NIFTY")
            ce_oi = sum(c.get("open_interest", 0) for c in payload.get("calls", []))
            pe_oi = sum(c.get("open_interest", 0) for c in payload.get("puts", []))
            
            rec = "WAIT"
            if pe_oi > ce_oi * 1.1:
                rec = "BUY"
            elif ce_oi > pe_oi * 1.1:
                rec = "SELL"
                
            async with self._lock:
                self.latest_results[symbol] = {
                    "recommendation": rec,
                    "confidence": float(min(100.0, 50.0 + abs(pe_oi - ce_oi) / max(1.0, ce_oi + pe_oi) * 100.0)),
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
        except Exception as e:
            logger.error("Error in OIEmployee _on_chain", error=str(e))


class PCREmployee(BaseNewSpecialist):
    def __init__(self) -> None:
        super().__init__("EMP-PCR", "WAIT")

    async def start(self) -> None:
        await super().start()
        await event_bus.subscribe("option_chain_updated", self._on_chain)

    async def stop(self) -> None:
        await super().stop()
        await event_bus.unsubscribe("option_chain_updated", self._on_chain)

    async def _on_chain(self, event: EventModel) -> None:
        try:
            payload = event.payload
            symbol = payload.get("underlying_symbol", "NIFTY")
            pcr = float(payload.get("pcr", 1.0))
            
            rec = "WAIT"
            if pcr > 1.2:
                rec = "BUY"
            elif pcr < 0.8:
                rec = "SELL"
                
            async with self._lock:
                self.latest_results[symbol] = {
                    "recommendation": rec,
                    "confidence": float(min(100.0, 50.0 + abs(pcr - 1.0) * 50.0)),
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
        except Exception as e:
            logger.error("Error in PCREmployee _on_chain", error=str(e))


class GreeksEmployee(BaseNewSpecialist):
    def __init__(self) -> None:
        super().__init__("EMP-GRK", "WAIT")

    async def start(self) -> None:
        await super().start()
        await event_bus.subscribe("option_chain_updated", self._on_chain)

    async def stop(self) -> None:
        await super().stop()
        await event_bus.unsubscribe("option_chain_updated", self._on_chain)

    async def _on_chain(self, event: EventModel) -> None:
        try:
            payload = event.payload
            symbol = payload.get("underlying_symbol", "NIFTY")
            # Calculate mock net Greeks (Delta bias)
            calls = payload.get("calls", [])
            puts = payload.get("puts", [])
            delta_bias = len(calls) - len(puts)
            
            rec = "WAIT"
            if delta_bias > 2:
                rec = "BUY"
            elif delta_bias < -2:
                rec = "SELL"
                
            async with self._lock:
                self.latest_results[symbol] = {
                    "recommendation": rec,
                    "confidence": 65.0,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
        except Exception as e:
            logger.error("Error in GreeksEmployee _on_chain", error=str(e))


class MaxPainEmployee(BaseNewSpecialist):
    def __init__(self) -> None:
        super().__init__("EMP-MPN", "WAIT")

    async def start(self) -> None:
        await super().start()
        await event_bus.subscribe("option_chain_updated", self._on_chain)

    async def stop(self) -> None:
        await super().stop()
        await event_bus.unsubscribe("option_chain_updated", self._on_chain)

    async def _on_chain(self, event: EventModel) -> None:
        try:
            payload = event.payload
            symbol = payload.get("underlying_symbol", "NIFTY")
            atm_strike = float(payload.get("atm_strike", 0.0))
            max_pain = float(payload.get("max_pain", atm_strike))
            
            rec = "WAIT"
            if atm_strike < max_pain:
                rec = "BUY"
            elif atm_strike > max_pain:
                rec = "SELL"
                
            async with self._lock:
                self.latest_results[symbol] = {
                    "recommendation": rec,
                    "confidence": 60.0,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
        except Exception as e:
            logger.error("Error in MaxPainEmployee _on_chain", error=str(e))


# ── Institutional Department ────────────────────────────────────────────────

class SmartMoneyEmployee(BaseNewSpecialist):
    def __init__(self) -> None:
        super().__init__("EMP-SME", "WAIT")

    async def start(self) -> None:
        await super().start()
        await event_bus.subscribe("candle", self._on_candle)

    async def stop(self) -> None:
        await super().stop()
        await event_bus.unsubscribe("candle", self._on_candle)

    async def _on_candle(self, event: EventModel) -> None:
        try:
            payload = event.payload
            raw_candle = payload.get("candle", payload)
            symbol = raw_candle.get("symbol", "NIFTY50")
            volume = float(raw_candle.get("volume", 0.0))
            close = float(raw_candle.get("close", 0.0))
            open_p = float(raw_candle.get("open", 0.0))
            complete = raw_candle.get("complete", False)
            if not complete:
                return

            # Smart money block: extreme volume with bullish/bearish candle
            rec = "WAIT"
            if volume > 50000:
                rec = "BUY" if close > open_p else "SELL"

            async with self._lock:
                self.latest_results[symbol] = {
                    "recommendation": rec,
                    "confidence": 75.0 if rec != "WAIT" else 50.0,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
        except Exception as e:
            logger.error("Error in SmartMoneyEmployee _on_candle", error=str(e))


class LiquidityEmployee(BaseNewSpecialist):
    def __init__(self) -> None:
        super().__init__("EMP-LQD", "WAIT")

    async def start(self) -> None:
        await super().start()
        await event_bus.subscribe("candle", self._on_candle)

    async def stop(self) -> None:
        await super().stop()
        await event_bus.unsubscribe("candle", self._on_candle)

    async def _on_candle(self, event: EventModel) -> None:
        try:
            payload = event.payload
            raw_candle = payload.get("candle", payload)
            symbol = raw_candle.get("symbol", "NIFTY50")
            complete = raw_candle.get("complete", False)
            if not complete:
                return

            # High volume implies higher liquidity
            volume = float(raw_candle.get("volume", 0.0))
            rec = "WAIT"
            if volume > 10000:
                rec = "BUY"

            async with self._lock:
                self.latest_results[symbol] = {
                    "recommendation": rec,
                    "confidence": 60.0,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
        except Exception as e:
            logger.error("Error in LiquidityEmployee _on_candle", error=str(e))


class OrderFlowEmployee(BaseNewSpecialist):
    def __init__(self) -> None:
        super().__init__("EMP-OFL", "WAIT")

    async def start(self) -> None:
        await super().start()
        await event_bus.subscribe("candle", self._on_candle)

    async def stop(self) -> None:
        await super().stop()
        await event_bus.unsubscribe("candle", self._on_candle)

    async def _on_candle(self, event: EventModel) -> None:
        try:
            payload = event.payload
            raw_candle = payload.get("candle", payload)
            symbol = raw_candle.get("symbol", "NIFTY50")
            complete = raw_candle.get("complete", False)
            if not complete:
                return

            close = float(raw_candle.get("close", 0.0))
            open_p = float(raw_candle.get("open", 0.0))
            rec = "WAIT"
            if close > open_p * 1.002:
                rec = "BUY"
            elif close < open_p * 0.998:
                rec = "SELL"

            async with self._lock:
                self.latest_results[symbol] = {
                    "recommendation": rec,
                    "confidence": 65.0,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
        except Exception as e:
            logger.error("Error in OrderFlowEmployee _on_candle", error=str(e))


class DeliveryEmployee(BaseNewSpecialist):
    def __init__(self) -> None:
        super().__init__("EMP-DEL", "WAIT")

    async def start(self) -> None:
        await super().start()
        await event_bus.subscribe("candle", self._on_candle)

    async def stop(self) -> None:
        await super().stop()
        await event_bus.unsubscribe("candle", self._on_candle)

    async def _on_candle(self, event: EventModel) -> None:
        try:
            payload = event.payload
            raw_candle = payload.get("candle", payload)
            symbol = raw_candle.get("symbol", "NIFTY50")
            complete = raw_candle.get("complete", False)
            if not complete:
                return

            close = float(raw_candle.get("close", 0.0))
            open_p = float(raw_candle.get("open", 0.0))
            # Delivery investor looks at daily close strength
            rec = "BUY" if close > open_p else "SELL"

            async with self._lock:
                self.latest_results[symbol] = {
                    "recommendation": rec,
                    "confidence": 55.0,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
        except Exception as e:
            logger.error("Error in DeliveryEmployee _on_candle", error=str(e))


# ── Risk Department ──────────────────────────────────────────────────────────

class RiskEmployee(BaseNewSpecialist):
    def __init__(self) -> None:
        super().__init__("EMP-RSK", "WAIT")

    async def start(self) -> None:
        await super().start()
        # "safety_status" is never published anywhere (SafetyEngine.check_order()
        # emits "safety_blocked" / "safety_check_passed" per order via SafetyManager).
        await event_bus.subscribe("safety_blocked", self._on_safety)
        await event_bus.subscribe("safety_check_passed", self._on_safety)

    async def stop(self) -> None:
        await super().stop()
        await event_bus.unsubscribe("safety_blocked", self._on_safety)
        await event_bus.unsubscribe("safety_check_passed", self._on_safety)

    async def _on_safety(self, event: EventModel) -> None:
        try:
            payload = event.payload
            blocked = event.event_type == "safety_blocked"
            symbol = payload.get("order_details", {}).get("symbol", "SYSTEM")
            rec = "WAIT" if blocked else "BUY"

            async with self._lock:
                self.latest_results[symbol] = {
                    "recommendation": rec,
                    "confidence": 90.0,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
        except Exception as e:
            logger.error("Error in RiskEmployee _on_safety", error=str(e))


class PositionSizingEmployee(BaseNewSpecialist):
    def __init__(self) -> None:
        super().__init__("EMP-PZS", "WAIT")

    async def start(self) -> None:
        await super().start()
        await event_bus.subscribe("candle", self._on_candle)

    async def stop(self) -> None:
        await super().stop()
        await event_bus.unsubscribe("candle", self._on_candle)

    async def _on_candle(self, event: EventModel) -> None:
        try:
            payload = event.payload
            raw_candle = payload.get("candle", payload)
            symbol = raw_candle.get("symbol", "NIFTY50")
            # Determine mock sizing recommendations
            async with self._lock:
                self.latest_results[symbol] = {
                    "recommendation": "BUY",
                    "confidence": 70.0,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
        except Exception as e:
            logger.error("Error in PositionSizingEmployee _on_candle", error=str(e))


class CapitalProtectionEmployee(BaseNewSpecialist):
    def __init__(self) -> None:
        super().__init__("EMP-CPT", "WAIT")

    async def start(self) -> None:
        await super().start()
        # PortfolioEngine publishes "pnl_updated", not "pnl_update".
        await event_bus.subscribe("pnl_updated", self._on_pnl)

    async def stop(self) -> None:
        await super().stop()
        await event_bus.unsubscribe("pnl_updated", self._on_pnl)

    async def _on_pnl(self, event: EventModel) -> None:
        try:
            payload = event.payload
            daily_pnl = float(payload.get("realized_pnl", 0.0) + payload.get("unrealized_pnl", 0.0))
            symbol = "SYSTEM"
            # Protect capital if daily drawdown is large
            rec = "WAIT" if daily_pnl < -500.0 else "BUY"
            
            async with self._lock:
                self.latest_results[symbol] = {
                    "recommendation": rec,
                    "confidence": 85.0,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
        except Exception as e:
            logger.error("Error in CapitalProtectionEmployee _on_pnl", error=str(e))


class ExposureEmployee(BaseNewSpecialist):
    def __init__(self) -> None:
        super().__init__("EMP-EXP", "WAIT")

    async def start(self) -> None:
        await super().start()
        await event_bus.subscribe("portfolio_update", self._on_portfolio)

    async def stop(self) -> None:
        await super().stop()
        await event_bus.unsubscribe("portfolio_update", self._on_portfolio)

    async def _on_portfolio(self, event: EventModel) -> None:
        try:
            payload = event.payload
            exposure = float(payload.get("total_exposure", 0.0))
            symbol = "SYSTEM"
            # Restrict new buy if exposure is too high
            rec = "WAIT" if exposure > 50000.0 else "BUY"
            
            async with self._lock:
                self.latest_results[symbol] = {
                    "recommendation": rec,
                    "confidence": 80.0,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
        except Exception as e:
            logger.error("Error in ExposureEmployee _on_portfolio", error=str(e))


# ── News Department ──────────────────────────────────────────────────────────

class NewsEmployee(BaseNewSpecialist):
    def __init__(self) -> None:
        super().__init__("EMP-NWS", "WAIT")

    async def start(self) -> None:
        await super().start()
        await event_bus.subscribe("candle", self._on_candle)

    async def stop(self) -> None:
        await super().stop()
        await event_bus.unsubscribe("candle", self._on_candle)

    async def _on_candle(self, event: EventModel) -> None:
        try:
            payload = event.payload
            raw_candle = payload.get("candle", payload)
            symbol = raw_candle.get("symbol", "NIFTY50")
            # Sentiment check
            async with self._lock:
                self.latest_results[symbol] = {
                    "recommendation": "BUY",
                    "confidence": 60.0,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
        except Exception as e:
            logger.error("Error in NewsEmployee _on_candle", error=str(e))


class EconomicCalendarEmployee(BaseNewSpecialist):
    def __init__(self) -> None:
        super().__init__("EMP-CAL", "WAIT")

    async def start(self) -> None:
        await super().start()
        await event_bus.subscribe("candle", self._on_candle)

    async def stop(self) -> None:
        await super().stop()
        await event_bus.unsubscribe("candle", self._on_candle)

    async def _on_candle(self, event: EventModel) -> None:
        try:
            payload = event.payload
            raw_candle = payload.get("candle", payload)
            symbol = raw_candle.get("symbol", "NIFTY50")
            async with self._lock:
                self.latest_results[symbol] = {
                    "recommendation": "BUY",
                    "confidence": 55.0,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
        except Exception as e:
            logger.error("Error in EconomicCalendarEmployee _on_candle", error=str(e))


class EventRiskEmployee(BaseNewSpecialist):
    def __init__(self) -> None:
        super().__init__("EMP-EVR", "WAIT")

    async def start(self) -> None:
        await super().start()
        await event_bus.subscribe("candle", self._on_candle)

    async def stop(self) -> None:
        await super().stop()
        await event_bus.unsubscribe("candle", self._on_candle)

    async def _on_candle(self, event: EventModel) -> None:
        try:
            payload = event.payload
            raw_candle = payload.get("candle", payload)
            symbol = raw_candle.get("symbol", "NIFTY50")
            async with self._lock:
                self.latest_results[symbol] = {
                    "recommendation": "BUY",
                    "confidence": 60.0,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
        except Exception as e:
            logger.error("Error in EventRiskEmployee _on_candle", error=str(e))


# ── Execution Department ─────────────────────────────────────────────────────

class ExecutionEmployee(BaseNewSpecialist):
    def __init__(self) -> None:
        super().__init__("EMP-EXE", "WAIT")

    async def start(self) -> None:
        await super().start()
        await event_bus.subscribe("order_filled", self._on_order)

    async def stop(self) -> None:
        await super().stop()
        await event_bus.unsubscribe("order_filled", self._on_order)

    async def _on_order(self, event: EventModel) -> None:
        try:
            payload = event.payload
            symbol = payload.get("symbol", "NIFTY50")
            async with self._lock:
                self.latest_results[symbol] = {
                    "recommendation": "BUY",
                    "confidence": 75.0,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
        except Exception as e:
            logger.error("Error in ExecutionEmployee _on_order", error=str(e))


class PortfolioEmployee(BaseNewSpecialist):
    def __init__(self) -> None:
        super().__init__("EMP-PTF", "WAIT")

    async def start(self) -> None:
        await super().start()
        await event_bus.subscribe("portfolio_update", self._on_portfolio)

    async def stop(self) -> None:
        await super().stop()
        await event_bus.unsubscribe("portfolio_update", self._on_portfolio)

    async def _on_portfolio(self, event: EventModel) -> None:
        try:
            payload = event.payload
            symbol = "SYSTEM"
            async with self._lock:
                self.latest_results[symbol] = {
                    "recommendation": "BUY",
                    "confidence": 70.0,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
        except Exception as e:
            logger.error("Error in PortfolioEmployee _on_portfolio", error=str(e))


class PaperTradingEmployee(BaseNewSpecialist):
    def __init__(self) -> None:
        super().__init__("EMP-PPR", "WAIT")

    async def start(self) -> None:
        await super().start()
        # "paper_status_changed" is never published; PaperTradingEngine publishes
        # "paper_trade_started" / "paper_trade_stopped" for session lifecycle.
        await event_bus.subscribe("paper_trade_started", self._on_paper_status)
        await event_bus.subscribe("paper_trade_stopped", self._on_paper_status)

    async def stop(self) -> None:
        await super().stop()
        await event_bus.unsubscribe("paper_trade_started", self._on_paper_status)
        await event_bus.unsubscribe("paper_trade_stopped", self._on_paper_status)

    async def _on_paper_status(self, event: EventModel) -> None:
        try:
            symbol = "SYSTEM"
            is_active = event.event_type == "paper_trade_started"
            rec = "BUY" if is_active else "WAIT"
            async with self._lock:
                self.latest_results[symbol] = {
                    "recommendation": rec,
                    "confidence": 80.0,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
        except Exception as e:
            logger.error("Error in PaperTradingEmployee _on_paper_status", error=str(e))


# ── Extra Predefined Specialists ─────────────────────────────────────────────

class OptionsSpecialistEmployee(BaseNewSpecialist):
    def __init__(self) -> None:
        super().__init__("EMP-OPT", "WAIT")

    async def start(self) -> None:
        await super().start()
        await event_bus.subscribe("option_chain_updated", self._on_chain)

    async def stop(self) -> None:
        await super().stop()
        await event_bus.unsubscribe("option_chain_updated", self._on_chain)

    async def _on_chain(self, event: EventModel) -> None:
        try:
            payload = event.payload
            symbol = payload.get("underlying_symbol", "NIFTY")
            async with self._lock:
                self.latest_results[symbol] = {
                    "recommendation": "BUY",
                    "confidence": 70.0,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
        except Exception as e:
            logger.error("Error in OptionsSpecialistEmployee _on_chain", error=str(e))


class EquityIntradaySpecialistEmployee(BaseNewSpecialist):
    def __init__(self) -> None:
        super().__init__("EMP-EQI", "WAIT")

    async def start(self) -> None:
        await super().start()
        await event_bus.subscribe("candle", self._on_candle)

    async def stop(self) -> None:
        await super().stop()
        await event_bus.unsubscribe("candle", self._on_candle)

    async def _on_candle(self, event: EventModel) -> None:
        try:
            payload = event.payload
            raw_candle = payload.get("candle", payload)
            symbol = raw_candle.get("symbol", "NIFTY50")
            async with self._lock:
                self.latest_results[symbol] = {
                    "recommendation": "BUY",
                    "confidence": 65.0,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
        except Exception as e:
            logger.error("Error in EquityIntradaySpecialistEmployee _on_candle", error=str(e))


class EquitySwingSpecialistEmployee(BaseNewSpecialist):
    def __init__(self) -> None:
        super().__init__("EMP-EQS", "WAIT")

    async def start(self) -> None:
        await super().start()
        await event_bus.subscribe("candle", self._on_candle)

    async def stop(self) -> None:
        await super().stop()
        await event_bus.unsubscribe("candle", self._on_candle)

    async def _on_candle(self, event: EventModel) -> None:
        try:
            payload = event.payload
            raw_candle = payload.get("candle", payload)
            symbol = raw_candle.get("symbol", "NIFTY50")
            async with self._lock:
                self.latest_results[symbol] = {
                    "recommendation": "BUY",
                    "confidence": 60.0,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
        except Exception as e:
            logger.error("Error in EquitySwingSpecialistEmployee _on_candle", error=str(e))


class CommoditySpecialistEmployee(BaseNewSpecialist):
    def __init__(self) -> None:
        super().__init__("EMP-COM", "WAIT")

    async def start(self) -> None:
        await super().start()
        await event_bus.subscribe("candle", self._on_candle)

    async def stop(self) -> None:
        await super().stop()
        await event_bus.unsubscribe("candle", self._on_candle)

    async def _on_candle(self, event: EventModel) -> None:
        try:
            payload = event.payload
            raw_candle = payload.get("candle", payload)
            symbol = raw_candle.get("symbol", "GOLD")
            async with self._lock:
                self.latest_results[symbol] = {
                    "recommendation": "BUY",
                    "confidence": 68.0,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
        except Exception as e:
            logger.error("Error in CommoditySpecialistEmployee _on_candle", error=str(e))


class CurrencySpecialistEmployee(BaseNewSpecialist):
    def __init__(self) -> None:
        super().__init__("EMP-CUR", "WAIT")

    async def start(self) -> None:
        await super().start()
        await event_bus.subscribe("candle", self._on_candle)

    async def stop(self) -> None:
        await super().stop()
        await event_bus.unsubscribe("candle", self._on_candle)

    async def _on_candle(self, event: EventModel) -> None:
        try:
            payload = event.payload
            raw_candle = payload.get("candle", payload)
            symbol = raw_candle.get("symbol", "USDINR")
            async with self._lock:
                self.latest_results[symbol] = {
                    "recommendation": "BUY",
                    "confidence": 62.0,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
        except Exception as e:
            logger.error("Error in CurrencySpecialistEmployee _on_candle", error=str(e))


class PortfolioManagerEmployee(BaseNewSpecialist):
    def __init__(self) -> None:
        super().__init__("EMP-PM", "WAIT")

    async def start(self) -> None:
        await super().start()
        await event_bus.subscribe("portfolio_update", self._on_portfolio)

    async def stop(self) -> None:
        await super().stop()
        await event_bus.unsubscribe("portfolio_update", self._on_portfolio)

    async def _on_portfolio(self, event: EventModel) -> None:
        try:
            payload = event.payload
            symbol = "SYSTEM"
            async with self._lock:
                self.latest_results[symbol] = {
                    "recommendation": "BUY",
                    "confidence": 70.0,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
        except Exception as e:
            logger.error("Error in PortfolioManagerEmployee _on_portfolio", error=str(e))
