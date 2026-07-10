import asyncio
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from core.bus import event_bus, EventModel
from core.logging import get_logger
from market.models import Timeframe, Candle
from indicators.engine import indicator_engine
from regime.engine import regime_engine
from risk.engine import risk_engine
from market.manager import market_data_manager

from strategies.models import (
    Strategy, StrategyResponse, StrategyMatchedEvent, StrategyRejectedEvent
)
from strategies.loader import StrategyRegistry, StrategyLoader
from strategies.evaluator import RuleEngine

logger = get_logger("strategy_engine")

class StrategyEngine:
    """
    Orchestrates the Strategy Rules Engine.
    Loads/reloads YAML strategies, builds feature contexts on new candle events,
    evaluates rules, and publishes Matched/Rejected events.
    Does not execute trades.
    """
    def __init__(self, directory: str = "config/strategies") -> None:
        self.registry = StrategyRegistry()
        self.loader = StrategyLoader(self.registry, directory)
        self.evaluator = RuleEngine()
        self._running = False
        self._db_loaded_names: set = set()

        # Load strategies on startup
        self.loader.load_all()

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        await self.load_from_db()
        await event_bus.subscribe("candle", self._on_candle_event)
        logger.info("StrategyEngine started and subscribed to candle events.")

    async def load_from_db(self) -> None:
        """
        Registers every Strategy Studio strategy's active version into the
        same registry YAML-file strategies use. Studio-authored strategies
        are evaluated by the identical rule engine — the Studio is purely
        an authoring/persistence layer, not a separate execution path. Safe
        to call again any time the Studio saves/activates/deletes a
        strategy, to hot-reload without a restart.
        """
        from database.models import StrategyDefinitionModel
        from database.connection import async_session
        from sqlalchemy import select

        try:
            async with async_session() as session:
                result = await session.execute(
                    select(StrategyDefinitionModel).where(StrategyDefinitionModel.is_active == True)  # noqa: E712
                )
                rows = result.scalars().all()
        except Exception as e:
            logger.error("Failed to load Strategy Studio strategies from DB", error=str(e))
            return

        # Drop any previously DB-loaded strategies that are no longer active
        # (deleted or deactivated), without touching YAML-loaded strategies.
        db_names_now = {row.strategy_name for row in rows}
        for name in list(self.registry._strategies.keys()):
            if name in self._db_loaded_names and name not in db_names_now:
                self.registry.unregister(name)

        self._db_loaded_names = db_names_now
        for row in rows:
            try:
                strategy = Strategy(**row.definition)
                self.registry.register(strategy)
            except Exception as e:
                logger.error(f"Failed to register Studio strategy '{row.strategy_name}'", error=str(e))

    async def stop(self) -> None:
        self._running = False
        await event_bus.unsubscribe("candle", self._on_candle_event)
        logger.info("StrategyEngine stopped.")

    async def _on_candle_event(self, event: EventModel) -> None:
        try:
            payload = event.payload
            raw_candle = payload.get("candle", payload)
            candle = Candle(**raw_candle)
            if candle.complete:
                await self.evaluate_all(candle.symbol, candle.timeframe)
        except Exception as e:
            logger.error("Error processing candle event in StrategyEngine", error=str(e))

    async def evaluate_all(self, symbol: str, timeframe: Timeframe) -> List[StrategyResponse]:
        """
        Builds the context for symbol/timeframe and evaluates all active strategies.
        """
        context = await self._build_context(symbol, timeframe)
        responses: List[StrategyResponse] = []

        active_strategies = self.registry.get_active()
        for strategy in active_strategies:
            try:
                matched = self.evaluator.evaluate_group(strategy.rules, context)

                # Entry and exit conditions are designed to be mutually
                # exclusive (e.g. Golden Cross vs Death Cross), so exit_rules
                # is only checked when the entry side didn't match — this
                # avoids either side needing to know about the other.
                exit_matched = False
                if not matched and strategy.exit_rules is not None:
                    exit_matched = self.evaluator.evaluate_group(strategy.exit_rules, context)

                status_str = "ACTIVE" if strategy.enabled else "DISABLED"

                if matched or exit_matched:
                    action_info = strategy.actions.get("exit" if exit_matched else "matched", {})
                    resp = StrategyResponse(
                        strategy_name=strategy.name,
                        version=strategy.version,
                        status=status_str,
                        matched=True,
                        confidence=float(action_info.get("confidence", 100.0)),
                        reason=action_info.get("reason", "All rules matched successfully."),
                        required_action=action_info.get("action", "SIGNAL_NONE")
                    )
                    responses.append(resp)

                    # Publish Matched event
                    evt = StrategyMatchedEvent(
                        strategy_response=resp,
                        context=context.get("current", {})
                    )
                    await event_bus.publish(EventModel(
                        event_type="strategy_matched",
                        source_agent="strategy_engine",
                        payload=evt.model_dump()
                    ))
                else:
                    resp = StrategyResponse(
                        strategy_name=strategy.name,
                        version=strategy.version,
                        status=status_str,
                        matched=False,
                        confidence=0.0,
                        reason="One or more rules failed matching.",
                        required_action=None
                    )
                    responses.append(resp)
                    
                    # Publish Rejected event
                    evt = StrategyRejectedEvent(
                        strategy_response=resp,
                        context=context.get("current", {})
                    )
                    await event_bus.publish(EventModel(
                        event_type="strategy_rejected",
                        source_agent="strategy_engine",
                        payload=evt.model_dump()
                    ))
            except Exception as e:
                logger.error(f"Failed to evaluate strategy {strategy.name}", error=str(e))

        return responses

    async def _build_context(self, symbol: str, timeframe: Timeframe) -> Dict[str, Any]:
        """Gathers latest values and previous values into a rule context."""
        # ── 1. Gather Current Context ─────────────────────────────────────────
        curr_indicators = {}
        prev_indicators = {}
        
        # Load all registered indicators
        from indicators.registry import INDICATOR_REGISTRY
        for name in INDICATOR_REGISTRY.keys():
            # Current
            r = await indicator_engine.cache.get_latest(symbol, timeframe, name)
            if r:
                curr_indicators[name] = r.value
            # Previous
            pr = await indicator_engine.cache.get_previous(symbol, timeframe, name)
            if pr:
                prev_indicators[name] = pr.value

        # Market Regime
        regime_val = None
        r_reg = await regime_engine.cache.get_latest(symbol, timeframe)
        if r_reg:
            regime_val = r_reg.market_regime.value

        # Risk Status
        risk_status_val = "ALLOW"
        try:
            risk_metrics = await risk_engine.get_dashboard_metrics()
            risk_status_val = risk_metrics.get("risk_status", "ALLOW")
        except Exception:
            pass

        # Market Data
        mkt_dict = {}
        tick = await market_data_manager.cache.get_tick(symbol)
        if tick:
            mkt_dict = {
                "ltp": tick.ltp,
                "vwap": tick.vwap,
                "volume": tick.volume,
                "oi": tick.open_interest,
                "open": tick.open,
                "high": tick.high,
                "low": tick.low,
                "close": tick.close,
                "prev_close": tick.prev_close
            }

        now = datetime.now(timezone.utc)
        current_context = {
            "indicators": curr_indicators,
            "market_regime": regime_val,
            "risk_status": risk_status_val,
            "market_data": mkt_dict,
            "time": now.strftime("%H:%M"),
            "session": market_data_manager.status.get_status().value
        }

        # ── 2. Gather Previous Context ────────────────────────────────────────
        previous_context = {
            "indicators": prev_indicators,
            "market_regime": None,
            "risk_status": None,
            "market_data": {},
            "time": None,
            "session": None
        }

        return {
            "current": current_context,
            "prev": previous_context
        }

# Singleton instance
strategy_engine = StrategyEngine()
