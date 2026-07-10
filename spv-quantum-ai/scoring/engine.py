import asyncio
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from core.bus import event_bus, EventModel
from core.logging import get_logger
from market.models import Timeframe
from analysis.models import MarketAnalysisReport
from analysis.engine import market_analysis_engine
from regime.engine import regime_engine
from risk.engine import risk_engine
from strategies.engine import strategy_engine

from scoring.models import DecisionScoreResult, DecisionQuality
from scoring.weights import WeightManager
from scoring.calculator import ConfidenceCalculator
from scoring.publisher import DecisionPublisher

logger = get_logger("scoring_engine")

class DecisionScoringEngine:
    """
    Decision Scoring Engine.
    Evaluates analysis outputs, regimes, strategies, and risk to calculate a weighted confidence score.
    Does not place trades.
    """
    def __init__(self) -> None:
        self.wm = WeightManager()
        self.calculator = ConfidenceCalculator(self.wm)
        self.publisher = DecisionPublisher()
        self._cache: Dict[tuple, DecisionScoreResult] = {}
        self._lock = asyncio.Lock()
        self._running = False

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        # Evaluate automatically when new market analysis reports are published
        await event_bus.subscribe("market_analysis", self._on_market_analysis)
        # Evaluate automatically when the Scanner surfaces a new opportunity
        await event_bus.subscribe("scanner_match", self._on_scanner_match)
        logger.info("DecisionScoringEngine started and subscribed to market_analysis and scanner_match events.")

    async def stop(self) -> None:
        self._running = False
        await event_bus.unsubscribe("market_analysis", self._on_market_analysis)
        await event_bus.unsubscribe("scanner_match", self._on_scanner_match)
        logger.info("DecisionScoringEngine stopped.")

    async def _on_market_analysis(self, event: EventModel) -> None:
        try:
            payload = event.payload
            # Extract report
            raw_report = payload.get("report", payload)
            report = MarketAnalysisReport(**raw_report)
            tf = Timeframe(report.timeframe)
            await self.evaluate_decision(report.symbol, tf, report)
        except Exception as e:
            logger.error("Error processing market analysis event in DecisionScoringEngine", error=str(e))

    async def _on_scanner_match(self, event: EventModel) -> None:
        try:
            payload = event.payload
            scan_result = payload.get("scan_result", payload)
            symbol = scan_result.get("symbol")
            if not symbol:
                return
            # Scanner engine evaluates opportunities on the M1 timeframe
            await self.evaluate_decision(symbol, Timeframe.M1)
        except Exception as e:
            logger.error("Error processing scanner match event in DecisionScoringEngine", error=str(e))

    async def evaluate_decision(
        self, symbol: str, timeframe: Timeframe, provided_report: Optional[MarketAnalysisReport] = None
    ) -> DecisionScoreResult:
        """
        Compiles the inputs and calculates the overall decision score.
        """
        # 1. Resolve inputs
        report = provided_report
        if not report:
            report = await market_analysis_engine.cache.get_latest(symbol, timeframe.value)

        # Regime
        regime_val = "UNKNOWN"
        r_reg = await regime_engine.cache.get_latest(symbol, timeframe)
        if r_reg:
            regime_val = r_reg.market_regime.value

        # Risk
        # RiskEngine.get_dashboard_metrics() reports OPERATIONAL/RESTRICTED (system-wide
        # health), which must be translated to the ALLOW/BLOCK vocabulary the confidence
        # calculator expects (the same vocabulary RiskEngine.validate_order() returns).
        risk_status = "BLOCK"
        try:
            risk_metrics = await risk_engine.get_dashboard_metrics()
            risk_status = "ALLOW" if risk_metrics.get("risk_status") == "OPERATIONAL" else "BLOCK"
        except Exception:
            pass

        # Strategy match
        strategy_matched = False
        strategy_action = "SIGNAL_NONE"
        try:
            strategy_responses = await strategy_engine.evaluate_all(symbol, timeframe)
            for r in strategy_responses:
                if r.matched:
                    strategy_matched = True
                    strategy_action = r.required_action or "SIGNAL_NONE"
                    break
        except Exception:
            pass

        inputs = {
            "market_analysis": report,
            "market_regime": regime_val,
            "risk_status": risk_status,
            "strategy_matched": strategy_matched,
            "strategy_action": strategy_action
        }

        # 2. Run calculation
        conf_score, comp_scores, quality, missing_reqs, conflicts = self.calculator.calculate(inputs)

        # 3. Build reasoning summary
        reasoning = (
            f"Overall confidence {conf_score}% with quality {quality.value}. "
            f"Component breakdown: Analysis={comp_scores.get('market_analysis', 0):.1f}, "
            f"Regime={comp_scores.get('market_regime', 0):.1f}, "
            f"Strategy={comp_scores.get('strategy_match', 0):.1f}, "
            f"Risk={comp_scores.get('risk_status', 0):.1f}. "
        )
        if conflicts:
            reasoning += f"Conflicts detected: {', '.join(conflicts)}."
        else:
            reasoning += "No conflicting signals detected."

        result = DecisionScoreResult(
            symbol=symbol,
            timeframe=timeframe.value,
            overall_confidence=conf_score,
            component_scores=comp_scores,
            risk_status=risk_status,
            recommended_strategy=(report.recommended_strategy if report else None),
            decision_quality=quality,
            missing_requirements=missing_reqs,
            conflicting_signals=conflicts,
            reasoning_summary=reasoning,
            strategy_action=strategy_action,
            timestamp=datetime.now(timezone.utc)
        )

        # 4. Cache
        async with self._lock:
            self._cache[(symbol, timeframe.value)] = result

        # 5. Publish
        await self.publisher.publish(result)
        return result

    async def get_latest(self, symbol: str, timeframe: str) -> Optional[DecisionScoreResult]:
        async with self._lock:
            return self._cache.get((symbol, timeframe))

    async def get_all_latest(self) -> Dict[tuple, DecisionScoreResult]:
        async with self._lock:
            return self._cache.copy()

# Singleton
decision_scoring_engine = DecisionScoringEngine()
