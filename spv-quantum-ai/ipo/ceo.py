from typing import Any, Dict, List, Optional

from sqlalchemy import select, delete
from database.connection import async_session
from database.models import IPOIssueModel, IPOAnalystReportModel, IPORecommendationModel
from ipo.analysts import IPO_ANALYST_REGISTRY
from core.logging import get_logger

logger = get_logger("ipo_ceo")

RECOMMENDATIONS = ("APPLY", "AVOID", "LISTING_GAIN_ONLY", "LONG_TERM_INVESTMENT", "WAIT")

# Long-term investment quality requires real fundamental/financial data
# (revenue trend, margins, debt, promoter quality) that this Phase-1 system
# does not have a source for yet — see ValuationAnalyst's docstring. The CEO
# therefore never emits LONG_TERM_INVESTMENT; it's included in RECOMMENDATIONS
# only to document the full intended output space for when that data exists.


class IPOCeoEngine:
    """
    Runs every registered analyst against an IPO, stores whichever reports
    come back with real data, and produces one recommendation with full
    reasoning. An analyst that returns None (no real data for its
    specialty) simply doesn't contribute — the recommendation's
    data_completeness_pct is exactly how many of the available analysts
    had something real to say about this specific IPO.
    """

    async def analyze(self, symbol: str) -> Dict[str, Any]:
        async with async_session() as session:
            result = await session.execute(select(IPOIssueModel).where(IPOIssueModel.symbol == symbol))
            issue = result.scalars().first()
        if issue is None:
            raise ValueError(f"No IPO found for symbol '{symbol}'. Run the collector first.")

        reports: List[Dict[str, Any]] = []
        for analyst in IPO_ANALYST_REGISTRY:
            try:
                report = await analyst.analyze(issue)
            except Exception as e:
                logger.error(f"{analyst.name} failed for {symbol}", error=str(e))
                report = None
            if report is not None:
                reports.append(report)

        async with async_session() as session:
            await session.execute(delete(IPOAnalystReportModel).where(IPOAnalystReportModel.ipo_symbol == symbol))
            for r in reports:
                session.add(IPOAnalystReportModel(
                    ipo_symbol=symbol, analyst_name=r["analyst_name"], score=r["score"],
                    confidence=r["confidence"], reason=r["reason"],
                    advantages=r.get("advantages", []), risks=r.get("risks", []),
                ))
            await session.commit()

        recommendation, confidence, reasoning = self._decide(issue, reports)
        completeness = round(len(reports) / len(IPO_ANALYST_REGISTRY) * 100.0, 1) if IPO_ANALYST_REGISTRY else 0.0

        async with async_session() as session:
            await session.execute(delete(IPORecommendationModel).where(IPORecommendationModel.ipo_symbol == symbol))
            session.add(IPORecommendationModel(
                ipo_symbol=symbol, recommendation=recommendation, confidence=confidence,
                reasoning=reasoning, analysts_used=[r["analyst_name"] for r in reports],
                data_completeness_pct=completeness,
            ))
            await session.commit()

        return {
            "symbol": symbol,
            "recommendation": recommendation,
            "confidence": confidence,
            "reasoning": reasoning,
            "reports": reports,
            "data_completeness_pct": completeness,
            "analysts_available": len(IPO_ANALYST_REGISTRY),
            "analysts_reporting": len(reports),
        }

    @staticmethod
    def _decide(issue: IPOIssueModel, reports: List[Dict[str, Any]]) -> tuple[str, float, str]:
        if not reports:
            return (
                "WAIT", 0.0,
                f"No real data is available yet for {issue.symbol} ({issue.status}). "
                "Once subscription figures or a confirmed price band are collected, a recommendation can be formed."
            )

        avg_score = sum(r["score"] for r in reports) / len(reports)
        avg_confidence = sum(r["confidence"] for r in reports) / len(reports)
        contributing = ", ".join(r["analyst_name"] for r in reports)
        risk_flags = [risk for r in reports for risk in r.get("risks", [])]

        subscription_report = next((r for r in reports if r["analyst_name"] == "Subscription Analyst"), None)
        undersubscribed = subscription_report is not None and subscription_report["score"] <= 20.0

        base_reason = (
            f"Based on {len(reports)} of {len(IPO_ANALYST_REGISTRY)} available analysts ({contributing}), "
            f"average score {avg_score:.1f}/100. "
        )
        phase_note = (
            " Long-term investment quality is not assessed in this system yet — that needs financial "
            "statement data (revenue, margins, debt) not currently integrated (Phase 2)."
        )

        if undersubscribed:
            return "AVOID", avg_confidence, base_reason + "Undersubscription is a real, strong negative signal." + phase_note

        if avg_score >= 75.0:
            return "APPLY", avg_confidence, base_reason + "Strong demand and pricing signals support applying." + phase_note
        if avg_score >= 55.0:
            return "LISTING_GAIN_ONLY", avg_confidence, base_reason + "Signals support a listing-day view, not a conviction long-term call." + phase_note
        if avg_score >= 35.0:
            return "WAIT", avg_confidence, base_reason + "Mixed signals — not strong enough either way yet." + phase_note

        return "AVOID", avg_confidence, base_reason + "Weak signals across the available analysts." + phase_note


# Singleton
ipo_ceo = IPOCeoEngine()
