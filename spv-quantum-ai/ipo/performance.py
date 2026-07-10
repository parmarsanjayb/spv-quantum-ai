from typing import Any, Dict, List, Optional

from sqlalchemy import select
from database.connection import async_session
from database.models import IPOIssueModel, IPORecommendationModel, IPOPerformanceModel, IPOAnalystReportModel
from core.logging import get_logger

logger = get_logger("ipo_performance")


def _judge(recommendation: str, listing_gain_pct: Optional[float]) -> Optional[bool]:
    """Whether the recommendation matched what actually happened at listing.
    None where there's no meaningful right/wrong (WAIT — it didn't commit to
    a direction) rather than forcing a verdict."""
    if listing_gain_pct is None:
        return None
    if recommendation in ("APPLY", "LISTING_GAIN_ONLY"):
        return listing_gain_pct > 0
    if recommendation == "AVOID":
        return listing_gain_pct <= 0
    return None  # WAIT / LONG_TERM_INVESTMENT (latter never emitted in Phase 1)


class IPOPerformanceTracker:
    """
    Closes the feedback loop: once an IPO actually lists (real listing_price
    collected from NSE's public-past-issues), compares it against whatever
    recommendation the CEO made beforehand. This is the raw material for
    future analyst trust-weighting — Phase 1 only computes and stores the
    comparison; weighting analyst influence by historical accuracy is a
    later phase once enough real outcomes have accumulated.
    """

    async def evaluate_symbol(self, symbol: str) -> Optional[Dict[str, Any]]:
        async with async_session() as session:
            issue = (await session.execute(
                select(IPOIssueModel).where(IPOIssueModel.symbol == symbol)
            )).scalars().first()
            recommendation = (await session.execute(
                select(IPORecommendationModel).where(IPORecommendationModel.ipo_symbol == symbol)
            )).scalars().first()

        if issue is None or issue.listing_price is None:
            return None  # not listed yet — nothing real to compare against
        if recommendation is None:
            return None  # no prediction was ever made for this IPO

        issue_price_high = issue.price_band_high
        listing_gain_pct = None
        if issue_price_high and issue_price_high > 0:
            listing_gain_pct = round((issue.listing_price - issue_price_high) / issue_price_high * 100.0, 2)

        was_correct = _judge(recommendation.recommendation, listing_gain_pct)

        async with async_session() as session:
            existing = (await session.execute(
                select(IPOPerformanceModel).where(IPOPerformanceModel.ipo_symbol == symbol)
            )).scalars().first()
            if existing is None:
                existing = IPOPerformanceModel(ipo_symbol=symbol)
                session.add(existing)
            existing.predicted_recommendation = recommendation.recommendation
            existing.predicted_confidence = recommendation.confidence
            existing.issue_price_high = issue_price_high
            existing.listing_price = issue.listing_price
            existing.listing_gain_pct = listing_gain_pct
            existing.was_correct = was_correct
            await session.commit()

        logger.info(f"IPO performance evaluated for {symbol}: gain={listing_gain_pct}%, correct={was_correct}")
        return {
            "symbol": symbol,
            "predicted_recommendation": recommendation.recommendation,
            "listing_gain_pct": listing_gain_pct,
            "was_correct": was_correct,
        }

    async def evaluate_all_pending(self) -> List[Dict[str, Any]]:
        """Finds every listed IPO with a recommendation but no performance
        row yet, and evaluates each."""
        async with async_session() as session:
            listed_symbols = (await session.execute(
                select(IPOIssueModel.symbol).where(
                    IPOIssueModel.status == "LISTED", IPOIssueModel.listing_price.isnot(None)
                )
            )).scalars().all()
            recommended_symbols = set((await session.execute(select(IPORecommendationModel.ipo_symbol))).scalars().all())
            already_evaluated = set((await session.execute(select(IPOPerformanceModel.ipo_symbol))).scalars().all())

        pending = [s for s in listed_symbols if s in recommended_symbols and s not in already_evaluated]
        results = []
        for symbol in pending:
            result = await self.evaluate_symbol(symbol)
            if result:
                results.append(result)
        logger.info(f"IPO performance: evaluated {len(results)} newly-listed IPOs.")
        return results

    async def get_accuracy_summary(self) -> Dict[str, Any]:
        """Aggregate accuracy across all judged (was_correct is not null)
        recommendations, plus a per-analyst breakdown — real historical
        accuracy, not a fabricated trust score."""
        async with async_session() as session:
            perf_rows = (await session.execute(
                select(IPOPerformanceModel).where(IPOPerformanceModel.was_correct.isnot(None))
            )).scalars().all()

        if not perf_rows:
            return {"total_judged": 0, "accuracy_pct": None, "by_recommendation": {}}

        correct = sum(1 for p in perf_rows if p.was_correct)
        by_rec: Dict[str, Dict[str, int]] = {}
        for p in perf_rows:
            bucket = by_rec.setdefault(p.predicted_recommendation, {"total": 0, "correct": 0})
            bucket["total"] += 1
            if p.was_correct:
                bucket["correct"] += 1

        return {
            "total_judged": len(perf_rows),
            "accuracy_pct": round(correct / len(perf_rows) * 100.0, 1),
            "by_recommendation": {
                rec: {"total": b["total"], "correct": b["correct"],
                      "accuracy_pct": round(b["correct"] / b["total"] * 100.0, 1)}
                for rec, b in by_rec.items()
            },
        }


# Singleton
ipo_performance_tracker = IPOPerformanceTracker()
