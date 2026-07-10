import asyncio
from typing import Any, Dict, List, Optional

from sqlalchemy import select
from database.connection import async_session
from database.models import IPOIssueModel
from ipo.collector import ipo_collector
from ipo.ceo import ipo_ceo
from ipo.performance import ipo_performance_tracker
from core.logging import get_logger

logger = get_logger("ipo_engine")

COLLECTION_INTERVAL_SEC = 30 * 60  # IPO data doesn't move fast enough to need tighter polling


class IPOEngine:
    """
    Orchestrates the IPO module's background lifecycle: periodic collection
    from real NSE sources, refreshing CEO recommendations for open IPOs, and
    evaluating performance for anything that has newly listed. Fully
    independent of the trading engines — imports nothing from
    strategies/backtest/execution/portfolio, and nothing there imports this.
    """

    def __init__(self) -> None:
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self.last_collection_counts: Dict[str, int] = {}

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._loop())
        logger.info("IPOEngine started.")

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        logger.info("IPOEngine stopped.")

    async def _loop(self) -> None:
        while self._running:
            try:
                await self.refresh_now()
            except Exception as e:
                logger.error("IPO collection cycle failed", error=str(e))
            await asyncio.sleep(COLLECTION_INTERVAL_SEC)

    async def refresh_now(self) -> Dict[str, Any]:
        """One full cycle: collect real data, re-run the CEO for every open
        IPO, evaluate performance for anything newly listed. Also callable
        on demand (manual refresh button)."""
        self.last_collection_counts = await ipo_collector.collect_all()

        async with async_session() as session:
            open_symbols = (await session.execute(
                select(IPOIssueModel.symbol).where(IPOIssueModel.status == "OPEN")
            )).scalars().all()

        for symbol in open_symbols:
            try:
                await ipo_ceo.analyze(symbol)
            except Exception as e:
                logger.error(f"IPO CEO analysis failed for {symbol}", error=str(e))

        performance_results = await ipo_performance_tracker.evaluate_all_pending()

        return {
            "collection": self.last_collection_counts,
            "recommendations_refreshed": len(open_symbols),
            "performance_evaluated": len(performance_results),
        }


# Singleton
ipo_engine = IPOEngine()
