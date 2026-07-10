from core.bus import event_bus, EventModel
from core.logging import get_logger
from database.connection import async_session
from database.models import MarketDataModel

logger = get_logger("market_persistence")


class MarketDataPersistence:
    """
    Writes every completed real-tick-derived candle to the market_data table.
    This is the ONLY historical data source the Backtesting Engine reads from —
    there is no synthetic/fabricated candle generator. A symbol/timeframe only
    has backtestable history once this has been running long enough, while the
    market was open, to have accumulated real closed candles for it.
    """

    def __init__(self) -> None:
        self._running = False

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        await event_bus.subscribe("candle", self._on_candle)
        logger.info("MarketDataPersistence subscribed to candle events.")

    async def stop(self) -> None:
        if not self._running:
            return
        self._running = False
        await event_bus.unsubscribe("candle", self._on_candle)
        logger.info("MarketDataPersistence stopped.")

    async def _on_candle(self, event: EventModel) -> None:
        candle = event.payload.get("candle", event.payload)
        if not candle.get("complete", True):
            return
        try:
            async with async_session() as session:
                session.add(MarketDataModel(
                    symbol    = candle["symbol"],
                    timestamp = candle["timestamp"],
                    interval  = candle["timeframe"],
                    open      = candle["open"],
                    high      = candle["high"],
                    low       = candle["low"],
                    close     = candle["close"],
                    volume    = candle["volume"],
                ))
                await session.commit()
        except Exception as e:
            logger.error("Failed to persist candle", symbol=candle.get("symbol"), error=str(e))


# Module-level singleton
market_data_persistence = MarketDataPersistence()
