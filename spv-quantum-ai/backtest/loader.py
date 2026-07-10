from datetime import datetime
from typing import List
from sqlalchemy import select
from market.models import Candle, Timeframe
from database.connection import async_session
from database.models import MarketDataModel


class HistoricalDataLoader:
    """
    Loads historical candles for backtesting from the market_data table —
    real candles persisted by MarketDataPersistence from live Kotak Neo ticks.
    There is no synthetic/fabricated fallback: a symbol/timeframe/date range
    with no real recorded history simply returns an empty list, and the
    BacktestingEngine reports "No data found" rather than inventing prices.
    """
    async def load_candles(self, symbol: str, timeframe: str, start: datetime, end: datetime) -> List[Candle]:
        async with async_session() as session:
            result = await session.execute(
                select(MarketDataModel)
                .where(MarketDataModel.symbol == symbol)
                .where(MarketDataModel.interval == timeframe)
                .where(MarketDataModel.timestamp >= start)
                .where(MarketDataModel.timestamp <= end)
                .order_by(MarketDataModel.timestamp.asc())
            )
            rows = result.scalars().all()

        return [
            Candle(
                symbol=row.symbol,
                timeframe=Timeframe(row.interval),
                timestamp=row.timestamp,
                open=row.open,
                high=row.high,
                low=row.low,
                close=row.close,
                volume=row.volume,
                complete=True,
            )
            for row in rows
        ]
