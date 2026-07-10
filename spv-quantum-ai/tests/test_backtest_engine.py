import pytest
import asyncio
from datetime import datetime, timezone, timedelta
from sqlalchemy import delete
from backtest.models import BacktestConfig, BacktestProgress
from backtest.engine import backtesting_engine
from core.bus import event_bus, EventModel
from database.connection import async_session
from database.models import MarketDataModel

# ── Loader Tests ──────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_historical_data_loader_returns_real_persisted_candles():
    """The loader must read real rows from market_data — never fabricate
    candles for a symbol/range that has no recorded history."""
    from backtest.loader import HistoricalDataLoader
    loader = HistoricalDataLoader()

    start = datetime(2026, 7, 4, 9, 0, 0, tzinfo=timezone.utc)
    end = datetime(2026, 7, 4, 9, 10, 0, tzinfo=timezone.utc)

    async with async_session() as session:
        await session.execute(delete(MarketDataModel).where(MarketDataModel.symbol == "LOADERTEST"))
        session.add(MarketDataModel(
            symbol="LOADERTEST", timestamp=start + timedelta(minutes=1), interval="1m",
            open=100.0, high=101.0, low=99.5, close=100.5, volume=1000.0,
        ))
        await session.commit()

    candles = await loader.load_candles("LOADERTEST", "1m", start, end)
    assert len(candles) == 1
    assert candles[0].symbol == "LOADERTEST"
    assert candles[0].close == 100.5
    assert candles[0].complete is True


@pytest.mark.asyncio
async def test_historical_data_loader_returns_empty_when_no_real_data_exists():
    """No fabricated fallback: an untouched symbol/range must return []."""
    from backtest.loader import HistoricalDataLoader
    loader = HistoricalDataLoader()

    start = datetime(2026, 7, 4, 9, 0, 0, tzinfo=timezone.utc)
    end = datetime(2026, 7, 4, 9, 10, 0, tzinfo=timezone.utc)

    candles = await loader.load_candles("NEVER_RECORDED_SYMBOL", "1m", start, end)
    assert candles == []


# ── Engine E2E Simulation Tests ───────────────────────────────────────────────

@pytest.mark.asyncio
async def test_backtesting_engine_simulation():
    event_bus.start()

    # 1. Configure backtest, seeding real candle rows to replay — the engine
    # has no synthetic fallback, so the simulation loop only has work to do
    # if real market_data rows exist for this symbol/timeframe/range.
    start = datetime(2026, 7, 4, 9, 0, 0, tzinfo=timezone.utc)
    end = datetime(2026, 7, 4, 9, 5, 0, tzinfo=timezone.utc)

    async with async_session() as session:
        await session.execute(delete(MarketDataModel).where(
            (MarketDataModel.symbol == "BTCUSD") & (MarketDataModel.interval == "1m")
        ))
        for i in range(6):
            ts = start + timedelta(minutes=i)
            price = 100.0 + i * 0.5
            session.add(MarketDataModel(
                symbol="BTCUSD", timestamp=ts, interval="1m",
                open=price, high=price + 0.3, low=price - 0.3, close=price + 0.1,
                volume=500.0,
            ))
        await session.commit()

    config = BacktestConfig(
        symbols=["BTCUSD"],
        timeframe="1m",
        start_date=start,
        end_date=end,
        initial_capital=100000.0
    )
    
    # Track backtest events
    backtest_events = []
    async def cb(evt: EventModel):
        backtest_events.append(evt)
        
    await event_bus.subscribe("backtest_completed", cb)
    
    # 2. Run simulation
    await backtesting_engine.start()
    backtest_id = await backtesting_engine.run_backtest(config)
    assert backtest_id.startswith("BKT-")
    
    # Wait for completion
    for _ in range(50):
        status_dict = await backtesting_engine.get_dashboard_status()
        if status_dict["status"] in ("COMPLETED", "FAILED"):
            break
        await asyncio.sleep(0.1)
        
    status_dict = await backtesting_engine.get_dashboard_status()
    assert status_dict["status"] == "COMPLETED"
    assert status_dict["progress_pct"] == 100.0
    
    # Verify event delivery
    for _ in range(20):
        if len(backtest_events) >= 1:
            break
        await asyncio.sleep(0.05)
        
    assert len(backtest_events) == 1
    assert backtest_events[0].payload["backtest_id"] == backtest_id
    assert "metrics" in backtest_events[0].payload
    
    await backtesting_engine.stop()
    await event_bus.unsubscribe("backtest_completed", cb)
    await event_bus.stop()


# ── Persistence Tests ────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_market_data_persistence_writes_completed_candles():
    """A completed candle event must land as a real row in market_data —
    this is the only mechanism that gives the backtester real history."""
    from market.persistence import market_data_persistence
    from sqlalchemy import select

    event_bus.start()
    await market_data_persistence.start()

    async with async_session() as session:
        await session.execute(delete(MarketDataModel).where(MarketDataModel.symbol == "PERSISTTEST"))
        await session.commit()

    candle_payload = {
        "candle": {
            "symbol": "PERSISTTEST", "timeframe": "1m",
            "timestamp": datetime(2026, 7, 5, 10, 0, 0, tzinfo=timezone.utc),
            "open": 50.0, "high": 51.0, "low": 49.0, "close": 50.5,
            "volume": 200.0, "vwap": 50.2, "complete": True,
        }
    }
    await event_bus.publish(EventModel(
        event_type="candle", source_agent="test", payload=candle_payload,
    ))
    await asyncio.sleep(0.1)

    async with async_session() as session:
        result = await session.execute(
            select(MarketDataModel).where(MarketDataModel.symbol == "PERSISTTEST")
        )
        rows = result.scalars().all()

    assert len(rows) == 1
    assert rows[0].close == 50.5
    assert rows[0].interval == "1m"

    await market_data_persistence.stop()
    await event_bus.stop()


@pytest.mark.asyncio
async def test_market_data_persistence_ignores_incomplete_candles():
    """A live/incomplete (still-forming) candle must never be persisted —
    only closed bars are real, finished history."""
    from market.persistence import market_data_persistence
    from sqlalchemy import select

    event_bus.start()
    await market_data_persistence.start()

    candle_payload = {
        "candle": {
            "symbol": "INCOMPLETETEST", "timeframe": "1m",
            "timestamp": datetime(2026, 7, 5, 10, 1, 0, tzinfo=timezone.utc),
            "open": 50.0, "high": 51.0, "low": 49.0, "close": 50.5,
            "volume": 200.0, "vwap": 50.2, "complete": False,
        }
    }
    await event_bus.publish(EventModel(
        event_type="candle", source_agent="test", payload=candle_payload,
    ))
    await asyncio.sleep(0.1)

    async with async_session() as session:
        result = await session.execute(
            select(MarketDataModel).where(MarketDataModel.symbol == "INCOMPLETETEST")
        )
        rows = result.scalars().all()

    assert len(rows) == 0

    await market_data_persistence.stop()
    await event_bus.stop()
