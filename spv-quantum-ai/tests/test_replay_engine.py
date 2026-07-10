import pytest
import asyncio
from datetime import datetime, timezone, timedelta
from replay.models import ReplayConfig, ReplayState
from replay.engine import replay_engine
from core.bus import event_bus, EventModel
from sqlalchemy import delete
from database.connection import async_session
from database.models import MarketDataModel
from brokers.manager import broker_manager

@pytest.mark.asyncio
async def test_market_replay_controls():
    event_bus.start()

    start = datetime(2026, 7, 4, 9, 0, 0, tzinfo=timezone.utc)
    end = datetime(2026, 7, 4, 9, 5, 0, tzinfo=timezone.utc)

    # replay_engine has no synthetic fallback — seed real candle rows so
    # there's actual history to replay.
    async with async_session() as session:
        await session.execute(delete(MarketDataModel).where(MarketDataModel.symbol == "TCS"))
        for i in range(6):
            price = 3500.0 + i * 2.0
            session.add(MarketDataModel(
                symbol="TCS", timestamp=start + timedelta(minutes=i), interval="1m",
                open=price, high=price + 1.0, low=price - 1.0, close=price + 0.5,
                volume=300.0,
            ))
        await session.commit()

    config = ReplayConfig(
        symbols=["TCS"],
        timeframe="1m",
        start_date=start,
        end_date=end,
        speed="1x",
        mode="Full Trading System"
    )

    # 1. Setup Replay
    await replay_engine.start()
    replay_id = await replay_engine.setup_replay(config)
    assert replay_id.startswith("RPL-")
    
    status = await replay_engine.get_dashboard_status()
    assert status["status"] == "PENDING"
    assert status["total_candles"] >= 1
    
    # 2. Play
    await replay_engine.play()
    status = await replay_engine.get_dashboard_status()
    assert status["status"] == "PLAYING"
    
    # 3. Pause
    await replay_engine.pause()
    status = await replay_engine.get_dashboard_status()
    assert status["status"] == "PAUSED"
    
    index_before = status["current_index"]
    
    # 4. Next Candle
    await replay_engine.next_candle()
    status = await replay_engine.get_dashboard_status()
    assert status["current_index"] == index_before + 1
    
    # 5. Previous Candle
    await replay_engine.previous_candle()
    status = await replay_engine.get_dashboard_status()
    assert status["current_index"] == index_before
    
    # 6. Resume & Speed increase to complete
    await replay_engine.set_speed("Unlimited")
    await replay_engine.resume()
    
    # Wait for completion
    for _ in range(50):
        status = await replay_engine.get_dashboard_status()
        if status["status"] in ("COMPLETED", "FAILED"):
            break
        await asyncio.sleep(0.05)
        
    status = await replay_engine.get_dashboard_status()
    assert status["status"] == "COMPLETED"
    assert status["progress_pct"] == 100.0
    
    await replay_engine.stop()
    await event_bus.stop()


@pytest.mark.asyncio
async def test_replay_forces_paper_broker_and_restores_original_after():
    """Same real-money guarantee as the backtest engine: a replay run must
    force paper_broker even if kotak_neo is active elsewhere, and restore
    the original active broker once stopped."""
    event_bus.start()
    original = broker_manager._active_broker_name
    broker_manager._active_broker_name = "kotak_neo"

    start = datetime(2026, 7, 4, 9, 0, 0, tzinfo=timezone.utc)
    end = datetime(2026, 7, 4, 9, 1, 0, tzinfo=timezone.utc)
    async with async_session() as session:
        await session.execute(delete(MarketDataModel).where(MarketDataModel.symbol == "REPLAYSAFETYTEST"))
        session.add(MarketDataModel(
            symbol="REPLAYSAFETYTEST", timestamp=start, interval="1m",
            open=100.0, high=101.0, low=99.0, close=100.5, volume=100.0,
        ))
        await session.commit()

    config = ReplayConfig(
        symbols=["REPLAYSAFETYTEST"], timeframe="1m",
        start_date=start, end_date=end, speed="1x", mode="Full Trading System",
    )

    try:
        await replay_engine.start()
        await replay_engine.setup_replay(config)
        # setup_replay pins the broker while resetting trading state
        assert broker_manager._active_broker_name == "paper_broker"
    finally:
        await replay_engine.stop()

    assert broker_manager._active_broker_name == "kotak_neo"
    broker_manager._active_broker_name = original
    await event_bus.stop()
