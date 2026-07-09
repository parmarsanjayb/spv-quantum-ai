import pytest
import asyncio
from brokers.paper import PaperBroker
from brokers.models import OrderSide, OrderType
from core.bus import event_bus, EventModel

@pytest.mark.asyncio
async def test_paper_trading_sl_target_hit():
    event_bus.start()
    
    broker = PaperBroker()
    await broker.connect()

    # 1. Place buy order with SL & Target
    symbol = "NIFTY 50"
    resp = await broker.place_order(
        symbol=symbol,
        side=OrderSide.BUY,
        quantity=10.0,
        order_type=OrderType.MARKET,
        price=24200.0,
        stop_loss=24150.0,
        target=24350.0
    )

    assert resp.success
    pos = broker._positions.get(symbol)
    assert pos is not None
    assert pos.stop_loss == 24150.0
    assert pos.target == 24350.0

    # 2. Simulate tick that hits the Stop Loss
    await event_bus.publish(EventModel(
        event_type="tick",
        source_agent="test",
        payload={
            "symbol": symbol,
            "close": 24100.0,
            "volume": 500.0
        }
    ))

    # Wait for async exit task to run
    await asyncio.sleep(0.2)

    # 3. Verify position has been closed/removed
    assert symbol not in broker._positions

    await broker.disconnect()
    await event_bus.stop()
