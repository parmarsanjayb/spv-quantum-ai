import os
import sys
import asyncio
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from datetime import datetime, timezone
from core.bus import event_bus, EventModel
from brokers.manager import broker_manager
from brokers.models import OrderSide, OrderType
from portfolio.engine import portfolio_engine
from execution.engine import execution_engine
from paper.engine import paper_trading_engine
from paper.models import PaperTradingConfig
from database.connection import init_db

async def run_verification():
    print("Initializing Database and Event Bus...")
    await init_db()
    event_bus.start()
    
    # Start engines
    await portfolio_engine.start()
    from journal.engine import trade_journal_engine
    await trade_journal_engine.start()
    await execution_engine.start()
    await paper_trading_engine.start()
    
    # Start Paper Session
    cfg = PaperTradingConfig(initial_capital=100000.0, latency_ms=10.0, slippage_pct=0.0, spread_pct=0.0)
    session_id = await paper_trading_engine.start_session(cfg)
    print(f"Paper session started: {session_id}")
    
    broker = broker_manager.get_active()
    # Force 0 rejection / partial fills
    broker._rejection_rate = 0.0
    broker._partial_fill_rate = 0.0
    
    # Verify initial state
    bal_resp = await broker.get_balance()
    initial_equity = bal_resp.data["equity"]
    initial_margin = bal_resp.data["available_margin"]
    print(f"Initial: Equity={initial_equity}, Available Margin={initial_margin}")
    assert initial_equity == 100000.0
    assert initial_margin == 100000.0
    
    # ── Step 1: Submit BUY Order ──
    print("\n--- Step 1: Submitting BUY order for 10 RELIANCE at 2500 ---")
    order_filled_future = asyncio.get_running_loop().create_future()
    
    async def on_fill(evt: EventModel):
        payload = evt.payload
        order_data = payload.get("order", payload)
        if order_data.get("symbol") == "RELIANCE":
            order_filled_future.set_result(order_data)
            
    await event_bus.subscribe("order_filled", on_fill)
    
    # Bypass safety engine checks by making sure safety engine allows it
    from safety.engine import safety_engine
    safety_engine.config["market_closing_guard"] = False
    safety_engine.config["trading_session_guard"] = False
    safety_engine.config["holiday_guard"] = False
    safety_engine.config["broker_disconnect_guard"] = False
    safety_engine.config["cooldown_between_trades_sec"] = 0.0
    safety_engine.config["duplicate_symbol_protection"] = True

    # Reset daily limit counts
    from risk.engine import risk_engine
    risk_engine.limit_mgr.daily_trades_count = 0
    risk_engine.limit_mgr.cooldown_until = None
    
    buy_order = await execution_engine.submit_order_request({
        "symbol": "RELIANCE",
        "side": "BUY",
        "quantity": 10.0,
        "price": 2500.0,
        "type": "LIMIT",
        "product_type": "MIS"
    })
    
    # Wait for fill
    try:
        fill_data = await asyncio.wait_for(order_filled_future, timeout=5.0)
        print(f"BUY Filled! Order ID: {fill_data['order_id']}, price: {fill_data['avg_fill_price']}")
    except asyncio.TimeoutError:
        print("ERROR: BUY order not filled within timeout!")
        sys.exit(1)
        
    # Check balance and margin after BUY
    bal_resp = await broker.get_balance()
    buy_equity = bal_resp.data["equity"]
    buy_margin = bal_resp.data["available_margin"]
    buy_used = bal_resp.data["used_margin"]
    print(f"After BUY: Equity={buy_equity}, Available Margin={buy_margin}, Used Margin={buy_used}")
    
    # Check open positions in PortfolioEngine
    open_positions = await portfolio_engine.positions.get_open_positions()
    print(f"Open Positions count in Portfolio: {len(open_positions)}")
    assert len(open_positions) == 1
    assert open_positions[0].symbol == "RELIANCE"
    assert open_positions[0].quantity == 10.0
    assert open_positions[0].avg_price == 2500.0
    
    # ── Step 2: Submit SELL Order (Cover/Close) ──
    print("\n--- Step 2: Submitting SELL order for 10 RELIANCE at 2600 ---")
    await event_bus.unsubscribe("order_filled", on_fill)
    
    order_filled_future_2 = asyncio.get_running_loop().create_future()
    async def on_fill_2(evt: EventModel):
        payload = evt.payload
        order_data = payload.get("order", payload)
        if order_data.get("symbol") == "RELIANCE":
            order_filled_future_2.set_result(order_data)
            
    await event_bus.subscribe("order_filled", on_fill_2)
    
    # Submit sell cover order
    sell_order = await execution_engine.submit_order_request({
        "symbol": "RELIANCE",
        "side": "SELL",
        "quantity": 10.0,
        "price": 2600.0,
        "type": "LIMIT",
        "product_type": "MIS"
    })
    
    # Wait for fill
    try:
        fill_data_2 = await asyncio.wait_for(order_filled_future_2, timeout=5.0)
        print(f"SELL Filled! Order ID: {fill_data_2['order_id']}, price: {fill_data_2['avg_fill_price']}")
    except asyncio.TimeoutError:
        print("ERROR: SELL order not filled within timeout!")
        sys.exit(1)
        
    # Check balance and margin after SELL
    bal_resp = await broker.get_balance()
    sell_equity = bal_resp.data["equity"]
    sell_margin = bal_resp.data["available_margin"]
    sell_used = bal_resp.data["used_margin"]
    print(f"After SELL: Equity={sell_equity}, Available Margin={sell_margin}, Used Margin={sell_used}")
    
    # Check open positions in PortfolioEngine
    open_positions = await portfolio_engine.positions.get_open_positions()
    closed_positions = await portfolio_engine.positions.get_closed_positions()
    print(f"Open Positions count: {len(open_positions)}, Closed Positions count: {len(closed_positions)}")
    
    assert len(open_positions) == 0
    assert len(closed_positions) == 1
    assert closed_positions[0].symbol == "RELIANCE"
    assert closed_positions[0].state.value == "CLOSED"
    print(f"Closed Position PNL: {closed_positions[0].realized_pnl}")
    
    # PNL calculation checks:
    # BUY commission = 25000 * 0.0003 = 7.5
    # SELL commission = 26000 * 0.0003 = 7.8
    # Realized PNL = (2600 - 2500) * 10 = 1000
    # Expected final equity = 100000 + 1000 - (7.5 + 7.8) = 100984.7
    expected_equity = 100984.7
    print(f"Expected Equity: {expected_equity}, Actual Equity: {sell_equity}")
    assert abs(sell_equity - expected_equity) < 0.1
    assert sell_margin == sell_equity
    assert sell_used == 0.0
    
    print("\nSUCCESS: All paper trading engine and netting verifications passed successfully!")
    
    # Clean up
    await event_bus.unsubscribe("order_filled", on_fill_2)
    await paper_trading_engine.stop_session()
    await portfolio_engine.stop()
    await trade_journal_engine.stop()
    await execution_engine.stop()
    await paper_trading_engine.stop()
    await event_bus.stop()

if __name__ == "__main__":
    asyncio.run(run_verification())
