import asyncio
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional
from fastapi import FastAPI, Depends, WebSocket, WebSocketDisconnect, HTTPException, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel
from sqlalchemy import select, desc
from sqlalchemy.ext.asyncio import AsyncSession
from core.config import settings
from core.bus import event_bus, EventModel
from core.logging import get_logger
from core.middleware import CorrelationIDMiddleware, RequestLoggingMiddleware, BasicAuthMiddleware, check_basic_auth_header
from core.startup import validate_environment, run_startup_checks, cache_startup_results, get_cached_startup_results
from database.connection import init_db, get_db_session
from database.models import OrderModel, TradeModel
from agents.manager import AgentManager
from brokers.manager import broker_manager

logger = get_logger("dashboard_backend")
agent_manager = AgentManager()

class ConnectionManager:
    """Manages active WebSockets connections to broadcast event bus streams."""
    def __init__(self) -> None:
        self.active_connections: List[WebSocket] = []
        self._lock = asyncio.Lock()

    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        async with self._lock:
            self.active_connections.append(websocket)
        logger.debug("WebSocket client connected", count=len(self.active_connections))

    async def disconnect(self, websocket: WebSocket) -> None:
        async with self._lock:
            if websocket in self.active_connections:
                self.active_connections.remove(websocket)
        logger.debug("WebSocket client disconnected", count=len(self.active_connections))

    async def broadcast(self, message: Dict[str, Any]) -> None:
        """Publishes payload to all registered sockets."""
        async with self._lock:
            connections = list(self.active_connections)

        # Event payloads carry raw datetime/Enum values (from Pydantic .model_dump()),
        # which the stdlib json encoder used by WebSocket.send_json() cannot serialize.
        encoded = jsonable_encoder(message)

        for connection in connections:
            try:
                await connection.send_json(encoded)
            except Exception:
                # Silently handle dead connection removals
                async with self._lock:
                    if connection in self.active_connections:
                        self.active_connections.remove(connection)

ws_manager = ConnectionManager()

async def ws_event_broadcaster(event: EventModel) -> None:
    """Callback linked to Event Bus to stream logs/ticks/trades to UI."""
    payload = {
        "topic": event.event_type,
        "sender": event.source_agent,
        "timestamp": event.timestamp.isoformat(),
        "data": event.payload
    }
    await ws_manager.broadcast(payload)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Orchestrates system startup and shutdown procedures."""
    logger.info("Initializing SPV Quantum AI Core Engine...")

    # 0. Validate environment variables before anything else
    try:
        validate_environment()
    except Exception as env_err:
        logger.warning(f"Environment validation warning: {env_err}")
    
    # 1. Setup DB
    await init_db()
    
    # 2. Start Priority Queue event process worker
    event_bus.start()
    
    # 3. Bind WebSocket broadcaster to event bus
    await event_bus.subscribe_all(ws_event_broadcaster)
    
    # 4. Load active broker
    from brokers import broker_engine
    await broker_engine.connect()
    await broker_manager.start_health_monitor(interval_sec=30)

    # 5. Load and start Agent loop tasks
    agent_manager.load_agents()
    await agent_manager.start_all()

    # 6. Start Indicator Intelligence Engine
    from indicators.engine import indicator_engine
    await indicator_engine.start()

    # 7. Start Market Regime Engine
    from regime.engine import regime_engine
    await regime_engine.start()
    app.state.regime_engine = regime_engine

    # 8. Start Strategy Rules Engine
    from strategies.engine import strategy_engine
    await strategy_engine.start()
    app.state.strategy_engine = strategy_engine

    # 9. Start Decision Scoring Engine
    from scoring.engine import decision_scoring_engine
    await decision_scoring_engine.start()
    app.state.decision_scoring_engine = decision_scoring_engine

    # 10. Start Market Scanner Engine
    from scanner.engine import market_scanner_engine
    from scanner.scheduler import scanner_scheduler
    await market_scanner_engine.start()
    await scanner_scheduler.start()
    app.state.market_scanner_engine = market_scanner_engine
    app.state.scanner_scheduler = scanner_scheduler

    # 11. Start Execution Engine
    from execution.engine import execution_engine
    await execution_engine.start()
    app.state.execution_engine = execution_engine

    # 12. Start Portfolio Engine
    from portfolio.engine import portfolio_engine
    await portfolio_engine.start()
    app.state.portfolio_engine = portfolio_engine

    # 13. Start Trade Journal Engine
    from journal.engine import trade_journal_engine
    await trade_journal_engine.start()
    app.state.trade_journal_engine = trade_journal_engine

    # 14. Start Backtesting Engine
    from backtest.engine import backtesting_engine
    await backtesting_engine.start()
    app.state.backtesting_engine = backtesting_engine

    # 15. Start Replay Engine
    from replay.engine import replay_engine
    await replay_engine.start()
    app.state.replay_engine = replay_engine

    # 16. Start Performance Analytics Engine
    from analytics.engine import performance_analytics_engine
    await performance_analytics_engine.start()
    app.state.performance_analytics_engine = performance_analytics_engine

    # 17. Start Paper Trading Engine
    from paper.engine import paper_trading_engine
    await paper_trading_engine.start()
    app.state.paper_trading_engine = paper_trading_engine
    
    # 18. Start Charges Cost Manager
    from charges import trade_cost_manager
    await trade_cost_manager.start()
    app.state.trade_cost_manager = trade_cost_manager
    
    # 19. Start Safety & Protection Engine
    from safety import safety_engine
    await safety_engine.start()
    app.state.safety_engine = safety_engine
    
    # 20. Start System Health Engine
    from health import system_health_engine
    await system_health_engine.start()
    app.state.system_health_engine = system_health_engine
    
    # 21. Start AI Employee Profile Engine
    from employees import employee_engine
    await employee_engine.start()
    app.state.employee_engine = employee_engine
    
    logger.info("Engine fully operational.")

    # Run startup readiness checks and cache results
    _, checks = await run_startup_checks()
    cache_startup_results(checks)

    yield
    
    # Clean shutdown
    logger.info("Shutting down core engine...")
    from employees import employee_engine
    await employee_engine.stop()
    from health import system_health_engine
    await system_health_engine.stop()
    from safety import safety_engine
    await safety_engine.stop()
    await trade_cost_manager.stop()
    await paper_trading_engine.stop()
    await performance_analytics_engine.stop()
    await replay_engine.stop_engine()
    await backtesting_engine.stop()
    await trade_journal_engine.stop()
    await portfolio_engine.stop()
    await execution_engine.stop()
    await scanner_scheduler.stop()
    await market_scanner_engine.stop()
    await decision_scoring_engine.stop()
    await strategy_engine.stop()
    await regime_engine.stop()
    await indicator_engine.stop()
    await agent_manager.stop_all()
    await broker_manager.shutdown_all()
    await event_bus.unsubscribe_all(ws_event_broadcaster)
    await event_bus.stop()
    logger.info("Core engine terminated.")

app = FastAPI(title="SPV Quantum AI Operating System", lifespan=lifespan)

# ── Middleware ────────────────────────────────────────────────────────────────
app.add_middleware(RequestLoggingMiddleware)
app.add_middleware(CorrelationIDMiddleware)
# app.add_middleware(BasicAuthMiddleware)  # temporarily disabled

# ── Global Exception Handlers ─────────────────────────────────────────────────

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """Catches unhandled exceptions and returns a structured 500 response."""
    logger.error(
        f"Unhandled exception on {request.method} {request.url.path}: {exc}",
        path=request.url.path,
        method=request.method,
        error=str(exc),
    )
    return JSONResponse(
        status_code=500,
        content={"error": "INTERNAL_SERVER_ERROR", "message": str(exc)},
    )

@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException) -> JSONResponse:
    """Returns a consistent JSON body for all HTTPExceptions."""
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": "HTTP_ERROR", "message": exc.detail, "status_code": exc.status_code},
    )

# Mount static files folder
import os
from pathlib import Path
static_dir = Path(__file__).resolve().parent / "static"
static_dir.mkdir(exist_ok=True)
(static_dir / "css").mkdir(exist_ok=True)
(static_dir / "js").mkdir(exist_ok=True)
app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

# Models for request validation
class OrderRequest(BaseModel):
    symbol: str
    side: str
    quantity: float
    price: Optional[float] = None
    type: str = "MARKET"

class EstimateRequest(BaseModel):
    symbol: str
    side: str
    quantity: float
    price: float
    broker: Optional[str] = None
    segment: Optional[str] = None

# API ENDPOINTS

@app.get("/api/status")
async def get_system_status():
    """Returns dynamic system metadata and active agent listings."""
    return {
        "status": "OPERATIONAL",
        "version": settings.yaml_config.get("system", {}).get("version", "1.0.0"),
        "environment": settings.ENVIRONMENT,
        "agents": {
            name: {
                "id": agent.agent_id,
                "status": agent.status,
                "health": await agent.health_check(),
                "input_events": agent.input_event_types,
                "output_events": agent.output_event_types
            } for name, agent in agent_manager.active_agents.items()
        }
    }

@app.get("/api/agent_stats")
async def get_agent_statistics():
    """Returns dynamic statistics, telemetry metrics, and logs for active agents."""
    return {
        name: {
            "id": agent.agent_id,
            "description": agent.description,
            "status": agent.status,
            "health": await agent.health_check(),
            "confidence_score": agent.confidence_score,
            "execution_time_ms": agent.execution_time,
            "last_decision": agent.last_decision.model_dump() if agent.last_decision else None,
            "input_events": agent.input_event_types,
            "output_events": agent.output_event_types,
            "logs": agent.logs
        } for name, agent in agent_manager.active_agents.items()
    }

@app.get("/api/orders", response_model=List[Dict[str, Any]])
async def get_orders(limit: int = 50, db: AsyncSession = Depends(get_db_session)):
    """Fetches list of placed orders from database."""
    query = select(OrderModel).order_by(desc(OrderModel.created_at)).limit(limit)
    result = await db.execute(query)
    orders = result.scalars().all()
    return [
        {
            "id": o.id,
            "broker_order_id": o.broker_order_id,
            "symbol": o.symbol,
            "side": o.side,
            "type": o.type,
            "price": o.price,
            "quantity": o.quantity,
            "status": o.status,
            "broker": o.broker,
            "created_at": o.created_at.isoformat()
        } for o in orders
    ]

@app.get("/api/trades", response_model=List[Dict[str, Any]])
async def get_trades(limit: int = 50, db: AsyncSession = Depends(get_db_session)):
    """Fetches list of executed trades from database."""
    query = select(TradeModel).order_by(desc(TradeModel.executed_at)).limit(limit)
    result = await db.execute(query)
    trades = result.scalars().all()
    return [
        {
            "id": o.id,
            "order_id": o.order_id,
            "symbol": o.symbol,
            "side": o.side,
            "price": o.price,
            "quantity": o.quantity,
            "commission": o.commission,
            "executed_at": o.executed_at.isoformat(),
            "broker": o.broker
        } for o in trades
    ]

@app.post("/api/order")
async def place_order(order: OrderRequest):
    """
    Submits a new order request to the Event Bus.
    The order flows through: Web API -> Event Bus -> Risk Agent (Check) -> Execution Agent (Execute).
    """
    if order.side.upper() not in ["BUY", "SELL"]:
        raise HTTPException(status_code=400, detail="Side must be BUY or SELL")
    if order.quantity <= 0:
        raise HTTPException(status_code=400, detail="Quantity must be greater than zero")

    event_payload = {
        "symbol": order.symbol,
        "side": order.side.upper(),
        "quantity": order.quantity,
        "price": order.price,
        "type": order.type.upper()
    }
    
    # Send order request to the event bus
    await event_bus.publish("order_request", "web_api", event_payload)
    logger.info("Order request received and dispatched to bus", symbol=order.symbol, side=order.side)
    return {"status": "SUBMITTED", "message": "Order sent to risk check pipeline."}

# WebSocket Endpoint
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    # BaseHTTPMiddleware (BasicAuthMiddleware) does not run for websocket-scope
    # connections, so this route must check credentials itself.
    if False:  # BasicAuthMiddleware temporarily disabled
        await websocket.close(code=4401)
        return

    await ws_manager.connect(websocket)
    
    async def _keepalive() -> None:
        """Sends a ping frame every 30 seconds to keep the connection alive."""
        while True:
            await asyncio.sleep(30)
            try:
                await websocket.send_json({"topic": "ping", "sender": "server", "timestamp": "", "data": {}})
            except Exception:
                break
    
    keepalive_task = asyncio.create_task(_keepalive())
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        await ws_manager.disconnect(websocket)
    except Exception as e:
        logger.error("WebSocket endpoint error", error=str(e))
        await ws_manager.disconnect(websocket)
    finally:
        keepalive_task.cancel()
        try:
            await keepalive_task
        except asyncio.CancelledError:
            pass

# HTML Dashboard Routing
@app.get("/")
async def get_dashboard():
    """Serves the main HTML dashboard template."""
    template_path = Path(__file__).resolve().parent / "templates" / "index.html"
    return FileResponse(str(template_path))

# ── Broker API Endpoints ──────────────────────────────────────────────────────

@app.get("/api/broker/status")
async def get_broker_status():
    """Returns active broker name, connection state, and latency."""
    broker = broker_manager.get_active()
    health = broker_manager.get_health().get(broker.name, {})
    return {
        "broker":      broker.name,
        "connected":   broker.is_connected(),
        "latency_ms":  health.get("latency_ms", 0.0),
        "error":       health.get("error"),
    }

@app.get("/api/broker/funds")
async def get_broker_funds():
    """Returns current equity, available margin, and used margin."""
    broker = broker_manager.get_active()
    resp = await broker.get_balance()
    if not resp.success:
        raise HTTPException(status_code=503, detail=resp.error)
    return resp.data

@app.get("/api/broker/positions")
async def get_broker_positions():
    """Returns all open intraday/carryforward positions."""
    broker = broker_manager.get_active()
    resp = await broker.get_positions()
    if not resp.success:
        raise HTTPException(status_code=503, detail=resp.error)
    return resp.data

@app.get("/api/broker/orders")
async def get_broker_orders():
    """Returns all session orders from the active broker."""
    broker = broker_manager.get_active()
    resp = await broker.get_orders()
    if not resp.success:
        raise HTTPException(status_code=503, detail=resp.error)
    return resp.data

@app.get("/api/broker/health")
async def get_broker_health():
    """Pings the active broker and returns fresh latency + status."""
    health = await broker_manager.check_health()
    return health

# ── Market Data Engine API Endpoints ─────────────────────────────────────────
from market.manager import market_data_manager
from market.models import Timeframe

@app.get("/api/market/feed")
async def get_feed_status():
    """Returns feed connection status, health metrics, and current session state."""
    stats = await market_data_manager.get_feed_health()
    return {
        "feed_status": stats,
        "market_session": market_data_manager.status.get_status().value,
        "stream_connected": market_data_manager.stream.is_connected(),
    }

@app.get("/api/market/price/{symbol}")
async def get_current_price(symbol: str):
    """Returns the latest LTP and full tick snapshot for a symbol."""
    tick = await market_data_manager.cache.get_tick(symbol.upper())
    if not tick:
        raise HTTPException(status_code=404, detail=f"No data for {symbol}")
    return tick.model_dump()

@app.get("/api/market/candle/{symbol}/{timeframe}")
async def get_latest_candle(symbol: str, timeframe: str):
    """Returns the current (latest) candle for a symbol and timeframe."""
    try:
        tf = Timeframe(timeframe)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid timeframe: {timeframe}")
    candle = await market_data_manager.cache.get_candle(symbol.upper(), tf)
    if not candle:
        raise HTTPException(status_code=404, detail=f"No candle for {symbol}/{timeframe}")
    return candle.model_dump()

@app.get("/api/market/session/{symbol}")
async def get_session_summary(symbol: str):
    """Returns session high, low, VWAP, volume, OI, and previous close."""
    return await market_data_manager.get_session_summary(symbol.upper())

@app.get("/api/market/instruments")
async def get_instruments():
    """Returns all registered instruments and their specifications."""
    return market_data_manager.instruments.get_all()

@app.get("/api/market/symbols")
async def get_symbols():
    """Returns the set of currently tracked symbols."""
    return {"symbols": list(market_data_manager.registry.get_symbols())}

@app.get("/api/market/segments")
async def get_segments():
    """Returns each symbol's asset-class segment (INDEX/EQUITY/COMMODITY/CURRENCY/SPOT)."""
    return {
        symbol: meta.get("segment")
        for symbol, meta in market_data_manager.registry.get_all_meta().items()
    }

@app.get("/api/market/snapshot")
async def get_market_snapshot():
    """
    Bulk read of every symbol's latest known tick, keyed by symbol. Used to
    populate the dashboard on load without one request per symbol; live
    updates after that flow over the WebSocket as usual.
    """
    ticks = await market_data_manager.cache.get_all_ticks()
    snapshot = {}
    for symbol, tick in ticks.items():
        prev_close = tick.prev_close or tick.close or tick.ltp
        change = tick.ltp - prev_close if prev_close else 0.0
        change_pct = (change / prev_close * 100.0) if prev_close else 0.0
        snapshot[symbol] = {
            "ltp": tick.ltp,
            "change": round(change, 2),
            "change_pct": round(change_pct, 2),
            "volume": tick.volume,
            "timestamp": tick.timestamp.isoformat(),
        }
    return snapshot

# ── Indicator Intelligence Engine API Endpoints ───────────────────────────────
from indicators.engine import indicator_engine as _ind_engine
from indicators.registry import INDICATOR_REGISTRY

@app.get("/api/indicators/registry")
async def get_indicator_registry():
    """Returns the catalogue of all supported indicators."""
    return INDICATOR_REGISTRY

@app.get("/api/indicators/latest/{symbol}/{timeframe}")
async def get_latest_indicators(symbol: str, timeframe: str):
    """Returns all latest indicator values for a symbol/timeframe."""
    try:
        tf = Timeframe(timeframe)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid timeframe: {timeframe}")
    latest = await _ind_engine.cache.get_all_latest(symbol.upper(), tf)
    return {
        name: {
            "value":        r.value,
            "calc_time_ms": r.calc_time_ms,
            "timestamp":    r.timestamp.isoformat(),
        }
        for name, r in latest.items()
    }

@app.get("/api/indicators/latest/{symbol}/{timeframe}/{indicator}")
async def get_one_indicator(symbol: str, timeframe: str, indicator: str):
    """Returns the latest value for one specific indicator."""
    try:
        tf = Timeframe(timeframe)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid timeframe: {timeframe}")
    r = await _ind_engine.cache.get_latest(symbol.upper(), tf, indicator.upper())
    if not r:
        raise HTTPException(status_code=404, detail=f"No data for {indicator} on {symbol}/{timeframe}")
    return {
        "indicator":    r.indicator_name,
        "symbol":       r.symbol,
        "timeframe":    r.timeframe.value,
        "value":        r.value,
        "calc_time_ms": r.calc_time_ms,
        "timestamp":    r.timestamp.isoformat(),
        "metadata":     r.metadata,
    }

# ── Market Regime Engine API Endpoints ────────────────────────────────────────
from regime.models import MarketRegime

@app.get("/api/regime/{symbol}/{timeframe}")
async def get_current_regime(symbol: str, timeframe: str, request: Any = None):
    """Returns the latest classified market regime for a symbol/timeframe."""
    from starlette.requests import Request
    try:
        tf = Timeframe(timeframe)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid timeframe: {timeframe}")
    # Access regime engine stored on app state during lifespan
    regime_engine = getattr(app.state, "regime_engine", None)
    if not regime_engine:
        raise HTTPException(status_code=503, detail="Regime engine not yet started")
    result = await regime_engine.cache.get_latest(symbol.upper(), tf)
    if not result:
        # Force-classify now if no cached result exists
        result = await regime_engine.classify_now(symbol.upper(), tf)
    if not result:
        raise HTTPException(status_code=404, detail=f"No regime data for {symbol}/{timeframe}")
    return {
        "symbol":             result.symbol,
        "timeframe":          result.timeframe.value,
        "market_regime":      result.market_regime.value,
        "confidence":         result.confidence,
        "reason":             result.reason,
        "supporting_factors": result.supporting_factors,
        "last_updated":       result.timestamp.isoformat(),
    }

@app.get("/api/regime/all")
async def get_all_regimes():
    """Returns latest regime for all tracked symbol/timeframe pairs."""
    regime_engine = getattr(app.state, "regime_engine", None)
    if not regime_engine:
        raise HTTPException(status_code=503, detail="Regime engine not yet started")
    all_latest = await regime_engine.cache.get_all_latest()
    return {
        f"{sym}_{tf}": {
            "market_regime": r.market_regime.value,
            "confidence":    r.confidence,
            "reason":        r.reason,
            "last_updated":  r.timestamp.isoformat(),
        }
        for (sym, tf), r in all_latest.items()
    }

# ── Enterprise Risk Management Engine API Endpoints ─────────────────────────
from risk.engine import risk_engine as _re

@app.get("/api/risk/metrics")
async def get_risk_metrics():
    """Returns all current risk status and dashboard metrics."""
    return await _re.get_dashboard_metrics()

# ── Strategy Rules Engine API Endpoints ───────────────────────────────────────
from strategies.engine import strategy_engine as _se

@app.get("/api/strategies/all")
async def get_all_strategies():
    """Returns all loaded strategies from the registry."""
    return [
        {
            "name": s.name,
            "version": s.version,
            "description": s.description,
            "enabled": s.enabled,
            "rules": s.rules.model_dump(),
            "actions": s.actions
        }
        for s in _se.registry.get_all()
    ]

@app.get("/api/strategies/active")
async def get_active_strategies():
    """Returns only currently active/enabled strategies."""
    return [
        {
            "name": s.name,
            "version": s.version,
            "description": s.description,
            "enabled": s.enabled
        }
        for s in _se.registry.get_active()
    ]

@app.post("/api/strategies/reload")
async def reload_strategies():
    """Triggers hot-reload of strategies from YAML configurations."""
    _se.loader.hot_reload()
    return {"status": "SUCCESS", "message": f"Reloaded {_se.loader.directory} configs."}

@app.post("/api/strategies/toggle/{name}")
async def toggle_strategy(name: str, enabled: bool):
    """Enables or disables a specific strategy by name."""
    success = _se.registry.set_enabled(name, enabled)
    if not success:
        raise HTTPException(status_code=404, detail=f"Strategy '{name}' not found.")
    return {"status": "SUCCESS", "message": f"Strategy '{name}' enabled set to {enabled}."}

@app.post("/api/strategies/evaluate/{symbol}/{timeframe}")
async def evaluate_strategies_now(symbol: str, timeframe: str):
    """Runs immediate rule evaluation for a symbol and timeframe."""
    try:
        tf = Timeframe(timeframe)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid timeframe: {timeframe}")
    results = await _se.evaluate_all(symbol.upper(), tf)
    return [r.model_dump() for r in results]

# ── Market Analysis Intelligence Layer API Endpoints ────────────────────────
from analysis.engine import market_analysis_engine as _mae

@app.get("/api/analysis/{symbol}/{timeframe}")
async def get_market_analysis(symbol: str, timeframe: str):
    """Returns the latest market analysis report for a symbol/timeframe."""
    try:
        tf = Timeframe(timeframe)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid timeframe: {timeframe}")
    
    report = await _mae.cache.get_latest(symbol.upper(), timeframe)
    if not report:
        # Generate analysis now if not cached
        report = await _mae.analyze_market(symbol.upper(), tf)
        
    return report.model_dump()

@app.get("/api/analysis/all")
async def get_all_market_analyses():
    """Returns all latest market analysis reports from the cache."""
    cache_dict = await _mae.cache.get_all_latest()
    return {
        f"{sym}_{tf}": report.model_dump()
        for (sym, tf), report in cache_dict.items()
    }

# ── Decision Scoring Engine API Endpoints ────────────────────────────────────
from scoring.engine import decision_scoring_engine as _dse

@app.get("/api/scoring/{symbol}/{timeframe}")
async def get_decision_score(symbol: str, timeframe: str):
    """Returns the latest decision score report for a symbol/timeframe."""
    try:
        tf = Timeframe(timeframe)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid timeframe: {timeframe}")
    
    score = await _dse.get_latest(symbol.upper(), timeframe)
    if not score:
        score = await _dse.evaluate_decision(symbol.upper(), tf)
        
    return score.model_dump()

@app.get("/api/scoring/all")
async def get_all_decision_scores():
    """Returns all latest decision scores from the cache."""
    cache_dict = await _dse.get_all_latest()
    return {
        f"{sym}_{tf}": score.model_dump()
        for (sym, tf), score in cache_dict.items()
    }

# ── Market Scanner Engine API Endpoints ───────────────────────────────────────
from scanner.engine import market_scanner_engine as _mse

@app.get("/api/scanner/health")
async def get_scanner_health():
    """Returns health status and the execution duration of the latest scan."""
    return {
        "status": _mse.health_status,
        "scan_time_ms": _mse.scan_time_ms,
        "active_scanners_count": len(_mse.registry.get_active())
    }

@app.get("/api/scanner/configs")
async def get_scanner_configs():
    """Returns the configurations of all loaded scanners."""
    return [c.model_dump() for c in _mse.registry.get_all()]

@app.get("/api/scanner/matches")
async def get_scanner_matches():
    """Returns all currently cached scanning opportunities matched."""
    matches_dict = await _mse.cache.get_all_matches()
    return {
        scanner_name: [r.model_dump() for r in results]
        for scanner_name, results in matches_dict.items()
    }

@app.post("/api/scanner/reload")
async def reload_scanner_configs():
    """Triggers hot-reload of scanner configurations from YAML."""
    _mse.registry.hot_reload()
    return {"status": "SUCCESS", "message": f"Reloaded {_mse.registry.directory} configs."}

@app.post("/api/scanner/toggle/{name}")
async def toggle_scanner(name: str, enabled: bool):
    """Enables or disables a specific scanner configuration."""
    success = _mse.registry.set_enabled(name, enabled)
    if not success:
        raise HTTPException(status_code=404, detail=f"Scanner '{name}' not found.")
    return {"status": "SUCCESS", "message": f"Scanner '{name}' enabled set to {enabled}."}

@app.post("/api/scanner/scan")
async def trigger_scan_now():
    """Triggers an immediate scan run and returns matched results."""
    results = await _mse.run_scan()
    return [r.model_dump() for r in results]

# ── Enterprise Execution Engine API Endpoints ────────────────────────────────
from execution.engine import execution_engine as _ee

@app.get("/api/execution/metrics")
async def get_execution_metrics():
    """Returns latency response time, execution queue state, and ordered lists."""
    return await _ee.get_dashboard_metrics()

@app.post("/api/execution/submit")
async def submit_order_now(order_data: Dict[str, Any]):
    """Submits an execution order request directly to the execution pipeline."""
    order = await _ee.submit_order_request(order_data)
    return order.model_dump()

# ── Portfolio & Position Management Engine API Endpoints ─────────────────────
from portfolio.engine import portfolio_engine as _pe

@app.get("/api/portfolio/summary")
async def get_portfolio_summary():
    """Returns dynamic portfolio summary (MTM, realized/unrealized PNL, capital & margins)."""
    # Trigger recalculation
    summary = await _pe.recalculate_summary()
    return summary.model_dump()

@app.get("/api/portfolio/positions")
async def get_portfolio_positions():
    """Returns open and closed positions details."""
    open_pos = [p.model_dump() for p in await _pe.positions.get_open_positions()]
    closed_pos = [p.model_dump() for p in await _pe.positions.get_closed_positions()]
    return {
        "open_positions": open_pos,
        "closed_positions": closed_pos
    }

# ── Trade Journal & Audit Engine API Endpoints ───────────────────────────────
from journal.engine import trade_journal_engine as _tje

@app.get("/api/journal/trades")
async def get_journal_trades(
    strategy: Optional[str] = None,
    segment: Optional[str] = None,
    pnl_min: Optional[float] = None,
    pnl_max: Optional[float] = None
):
    """Lists all trade records matching optional filter specifications."""
    filters = {
        "strategy": strategy,
        "segment": segment,
        "pnl_min": pnl_min,
        "pnl_max": pnl_max
    }
    trades = await _tje.repo.search_trades(filters)
    return [t.model_dump() for t in trades]

@app.get("/api/journal/audits")
async def get_journal_audits():
    """Lists all recorded decision audits."""
    audits = await _tje.repo.get_all_audits()
    return [a.model_dump() for a in audits]

@app.get("/api/journal/stats")
async def get_journal_stats():
    """Compiles total trades, win rates, daily, weekly, monthly, and strategy summaries."""
    return await _tje.get_performance_stats()

# ── Backtesting Engine API Endpoints ──────────────────────────────────────────
from backtest.engine import backtesting_engine as _bte
from backtest.models import BacktestConfig as _BacktestConfig

@app.get("/api/backtest/status")
async def get_backtest_status():
    """Returns backtest progress and simulation state."""
    return await _bte.get_dashboard_status()

@app.post("/api/backtest/run")
async def run_backtest(config: _BacktestConfig):
    """Triggers an asynchronous backtest run."""
    backtest_id = await _bte.run_backtest(config)
    return {"status": "SUCCESS", "backtest_id": backtest_id}

@app.post("/api/backtest/stop")
async def stop_backtest():
    """Cancels the active backtest simulation."""
    await _bte.stop()
    return {"status": "SUCCESS", "message": "Backtest cancelled successfully."}

@app.get("/api/backtest/result")
async def get_backtest_result():
    """Returns the last completed backtest's metrics and a plain-language
    profitable/not-profitable verdict."""
    return _bte.last_result or {"backtest_id": None, "metrics": {}, "verdict": None}

# ── Simplified Trading Controls (mode, active symbol, broker, pending confirmations) ──

class TradingModeUpdate(BaseModel):
    mode: str  # AUTO or MANUAL

@app.get("/api/trading/mode")
async def get_trading_mode():
    from trading.mode import trading_mode_manager
    return {"mode": trading_mode_manager.get_mode()}

@app.post("/api/trading/mode")
async def set_trading_mode(payload: TradingModeUpdate):
    from trading.mode import trading_mode_manager
    try:
        trading_mode_manager.set_mode(payload.mode)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {"status": "SUCCESS", "mode": trading_mode_manager.get_mode()}

@app.get("/api/trading/pending")
async def get_pending_trades():
    """Decisions APPROVED while in MANUAL mode, awaiting user confirmation."""
    from trading.mode import trading_mode_manager
    return trading_mode_manager.get_pending()

@app.post("/api/trading/confirm/{decision_id}")
async def confirm_pending_trade(decision_id: str):
    from trading.mode import trading_mode_manager
    from agents.chief_decision_agent import DecisionPublisher
    record = trading_mode_manager.pop_pending(decision_id)
    if not record:
        raise HTTPException(status_code=404, detail="No pending decision with that id.")
    await DecisionPublisher().publish_confirmed_order(record)
    return {"status": "SUCCESS", "message": f"Order confirmed for {record.get('symbol')}."}

@app.post("/api/trading/reject/{decision_id}")
async def reject_pending_trade(decision_id: str):
    from trading.mode import trading_mode_manager
    record = trading_mode_manager.pop_pending(decision_id)
    if not record:
        raise HTTPException(status_code=404, detail="No pending decision with that id.")
    return {"status": "SUCCESS", "message": f"Order rejected for {record.get('symbol')}."}


class ActiveSymbolUpdate(BaseModel):
    symbol: str

@app.get("/api/trading/active-symbol")
async def get_active_symbol():
    from trading.context import trading_context_manager
    return {"symbol": trading_context_manager.get_active_symbol()}

@app.post("/api/trading/active-symbol")
async def set_active_symbol(payload: ActiveSymbolUpdate):
    from trading.context import trading_context_manager
    trading_context_manager.set_active_symbol(payload.symbol)
    return {"status": "SUCCESS", "symbol": trading_context_manager.get_active_symbol()}


class BrokerSwitchRequest(BaseModel):
    broker: str  # paper_broker or kotak_neo

@app.get("/api/trading/broker")
async def get_active_broker_mode():
    from brokers.manager import broker_manager
    return {"broker": broker_manager.get_active().name}

@app.post("/api/trading/broker")
async def switch_active_broker(payload: BrokerSwitchRequest):
    """Hot-switches the live broker (paper vs real) — takes effect
    immediately, unlike the generic /api/settings form which only persists
    config for the next restart."""
    from brokers.manager import broker_manager
    from core.config import settings
    if payload.broker not in ["paper_broker", "kotak_neo"]:
        raise HTTPException(status_code=400, detail="Invalid broker choice.")
    await broker_manager.switch_broker(payload.broker)
    settings.yaml_config.setdefault("brokers", {})["active"] = payload.broker
    settings.save_yaml_config()
    return {"status": "SUCCESS", "broker": payload.broker}


@app.get("/api/employees/relevant")
async def get_relevant_employees():
    """Returns only the employees actually relevant to the active strategy
    and active symbol's segment, instead of all 30."""
    from strategies.engine import strategy_engine
    from trading.context import trading_context_manager
    from market.manager import market_data_manager
    from employees.relevance import get_relevant_employee_codes

    active_strategies = strategy_engine.registry.get_active()
    if not active_strategies:
        return {"relevant_codes": [], "employees": []}
    strategy = active_strategies[0]

    symbol = trading_context_manager.get_active_symbol()
    meta = market_data_manager.registry.get_meta(symbol) if symbol else None
    segment = (meta or {}).get("segment", "EQUITY")

    relevant_codes = get_relevant_employee_codes(strategy, segment=segment)

    from employees.engine import employee_engine
    all_profiles = employee_engine.manager.profiles
    employees = [p.model_dump() for code, p in all_profiles.items() if code in relevant_codes]
    return {"strategy_name": strategy.name, "segment": segment, "relevant_codes": relevant_codes, "employees": employees}

# ── Market Replay Engine API Endpoints ────────────────────────────────────────
from replay.engine import replay_engine as _rpe
from replay.models import ReplayConfig as _ReplayConfig

@app.get("/api/replay/status")
async def get_replay_status():
    """Returns current replay status metrics."""
    return await _rpe.get_dashboard_status()

@app.post("/api/replay/setup")
async def setup_replay(config: _ReplayConfig):
    """Loads historical data and resets simulation state."""
    replay_id = await _rpe.setup_replay(config)
    return {"status": "SUCCESS", "replay_id": replay_id}

@app.post("/api/replay/play")
async def play_replay():
    """Starts sequential playback loop."""
    await _rpe.play()
    return {"status": "SUCCESS"}

@app.post("/api/replay/pause")
async def pause_replay():
    """Pauses playback loop."""
    await _rpe.pause()
    return {"status": "SUCCESS"}

@app.post("/api/replay/resume")
async def resume_replay():
    """Resumes playback loop."""
    await _rpe.resume()
    return {"status": "SUCCESS"}

@app.post("/api/replay/stop")
async def stop_replay():
    """Stops playback loop."""
    await _rpe.stop()
    return {"status": "SUCCESS"}

@app.post("/api/replay/next")
async def next_replay_candle():
    """Manually steps forward by 1 candle (when paused)."""
    await _rpe.next_candle()
    return {"status": "SUCCESS"}

@app.post("/api/replay/previous")
async def previous_replay_candle():
    """Manually steps backward by 1 candle (when paused)."""
    await _rpe.previous_candle()
    return {"status": "SUCCESS"}

@app.post("/api/replay/speed")
async def set_replay_speed(speed: str):
    """Changes playback speed factor (e.g. 1x, 5x, 100x)."""
    await _rpe.set_speed(speed)
    return {"status": "SUCCESS"}

# ── Performance Analytics Engine API Endpoints ───────────────────────────────
from analytics.engine import performance_analytics_engine as _pae

@app.get("/api/analytics/summary")
async def get_performance_summary():
    """Returns dynamic trading performance metrics and rankings."""
    metrics = await _pae.recalculate_metrics()
    return metrics.model_dump()

@app.get("/api/analytics/report")
async def get_performance_report(type: str = "Portfolio"):
    """Generates detailed performance report including equity & drawdown curves."""
    report = await _pae.generate_report(type)
    return report.model_dump()

# ── Chief Decision Agent API Endpoints ───────────────────────────────────────

@app.get("/api/chief/status")
async def get_chief_status():
    """Returns lists of approved, rejected, and blocked trades from the Chief Decision Agent."""
    chief = agent_manager.active_agents.get("chief_decision_agent")
    if not chief:
        return {"status": "ERROR", "message": "ChiefDecisionAgent not loaded."}
        
    return {
        "approved_trades": chief.approved_queue,
        "rejected_trades": chief.rejected_queue,
        "blocked_trades": chief.blocked_queue
    }

@app.get("/api/chief/history")
async def get_chief_history():
    """Compiles the full history of all decision coordinator entries."""
    chief = agent_manager.active_agents.get("chief_decision_agent")
    if not chief:
        return []
    # History is the combination of all queues
    return chief.approved_queue + chief.rejected_queue + chief.blocked_queue

# ── Paper Trading Engine API Endpoints ────────────────────────────────────────
from paper.engine import paper_trading_engine as _pte
from paper.models import PaperTradingConfig as _PaperTradingConfig

@app.get("/api/paper/status")
async def get_paper_status():
    """Returns dynamic virtual portfolio status, capital, PNL, and win rates."""
    return await _pte.get_dashboard_status()

@app.post("/api/paper/start")
async def start_paper_session(config: _PaperTradingConfig):
    """Triggers and launches a virtual paper trading session."""
    session_id = await _pte.start_session(config)
    return {"status": "SUCCESS", "session_id": session_id}

@app.post("/api/paper/stop")
async def stop_paper_session():
    """Cancels and stops the active paper trading session."""
    await _pte.stop_session()
    return {"status": "SUCCESS"}

# ── Charges & Cost Engine API Endpoints ───────────────────────────────────────
from charges import trade_cost_manager as _tcm, charges_engine as _ce

@app.get("/api/charges/status")
async def get_charges_status():
    """Returns dynamic virtual portfolio charges, capital, PNL, and breakdowns."""
    return await _tcm.get_dashboard_summary()

@app.post("/api/charges/estimate")
async def estimate_charges(req: EstimateRequest):
    """Calculates potential charges and costs for a hypothetical trade execution."""
    profile = await _ce.get_active_profile()
    if req.broker:
        profile = _ce.profiles.get(req.broker.lower(), profile)
        
    segment = req.segment
    if not segment:
        sym = req.symbol.upper()
        if sym.endswith("FUT") or "FUT" in sym:
            segment = "Futures"
        elif any(x in sym for x in ["CE", "PE", "OPT"]):
            segment = "Options"
        else:
            segment = "Equity Intraday"
            
    from charges.calculators import BrokerageCalculator, TaxCalculator, ExchangeChargeCalculator
    brokerage = BrokerageCalculator.calculate(profile, segment, req.side, req.quantity, req.price)
    stt = TaxCalculator.calculate_stt(profile, segment, req.side, req.quantity, req.price)
    stamp_duty = TaxCalculator.calculate_stamp_duty(profile, segment, req.side, req.quantity, req.price)
    sebi = TaxCalculator.calculate_sebi_charges(profile, req.quantity, req.price)
    exchange_txn = ExchangeChargeCalculator.calculate_exchange_txn(profile, segment, req.quantity, req.price)
    dp_charges = ExchangeChargeCalculator.calculate_dp_charges(profile, segment, req.side)
    gst = TaxCalculator.calculate_gst(profile, brokerage, exchange_txn)
    total = brokerage + stt + exchange_txn + gst + sebi + stamp_duty + dp_charges
    
    return {
        "brokerage": round(brokerage, 4),
        "stt": round(stt, 4),
        "exchange_txn": round(exchange_txn, 4),
        "gst": round(gst, 4),
        "sebi": round(sebi, 4),
        "stamp_duty": round(stamp_duty, 4),
        "dp_charges": round(dp_charges, 4),
        "total_charges": round(total, 4)
    }


@app.get("/api/broker/full-status")
async def get_broker_full_status():
    """Returns full details about active broker: connection state, health, funds, margin, orders and positions."""
    from brokers import broker_engine
    from brokers.resolver import BrokerResolver
    from brokers.manager import broker_manager
    from portfolio.engine import portfolio_engine
    from execution.engine import execution_engine

    active_name = BrokerResolver.resolve_active_name()
    state = broker_engine.get_broker_state(active_name)
    
    # Query active broker details
    active_broker = broker_manager.get_active()
    session_status = getattr(getattr(active_broker, "session_mgr", None), "session_status", "UNKNOWN")
    
    profile_data = {}
    profile_resp = await active_broker.get_profile()
    if profile_resp.success and profile_resp.data:
        profile_data = profile_resp.data

    funds_data = {}
    funds_resp = await broker_engine.get_funds()
    if funds_resp.success and funds_resp.data:
        funds_data = funds_resp.data

    margin_data = {}
    margin_resp = await broker_engine.get_margin()
    if margin_resp.success and margin_resp.data:
        margin_data = margin_resp.data

    health = broker_manager.get_health().get(active_name, {"connected": False, "latency_ms": -1})
    
    pos_list = [p.model_dump() for p in await portfolio_engine.positions.get_all_positions()]
    exec_metrics = await execution_engine.get_dashboard_metrics()
    orders_list = exec_metrics.get("completed_orders", []) + exec_metrics.get("open_orders", [])

    return {
        "connected_broker": active_name,
        "connection_status": state,
        "broker_status": state,
        "session_status": session_status,
        "account_information": profile_data,
        "available_funds": funds_data,
        "margins": margin_data,
        "connection_health": {
            "is_healthy": health.get("connected", False),
            "latency_ms": health.get("latency_ms", 0.0),
            "error": health.get("error")
        },
        "orders": orders_list,
        "positions": pos_list
    }


@app.get("/api/safety/status")
async def get_safety_status():
    """Returns details about Safety Status, Today's Risk, Current Exposure, Emergency, Trailing and Hidden SL."""
    from safety import safety_engine
    return await safety_engine.get_dashboard_metrics()

@app.post("/api/safety/emergency/kill")
async def trigger_kill_switch(req: dict = None):
    """Triggers system-wide emergency kill switch and closes all positions."""
    from safety import safety_engine
    reason = (req or {}).get("reason", "Manual emergency switch triggered")
    await safety_engine.manager.emergency.trigger_kill_switch(reason)
    return {"status": "success", "message": f"Emergency kill switch triggered: {reason}"}

@app.post("/api/safety/emergency/reset")
async def reset_kill_switch():
    """Resets system emergency kill switch."""
    from safety import safety_engine
    await safety_engine.manager.emergency.reset_kill_switch()
    return {"status": "success", "message": "Emergency kill switch reset."}

@app.post("/api/safety/emergency/pause")
async def pause_trading():
    """Pauses trading operations."""
    from safety import safety_engine
    await safety_engine.manager.emergency.pause_trading()
    return {"status": "success", "message": "Trading paused."}

@app.post("/api/safety/emergency/resume")
async def resume_trading():
    """Resumes trading operations."""
    from safety import safety_engine
    await safety_engine.manager.emergency.resume_trading()
    return {"status": "success", "message": "Trading resumed."}

@app.get("/api/volume_intelligence/status")
async def get_volume_intelligence_status(symbol: Optional[str] = None):
    """Returns the latest volume intelligence analysis results."""
    from employees.engine import employee_engine
    from datetime import datetime, timezone
    vi = employee_engine.volume_intelligence
    if symbol:
        res = vi.latest_results.get(symbol)
        if not res:
            return {
                "symbol": symbol,
                "volume_score": 0.0,
                "rvol": 1.0,
                "volume_trend": "NEUTRAL",
                "confirmation_status": "NEUTRAL",
                "confidence": 50.0,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
        return res
    return vi.latest_results


@app.get("/api/option_flow/status")
async def get_option_flow_status(symbol: Optional[str] = None):
    """Returns the latest option flow analysis results."""
    from employees.engine import employee_engine
    from datetime import datetime, timezone
    of_mgr = employee_engine.option_flow
    if symbol:
        res = of_mgr.latest_results.get(symbol)
        if not res:
            return {
                "underlying": symbol,
                "atm_strike": 0.0,
                "ce_strength": 0.0,
                "pe_strength": 0.0,
                "pcr": 1.0,
                "option_flow_score": 50.0,
                "classification": "SIDEWAYS",
                "confidence": 50.0,
                "strength": 0.0,
                "risk": "MEDIUM",
                "recommendation": "WAIT",
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
        return res
    return of_mgr.latest_results


@app.get("/api/trend_intelligence/status")
async def get_trend_intelligence_status(symbol: Optional[str] = None):
    """Returns the latest trend intelligence analysis results."""
    from employees.engine import employee_engine
    from datetime import datetime, timezone
    trend_mgr = employee_engine.trend_intelligence
    if symbol:
        res = trend_mgr.latest_results.get(symbol)
        if not res:
            return {
                "symbol": symbol,
                "timeframe": "1m",
                "trend": "NO TRADE",
                "confidence": 0.0,
                "trend_strength": "Weak Trend",
                "momentum_score": 50.0,
                "direction": "NEUTRAL",
                "recommendation": "WAIT",
                "ema_alignment": "NONE",
                "vwap_status": "NONE",
                "adx": 0.0,
                "rsi": 50.0,
                "macd": {"macd": 0.0, "signal": 0.0, "histogram": 0.0},
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
        return res
    return trend_mgr.latest_results


@app.get("/api/health/status")
async def get_system_health():
    """Returns details about overall system health, uptime, cpu/memory usage, latency and queue states."""
    from health import system_health_engine
    return await system_health_engine.get_dashboard_metrics()


class SystemSettingsUpdate(BaseModel):
    active_broker: str
    client_id: str
    api_key: str
    max_daily_loss: float
    max_exposure: float


@app.get("/api/settings")
async def get_system_settings():
    """Returns the current system, broker and risk configurations from settings.yaml."""
    from core.config import settings
    
    brokers_cfg = settings.yaml_config.get("brokers", {})
    kotak_cfg = brokers_cfg.get("kotak_neo", {})
    risk_cfg = settings.yaml_config.get("risk_limits", {})
    
    return {
        "active_broker": brokers_cfg.get("active", "paper_broker"),
        "client_id": kotak_cfg.get("client_id", ""),
        "api_key": kotak_cfg.get("api_key", ""),
        "max_daily_loss": risk_cfg.get("daily_loss_limit_usd", 500.0),
        "max_exposure": risk_cfg.get("max_position_size_usd", 10000.0)
    }


@app.post("/api/settings")
async def update_system_settings(payload: SystemSettingsUpdate):
    """Updates settings.yaml with the new credentials/broker selections and reloads configuration in memory."""
    from core.config import settings
    from core.bus import event_bus, EventModel
    
    try:
        if payload.active_broker not in ["paper_broker", "kotak_neo"]:
            raise HTTPException(status_code=400, detail="Invalid active broker choice.")
            
        if "brokers" not in settings.yaml_config:
            settings.yaml_config["brokers"] = {}
        settings.yaml_config["brokers"]["active"] = payload.active_broker
        
        if "kotak_neo" not in settings.yaml_config["brokers"]:
            settings.yaml_config["brokers"]["kotak_neo"] = {}
        settings.yaml_config["brokers"]["kotak_neo"]["enabled"] = (payload.active_broker == "kotak_neo")
        settings.yaml_config["brokers"]["kotak_neo"]["client_id"] = payload.client_id
        settings.yaml_config["brokers"]["kotak_neo"]["api_key"] = payload.api_key
        
        if "risk_limits" not in settings.yaml_config:
            settings.yaml_config["risk_limits"] = {}
        settings.yaml_config["risk_limits"]["daily_loss_limit_usd"] = payload.max_daily_loss
        settings.yaml_config["risk_limits"]["max_position_size_usd"] = payload.max_exposure
        
        settings.save_yaml_config()
        
        from risk.engine import risk_engine
        risk_engine.config = settings.yaml_config.get("risk_limits", {})
        
        await event_bus.publish(EventModel(
            event_type="system_config_updated",
            source_agent="dashboard_api",
            payload={
                "active_broker": payload.active_broker,
                "daily_loss_limit_usd": payload.max_daily_loss,
                "max_position_size_usd": payload.max_exposure
            }
        ))
        
        return {"success": True, "message": "Settings updated and reloaded successfully."}
    except Exception as ex:
        raise HTTPException(status_code=500, detail=str(ex))


@app.get("/api/employees")
async def get_employees(tenant_id: Optional[str] = None):
    """Returns details for all registered AI Employees, filtered by SaaS tenant ID if supplied."""
    from employees import employee_engine
    return await employee_engine.get_dashboard_metrics(tenant_id)

@app.post("/api/employees/activate")
async def activate_employee(code: str):
    """Activates an AI Employee by code and configures Safety Engine thresholds."""
    from employees import employee_engine
    success = await employee_engine.activate_employee(code)
    if not success:
        return {"status": "error", "message": f"AI Employee profile '{code}' not found."}
    return {"status": "success", "message": f"AI Employee '{code}' activated."}

@app.post("/api/employees/state")
async def set_employee_state(code: str, state: str):
    """Updates an AI Employee state (ACTIVE, PAUSED, DISABLED, etc.)."""
    from employees import employee_engine
    from employees.models import EmployeeState
    try:
        emp_state = EmployeeState(state.upper())
    except ValueError:
        return {"status": "error", "message": f"Invalid state '{state}'."}
        
    success = await employee_engine.manager.set_employee_state(code, emp_state)
    if not success:
        return {"status": "error", "message": f"AI Employee profile '{code}' not found."}
    return {"status": "success", "message": f"AI Employee '{code}' transitioned to state '{state}'."}

@app.post("/api/employees/allocation")
async def set_employee_allocation(code: str, allocation: float):
    """Updates capital allocation limit for an AI Employee."""
    from employees import employee_engine
    success = await employee_engine.manager.update_allocation(code, allocation)
    if not success:
        return {"status": "error", "message": f"AI Employee profile '{code}' not found."}
    return {"status": "success", "message": f"Capital allocation updated for employee '{code}'."}


# ── Production Readiness & Metrics Endpoints ─────────────────────────────────

import time as _time
import psutil as _psutil

@app.get("/api/readiness")
async def get_readiness():
    """
    Production readiness probe.
    Returns 200 when all startup checks passed, 503 when any check failed.
    Used by load balancers and container orchestrators (K8s readinessProbe).
    """
    checks, startup_ts = get_cached_startup_results()
    all_passed = all(c.get("status") == "PASS" for c in checks) if checks else True
    return JSONResponse(
        status_code=200 if all_passed else 503,
        content={
            "ready": all_passed,
            "startup_timestamp": startup_ts,
            "uptime_sec": round(_time.time() - startup_ts, 1) if startup_ts else 0,
            "checks": checks,
        },
    )


@app.get("/api/metrics")
async def get_system_metrics():
    """
    System metrics endpoint for monitoring.
    Returns CPU, memory, event bus queue size, and active WebSocket connections.
    """
    proc = _psutil.Process()
    mem = proc.memory_info()
    cpu_pct = _psutil.cpu_percent(interval=None)
    mem_pct = _psutil.virtual_memory().percent

    queue_size = event_bus._queue.qsize() if event_bus._queue else 0
    ws_connections = len(ws_manager.active_connections)

    from execution.engine import execution_engine
    exec_metrics = await execution_engine.get_dashboard_metrics()

    return {
        "system": {
            "cpu_usage_pct": cpu_pct,
            "memory_usage_pct": mem_pct,
            "process_rss_mb": round(mem.rss / (1024 * 1024), 2),
            "process_vms_mb": round(mem.vms / (1024 * 1024), 2),
        },
        "event_bus": {
            "queue_size": queue_size,
            "is_running": event_bus._is_running,
            "inflight_tasks": len(event_bus._inflight_tasks),
        },
        "websocket": {
            "active_connections": ws_connections,
        },
        "execution": {
            "queue_size": exec_metrics.get("execution_queue_size", 0),
            "broker_response_time_ms": exec_metrics.get("broker_response_time_ms", 0.0),
        },
    }





