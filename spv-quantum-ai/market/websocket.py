import asyncio
import random
import json
from typing import Any, Callable, Dict, Optional
import websockets
from market.models import FeedDisconnectedEvent, FeedConnectedEvent
from market.health import FeedHealthMonitor
from core.bus import event_bus, EventModel
from core.logging import get_logger
from core.config import settings

logger = get_logger("websocket_stream")

class WebSocketStreamManager:
    """
    Manages the live feed WebSocket connection.
    Supports:
      - 'mock': Generates simulated price ticks locally.
      - 'binance': Streams live public BTCUSD and ETHUSD ticker data from Binance.
      - 'kotak': Streams market data using Kotak Neo API websocket parameters.
    Notifies FeedHealthMonitor on connect / disconnect / stale data.
    Auto-reconnects up to _max_reconnects before raising FeedDisconnectedEvent.
    """

    def __init__(
        self,
        on_raw_tick:    Callable[[Dict[str, Any]], Any],
        health_monitor: FeedHealthMonitor,
    ) -> None:
        self._on_raw_tick    = on_raw_tick
        self._health         = health_monitor
        self._connected:     bool = False
        self._running:       bool = False
        self._max_reconnects: int = 5
        self._reconnect_attempts: int = 0
        self._loop_task: Optional[asyncio.Task] = None

        # Load configuration
        feed_config = settings.yaml_config.get("market_feed", {})
        self._feed_mode = feed_config.get("active", "mock").lower()

        # Mock price seed — spans Index, Equity, Currency, Commodity, and Crypto segments
        self._prices = {
            # Index
            "NIFTY50": 24200.0, "BANKNIFTY": 52000.0, "FINNIFTY": 23500.0,
            # Equity
            "RELIANCE": 2950.0, "TCS": 3850.0, "HDFCBANK": 1650.0, "INFY": 1850.0,
            "ICICIBANK": 1150.0, "SBIN": 830.0, "ITC": 460.0,
            # Currency
            "USDINR": 83.50,
            # Commodity
            "CRUDEOIL": 6800.0, "GOLD": 72000.0, "SILVER": 92000.0, "NATURALGAS": 250.0,
            # Crypto
            "BTCUSD": 65000.0, "ETHUSD": 3500.0,
        }

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def connect(self) -> None:
        logger.info("Connecting to market data feed...", mode=self._feed_mode)
        await asyncio.sleep(0.1)
        self._connected = True
        self._reconnect_attempts = 0
        self._health.signal_connected()
        logger.info("Market data feed connected.", mode=self._feed_mode)

    async def start(self) -> None:
        self._running = True
        await self.connect()
        self._loop_task = asyncio.create_task(self._stream_loop())

    async def stop(self) -> None:
        self._running = False
        if self._loop_task:
            self._loop_task.cancel()
            try:
                await self._loop_task
            except asyncio.CancelledError:
                pass
        self._connected = False
        self._health.signal_disconnected("Graceful shutdown")
        logger.info("Market data feed stopped.")

    def is_connected(self) -> bool:
        return self._connected

    # ── Stream loop router ───────────────────────────────────────────────────

    async def _stream_loop(self) -> None:
        if self._feed_mode == "binance":
            await self._binance_loop()
        elif self._feed_mode == "kotak":
            await self._kotak_loop()
        else:
            await self._mock_loop()

    # ── Mock Feed Loop ───────────────────────────────────────────────────────

    async def _mock_loop(self) -> None:
        while self._running:
            try:
                if not self._connected:
                    await self._reconnect()
                    continue

                await asyncio.sleep(0.5)

                # Simulate 0.5 % random drop
                if random.random() < 0.005:
                    logger.warning("Simulated feed drop.")
                    self._connected = False
                    self._health.signal_disconnected("Simulated drop")
                    continue

                sym    = random.choice(list(self._prices.keys()))
                change = random.uniform(-0.0003, 0.0003)
                self._prices[sym] *= (1.0 + change)
                p = round(self._prices[sym], 2)

                raw = {
                    "symbol":        sym,
                    "price":         p,
                    "ltp":           p,
                    "bid":           round(p * 0.9998, 2),
                    "ask":           round(p * 1.0002, 2),
                    "volume":        round(random.uniform(1.0, 30.0), 3),
                    "open_interest": round(random.uniform(1000, 8000), 0) if sym in ("NIFTY50", "BANKNIFTY") else 0.0,
                    "vwap":          p,
                    "atp":           p,
                    "open":          round(self._prices[sym] * random.uniform(0.998, 1.002), 2),
                    "high":          round(p * random.uniform(1.000, 1.003), 2),
                    "low":           round(p * random.uniform(0.997, 1.000), 2),
                    "close":         p,
                    "prev_close":    round(p * random.uniform(0.995, 1.005), 2),
                }
                self._health.record_tick()
                asyncio.create_task(self._on_raw_tick(raw))

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Mock stream loop error", error=str(e))
                self._connected = False
                await asyncio.sleep(1.0)

    # ── Binance Live Feed Loop ────────────────────────────────────────────────

    async def _binance_loop(self) -> None:
        url = settings.yaml_config.get("market_feed", {}).get("binance", {}).get("url", "wss://stream.binance.com:9443/ws")
        logger.info("Starting Binance live WebSocket feed...", url=url)

        while self._running:
            try:
                if not self._connected:
                    await self._reconnect()
                    continue

                async with websockets.connect(url) as ws:
                    self._reconnect_attempts = 0
                    self._health.signal_connected()
                    logger.info("Binance WebSocket feed connected.")

                    # Subscribe to tickers
                    subscribe_msg = {
                        "method": "SUBSCRIBE",
                        "params": [
                            "btcusdt@ticker",
                            "ethusdt@ticker"
                        ],
                        "id": 1
                    }
                    await ws.send(json.dumps(subscribe_msg))

                    while self._running and self._connected:
                        msg_raw = await ws.recv()
                        msg = json.loads(msg_raw)

                        # Process ticker data
                        if isinstance(msg, dict) and msg.get("e") == "24hrTicker":
                            binance_sym = msg.get("s", "")
                            canonical_sym = "BTCUSD" if "BTC" in binance_sym else "ETHUSD" if "ETH" in binance_sym else binance_sym
                            p = float(msg.get("c", 0.0))
                            raw = {
                                "symbol":        canonical_sym,
                                "price":         p,
                                "ltp":           p,
                                "bid":           float(msg.get("b", p * 0.9998)),
                                "ask":           float(msg.get("a", p * 1.0002)),
                                "volume":        float(msg.get("v", 0.0)),
                                "open_interest": 0.0,
                                "vwap":          float(msg.get("w", p)),
                                "atp":           float(msg.get("w", p)),
                                "open":          float(msg.get("o", p)),
                                "high":          float(msg.get("h", p)),
                                "low":           float(msg.get("l", p)),
                                "close":         p,
                                "prev_close":    float(msg.get("o", p)),
                            }
                            self._health.record_tick()
                            asyncio.create_task(self._on_raw_tick(raw))
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Binance stream loop error", error=str(e))
                self._connected = False
                self._health.signal_disconnected(str(e))
                await asyncio.sleep(1.0)

    # ── Kotak Neo Live Feed Loop ──────────────────────────────────────────────

    async def _kotak_loop(self) -> None:
        url = settings.yaml_config.get("market_feed", {}).get("kotak", {}).get("url", "")
        logger.info("Starting Kotak Neo live WebSocket feed...", url=url)

        while self._running:
            try:
                if not self._connected:
                    await self._reconnect()
                    continue

                # Check/retrieve token
                from brokers.kotak_neo import KotakAuthenticationManager
                auth_mgr = KotakAuthenticationManager()
                await auth_mgr.authenticate()
                token = auth_mgr.session_token

                async with websockets.connect(url) as ws:
                    self._reconnect_attempts = 0
                    self._health.signal_connected()
                    logger.info("Kotak Neo WebSocket feed connected.")

                    # Send authentication and subscription message
                    auth_msg = {
                        "Authorization": f"Bearer {token}",
                        "action": "subscribe",
                        "symbols": ["NIFTY50", "BANKNIFTY"]
                    }
                    await ws.send(json.dumps(auth_msg))

                    while self._running and self._connected:
                        msg_raw = await ws.recv()
                        msg = json.loads(msg_raw)

                        if isinstance(msg, dict) and "symbol" in msg:
                            sym = msg.get("symbol")
                            p = float(msg.get("price", msg.get("ltp", 0.0)))
                            raw = {
                                "symbol":        sym,
                                "price":         p,
                                "ltp":           p,
                                "bid":           float(msg.get("bid", p * 0.9998)),
                                "ask":           float(msg.get("ask", p * 1.0002)),
                                "volume":        float(msg.get("volume", 0.0)),
                                "open_interest": float(msg.get("open_interest", 0.0)),
                                "vwap":          float(msg.get("vwap", p)),
                                "atp":           float(msg.get("atp", p)),
                                "open":          float(msg.get("open", p)),
                                "high":          float(msg.get("high", p)),
                                "low":           float(msg.get("low", p)),
                                "close":         p,
                                "prev_close":    float(msg.get("prev_close", p)),
                            }
                            self._health.record_tick()
                            asyncio.create_task(self._on_raw_tick(raw))
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Kotak Neo stream loop error", error=str(e))
                self._connected = False
                self._health.signal_disconnected(str(e))
                await asyncio.sleep(1.0)

    async def _reconnect(self) -> None:
        self._reconnect_attempts += 1
        logger.info("Reconnecting feed...", attempt=self._reconnect_attempts, max=self._max_reconnects)
        await asyncio.sleep(1.0)

        if self._reconnect_attempts >= self._max_reconnects:
            logger.error("Max reconnect attempts reached. Feed declared dead.")
            await event_bus.publish(EventModel(
                event_type   = "feed_disconnected",
                source_agent = "websocket_stream",
                payload      = FeedDisconnectedEvent(reason="Max reconnect attempts exceeded").model_dump(),
                priority     = 0,
            ))
            await asyncio.sleep(10.0)
            self._reconnect_attempts = 0
        else:
            if random.random() < 0.85:
                self._connected = True
                self._reconnect_attempts = 0
                self._health.signal_connected()
                logger.info("Feed reconnected successfully.")

