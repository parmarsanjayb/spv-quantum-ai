import asyncio
from typing import Any, Callable, Dict, Optional, Tuple
from market.models import FeedDisconnectedEvent
from market.health import FeedHealthMonitor
from market.instrument import InstrumentManager
from market.registry import SymbolRegistry
from core.bus import event_bus, EventModel
from core.logging import get_logger

logger = get_logger("websocket_stream")

class WebSocketStreamManager:
    """
    Manages the live feed WebSocket connection via the official Kotak Neo
    Trade API SDK (neo_api_client). This is the only runtime source of price
    ticks — there is no simulated/mock fallback. If the Kotak feed cannot be
    authenticated or reached, the manager stays disconnected and
    FeedHealthMonitor publishes feed_disconnected so the UI can show
    "Feed Disconnected" instead of any price data.

    The SDK's live feed (NeoAPI.subscribe -> NeoWebSocket) runs its network
    I/O on a background thread using synchronous callbacks (on_open/on_message/
    on_close/on_error). Those callbacks bridge back into this manager's asyncio
    loop via asyncio.run_coroutine_threadsafe.
    """

    def __init__(
        self,
        on_raw_tick:    Callable[[Dict[str, Any]], Any],
        health_monitor: FeedHealthMonitor,
        instruments:    InstrumentManager,
        registry:       SymbolRegistry,
    ) -> None:
        self._on_raw_tick    = on_raw_tick
        self._health         = health_monitor
        self._instruments    = instruments
        self._registry       = registry
        self._connected:     bool = False
        self._running:       bool = False
        self._max_reconnects: int = 5
        self._reconnect_attempts: int = 0
        self._loop_task: Optional[asyncio.Task] = None
        self._auth_mgr = None
        # (exchange_segment, instrument_token) -> canonical symbol, for mapping
        # raw feed updates back onto our symbols.
        self._token_by_key: Dict[Tuple[Optional[str], str], str] = {}

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def start(self) -> None:
        self._running = True
        self._loop_task = asyncio.create_task(self._kotak_loop())

    async def stop(self) -> None:
        self._running = False
        if self._loop_task:
            self._loop_task.cancel()
            try:
                await self._loop_task
            except asyncio.CancelledError:
                pass
        self._close_socket()
        self._connected = False
        self._health.signal_disconnected("Graceful shutdown")
        logger.info("Market data feed stopped.")

    def is_connected(self) -> bool:
        return self._connected

    def _close_socket(self) -> None:
        client = self._auth_mgr.client if self._auth_mgr else None
        ws = getattr(client, "NeoWebSocket", None)
        hs = getattr(ws, "hsWebsocket", None)
        if hs:
            try:
                hs.close()
            except Exception:
                pass

    # ── Kotak Neo Live Feed Loop ──────────────────────────────────────────────

    def _build_subscription_lists(self):
        """Splits tracked instruments into index vs. non-index Kotak subscriptions.
        Crypto symbols are skipped — Kotak Neo doesn't carry that segment."""
        index_tokens, scrip_tokens = [], []
        self._token_by_key = {}
        for symbol, inst in self._instruments.get_all().items():
            if inst.get("exchange") == "CRYPTO":
                continue
            token = inst.get("token")
            segment = inst.get("segment")
            if not token or not segment:
                continue
            entry = {"instrument_token": token, "exchange_segment": segment}
            self._token_by_key[(segment, str(token))] = symbol
            meta = self._registry.get_meta(symbol) or {}
            if meta.get("segment") == "INDEX":
                index_tokens.append(entry)
            else:
                scrip_tokens.append(entry)
        return index_tokens, scrip_tokens

    async def _kotak_loop(self) -> None:
        from brokers.kotak_neo import KotakAuthenticationManager

        while self._running:
            try:
                auth_mgr = KotakAuthenticationManager()
                authenticated = await auth_mgr.authenticate()
                if not authenticated:
                    raise ConnectionError("Kotak Neo authentication failed")

                self._auth_mgr = auth_mgr
                client = auth_mgr.client
                index_tokens, scrip_tokens = self._build_subscription_lists()
                if not index_tokens and not scrip_tokens:
                    raise RuntimeError("No Kotak-tradable instruments registered to subscribe to")

                loop = asyncio.get_running_loop()
                client.on_open    = self._make_on_open(loop)
                client.on_close   = self._make_on_close(loop)
                client.on_error   = self._make_on_error(loop)
                client.on_message = self._make_on_message(loop)

                if index_tokens:
                    client.subscribe(instrument_tokens=index_tokens, isIndex=True)
                    await asyncio.sleep(1.0)
                if scrip_tokens:
                    client.subscribe(instrument_tokens=scrip_tokens, isIndex=False)

                logger.info(
                    "Kotak Neo live feed subscription requested.",
                    index_count=len(index_tokens), scrip_count=len(scrip_tokens),
                )

                # Stay on this authenticated session until the token needs
                # refreshing or the feed reports itself disconnected.
                while self._running and auth_mgr.is_token_valid():
                    await asyncio.sleep(5.0)

                if not self._running:
                    break

                logger.info("Kotak Neo session token expiring; re-authenticating and re-subscribing.")
                self._connected = False
                self._close_socket()
                self._health.signal_disconnected("Session refresh")

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Kotak Neo stream loop error", error=str(e))
                self._connected = False
                self._health.signal_disconnected(str(e))
                await self._backoff()

    # ── Sync SDK callbacks, bridged onto the asyncio loop ─────────────────────

    def _make_on_open(self, loop: asyncio.AbstractEventLoop) -> Callable[..., None]:
        # NeoAPI.__on_open calls self.on_open("The Session has been Opened!") —
        # accept *args defensively rather than hardcode that arity.
        def on_open(*_args: Any) -> None:
            self._connected = True
            asyncio.run_coroutine_threadsafe(self._signal_connected(), loop)
        return on_open

    def _make_on_close(self, loop: asyncio.AbstractEventLoop) -> Callable[..., None]:
        # NeoAPI.__on_close calls self.on_close("The Session has been Closed!").
        def on_close(*_args: Any) -> None:
            self._connected = False
            asyncio.run_coroutine_threadsafe(self._signal_disconnected("Kotak Neo feed closed"), loop)
        return on_close

    def _make_on_error(self, loop: asyncio.AbstractEventLoop) -> Callable[[Any], None]:
        def on_error(error: Any) -> None:
            self._connected = False
            asyncio.run_coroutine_threadsafe(self._signal_disconnected(str(error)), loop)
        return on_error

    def _make_on_message(self, loop: asyncio.AbstractEventLoop) -> Callable[[Any], None]:
        def on_message(message: Any) -> None:
            self._handle_feed_message(message, loop)
        return on_message

    async def _signal_connected(self) -> None:
        self._reconnect_attempts = 0
        self._health.signal_connected()

    async def _signal_disconnected(self, reason: str) -> None:
        self._health.signal_disconnected(reason)

    # ── Feed message parsing ──────────────────────────────────────────────────

    def _handle_feed_message(self, message: Any, loop: asyncio.AbstractEventLoop) -> None:
        if not isinstance(message, dict) or message.get("type") != "stock_feed":
            return
        data = message.get("data")
        if not isinstance(data, list):
            return
        for item in data:
            if not isinstance(item, dict) or "tk" not in item:
                continue
            symbol = self._token_by_key.get((item.get("e"), str(item.get("tk"))))
            if not symbol:
                continue
            raw = self._parse_feed_item(symbol, item)
            if raw:
                self._health.record_tick()
                asyncio.run_coroutine_threadsafe(self._on_raw_tick(raw), loop)

    @staticmethod
    def _to_float(item: Dict[str, Any], key: str, default: float = 0.0) -> float:
        val = item.get(key)
        try:
            return float(val) if val is not None else default
        except (TypeError, ValueError):
            return default

    def _parse_feed_item(self, symbol: str, item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        f = lambda key, default=0.0: self._to_float(item, key, default)
        if "iv" in item:
            # Index feed: short keys per neo_api_client index_key_mapping.
            ltp = f("iv")
            prev_close = f("ic", ltp)
            return {
                "symbol": symbol, "price": ltp, "ltp": ltp,
                "bid": ltp, "ask": ltp, "volume": 0.0, "open_interest": 0.0,
                "vwap": ltp, "atp": ltp,
                "open": f("openingPrice", ltp), "high": f("highPrice", ltp), "low": f("lowPrice", ltp),
                "close": ltp, "prev_close": prev_close,
            }
        if "ltp" in item:
            # Scrip feed: short keys per neo_api_client stock_key_mapping.
            ltp = f("ltp")
            return {
                "symbol": symbol, "price": ltp, "ltp": ltp,
                "bid": f("bp", ltp), "ask": f("sp", ltp),
                "volume": f("v"), "open_interest": f("oi"),
                "vwap": f("ap", ltp), "atp": f("ap", ltp),
                "open": f("op", ltp), "high": f("h", ltp), "low": f("lo", ltp),
                "close": f("c", ltp), "prev_close": f("c", ltp),
            }
        return None

    # ── Backoff ────────────────────────────────────────────────────────────────

    async def _backoff(self) -> None:
        """Waits before the next real reconnect attempt. Never fabricates a connection."""
        self._reconnect_attempts += 1
        if self._reconnect_attempts >= self._max_reconnects:
            logger.error("Max reconnect attempts reached. Feed declared dead.", attempts=self._reconnect_attempts)
            await event_bus.publish(EventModel(
                event_type   = "feed_disconnected",
                source_agent = "websocket_stream",
                payload      = FeedDisconnectedEvent(reason="Max reconnect attempts exceeded").model_dump(),
                priority     = 0,
            ))
            await asyncio.sleep(10.0)
            self._reconnect_attempts = 0
        else:
            logger.info("Retrying Kotak Neo feed connection...", attempt=self._reconnect_attempts, max=self._max_reconnects)
            await asyncio.sleep(min(2 ** self._reconnect_attempts, 30))
