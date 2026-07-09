import base64
import pytest
import asyncio
from datetime import datetime, timezone
from fastapi.testclient import TestClient
from core.bus import event_bus, EventModel
from core.config import settings
from dashboard.main import app

def _basic_auth_header() -> dict:
    token = base64.b64encode(f"{settings.DASHBOARD_USERNAME}:{settings.DASHBOARD_PASSWORD}".encode()).decode()
    return {"Authorization": f"Basic {token}"}

@pytest.mark.asyncio
async def test_websocket_connection_and_broadcast():
    client = TestClient(app, headers=_basic_auth_header())
    
    # Start event bus (uses currently running asyncio test loop)
    event_bus.start()
    
    try:
        # In TestClient, websocket_connect is a synchronous blocking context manager
        # But it runs the app's lifespans and websocket inside the event loop.
        # We can run it in a thread or directly if it's non-blocking.
        with client.websocket_connect("/ws") as websocket:
            # Subscribe the broadcaster callback
            from dashboard.main import ws_event_broadcaster
            await event_bus.subscribe("test_topic", ws_event_broadcaster)
            
            # Publish a test event
            evt = EventModel(
                event_type="test_topic",
                source_agent="test_agent",
                payload={"message": "hello websocket"}
            )
            await event_bus.publish(evt)
            await asyncio.sleep(0.05)
            
            # Receive data via websocket
            data = websocket.receive_json()
            assert data["topic"] == "test_topic"
            assert data["sender"] == "test_agent"
            assert data["data"]["message"] == "hello websocket"
            
            # Clean up subscription
            await event_bus.unsubscribe("test_topic", ws_event_broadcaster)

    finally:
        await event_bus.stop()


@pytest.mark.asyncio
async def test_websocket_broadcast_serializes_nested_datetime():
    """Real event payloads (ticks, candles, decisions, trades) carry nested
    datetime/Enum values from Pydantic .model_dump(). The stdlib json encoder
    behind WebSocket.send_json() cannot serialize those directly, so the
    broadcaster must run payloads through jsonable_encoder() first — otherwise
    every broadcast silently fails and the client never receives anything."""
    client = TestClient(app, headers=_basic_auth_header())
    event_bus.start()

    try:
        with client.websocket_connect("/ws") as websocket:
            from dashboard.main import ws_event_broadcaster
            await event_bus.subscribe("test_topic_dt", ws_event_broadcaster)

            evt = EventModel(
                event_type="test_topic_dt",
                source_agent="test_agent",
                payload={
                    "tick": {
                        "symbol": "NIFTY50",
                        "timestamp": datetime.now(timezone.utc),
                        "close": 24200.0,
                    }
                }
            )
            await event_bus.publish(evt)
            await asyncio.sleep(0.05)

            data = websocket.receive_json()
            assert data["topic"] == "test_topic_dt"
            assert data["data"]["tick"]["symbol"] == "NIFTY50"
            assert data["data"]["tick"]["close"] == 24200.0
            assert isinstance(data["data"]["tick"]["timestamp"], str)

            await event_bus.unsubscribe("test_topic_dt", ws_event_broadcaster)

    finally:
        await event_bus.stop()
