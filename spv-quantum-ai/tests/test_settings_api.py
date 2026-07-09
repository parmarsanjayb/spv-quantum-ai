import base64
import pytest
from fastapi.testclient import TestClient
from dashboard.main import app
from core.config import settings

def _basic_auth_header() -> dict:
    token = base64.b64encode(f"{settings.DASHBOARD_USERNAME}:{settings.DASHBOARD_PASSWORD}".encode()).decode()
    return {"Authorization": f"Basic {token}"}

@pytest.fixture
def client():
    return TestClient(app, headers=_basic_auth_header())

def test_get_settings(client):
    response = client.get("/api/settings")
    assert response.status_code == 200
    data = response.json()
    assert "active_broker" in data
    assert "client_id" in data
    assert "api_key" in data
    assert "max_daily_loss" in data
    assert "max_exposure" in data

def test_post_settings_success(client):
    # Store original configuration to restore later
    orig_config = settings.yaml_config.copy()
    
    try:
        payload = {
            "active_broker": "paper_broker",
            "client_id": "9999999999",
            "api_key": "test_api_key_123",
            "max_daily_loss": 600.0,
            "max_exposure": 12000.0
        }
        response = client.post("/api/settings", json=payload)
        assert response.status_code == 200
        assert response.json()["success"] is True
        
        # Verify in memory config was reloaded
        assert settings.yaml_config["brokers"]["active"] == "paper_broker"
        assert settings.yaml_config["brokers"]["kotak_neo"]["client_id"] == "9999999999"
        assert settings.yaml_config["brokers"]["kotak_neo"]["api_key"] == "test_api_key_123"
        assert settings.yaml_config["risk_limits"]["daily_loss_limit_usd"] == 600.0
        assert settings.yaml_config["risk_limits"]["max_position_size_usd"] == 12000.0
        
    finally:
        # Restore original configuration
        settings.yaml_config = orig_config
        settings.save_yaml_config()

def test_post_settings_invalid_broker(client):
    payload = {
        "active_broker": "invalid_broker_choice",
        "client_id": "9999999999",
        "api_key": "test_api_key_123",
        "max_daily_loss": 600.0,
        "max_exposure": 12000.0
    }
    response = client.post("/api/settings", json=payload)
    assert response.status_code == 500  # returns HTTPException status code in try-catch
