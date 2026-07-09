import os
from pathlib import Path
from typing import Any, Dict, Optional
import yaml
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    # App Settings
    ENVIRONMENT: str = "development"
    LOG_LEVEL: str = "INFO"
    HOST: str = "0.0.0.0"
    PORT: int = 8000

    # PostgreSQL Database Settings
    POSTGRES_USER: str = "postgres"
    POSTGRES_PASSWORD: str = "postgres"
    POSTGRES_DB: str = "spv_quantum_ai"
    POSTGRES_HOST: str = "localhost"
    POSTGRES_PORT: int = 5432
    DATABASE_URL: Optional[str] = None
    DATABASE_URL_LOCAL: Optional[str] = None

    # Telegram Notification Settings
    TELEGRAM_BOT_TOKEN: Optional[str] = None
    TELEGRAM_CHAT_ID: Optional[str] = None

    # Dashboard HTTP Basic Auth — required before exposing this to any public
    # network. The app can place/cancel real orders; BasicAuthMiddleware fails
    # closed (blocks everything) if DASHBOARD_PASSWORD is unset.
    DASHBOARD_USERNAME: str = "admin"
    DASHBOARD_PASSWORD: Optional[str] = None

    # Kotak Neo Trade API (live market data feed — TOTP + MPIN login)
    KOTAK_NEO_CONSUMER_KEY: Optional[str] = None
    KOTAK_NEO_ENVIRONMENT: str = "prod"
    KOTAK_NEO_MOBILE_NUMBER: Optional[str] = None
    KOTAK_NEO_UCC: Optional[str] = None
    KOTAK_NEO_MPIN: Optional[str] = None
    KOTAK_NEO_TOTP_SECRET: Optional[str] = None

    # Holds configurations loaded from YAML file
    yaml_config: Dict[str, Any] = Field(default_factory=dict)

    model_config = SettingsConfigDict(
        env_file=str(Path(__file__).resolve().parent.parent / ".env"),
        env_file_encoding="utf-8",
        extra="ignore"
    )

    def __init__(self, **values: Any):
        super().__init__(**values)
        self.load_yaml_config()

    def load_yaml_config(self) -> None:
        """Loads configuration from YAML file and merges it into yaml_config."""
        root_dir = Path(__file__).resolve().parent.parent
        yaml_path = root_dir / "config" / "settings.yaml"
        if yaml_path.exists():
            try:
                with open(yaml_path, "r") as f:
                    content = yaml.safe_load(f)
                    if content:
                        self.yaml_config = content
            except Exception as e:
                print(f"Warning: Failed to load settings.yaml: {e}")

    def save_yaml_config(self) -> None:
        """Saves current yaml_config back to config/settings.yaml file."""
        root_dir = Path(__file__).resolve().parent.parent
        yaml_path = root_dir / "config" / "settings.yaml"
        try:
            with open(yaml_path, "w") as f:
                yaml.safe_dump(self.yaml_config, f, default_flow_style=False)
        except Exception as e:
            print(f"Warning: Failed to save settings.yaml: {e}")
            raise e

    def get_database_url(self) -> str:
        """Determines the appropriate database URL based on environment context."""
        # Check if running inside docker (DOCKER_CONTAINER env set in Dockerfile/compose)
        is_docker = os.environ.get("DOCKER_CONTAINER", "false").lower() == "true"
        if is_docker:
            if self.DATABASE_URL:
                return self.DATABASE_URL
            return f"postgresql+asyncpg://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        else:
            if self.DATABASE_URL_LOCAL:
                return self.DATABASE_URL_LOCAL
            return f"postgresql+asyncpg://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@localhost:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"

# Singleton settings instance
settings = Settings()
