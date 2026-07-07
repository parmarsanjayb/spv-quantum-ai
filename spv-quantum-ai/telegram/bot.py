import httpx
from typing import Optional
from core.config import settings
from core.logging import get_logger

logger = get_logger("telegram_bot")

class TelegramBotClient:
    """
    Wrapper client around Telegram's HTTP API.
    Sends markdown or HTML formatted strings directly to a chat room.
    """
    def __init__(self) -> None:
        self.token = settings.TELEGRAM_BOT_TOKEN
        self.chat_id = settings.TELEGRAM_CHAT_ID
        self.client: Optional[httpx.AsyncClient] = None

        if self.token:
            self.api_url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        else:
            self.api_url = None

    async def start(self) -> None:
        """Initializes the HTTP client session."""
        self.client = httpx.AsyncClient()
        if not self.token or not self.chat_id:
            logger.warning("Telegram Bot Token or Chat ID is missing. Notifications will bypass HTTP send.")

    async def close(self) -> None:
        """Closes the HTTP client session."""
        if self.client:
            await self.client.aclose()
            self.client = None

    async def send_message(self, message: str, parse_mode: str = "HTML") -> bool:
        """
        Sends a message to the configured Chat ID using Telegram Bot API.
        Args:
            message: Formatted text to send.
            parse_mode: Formatting syntax choice ('HTML' or 'MarkdownV2').
        """
        if not self.api_url or not self.chat_id:
            logger.debug("Bypassed sending Telegram message (credentials absent)", telegram_message=message)
            return False

        if not self.client:
            logger.error("Telegram bot client session is closed. Call start() first.")
            return False

        payload = {
            "chat_id": self.chat_id,
            "text": message,
            "parse_mode": parse_mode
        }

        try:
            logger.debug("Posting message to Telegram...", chat_id=self.chat_id)
            # 5-second timeout to prevent stalling the async loop on slow APIs
            response = await self.client.post(self.api_url, json=payload, timeout=5.0)
            
            if response.status_code == 200:
                logger.debug("Telegram message sent successfully.")
                return True
            else:
                logger.error(
                    "Telegram API rejected request",
                    status=response.status_code,
                    body=response.text
                )
                return False
        except Exception as e:
            logger.exception("Error posting to Telegram API", error=str(e))
            return False
