import pytest
from telegram.bot import TelegramBotClient


@pytest.mark.asyncio
async def test_send_message_without_credentials_does_not_crash():
    """send_message() used to call logger.debug(msg, msg=message), which crashes
    with 'StructuredLogger.debug() got multiple values for argument msg' because
    the keyword collided with the positional parameter of the same name. This
    crashed the TelegramNotifier agent every time an execution_failed/order_filled
    event fired while no bot token/chat ID was configured."""
    client = TelegramBotClient()
    client.api_url = None
    client.chat_id = None

    result = await client.send_message("test message")

    assert result is False
