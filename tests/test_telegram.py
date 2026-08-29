import httpx
import pytest

from app.integrations.telegram_bot import TelegramNotifier


def test_disabled_when_not_configured():
    notifier = TelegramNotifier(token=None, chat_id=None)
    assert notifier.enabled is False


def test_enabled_when_configured():
    notifier = TelegramNotifier(token="abc123", chat_id="12345")
    assert notifier.enabled is True


@pytest.mark.asyncio
async def test_send_message_returns_error_when_not_configured():
    notifier = TelegramNotifier(token=None, chat_id=None)
    result = await notifier.send_message("hello")
    assert result["ok"] is False
    assert "not configured" in result["error"].lower()


@pytest.mark.asyncio
async def test_send_message_success(monkeypatch):
    notifier = TelegramNotifier(token="abc123", chat_id="12345")

    class FakeResponse:
        def raise_for_status(self):
            pass

        def json(self):
            return {"ok": True, "result": {"message_id": 1}}

    class FakeAsyncClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def post(self, url, json):
            assert "abc123" in url
            assert json["chat_id"] == "12345"
            return FakeResponse()

    monkeypatch.setattr(httpx, "AsyncClient", FakeAsyncClient)

    result = await notifier.send_message("hello")
    assert result["ok"] is True


@pytest.mark.asyncio
async def test_send_message_handles_http_error(monkeypatch):
    notifier = TelegramNotifier(token="abc123", chat_id="12345")

    class FakeAsyncClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def post(self, url, json):
            raise httpx.ConnectError("connection failed")

    monkeypatch.setattr(httpx, "AsyncClient", FakeAsyncClient)

    result = await notifier.send_message("hello")
    assert result["ok"] is False
    assert "error" in result


def test_format_signal_message_contains_key_fields():
    event = {
        "source": "otc",
        "symbol": "EURUSD-OTC",
        "timeframe": "M1",
        "direction": "CALL",
        "timestamp": "2024-01-01T00:00:00+00:00",
    }
    text = TelegramNotifier.format_signal_message(event)
    assert "EURUSD-OTC" in text
    assert "CALL" in text
    assert "not trading advice" in text.lower()


def test_test_message_is_clearly_labelled():
    text = TelegramNotifier.test_message()
    assert "TEST ALERT" in text
