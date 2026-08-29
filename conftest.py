"""Root conftest.

Disables all background event sources before `app.main` is ever imported,
so API/WebSocket tests get deterministic counters regardless of which
test module happens to import it first (config is read once, at import
time, in app/main.py).
"""

import os

os.environ.setdefault("DEMO_SOURCE_ENABLED", "false")
os.environ.setdefault("OTC_SOURCE_ENABLED", "false")
os.environ.setdefault("LIVE_SOURCE_ENABLED", "false")

# Never let the test suite hit the real Telegram API, even if these
# happen to be set in the ambient environment.
os.environ.pop("TELEGRAM_BOT_TOKEN", None)
os.environ.pop("TELEGRAM_CHAT_ID", None)
