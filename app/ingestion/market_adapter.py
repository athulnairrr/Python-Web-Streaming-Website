"""Extension point for a real market/broker data source (e.g. a Quotex-
style API, a broker WebSocket feed, an exchange API).

This prototype intentionally ships NO scraping and NO real-broker
integration - only synthetic mock sources (`otc_source.py`,
`live_source.py`). Wiring up a real feed is a separate, deliberate task
governed by that provider's terms of service; `MarketDataAdapter` exists
so doing so later doesn't require touching processing or broadcasting at
all.

To plug in a real source:
  1. Subclass `MarketDataAdapter` and implement `stream()`, yielding
     dicts shaped exactly like the mock sources do:
     `{id, source, symbol, timeframe, price, timestamp, ts_generated}`.
  2. Instantiate it and start it in `app/main.py`'s `lifespan`, pushing
     onto `app.state.signal_queue` - exactly like `OTCMockSource` /
     `LiveMockSource` already do.

Nothing else in the app needs to change: the signal-generation step, the
pipeline, and the dashboard all consume the same event shape regardless
of where it came from.
"""

from typing import AsyncIterator, List

from .base import Event, EventSource


class MarketDataAdapter(EventSource):
    """Base class for a real market data source. Not implemented here -
    this is a contract, not a working integration."""

    def __init__(self, symbols: List[str], timeframes: List[str]):
        self.symbols = symbols
        self.timeframes = timeframes

    async def stream(self) -> AsyncIterator[Event]:
        raise NotImplementedError(
            "MarketDataAdapter is an extension point, not a working "
            "implementation. Subclass it and implement stream() against "
            "a real data source/broker API to replace the mock sources."
        )
        yield  # pragma: no cover - keeps this an async generator for type checkers
