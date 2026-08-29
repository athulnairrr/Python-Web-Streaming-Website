"""Synthetic OTC (over-the-counter / binary-options-style) market source.

Emits fake price events for a configurable set of OTC symbols, rotating
through a configurable set of timeframes. Purely synthetic demo data -
see `mock_market.py` - and NOT connected to any real broker or exchange.
This is the source a real OTC/broker feed (e.g. a Quotex-style API) would
eventually replace; see `market_adapter.py` for that extension point.
"""

import asyncio
import itertools
import time
from datetime import datetime, timezone
from typing import AsyncIterator, List

from .base import Event, EventSource
from .mock_market import PriceWalker


class OTCMockSource(EventSource):
    def __init__(
        self,
        symbols: List[str],
        timeframes: List[str],
        interval_seconds: float = 3.0,
    ):
        self.symbols = symbols
        self.timeframes = timeframes
        self.interval_seconds = interval_seconds
        self._walkers = {
            symbol: PriceWalker(start_price=1.0 + i * 0.15)
            for i, symbol in enumerate(symbols)
        }
        self._counter = itertools.count(1)

    async def stream(self) -> AsyncIterator[Event]:
        timeframe_cycle = itertools.cycle(self.timeframes)
        while True:
            for symbol in self.symbols:
                now = datetime.now(timezone.utc)
                yield {
                    "id": f"otc-{next(self._counter)}",
                    "source": "otc",
                    "symbol": symbol,
                    "timeframe": next(timeframe_cycle),
                    "price": self._walkers[symbol].next_price(),
                    "timestamp": now.isoformat(),
                    "ts_generated": time.time(),
                    "mock": True,
                }
            await asyncio.sleep(self.interval_seconds)
