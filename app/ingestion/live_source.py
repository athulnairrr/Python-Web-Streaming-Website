"""Synthetic "live market" source - stands in for a real exchange/broker
feed. Purely synthetic demo data - see `mock_market.py` - and NOT
connected to any real exchange. See `market_adapter.py` for the
extension point a real live feed would implement instead.
"""

import asyncio
import itertools
import time
from datetime import datetime, timezone
from typing import AsyncIterator, List

from .base import Event, EventSource
from .mock_market import PriceWalker


class LiveMockSource(EventSource):
    def __init__(
        self,
        symbols: List[str],
        timeframes: List[str],
        interval_seconds: float = 4.0,
    ):
        self.symbols = symbols
        self.timeframes = timeframes
        self.interval_seconds = interval_seconds
        self._walkers = {
            symbol: PriceWalker(start_price=100.0 + i * 250.0, volatility=0.15)
            for i, symbol in enumerate(symbols)
        }
        self._counter = itertools.count(1)

    async def stream(self) -> AsyncIterator[Event]:
        timeframe_cycle = itertools.cycle(self.timeframes)
        while True:
            for symbol in self.symbols:
                now = datetime.now(timezone.utc)
                yield {
                    "id": f"live-{next(self._counter)}",
                    "source": "live",
                    "symbol": symbol,
                    "timeframe": next(timeframe_cycle),
                    "price": self._walkers[symbol].next_price(),
                    "timestamp": now.isoformat(),
                    "ts_generated": time.time(),
                    "mock": True,
                }
            await asyncio.sleep(self.interval_seconds)
