"""Shared helpers for the synthetic OTC / live-market sources.

Everything here is fake data generated for demo purposes - a simple random
walk, not a real price feed and not sourced from any broker or exchange.
See `market_adapter.py` for the extension point a real data source would
implement instead.
"""

import random
from typing import List

DEFAULT_OTC_SYMBOLS = ["EURUSD-OTC", "GBPUSD-OTC", "USDJPY-OTC", "AUDCAD-OTC"]
DEFAULT_LIVE_SYMBOLS = ["EURUSD", "GBPUSD", "BTCUSD", "ETHUSD"]
DEFAULT_TIMEFRAMES = ["M1", "M5"]


class PriceWalker:
    """Tiny random-walk price simulator for one symbol. Not a real feed."""

    def __init__(self, start_price: float, volatility: float = 0.0006):
        self.price = start_price
        self.volatility = volatility

    def next_price(self) -> float:
        self.price = max(self.price + random.gauss(0, self.volatility), 0.0001)
        return round(self.price, 5)


def parse_list(raw: str, default: List[str], upper: bool = False) -> List[str]:
    """Parse a comma-separated env var into a list, falling back to
    `default` if empty/unset."""
    items = [item.strip() for item in raw.split(",") if item.strip()]
    if upper:
        items = [item.upper() for item in items]
    return items or list(default)
