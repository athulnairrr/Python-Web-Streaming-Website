"""Signal-generation step: turns a raw market event (symbol/price/
timeframe) into a trade signal by adding a `direction`, via a pluggable
strategy.

This is the one place decision logic lives. Swap `RandomSignalStrategy`
for a real strategy (technical indicators, a model, a rules engine) by
implementing `SignalStrategy` - nothing in ingestion, the rest of the
pipeline, or broadcasting needs to change.

IMPORTANT: `RandomSignalStrategy` picks a direction uniformly at random.
It has no predictive value and is not trading advice - it exists purely
to demonstrate the real-time pipeline end to end with plausible-looking
output.
"""

import random
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

from .steps import Step

Event = Dict[str, Any]

DIRECTIONS = ("CALL", "PUT")


class SignalStrategy(ABC):
    """Decision logic: given a raw market event, decide a direction."""

    @abstractmethod
    def generate(self, event: Event) -> Optional[Event]:
        """Return the event with `direction` (and any extra fields)
        added, or `None` to skip emitting a signal for this event."""
        raise NotImplementedError


class RandomSignalStrategy(SignalStrategy):
    """Default placeholder strategy used by this prototype.

    Picks a uniformly random direction and a cosmetic confidence score.
    Replace this class with real signal logic later; the rest of the
    pipeline is unaffected either way.
    """

    def generate(self, event: Event) -> Optional[Event]:
        enriched = dict(event)
        enriched["direction"] = random.choice(DIRECTIONS)
        enriched["confidence"] = round(random.uniform(55.0, 90.0), 1)
        return enriched


def make_signal_step(strategy: SignalStrategy) -> Step:
    """Wrap a `SignalStrategy` as a pipeline step."""

    def _step(event: Event) -> Optional[Event]:
        return strategy.generate(event)

    return _step
