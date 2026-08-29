"""Individual processing steps.

Each step is a plain callable: `Event -> Event | None`. Returning `None`
drops the event (used for filtering). Steps never mutate their input so
they stay easy to test and reorder.
"""

import statistics
import time
from collections import defaultdict, deque
from typing import Any, Callable, Deque, Dict, Optional

Event = Dict[str, Any]
Step = Callable[[Event], Optional[Event]]


def make_filter_step(predicate: Callable[[Event], bool]) -> Step:
    """Build a step that drops any event failing `predicate`."""

    def _step(event: Event) -> Optional[Event]:
        return event if predicate(event) else None

    return _step


def threshold_filter(min_value: float = float("-inf")) -> Step:
    """Drop events whose `value` is below `min_value`."""
    return make_filter_step(lambda e: e.get("value", 0) >= min_value)


class RollingAggregateStep:
    """Enriches each event with a running mean/stdev for its category.

    Maintains a fixed-size sliding window per category so aggregation
    happens online, per event, rather than in a separate batch job -
    this is the "aggregation" stage of the pipeline.
    """

    def __init__(self, window_size: int = 20):
        self.window_size = window_size
        self._windows: Dict[str, Deque[float]] = defaultdict(
            lambda: deque(maxlen=window_size)
        )

    def __call__(self, event: Event) -> Optional[Event]:
        category = event.get("category", "default")
        value = event.get("value", 0.0)
        window = self._windows[category]
        window.append(value)

        mean = statistics.fmean(window)
        stdev = statistics.pstdev(window) if len(window) > 1 else 0.0

        enriched = dict(event)
        enriched["rolling_mean"] = round(mean, 2)
        enriched["rolling_stdev"] = round(stdev, 2)
        enriched["window_size"] = len(window)
        return enriched


def enrich_step(event: Event) -> Optional[Event]:
    """Stamp processing time and compute end-to-end latency so far.

    This is the "enrichment" stage - it adds derived fields the raw
    source never had.
    """
    enriched = dict(event)
    now = time.time()
    enriched["ts_processed"] = now
    if "ts_generated" in enriched:
        enriched["latency_ms"] = round((now - enriched["ts_generated"]) * 1000, 2)
    return enriched
