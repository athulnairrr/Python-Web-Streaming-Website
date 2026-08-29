"""Synthetic event generator.

Stands in for a real feed (Kafka topic, MQTT broker, exchange tick stream,
IoT sensor bus, ...) so the pipeline has something to process out of the
box. Swapping this out for a real source later means writing one more
class that implements `EventSource` - the processing and broadcasting
layers do not change.
"""

import asyncio
import itertools
import random
import time
from typing import AsyncIterator

from .base import Event, EventSource

CATEGORIES = ["cpu", "memory", "network", "disk"]


class DemoEventSource(EventSource):
    """Generates ~`rate` synthetic metric events per second.

    Paced on a fixed ~50ms tick rather than one `asyncio.sleep` per event:
    each tick computes how many events *should* have been emitted by now
    given real elapsed time, and emits that many. This keeps the long-run
    rate accurate (self-correcting, no drift) while only ever scheduling a
    handful of timers per second - sub-10ms sleep-per-event loops are
    unreliable under load on some platforms (observed badly on Windows'
    ProactorEventLoop once another task is also busy), so this sidesteps
    that entirely instead of depending on fine-grained sleep precision.
    """

    TICK_SECONDS = 0.05

    def __init__(self, rate: float = 100.0):
        self._rate = max(rate, 0.0)
        self._counter = itertools.count(1)

    async def stream(self) -> AsyncIterator[Event]:
        loop = asyncio.get_event_loop()
        start = loop.time()
        emitted = 0
        while True:
            elapsed = loop.time() - start
            due = int(elapsed * self._rate) - emitted
            for _ in range(max(due, 0)):
                yield {
                    "id": next(self._counter),
                    "category": random.choice(CATEGORIES),
                    "value": round(random.gauss(50, 15), 2),
                    "ts_generated": time.time(),
                }
                emitted += 1
            await asyncio.sleep(self.TICK_SECONDS)
