"""Ingestion layer contract.

Anything that produces raw events - a synthetic generator today, a Kafka or
MQTT consumer tomorrow - implements this interface. Nothing downstream
(processing, broadcasting) knows or cares which one is plugged in.
"""

from abc import ABC, abstractmethod
from typing import Any, AsyncIterator, Dict

Event = Dict[str, Any]


class EventSource(ABC):
    """Abstract base for a raw-event producer."""

    @abstractmethod
    async def stream(self) -> AsyncIterator[Event]:
        """Yield raw event dicts indefinitely (or until cancelled)."""
        raise NotImplementedError
        yield  # pragma: no cover - makes this an async generator for type checkers
