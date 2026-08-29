"""Composable processing pipeline.

Chains an ordered list of steps (filter / aggregate / enrich / ...).
Any step that returns `None` short-circuits the rest of the chain and
drops the event.
"""

from typing import Any, Dict, List, Optional

from .steps import Step

Event = Dict[str, Any]


class Pipeline:
    def __init__(self, steps: List[Step]):
        self.steps = steps

    def process(self, event: Event) -> Optional[Event]:
        current: Optional[Event] = event
        for step in self.steps:
            if current is None:
                return None
            current = step(current)
        return current
