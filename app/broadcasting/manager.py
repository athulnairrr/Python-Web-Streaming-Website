"""Broadcasting layer.

Tracks connected WebSocket clients and fans processed events out to all
of them. Decoupled from both the processing pipeline (which just calls
`broadcast`) and from any specific transport detail beyond "has an async
`send_text`", which keeps it easy to unit test with fakes.
"""

import asyncio
import json
from typing import Any, Dict, Set


class ConnectionManager:
    def __init__(self):
        self._connections: Set[Any] = set()
        self._lock = asyncio.Lock()

    async def connect(self, websocket: Any) -> None:
        await websocket.accept()
        async with self._lock:
            self._connections.add(websocket)

    async def disconnect(self, websocket: Any) -> None:
        async with self._lock:
            self._connections.discard(websocket)

    async def broadcast(self, message: Dict[str, Any]) -> None:
        payload = json.dumps(message)
        async with self._lock:
            targets = list(self._connections)

        stale = []
        for ws in targets:
            try:
                await ws.send_text(payload)
            except Exception:
                stale.append(ws)

        if stale:
            async with self._lock:
                for ws in stale:
                    self._connections.discard(ws)

    @property
    def active_count(self) -> int:
        return len(self._connections)
