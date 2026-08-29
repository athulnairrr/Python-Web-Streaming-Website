"""FastAPI application wiring ingestion -> processing -> broadcasting together.

Data flow
---------
1. Ingestion: either the built-in `DemoEventSource` (synthetic ~100 evt/s
   generator) or an external producer POSTing to `/ingest` - both just push
   raw events onto the same `asyncio.Queue`.
2. Processing: a background task pulls events off the queue and runs them
   through the `Pipeline` (filter -> aggregate -> enrich).
3. Broadcasting: every event that survives the pipeline is fanned out to
   all connected `/ws` clients via `ConnectionManager`.

To plug in Kafka/MQTT later: write a class implementing `EventSource`
(see app/ingestion/base.py) that pushes onto the app's event queue, start
it in `lifespan` instead of/alongside `DemoEventSource`, and nothing else
in this file needs to change.

All mutable pipeline state (queue, connection manager, counters) lives on
`app.state`, created fresh in `lifespan` for each app run, rather than as
module-level globals - that keeps repeated app startups (as happens once
per `TestClient` in tests) fully isolated instead of leaking event-loop-
bound objects between runs.
"""

import asyncio
import logging
import os
import time
from collections import deque
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Dict

from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles

from app.broadcasting.manager import ConnectionManager
from app.ingestion.demo_source import DemoEventSource
from app.processing.pipeline import Pipeline
from app.processing.steps import RollingAggregateStep, enrich_step, threshold_filter

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("stream_app")

QUEUE_MAXSIZE = int(os.getenv("QUEUE_MAXSIZE", "5000"))
DEMO_SOURCE_ENABLED = os.getenv("DEMO_SOURCE_ENABLED", "true").lower() == "true"
DEMO_SOURCE_RATE = float(os.getenv("DEMO_SOURCE_RATE", "100"))
FILTER_MIN_VALUE = float(os.getenv("FILTER_MIN_VALUE", "0"))
CLIENT_DIR = Path(__file__).resolve().parent.parent / "client"


def build_pipeline() -> Pipeline:
    return Pipeline(
        [
            threshold_filter(FILTER_MIN_VALUE),
            RollingAggregateStep(window_size=20),
            enrich_step,
        ]
    )


async def _demo_producer(app: FastAPI) -> None:
    source = DemoEventSource(rate=DEMO_SOURCE_RATE)
    async for raw_event in source.stream():
        app.state.stats["ingested"] += 1
        try:
            app.state.event_queue.put_nowait(raw_event)
        except asyncio.QueueFull:
            logger.warning("Queue full, dropping demo event")


async def _process_and_broadcast(app: FastAPI) -> None:
    while True:
        raw_event = await app.state.event_queue.get()
        try:
            processed = app.state.pipeline.process(raw_event)
            if processed is not None:
                app.state.stats["processed"] += 1
                app.state.recent_processed_ts.append(time.time())
                await app.state.manager.broadcast(processed)
            else:
                app.state.stats["dropped"] += 1
        except Exception:
            logger.exception("Error processing event: %s", raw_event)


def _events_per_second(app: FastAPI, window_seconds: float = 2.0) -> float:
    cutoff = time.time() - window_seconds
    count = sum(1 for ts in app.state.recent_processed_ts if ts >= cutoff)
    return round(count / window_seconds, 1)


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.event_queue = asyncio.Queue(maxsize=QUEUE_MAXSIZE)
    app.state.manager = ConnectionManager()
    app.state.pipeline = build_pipeline()
    app.state.stats = {"ingested": 0, "processed": 0, "dropped": 0}
    app.state.recent_processed_ts = deque(maxlen=2000)

    tasks = [asyncio.create_task(_process_and_broadcast(app))]
    if DEMO_SOURCE_ENABLED:
        tasks.append(asyncio.create_task(_demo_producer(app)))
    app.state.background_tasks = tasks

    logger.info(
        "Stream processing pipeline started (demo_source=%s, rate=%s/s)",
        DEMO_SOURCE_ENABLED,
        DEMO_SOURCE_RATE,
    )
    try:
        yield
    finally:
        for task in tasks:
            task.cancel()
        for task in tasks:
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass


app = FastAPI(title="Python Web Stream Processing", lifespan=lifespan)


@app.get("/health")
async def health(request: Request) -> Dict[str, Any]:
    state = request.app.state
    return {
        "status": "ok",
        "clients": state.manager.active_count,
        "queue_size": state.event_queue.qsize(),
        "ingested": state.stats["ingested"],
        "processed": state.stats["processed"],
        "dropped": state.stats["dropped"],
        "events_per_second": _events_per_second(request.app),
    }


@app.post("/ingest")
async def ingest(event: Dict[str, Any], request: Request) -> Dict[str, str]:
    """External producers (a load test script, a Kafka/MQTT bridge, ...)
    push raw events here instead of the built-in demo generator.
    """
    state = request.app.state
    event.setdefault("ts_generated", time.time())
    state.stats["ingested"] += 1
    try:
        state.event_queue.put_nowait(event)
    except asyncio.QueueFull:
        return {"status": "dropped"}
    return {"status": "queued"}


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket) -> None:
    manager: ConnectionManager = websocket.app.state.manager
    await manager.connect(websocket)
    try:
        while True:
            # Dashboard clients don't send anything meaningful; this just
            # keeps the connection open and detects disconnects promptly.
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        await manager.disconnect(websocket)


app.mount("/", StaticFiles(directory=str(CLIENT_DIR), html=True), name="client")
