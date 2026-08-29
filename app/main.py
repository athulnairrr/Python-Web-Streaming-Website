"""FastAPI application wiring ingestion -> processing -> broadcasting together.

This app runs two independent, parallel pipelines that share the same
architecture and code (`Pipeline`, `ConnectionManager`):

1. The original generic demo pipeline (`/ingest`, `/ws`, `/health`) -
   unchanged from the initial MVP.
2. A market-signals pipeline (`/signals/ingest`, `/ws/signals`,
   `/signals/health`) - synthetic OTC + live-market sources feeding a
   pluggable signal-generation step. See PROJECT_DOCUMENTATION.md for the
   full picture; the short version:

   Ingestion (OTC/live mock sources) -> asyncio.Queue -> Processing
   (signal-generation step -> enrich) -> Broadcasting (WebSocket fan-out)
   -> dashboard (client/signals.html)

To plug in Kafka/MQTT (generic pipeline) or a real market/broker feed
(signals pipeline) later: write a class implementing `EventSource` (see
app/ingestion/base.py, or app/ingestion/market_adapter.py for the
signals-specific contract) that pushes onto the relevant queue, start it
in `lifespan`, and nothing else in this file needs to change.

All mutable pipeline state (queues, connection managers, counters) lives
on `app.state`, created fresh in `lifespan` for each app run, rather than
as module-level globals - that keeps repeated app startups (as happens
once per `TestClient` in tests) fully isolated instead of leaking
event-loop-bound objects between runs.
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
from app.ingestion.live_source import LiveMockSource
from app.ingestion.mock_market import (
    DEFAULT_LIVE_SYMBOLS,
    DEFAULT_OTC_SYMBOLS,
    DEFAULT_TIMEFRAMES,
    parse_list,
)
from app.ingestion.otc_source import OTCMockSource
from app.integrations.telegram_bot import TelegramNotifier
from app.processing.pipeline import Pipeline
from app.processing.signal_steps import RandomSignalStrategy, make_signal_step
from app.processing.steps import RollingAggregateStep, enrich_step, threshold_filter

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("stream_app")

# --- generic demo pipeline config (unchanged from the initial MVP) ---
QUEUE_MAXSIZE = int(os.getenv("QUEUE_MAXSIZE", "5000"))
DEMO_SOURCE_ENABLED = os.getenv("DEMO_SOURCE_ENABLED", "true").lower() == "true"
DEMO_SOURCE_RATE = float(os.getenv("DEMO_SOURCE_RATE", "100"))
FILTER_MIN_VALUE = float(os.getenv("FILTER_MIN_VALUE", "0"))
CLIENT_DIR = Path(__file__).resolve().parent.parent / "client"

# --- market signals pipeline config ---
OTC_SOURCE_ENABLED = os.getenv("OTC_SOURCE_ENABLED", "true").lower() == "true"
LIVE_SOURCE_ENABLED = os.getenv("LIVE_SOURCE_ENABLED", "true").lower() == "true"
OTC_SYMBOLS = parse_list(os.getenv("OTC_SYMBOLS", ""), DEFAULT_OTC_SYMBOLS, upper=True)
LIVE_SYMBOLS = parse_list(os.getenv("LIVE_SYMBOLS", ""), DEFAULT_LIVE_SYMBOLS, upper=True)
SIGNAL_TIMEFRAMES = parse_list(os.getenv("SIGNAL_TIMEFRAMES", ""), DEFAULT_TIMEFRAMES, upper=True)
OTC_SIGNAL_INTERVAL_SECONDS = float(os.getenv("OTC_SIGNAL_INTERVAL_SECONDS", "3"))
LIVE_SIGNAL_INTERVAL_SECONDS = float(os.getenv("LIVE_SIGNAL_INTERVAL_SECONDS", "4"))
SIGNAL_QUEUE_MAXSIZE = int(os.getenv("SIGNAL_QUEUE_MAXSIZE", "2000"))


def build_pipeline() -> Pipeline:
    return Pipeline(
        [
            threshold_filter(FILTER_MIN_VALUE),
            RollingAggregateStep(window_size=20),
            enrich_step,
        ]
    )


def build_signal_pipeline() -> Pipeline:
    """Filter obviously-bad prices, generate a signal, then stamp latency.

    `make_signal_step` is the one swappable piece: replace
    `RandomSignalStrategy` with real decision logic later without
    touching ingestion, filtering, or broadcasting.
    """
    return Pipeline(
        [
            threshold_filter(0),
            make_signal_step(RandomSignalStrategy()),
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


def _events_per_second(
    recent_ts: deque, window_seconds: float = 2.0
) -> float:
    cutoff = time.time() - window_seconds
    count = sum(1 for ts in recent_ts if ts >= cutoff)
    return round(count / window_seconds, 1)


async def _market_producer(app: FastAPI, source) -> None:
    """Runs one OTC/live mock source (or, later, a real MarketDataAdapter)
    and pushes everything it yields onto the shared signal queue."""
    async for raw_event in source.stream():
        app.state.signal_stats["ingested"] += 1
        try:
            app.state.signal_queue.put_nowait(raw_event)
        except asyncio.QueueFull:
            logger.warning("Signal queue full, dropping event from %s", raw_event.get("source"))


async def _process_and_broadcast_signals(app: FastAPI) -> None:
    while True:
        raw_event = await app.state.signal_queue.get()
        try:
            processed = app.state.signal_pipeline.process(raw_event)
            if processed is not None:
                app.state.signal_stats["processed"] += 1
                app.state.recent_signal_ts.append(time.time())
                await app.state.signal_manager.broadcast(processed)
            else:
                app.state.signal_stats["dropped"] += 1
        except Exception:
            logger.exception("Error processing signal event: %s", raw_event)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # --- generic demo pipeline ---
    app.state.event_queue = asyncio.Queue(maxsize=QUEUE_MAXSIZE)
    app.state.manager = ConnectionManager()
    app.state.pipeline = build_pipeline()
    app.state.stats = {"ingested": 0, "processed": 0, "dropped": 0}
    app.state.recent_processed_ts = deque(maxlen=2000)

    # --- market signals pipeline ---
    app.state.signal_queue = asyncio.Queue(maxsize=SIGNAL_QUEUE_MAXSIZE)
    app.state.signal_manager = ConnectionManager()
    app.state.signal_pipeline = build_signal_pipeline()
    app.state.signal_stats = {"ingested": 0, "processed": 0, "dropped": 0}
    app.state.recent_signal_ts = deque(maxlen=2000)
    app.state.telegram = TelegramNotifier()

    tasks = [
        asyncio.create_task(_process_and_broadcast(app)),
        asyncio.create_task(_process_and_broadcast_signals(app)),
    ]
    if DEMO_SOURCE_ENABLED:
        tasks.append(asyncio.create_task(_demo_producer(app)))
    if OTC_SOURCE_ENABLED:
        otc_source = OTCMockSource(OTC_SYMBOLS, SIGNAL_TIMEFRAMES, OTC_SIGNAL_INTERVAL_SECONDS)
        tasks.append(asyncio.create_task(_market_producer(app, otc_source)))
    if LIVE_SOURCE_ENABLED:
        live_source = LiveMockSource(LIVE_SYMBOLS, SIGNAL_TIMEFRAMES, LIVE_SIGNAL_INTERVAL_SECONDS)
        tasks.append(asyncio.create_task(_market_producer(app, live_source)))
    app.state.background_tasks = tasks

    logger.info(
        "Stream processing pipeline started (demo_source=%s rate=%s/s | "
        "otc=%s symbols=%s | live=%s symbols=%s | timeframes=%s | "
        "telegram_enabled=%s)",
        DEMO_SOURCE_ENABLED,
        DEMO_SOURCE_RATE,
        OTC_SOURCE_ENABLED,
        OTC_SYMBOLS,
        LIVE_SOURCE_ENABLED,
        LIVE_SYMBOLS,
        SIGNAL_TIMEFRAMES,
        app.state.telegram.enabled,
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
        "events_per_second": _events_per_second(state.recent_processed_ts),
    }


@app.get("/signals/health")
async def signals_health(request: Request) -> Dict[str, Any]:
    state = request.app.state
    return {
        "status": "ok",
        "clients": state.signal_manager.active_count,
        "queue_size": state.signal_queue.qsize(),
        "ingested": state.signal_stats["ingested"],
        "processed": state.signal_stats["processed"],
        "dropped": state.signal_stats["dropped"],
        "events_per_second": _events_per_second(state.recent_signal_ts),
        "otc_enabled": OTC_SOURCE_ENABLED,
        "live_enabled": LIVE_SOURCE_ENABLED,
        "timeframes": SIGNAL_TIMEFRAMES,
        "telegram_enabled": state.telegram.enabled,
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


@app.post("/signals/ingest")
async def signals_ingest(event: Dict[str, Any], request: Request) -> Dict[str, str]:
    """Push a raw market event (symbol/price/timeframe/source) into the
    signals pipeline. This is the same kind of extension point `/ingest`
    is for the generic pipeline - a real market/broker adapter can push
    here over HTTP instead of (or in addition to) being wired directly
    into `lifespan` as a background task.
    """
    state = request.app.state
    event.setdefault("ts_generated", time.time())
    state.signal_stats["ingested"] += 1
    try:
        state.signal_queue.put_nowait(event)
    except asyncio.QueueFull:
        return {"status": "dropped"}
    return {"status": "queued"}


@app.post("/telegram/test-alert")
async def telegram_test_alert(request: Request) -> Dict[str, Any]:
    """Send a test message via the configured Telegram bot/channel. Used
    by the dashboard's "Send Test Alert" button. Returns a clear error
    (not a 500) if TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID aren't set.
    """
    telegram: TelegramNotifier = request.app.state.telegram
    result = await telegram.send_message(TelegramNotifier.test_message())
    return result


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


@app.websocket("/ws/signals")
async def websocket_signals_endpoint(websocket: WebSocket) -> None:
    manager: ConnectionManager = websocket.app.state.signal_manager
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        await manager.disconnect(websocket)


app.mount("/", StaticFiles(directory=str(CLIENT_DIR), html=True), name="client")
