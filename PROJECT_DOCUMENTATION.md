# Project Documentation — Python Web Stream Processing

## 1. What this application does

This is a real-time data stream processing service. Events arrive continuously,
are transformed on the fly (filtered, aggregated, enriched), and the results
are pushed immediately to any connected browser over WebSocket. A live
dashboard visualises the stream as it happens — events/sec, latency, a
rolling chart, and a table of recent events.

Out of the box, the app ships with a synthetic event generator so it's fully
demonstrable with no external dependencies. The architecture is deliberately
laid out so a real source (Kafka, MQTT, etc.) can be dropped in later without
touching the processing or broadcasting code.

## 2. Architecture and data flow

```
┌────────────┐   asyncio.Queue   ┌────────────┐   WebSocket    ┌──────────┐
│ Ingestion  │ ────────────────▶ │ Processing │ ──────────────▶│ Browser  │
│            │                   │  Pipeline  │   broadcast    │ dashboard│
└────────────┘                   └────────────┘                └──────────┘
```

There are three independent layers, connected by a single in-process queue:

1. **Ingestion** produces raw events and puts them on a queue.
2. **Processing** takes events off the queue, one at a time, and runs them
   through a pipeline of steps.
3. **Broadcasting** sends every event that survives the pipeline to all
   currently connected WebSocket clients.

Each layer only depends on the shape of the data passed between them (plain
Python dicts), not on how the neighbouring layer is implemented. That's what
lets the ingestion source be swapped later (see §9) without changing
processing or broadcasting at all.

All shared pipeline state (the queue, the WebSocket connection manager, the
pipeline instance, running counters) lives on FastAPI's `app.state`, created
fresh each time the app starts (see `app/main.py`, function `lifespan`).
There are no other global/module-level mutable objects.

## 3. Ingestion layer (`app/ingestion/`)

- `base.py` defines `EventSource`, an abstract class with one method:
  `async def stream(self) -> AsyncIterator[dict]`. Anything that yields raw
  event dicts qualifies.
- `demo_source.py` implements `DemoEventSource`, the built-in generator. It
  produces events shaped like:
  ```json
  {"id": 1, "category": "cpu", "value": 47.32, "ts_generated": 1699999999.123}
  ```
  `category` is one of `cpu`, `memory`, `network`, `disk`; `value` is drawn
  from a normal distribution (mean 50, stdev 15). It paces itself against a
  target rate (`DEMO_SOURCE_RATE`, default 100/sec) by checking elapsed wall
  time on a ~50ms tick and emitting however many events are due, rather than
  sleeping once per event — this keeps the long-run rate accurate and avoids
  relying on very short (<10ms) timers, which proved unreliable under load
  during testing (see §10).
- **External ingestion**: `POST /ingest` accepts a JSON event body and pushes
  it onto the same queue the demo generator uses. This is the integration
  point a load-test script or a future broker bridge uses today.

## 4. Processing layer (`app/processing/`)

A `Pipeline` (`pipeline.py`) is an ordered list of steps. Each step is a
plain function: `event -> event | None`. Returning `None` drops the event
and short-circuits the rest of the pipeline. The default pipeline
(assembled in `app/main.py`, `build_pipeline()`) runs, in order:

1. **Filtering** — `threshold_filter(min_value)`: drops any event whose
   `value` is below `FILTER_MIN_VALUE` (default `0`, so almost nothing is
   filtered unless configured otherwise). This demonstrates the filtering
   stage the brief asked for.
2. **Aggregation** — `RollingAggregateStep`: keeps a fixed-size sliding
   window (default 20 events) of recent values *per category*, and attaches
   the current rolling `mean`/`stdev` for that category to the event
   (`rolling_mean`, `rolling_stdev`, `window_size`). This is computed online,
   per event, rather than as a separate batch job.
3. **Enrichment** — `enrich_step`: stamps `ts_processed` (server processing
   time) and computes `latency_ms` — the time from when the event was
   generated (`ts_generated`) to when it finished processing. This is the
   field the dashboard and load test use to demonstrate end-to-end latency.

Steps never mutate their input dict; they return new dicts. This keeps the
pipeline predictable and easy to unit test (see `tests/test_steps.py`,
`tests/test_pipeline.py`).

## 5. Broadcasting layer (`app/broadcasting/`)

`ConnectionManager` (`manager.py`) tracks connected WebSocket clients in a
set and exposes `broadcast(event)`, which JSON-serialises the event once and
sends it to every connected client. If sending to a client fails (e.g. it
disconnected without a clean close), that connection is silently pruned from
the set on the next broadcast. This is the only place that knows about
WebSocket clients — the processing pipeline just calls `manager.broadcast(...)`.

## 6. WebSocket flow and dashboard

- The FastAPI route `WS /ws` accepts a connection, registers it with
  `ConnectionManager`, and then just waits (it doesn't expect the client to
  send anything meaningful — this loop exists mainly to detect disconnects
  promptly).
- The processing background task (`_process_and_broadcast` in `app/main.py`)
  continuously pulls from the queue, runs the pipeline, and broadcasts
  whatever comes out.
- The dashboard (`client/index.html`, `client/app.js`, `client/style.css`) is
  a dependency-free static page:
  - Opens a WebSocket to `/ws` on load and reconnects automatically if it
    drops.
  - Keeps a rolling in-memory history of recent events for the chart and
    table (no persistence — this is a live view, not a historical one).
  - Renders: events/sec, average and p95 latency (computed client-side from
    the last ~200 events), a two-series canvas chart (event value and
    latency over the last ~60 seconds), and a table of the most recent 40
    events with their category, value, rolling mean, and latency.
  - Polls `GET /health` once a second for server-side counters the
    WebSocket stream doesn't carry (queue depth, dropped count, connected
    client count).
  - The chart and stats are drawn with plain `<canvas>` and vanilla JS — no
    charting library, to keep the page dependency-free and fast to load.

## 7. API endpoints

| Endpoint | Method | Purpose |
|---|---|---|
| `/` | GET | Serves the dashboard (static files, via `client/`) |
| `/health` | GET | Status, connected client count, queue depth, and ingested/processed/dropped/events-per-second counters |
| `/ingest` | POST | Accepts a raw JSON event and pushes it into the pipeline |
| `/ws` | WebSocket | Subscribes to the live stream of processed events |

## 8. Docker setup

- `Dockerfile`: `python:3.12-slim`, installs `requirements.txt`, copies
  `app/` and `client/`, runs `uvicorn app.main:app` on port 8000.
- `docker-compose.yml`: builds the image, maps port 8000, and sets the
  three environment variables that control the demo generator and filter
  (`DEMO_SOURCE_ENABLED`, `DEMO_SOURCE_RATE`, `FILTER_MIN_VALUE`).
- `docker compose up --build` is the single command to get the whole stack
  running locally or on a host.

## 9. Testing and the 100 events/sec latency test

18 automated tests, run with `pytest`:

- `tests/test_steps.py` — filtering, rolling aggregation (including window
  eviction and per-category isolation), enrichment, and that steps don't
  mutate their input.
- `tests/test_pipeline.py` — steps chain in order, and a dropped event
  short-circuits the remaining steps.
- `tests/test_manager.py` — WebSocket fan-out and dead-connection pruning,
  using fake WebSocket objects.
- `tests/test_api.py` — `/health`, `/ingest`, and a full round trip: an
  event posted to `/ingest` comes back out over `/ws` fully processed.

**Load test** (`scripts/load_test.py`): posts events to `/ingest` at a
target rate (default 100/sec) for a configurable duration, listens on `/ws`
at the same time, and reports avg/p50/p95/p99 end-to-end latency from the
`latency_ms` field. Run with the demo generator disabled so the numbers
aren't mixed with its output:

```bash
DEMO_SOURCE_ENABLED=false uvicorn app.main:app
python scripts/load_test.py --rate 100 --duration 10
```

Measured result during development: **~99 events/sec sustained, p95 latency
around 16ms** — well inside the 200ms target. Numbers will vary with
hardware, but the pipeline itself adds only sub-millisecond overhead per
event; almost all measured latency is network/scheduling, not processing.

## 10. Swapping in Kafka/MQTT later

The ingestion layer is the only piece that needs to change:

1. Write a class that implements `EventSource` (`app/ingestion/base.py`) —
   e.g. `KafkaEventSource`, wrapping `aiokafka` or `confluent-kafka`'s
   consumer, or an MQTT client subscribing to a topic. Its `stream()` method
   just needs to yield dicts shaped like the events the pipeline already
   expects (at minimum a `value`; `ts_generated` if you want latency
   tracking).
2. In `app/main.py`, start it in `lifespan` the same way `DemoEventSource`
   is started today, pushing onto `app.state.event_queue`.
3. Nothing in `app/processing/` or `app/broadcasting/` changes — they only
   ever see plain dicts coming off the queue.

The `POST /ingest` endpoint already demonstrates this decoupling in a small
way: it's a second ingestion path (external HTTP push) that feeds the exact
same queue and pipeline as the demo generator, with no special-casing
downstream.

## 11. Key design decisions

- **`asyncio.Queue` as the seam between ingestion and processing.** It's the
  simplest possible decoupling point for a single-process app, and it's the
  same shape a Kafka/MQTT consumer would push into — the queue plays the
  role of a local "topic."
- **State on `app.state`, not module globals.** Early in development,
  module-level singletons (queue, connection manager) caused a subtle bug:
  they persisted across independent app startups (e.g. once per test), and
  a WebSocket consumer task from a previous run interfered with a later
  one. Scoping everything to `app.state`, recreated fresh in `lifespan`,
  fixed this and is the more correct pattern for anything the app might
  restart or run multiple isolated instances of.
- **Tick-based pacing instead of one `asyncio.sleep()` per event.** Both the
  demo generator and the load-test script initially slept once per event
  (e.g. 10ms for 100/sec). Under concurrent load this was observed to be
  unreliable — very short timers fired far more often than intended on the
  test machine's event loop, producing rates 100x higher than configured.
  Both were changed to a coarser ~50ms tick that computes how many events
  are due from real elapsed time and emits that many at once. This is more
  robust and, as a side effect, self-corrects any drift instead of
  accumulating it.
- **No charting library.** The dashboard's chart is drawn directly on
  `<canvas>` in vanilla JS. For two rolling line series this is simple
  enough to hand-roll, and it keeps the client at zero dependencies —
  useful for a page meant to be dropped behind any static host or CDN.
- **Rolling aggregation, not batch.** The brief asked for aggregation as
  part of an on-the-fly pipeline, not a periodic batch job, so
  `RollingAggregateStep` maintains a small per-category window in memory and
  updates it per event. This trades unbounded historical accuracy for O(1)
  per-event cost and immediate output — appropriate for a live monitoring
  use case.
- **In-memory only — no persistence.** Nothing here is written to disk or a
  database; the dashboard's history is client-side and resets on reload.
  This matches the brief ("first working version," "raw JSON is fine for
  now") and keeps the MVP simple. Adding durable storage or replay would be
  a deliberate next step, not something this version does.
