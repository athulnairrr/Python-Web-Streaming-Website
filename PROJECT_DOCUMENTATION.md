# Project Documentation — Python Web Stream Processing

This repo's primary deliverable is the **market signals dashboard
prototype** (§1), built on top of a general-purpose real-time stream
processing engine (§2 onward), which it reuses without modification.

---

## 1. Market Signals Prototype (OTC / Live)

> **This is a prototype demo, not a trading system.** Every signal shown is
> generated from synthetic/mock data using a placeholder strategy that
> picks a direction at random. Nothing here is real market data, nothing
> is predictive, and nothing here is trading advice. The purpose of this
> feature is to demonstrate a real-time data pipeline convincingly, not to
> produce usable trading signals.

### 1.1 What it adds

A pipeline — built on exactly the same
ingestion → processing → broadcasting → WebSocket architecture as the
generic engine described from §2 onward — that simulates two market data
sources (OTC and "live") and turns their price data into mock trade
signals in real time, shown on a dedicated dashboard with Telegram
alerting.

It runs **alongside** the generic engine's own demo, in the same app,
without modifying it: same `Pipeline`/`ConnectionManager` classes, its own
`asyncio.Queue`, its own set of endpoints, its own dashboard page.

### 1.2 Architecture and data flow

```
┌───────────────┐                                  ┌──────────────────┐
│ OTCMockSource │──┐                                │ signals.html     │
└───────────────┘  │   asyncio.Queue   ┌──────────┐ │ (OTC | Live      │
                    ├──────────────────▶│ Pipeline │─▶│  sections)     │
┌────────────────┐  │                   │ filter → │ │  via /ws/signals│
│ LiveMockSource │──┘                   │ signal → │ └──────────────────┘
└────────────────┘                      │ enrich   │
   (+ POST /signals/ingest)             └──────────┘
```

- **Ingestion** — `OTCMockSource` and `LiveMockSource`
  (`app/ingestion/otc_source.py`, `live_source.py`) each run as their own
  background task, independently generating fake price ticks for their
  own configurable symbol list, at their own configurable interval. Both
  push onto the same `signal_queue`, tagging every event with
  `"source": "otc"` or `"source": "live"` so downstream consumers (and the
  dashboard) can split them apart. `POST /signals/ingest` is the same kind
  of external-push entry point `POST /ingest` is for the generic engine.
- **Processing** — one pipeline (`build_signal_pipeline()` in
  `app/main.py`) handles both sources: a basic sanity filter, then the
  **signal-generation step** (§1.3), then the same `enrich_step` used by
  the generic engine (latency tracking is identical).
- **Broadcasting** — a dedicated `ConnectionManager` fans processed
  signals out over `WS /ws/signals`.
- **Dashboard** — `client/signals.html` / `signals.js` connect to
  `/ws/signals` and route each incoming event into an "OTC" or "Live"
  table based on its `source` field, plus a stats bar and a Telegram test
  button.

### 1.3 Signal shape and modular generation

Every signal broadcast to the dashboard has the same core shape:

```json
{
  "id": "otc-482",
  "source": "otc",
  "symbol": "EURUSD-OTC",
  "timeframe": "M1",
  "direction": "CALL",
  "confidence": 71.4,
  "price": 1.08423,
  "timestamp": "2024-01-01T12:00:00.123456+00:00",
  "ts_generated": 1699999999.123,
  "ts_processed": 1699999999.126,
  "latency_ms": 2.7,
  "mock": true
}
```

`pair/symbol`, `direction`, `timeframe`, and `timestamp` — the four fields
the brief asked for — are present on every signal, along with the
`mock: true` flag and the enrichment/latency fields the generic engine
already provides.

Signal generation itself is isolated in one small, swappable piece:
`app/processing/signal_steps.py` defines a `SignalStrategy` interface with
one method, `generate(event) -> event`. The prototype ships
`RandomSignalStrategy`, which just picks `CALL`/`PUT` uniformly at random
and a cosmetic confidence score — this is what makes it a prototype and
not a trading system. Replacing it with a real strategy (technical
indicators, a model, a rules engine) means writing one new class and
changing one line in `build_signal_pipeline()`; ingestion, filtering,
enrichment, broadcasting, and the dashboard all stay exactly the same.

### 1.4 Timeframes

`SIGNAL_TIMEFRAMES` (default `M1,M5`) is a configurable, comma-separated
list; both mock sources round-robin through it, tagging each emitted
signal with one of the configured timeframes. **Simplification to be
transparent about:** the demo does not wait out real candle durations (a
real M5 candle takes 5 real minutes to close) — `timeframe` here is a
label for which chart a signal targets, paced instead by
`OTC_SIGNAL_INTERVAL_SECONDS` / `LIVE_SIGNAL_INTERVAL_SECONDS` (a few
seconds), so the dashboard stays lively for a demo. A real adapter (§1.7)
would presumably emit on the real candle boundary instead.

### 1.5 Configuration

All environment variables, all optional:

| Variable | Default | Meaning |
|---|---|---|
| `OTC_SOURCE_ENABLED` | `true` | Run the synthetic OTC source |
| `LIVE_SOURCE_ENABLED` | `true` | Run the synthetic live-market source |
| `OTC_SYMBOLS` | `EURUSD-OTC,GBPUSD-OTC,USDJPY-OTC,AUDCAD-OTC` | Comma-separated OTC symbol list |
| `LIVE_SYMBOLS` | `EURUSD,GBPUSD,BTCUSD,ETHUSD` | Comma-separated live symbol list |
| `SIGNAL_TIMEFRAMES` | `M1,M5` | Comma-separated timeframes signals rotate through |
| `OTC_SIGNAL_INTERVAL_SECONDS` | `3` | Seconds between OTC signal batches |
| `LIVE_SIGNAL_INTERVAL_SECONDS` | `4` | Seconds between live signal batches |
| `SIGNAL_QUEUE_MAXSIZE` | `2000` | Backpressure limit on the signal queue |
| `TELEGRAM_BOT_TOKEN` | unset | Bot token from [@BotFather](https://t.me/BotFather) |
| `TELEGRAM_CHAT_ID` | unset | Target chat/channel id (or `@channelusername`) |

### 1.6 Telegram setup

1. Message [@BotFather](https://t.me/BotFather) on Telegram, run
   `/newbot`, and copy the bot token it gives you.
2. Get a chat id to send to: add the bot to a group/channel (and give it
   permission to post), or message it directly and read your own numeric
   user id from `https://api.telegram.org/bot<token>/getUpdates`.
3. Set `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` (in `docker-compose.yml`,
   your shell, or a `.env` file) and restart the app.
4. Open the signals dashboard and click **Send Test Alert**. The
   "Telegram" stat card will read "Configured", and a confirmation message
   should arrive in the configured chat within a couple of seconds.

If the credentials are missing or wrong, `POST /telegram/test-alert`
returns `{"ok": false, "error": "..."}` (never a 500), and the dashboard
shows that error inline — this is the intended, safe fallback state.
`app/integrations/telegram_bot.py` is a plain `httpx` call to the Telegram
Bot API's `sendMessage` endpoint; no SDK dependency. Per-signal alerts
(rather than just the test button) are not wired up by default, since a
100-events/sec-class demo stream would immediately hit Telegram's rate
limits — `TelegramNotifier.send_message` is ready to be called from
`_process_and_broadcast_signals` with throttling if that's wanted later.

### 1.7 Where a real data adapter plugs in

`app/ingestion/market_adapter.py` defines `MarketDataAdapter(EventSource)`
— a base class with one abstract method, `stream()`, that a real
integration would subclass. **No scraping and no real-broker integration
is implemented in this prototype** — this is intentionally just the
contract:

1. Subclass `MarketDataAdapter`, implement `stream()` to yield events
   shaped exactly like the mock sources do (`id`, `source`, `symbol`,
   `timeframe`, `price`, `timestamp`, `ts_generated`).
2. Instantiate and start it in `app/main.py`'s `lifespan`, pushing onto
   `app.state.signal_queue` — the same pattern `OTCMockSource` and
   `LiveMockSource` already follow.
3. Nothing else changes: the signal-generation step, the pipeline, the
   broadcasting layer, and the dashboard all consume the same event shape
   regardless of where it came from. `POST /signals/ingest` is also
   available if a real adapter prefers to push over HTTP instead of
   running as a background task.

Building a real Quotex (or any other broker) integration is a distinct,
follow-on piece of work — it depends entirely on what that provider
actually offers (an official API vs. anything else) and its terms of
service, which is why it's deliberately out of scope here.

---

## 2. Underlying Engine: What It Does

This is a real-time data stream processing service. Events arrive continuously,
are transformed on the fly (filtered, aggregated, enriched), and the results
are pushed immediately to any connected browser over WebSocket. A live
dashboard visualises the stream as it happens — events/sec, latency, a
rolling chart, and a table of recent events.

Out of the box, the app ships with a synthetic event generator so it's fully
demonstrable with no external dependencies. The architecture is deliberately
laid out so a real source (Kafka, MQTT, etc.) can be dropped in later without
touching the processing or broadcasting code.

**§§2–11 below describe this generic engine.** The market signals prototype
(§1) is built on top of it, unmodified, running as a second, independent
pipeline in the same app.

## 3. Architecture and data flow

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
lets the ingestion source be swapped later (see §10) without changing
processing or broadcasting at all.

All shared pipeline state (the queue, the WebSocket connection manager, the
pipeline instance, running counters) lives on FastAPI's `app.state`, created
fresh each time the app starts (see `app/main.py`, function `lifespan`).
There are no other global/module-level mutable objects.

## 4. Ingestion layer (`app/ingestion/`)

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
  during testing (see §11).
- **External ingestion**: `POST /ingest` accepts a JSON event body and pushes
  it onto the same queue the demo generator uses. This is the integration
  point a load-test script or a future broker bridge uses today.

## 5. Processing layer (`app/processing/`)

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

## 6. Broadcasting layer (`app/broadcasting/`)

`ConnectionManager` (`manager.py`) tracks connected WebSocket clients in a
set and exposes `broadcast(event)`, which JSON-serialises the event once and
sends it to every connected client. If sending to a client fails (e.g. it
disconnected without a clean close), that connection is silently pruned from
the set on the next broadcast. This is the only place that knows about
WebSocket clients — the processing pipeline just calls `manager.broadcast(...)`.

## 7. WebSocket flow and dashboard

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

## 8. API endpoints

| Endpoint | Method | Purpose |
|---|---|---|
| `/` | GET | Serves the dashboard (static files, via `client/`) |
| `/health` | GET | Status, connected client count, queue depth, and ingested/processed/dropped/events-per-second counters |
| `/ingest` | POST | Accepts a raw JSON event and pushes it into the pipeline |
| `/ws` | WebSocket | Subscribes to the live stream of processed events |

## 9. Docker setup

- `Dockerfile`: `python:3.12-slim`, installs `requirements.txt`, copies
  `app/` and `client/`, runs `uvicorn app.main:app` on port 8000.
- `docker-compose.yml`: builds the image, maps port 8000, and sets the
  environment variables that control both the generic demo generator/filter
  and the market signals pipeline (see §1.5).
- `docker compose up --build` is the single command to get the whole stack
  running locally or on a host.

## 10. Testing and the 100 events/sec latency test

31 automated tests, run with `pytest`:

- `tests/test_steps.py` — filtering, rolling aggregation (including window
  eviction and per-category isolation), enrichment, and that steps don't
  mutate their input.
- `tests/test_pipeline.py` — steps chain in order, and a dropped event
  short-circuits the remaining steps.
- `tests/test_manager.py` — WebSocket fan-out and dead-connection pruning,
  using fake WebSocket objects.
- `tests/test_api.py` — `/health`, `/ingest`, and a full round trip: an
  event posted to `/ingest` comes back out over `/ws` fully processed.
- `tests/test_signal_steps.py` — the signal-generation step adds a valid
  direction without mutating its input.
- `tests/test_telegram.py` — the Telegram notifier's enabled/disabled
  logic and message formatting, with the HTTP call mocked (no real
  network calls in the test suite).
- `tests/test_signals_api.py` — `/signals/health`, `/signals/ingest` →
  `/ws/signals` round trip, and `/telegram/test-alert` reporting "not
  configured" cleanly when no credentials are set.

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
The market signals pipeline shares this exact processing/broadcasting code,
so the same latency characteristics apply there too.

## 11. Swapping in Kafka/MQTT later

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

(For the market signals pipeline's equivalent extension point, see §1.7.)

## 12. Key design decisions

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
- **Signal generation isolated as one swappable step (§1.3).** The market
  signals prototype's only "decision-making" code is `RandomSignalStrategy`
  — a placeholder chosen specifically so replacing it with real logic later
  never touches ingestion, broadcasting, or the dashboard.
