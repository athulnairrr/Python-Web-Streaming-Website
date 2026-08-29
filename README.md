# Python Web Stream Processing

A real-time market signals dashboard prototype (OTC + live, with Telegram
alerts), built on top of a general-purpose real-time stream processing
engine — ingestion → processing → broadcasting → WebSocket → dashboard.

## Market Signals Dashboard (OTC + Live)

> **Prototype demo only.** All signals are generated from synthetic/mock
> data using a random placeholder strategy. Nothing here is real market
> data, nothing is predictive, and nothing here is trading advice.

![Market signals dashboard](docs/signals_dashboard.png)

A second, independent pipeline — built on the exact same architecture as
the generic engine below — simulates two market data sources (OTC and
"live") and turns their price data into mock trade signals in real time,
shown on a dedicated dashboard (`client/signals.html`) with separate
OTC/Live sections and Telegram alerting.

- **Ingestion**: `OTCMockSource` and `LiveMockSource`
  (`app/ingestion/`) — synthetic random-walk price generators, each
  independently configurable (symbols, timeframes, interval).
  `MarketDataAdapter` is the clean extension point for a real
  market/broker feed later — no scraping is built in.
- **Processing**: signal generation is isolated as one pluggable step —
  `SignalStrategy` → `RandomSignalStrategy` (a placeholder with no
  predictive value, used only to demonstrate the pipeline end to end).
- **Broadcasting**: a dedicated `/ws/signals` WebSocket channel.
- **Telegram**: a working "Send Test Alert" button, configured via
  `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID` environment variables.

See [PROJECT_DOCUMENTATION.md](PROJECT_DOCUMENTATION.md) (§1) for the full
architecture, configuration reference, Telegram setup, mock-data caveats,
and exactly where a real data adapter plugs in.

## Underlying Engine: Generic Stream Processing

A synthetic event source is ingested, filtered/aggregated/enriched on the
fly, and broadcast over WebSocket to a live monitoring dashboard
(`client/index.html`). This is the original MVP the market signals
prototype above is built on top of.

![Live monitoring dashboard](docs/dashboard.png)

```
┌────────────┐    asyncio.Queue    ┌────────────┐    WebSocket    ┌──────────┐
│ Ingestion  │ ──────────────────▶ │ Processing │ ──────────────▶ │ Browser  │
│ (demo gen  │                     │  Pipeline  │   broadcast     │ dashboard│
│  or POST   │                     │ filter →   │                 │          │
│  /ingest)  │                     │ aggregate →│                 │          │
└────────────┘                     │ enrich     │                 └──────────┘
                                    └────────────┘
```

- **Ingestion** (`app/ingestion/`): a synthetic generator (`DemoEventSource`) produces
  ~100 events/sec out of the box, or external producers can `POST /ingest`. Both
  feed the same internal queue, so swapping in a Kafka or MQTT consumer later
  just means writing one more class that implements `EventSource` — nothing
  downstream changes.
- **Processing** (`app/processing/`): a composable `Pipeline` of steps —
  `threshold_filter` (filtering), `RollingAggregateStep` (per-category
  rolling mean/stdev — aggregation), `enrich_step` (adds processing
  timestamp + latency — enrichment).
- **Broadcasting** (`app/broadcasting/`): `ConnectionManager` fans processed
  events out to every connected WebSocket client and prunes dead ones.
- **Dashboard** (`client/`): a dependency-free HTML/JS page showing live
  events/sec, average & p95 latency, a rolling value/latency chart, and a
  live event table.

## Quick start (Docker)

```bash
docker compose up --build
```

Then open **http://localhost:8000/signals.html** for the market signals
dashboard, or **http://localhost:8000** for the generic stream demo — both
connect automatically and start streaming immediately.

## Quick start (local Python)

```bash
python -m venv .venv
.venv\Scripts\activate        # Windows
# source .venv/bin/activate   # macOS/Linux

pip install -r requirements.txt
uvicorn app.main:app --reload
```

Open **http://localhost:8000/signals.html** or **http://localhost:8000**.

## Configuration

Market signals pipeline (all optional; see
[PROJECT_DOCUMENTATION.md](PROJECT_DOCUMENTATION.md) §1.5 for the full
list, Telegram setup, and mock-data details):

| Variable | Default | Meaning |
|---|---|---|
| `OTC_SOURCE_ENABLED` / `LIVE_SOURCE_ENABLED` | `true` | Enable each mock signal source |
| `OTC_SYMBOLS` / `LIVE_SYMBOLS` | see docs | Comma-separated symbol lists |
| `SIGNAL_TIMEFRAMES` | `M1,M5` | Comma-separated timeframes signals rotate through |
| `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID` | unset | Enables the "Send Test Alert" button |

Generic demo pipeline (all optional):

| Variable              | Default | Meaning                                              |
|-----------------------|---------|-------------------------------------------------------|
| `DEMO_SOURCE_ENABLED` | `true`  | Run the built-in synthetic generator                  |
| `DEMO_SOURCE_RATE`    | `100`   | Events/sec produced by the demo generator             |
| `FILTER_MIN_VALUE`    | `0`     | Events below this value are dropped by the pipeline   |
| `QUEUE_MAXSIZE`       | `5000`  | Backpressure limit on the ingestion queue             |

## API

- `GET /signals/health`, `POST /signals/ingest`, `WS /ws/signals` — the
  market signals pipeline.
- `POST /telegram/test-alert` — sends a test message via the configured
  Telegram bot/channel; used by the dashboard's test-alert button.
- `GET /health` — status, connected client count, queue depth, and
  ingested/processed/dropped/events-per-second counters (generic pipeline).
- `POST /ingest` — push a raw event (JSON body) into the generic pipeline.
  Used by the load test script, and the integration point for a future
  Kafka/MQTT bridge.
- `WS /ws` — subscribe to the live stream of generic processed events.

## Tests

```bash
pytest
```

Covers the signal-generation step, the Telegram notifier (no real network
calls made in tests), and the `/signals/*` endpoints, plus the original
generic-pipeline coverage: filter/aggregate/enrich steps, pipeline
chaining and short-circuiting, the WebSocket connection manager (fan-out +
dead connection pruning), and an API smoke test asserting an ingested
event comes back out over `/ws` fully processed. 31 tests in total.

## Load test (100 events/sec, latency check)

This exercises the generic pipeline's `/ingest` → `/ws` path (the market
signals pipeline shares the same processing/broadcasting code, so the
same latency characteristics apply there too). With the server running
(disable the demo generator first so latency numbers aren't mixed with
its output):

```bash
# terminal 1
set DEMO_SOURCE_ENABLED=false        # PowerShell: $env:DEMO_SOURCE_ENABLED="false"
uvicorn app.main:app

# terminal 2
python scripts/load_test.py --rate 100 --duration 10
```

The script posts ~100 events/sec to `/ingest`, listens on `/ws`, and
reports avg/p50/p95/p99 end-to-end latency (client send → processed
broadcast), asserting the p95 stays under 200ms.

## Deploying for a quick client demo

**Fastest path — Cloudflare Tunnel, no account needed for a temporary link:**

```bash
docker compose up -d --build
cloudflared tunnel --url http://localhost:8000
```

This prints a public `https://*.trycloudflare.com` URL you can send
straight to the client — traffic is proxied through Cloudflare to your
local container, including the WebSocket connection.

**For a persistent URL:** push this repo to GitHub, deploy the container
(Docker image) to any host that runs long-lived processes with WebSocket
support (Fly.io, Railway, a small VPS, etc.), then point a Cloudflare DNS
record at it (proxied, with WebSockets enabled — on by default).
Cloudflare Pages is not suitable on its own since it doesn't run a Python
process — it only fronts a container running elsewhere.

## Swapping in a real source later

For the market signals pipeline, implement `MarketDataAdapter`
(`app/ingestion/market_adapter.py`) for a real market/broker feed — see
[PROJECT_DOCUMENTATION.md](PROJECT_DOCUMENTATION.md) §1.7 for details. No
scraping or real-broker integration is included in this prototype.

For the generic pipeline, implement `EventSource`
(see `app/ingestion/base.py`) for your Kafka consumer or MQTT client, have
it push events onto the same `event_queue` used in `app/main.py`, and
start it in the `lifespan` context manager alongside/instead of
`DemoEventSource`. The processing and broadcasting layers require no
changes either way.

## License

[PolyForm Noncommercial License 1.0.0](LICENSE) — free to view, run, and
modify for noncommercial purposes (evaluation, learning, personal use).
**Commercial use requires a separate agreement with the author.**
