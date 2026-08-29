"""End-to-end load test.

Fires synthetic events at the running server via POST /ingest at a target
rate (default 100/s), listens on the /ws broadcast channel at the same
time, and reports end-to-end latency (client send -> processed broadcast)
to demonstrate the <200ms target from the brief.

Usage:
    python scripts/load_test.py --host localhost --port 8000 --rate 100 --duration 10

Note: for a clean latency reading, disable the built-in demo generator
first (DEMO_SOURCE_ENABLED=false) so the WebSocket stream only carries
events this script produced.
"""

import argparse
import asyncio
import itertools
import json
import random
import statistics
import time

import httpx
import websockets

CATEGORIES = ["cpu", "memory", "network", "disk"]


async def producer(base_url: str, rate: float, duration: float, counter) -> None:
    """Fires events at `rate`/sec using a coarse ~50ms tick: each tick
    computes how many sends are due from real elapsed time and fires that
    many concurrently, rather than sleeping per-event. A per-event sleep
    at high rates (e.g. 10ms for 100/s) is unreliable under load on some
    platforms - notably Windows' ProactorEventLoop once another task is
    also busy, where such short timers can fire far too often.
    """
    tick_seconds = 0.05

    async def send_one(client: httpx.AsyncClient) -> None:
        payload = {
            "id": next(counter),
            "category": random.choice(CATEGORIES),
            "value": round(random.gauss(50, 15), 2),
            "ts_generated": time.time(),
        }
        try:
            await client.post(f"{base_url}/ingest", json=payload, timeout=5.0)
        except httpx.HTTPError as exc:
            print(f"ingest error: {exc}")

    start = time.monotonic()
    sent = 0
    deadline = start + duration
    async with httpx.AsyncClient() as client:
        while time.monotonic() < deadline:
            elapsed = time.monotonic() - start
            due = int(elapsed * rate) - sent
            if due > 0:
                await asyncio.gather(*(send_one(client) for _ in range(due)))
                sent += due
            await asyncio.sleep(tick_seconds)


async def consumer(ws_url: str, duration: float, latencies: list, grace: float = 2.0) -> None:
    deadline = time.monotonic() + duration + grace
    async with websockets.connect(ws_url) as ws:
        while time.monotonic() < deadline:
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            event = json.loads(raw)
            if "latency_ms" in event:
                latencies.append(event["latency_ms"])


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--rate", type=float, default=100.0, help="events per second")
    parser.add_argument("--duration", type=float, default=10.0, help="seconds to run")
    args = parser.parse_args()

    base_url = f"http://{args.host}:{args.port}"
    ws_url = f"ws://{args.host}:{args.port}/ws"

    latencies: list = []
    counter = itertools.count(1)

    print(f"Firing ~{args.rate:.0f} events/sec at {base_url}/ingest for {args.duration:.0f}s ...")

    await asyncio.gather(
        consumer(ws_url, args.duration, latencies),
        producer(base_url, args.rate, args.duration, counter),
    )

    if not latencies:
        print("No events received - is the server running?")
        return

    latencies.sort()
    p50 = statistics.median(latencies)
    p95 = latencies[int(len(latencies) * 0.95) - 1]
    p99 = latencies[int(len(latencies) * 0.99) - 1]
    avg = statistics.fmean(latencies)

    print()
    print(f"Received {len(latencies)} processed events")
    print(f"  avg latency: {avg:.2f} ms")
    print(f"  p50:         {p50:.2f} ms")
    print(f"  p95:         {p95:.2f} ms")
    print(f"  p99:         {p99:.2f} ms")
    print(f"  max:         {max(latencies):.2f} ms")
    print()
    print("PASS: p95 < 200ms" if p95 < 200 else "FAIL: p95 >= 200ms")


if __name__ == "__main__":
    asyncio.run(main())
