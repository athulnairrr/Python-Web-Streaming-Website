/**
 * Live dashboard client.
 *
 * Connects to /ws, keeps a rolling window of recent processed events for
 * the chart + table, and polls /health for server-side counters
 * (queue depth, dropped count, connected clients) that the WS stream
 * itself doesn't carry.
 */

const MAX_TABLE_ROWS = 40;
const MAX_HISTORY = 300; // ~60s at ~5 samples/sec redraw cadence

const tableBody = document.getElementById("eventTableBody");
const statRate = document.getElementById("statRate");
const statLatency = document.getElementById("statLatency");
const statP95 = document.getElementById("statP95");
const statProcessed = document.getElementById("statProcessed");
const statDropped = document.getElementById("statDropped");
const statClients = document.getElementById("statClients");
const canvas = document.getElementById("chart");
const ctx = canvas.getContext("2d");

let history = []; // {t, value, latency}
let recentLatencies = [];
let eventCount = 0;
let lastRateSampleTime = performance.now();
let lastRateSampleCount = 0;

function connect() {
  const proto = location.protocol === "https:" ? "wss:" : "ws:";
  const ws = new WebSocket(`${proto}//${location.host}/ws`);

  ws.onclose = () => {
    setTimeout(connect, 1500);
  };

  ws.onerror = () => ws.close();

  ws.onmessage = (msg) => {
    let event;
    try {
      event = JSON.parse(msg.data);
    } catch (e) {
      return;
    }
    handleEvent(event);
  };
}

function handleEvent(event) {
  eventCount += 1;
  const now = performance.now();

  history.push({ t: now, value: event.value, latency: event.latency_ms ?? 0 });
  if (history.length > MAX_HISTORY) history.shift();

  if (typeof event.latency_ms === "number") {
    recentLatencies.push(event.latency_ms);
    if (recentLatencies.length > 200) recentLatencies.shift();
  }

  prependRow(event);
  updateComputedStats(now);
}

function prependRow(event) {
  const row = document.createElement("tr");
  const latency = typeof event.latency_ms === "number" ? `${event.latency_ms.toFixed(1)} ms` : "–";
  row.innerHTML = `
    <td>${event.id ?? "–"}</td>
    <td><span class="category-pill">${event.category ?? "–"}</span></td>
    <td>${formatNum(event.value)}</td>
    <td>${formatNum(event.rolling_mean)}</td>
    <td>${latency}</td>
  `;
  tableBody.prepend(row);
  while (tableBody.rows.length > MAX_TABLE_ROWS) {
    tableBody.deleteRow(tableBody.rows.length - 1);
  }
}

function formatNum(n) {
  return typeof n === "number" ? n.toFixed(2) : "–";
}

function updateComputedStats(now) {
  // events/sec, sampled roughly once a second
  if (now - lastRateSampleTime >= 1000) {
    const rate = eventCount - lastRateSampleCount;
    statRate.textContent = rate.toString();
    lastRateSampleTime = now;
    lastRateSampleCount = eventCount;
  }

  if (recentLatencies.length) {
    const avg = recentLatencies.reduce((a, b) => a + b, 0) / recentLatencies.length;
    statLatency.textContent = `${avg.toFixed(1)} ms`;
    statLatency.className = `value ${avg < 100 ? "good" : avg < 200 ? "warn" : "bad"}`;

    const sorted = [...recentLatencies].sort((a, b) => a - b);
    const p95 = sorted[Math.floor(sorted.length * 0.95)] ?? sorted[sorted.length - 1];
    statP95.textContent = `${p95.toFixed(1)} ms`;
    statP95.className = `value ${p95 < 100 ? "good" : p95 < 200 ? "warn" : "bad"}`;
  }
}

async function pollHealth() {
  try {
    const res = await fetch("/health");
    const data = await res.json();
    statProcessed.textContent = data.processed ?? 0;
    statDropped.textContent = data.dropped ?? 0;
    statClients.textContent = data.clients ?? "–";
  } catch (e) {
    // server unreachable; leave last-known values
  }
}

function drawChart() {
  const w = canvas.width;
  const h = canvas.height;
  ctx.clearRect(0, 0, w, h);

  if (history.length < 2) {
    requestAnimationFrame(drawChart);
    return;
  }

  const values = history.map((p) => p.value);
  const latencies = history.map((p) => p.latency);
  const vMin = Math.min(...values);
  const vMax = Math.max(...values);
  const lMax = Math.max(20, ...latencies);

  drawSeries(values, vMin, vMax, "#63a4ff");
  drawSeries(latencies, 0, lMax, "#4fd1c5");

  requestAnimationFrame(drawChart);
}

function drawSeries(series, min, max, color) {
  const w = canvas.width;
  const h = canvas.height;
  const range = max - min || 1;
  const step = w / (MAX_HISTORY - 1);
  const offset = MAX_HISTORY - series.length;

  ctx.beginPath();
  ctx.strokeStyle = color;
  ctx.lineWidth = 1.5;
  series.forEach((v, i) => {
    const x = (offset + i) * step;
    const y = h - ((v - min) / range) * (h - 10) - 5;
    if (i === 0) ctx.moveTo(x, y);
    else ctx.lineTo(x, y);
  });
  ctx.stroke();
}

function resizeCanvas() {
  const rect = canvas.getBoundingClientRect();
  canvas.width = rect.width;
  canvas.height = rect.height;
}

window.addEventListener("resize", resizeCanvas);
resizeCanvas();
connect();
drawChart();
setInterval(pollHealth, 1000);
pollHealth();
