/**
 * Market signals dashboard client.
 *
 * Connects to /ws/signals and splits incoming events into the OTC or
 * Live table based on the `source` field. Polls /signals/health for
 * server-side counters and Telegram configuration status.
 */

const MAX_ROWS = 25;

const otcTableBody = document.getElementById("otcTableBody");
const liveTableBody = document.getElementById("liveTableBody");
const otcRateEl = document.getElementById("otcRate");
const liveRateEl = document.getElementById("liveRate");
const statLatency = document.getElementById("statLatency");
const statTimeframes = document.getElementById("statTimeframes");
const statTelegram = document.getElementById("statTelegram");
const testAlertBtn = document.getElementById("testAlertBtn");
const telegramStatus = document.getElementById("telegramStatus");

let otcCount = 0;
let liveCount = 0;
let lastSampleTime = performance.now();
let lastOtcCount = 0;
let lastLiveCount = 0;
let recentLatencies = [];

function connect() {
  const proto = location.protocol === "https:" ? "wss:" : "ws:";
  const ws = new WebSocket(`${proto}//${location.host}/ws/signals`);

  ws.onclose = () => setTimeout(connect, 1500);
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
  if (event.source === "otc") {
    otcCount += 1;
    prependRow(otcTableBody, event);
  } else if (event.source === "live") {
    liveCount += 1;
    prependRow(liveTableBody, event);
  }

  if (typeof event.latency_ms === "number") {
    recentLatencies.push(event.latency_ms);
    if (recentLatencies.length > 200) recentLatencies.shift();
  }

  updateRates();
}

function prependRow(tbody, event) {
  const direction = (event.direction || "").toLowerCase();
  const row = document.createElement("tr");
  row.innerHTML = `
    <td>${event.symbol ?? "–"}</td>
    <td><span class="timeframe-pill">${event.timeframe ?? "–"}</span></td>
    <td><span class="direction-pill ${direction}">${event.direction ?? "–"}</span></td>
    <td>${typeof event.price === "number" ? event.price.toFixed(5) : "–"}</td>
    <td>${formatTime(event.timestamp)}</td>
    <td>${typeof event.latency_ms === "number" ? event.latency_ms.toFixed(1) + " ms" : "–"}</td>
  `;
  tbody.prepend(row);
  while (tbody.rows.length > MAX_ROWS) {
    tbody.deleteRow(tbody.rows.length - 1);
  }
}

function formatTime(isoString) {
  if (!isoString) return "–";
  try {
    return new Date(isoString).toLocaleTimeString();
  } catch (e) {
    return isoString;
  }
}

function updateRates() {
  const now = performance.now();
  if (now - lastSampleTime >= 1000) {
    otcRateEl.textContent = (otcCount - lastOtcCount).toString();
    liveRateEl.textContent = (liveCount - lastLiveCount).toString();
    lastSampleTime = now;
    lastOtcCount = otcCount;
    lastLiveCount = liveCount;
  }

  if (recentLatencies.length) {
    const avg = recentLatencies.reduce((a, b) => a + b, 0) / recentLatencies.length;
    statLatency.textContent = `${avg.toFixed(1)} ms`;
  }
}

async function pollHealth() {
  try {
    const res = await fetch("/signals/health");
    const data = await res.json();
    statTimeframes.textContent = (data.timeframes || []).join(", ") || "–";
    statTelegram.textContent = data.telegram_enabled ? "Configured" : "Not configured";
    statTelegram.className = `value ${data.telegram_enabled ? "good" : "warn"}`;
  } catch (e) {
    // server unreachable; leave last-known values
  }
}

async function sendTestAlert() {
  testAlertBtn.disabled = true;
  telegramStatus.textContent = "Sending...";
  telegramStatus.className = "telegram-status";
  try {
    const res = await fetch("/telegram/test-alert", { method: "POST" });
    const data = await res.json();
    if (data.ok) {
      telegramStatus.textContent = "Test alert sent successfully.";
      telegramStatus.className = "telegram-status ok";
    } else {
      telegramStatus.textContent = `Not sent: ${data.error || "unknown error"}`;
      telegramStatus.className = "telegram-status error";
    }
  } catch (e) {
    telegramStatus.textContent = "Request failed - is the server running?";
    telegramStatus.className = "telegram-status error";
  } finally {
    testAlertBtn.disabled = false;
  }
}

testAlertBtn.addEventListener("click", sendTestAlert);
connect();
setInterval(pollHealth, 2000);
pollHealth();
