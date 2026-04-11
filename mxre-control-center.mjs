#!/usr/bin/env node
/**
 * MXRE Control Center
 *
 * Real-time observability dashboard for the MXRE 30-day sprint.
 * Single-file, no build step. Serves HTML + JSON API on port 3333.
 *
 * Run:  node mxre-control-center.mjs
 *
 * Panels:
 *   - Live processes (ingest workers, MundoX, Open WebUI)
 *   - Supabase row counts with deltas (records/min)
 *   - Ingestion rate + ETA to 150M target
 *   - GPU status (VRAM, utilization, temperature)
 *   - Recent error log lines
 *   - 30-day countdown
 */

import "dotenv/config";
import { Hono } from "hono";
import { serve } from "@hono/node-server";
import { createClient } from "@supabase/supabase-js";
import { execSync, spawn } from "node:child_process";
import { readFileSync, existsSync, statSync } from "node:fs";

// ─── Config ─────────────────────────────────────────────────────────

const PORT = Number(process.env.MXRE_CONTROL_PORT || 3333);

const COUNTDOWN_START = new Date("2026-04-06T00:00:00-04:00");
const COUNTDOWN_END = new Date("2026-05-06T00:00:00-04:00");
const TARGET_PROPERTIES = 150_000_000;

const LOG_FILES = [
  { name: "stable-ingest", path: "/tmp/stable-ingest.log" },
  { name: "openwebui", path: "C:/Users/msanc/openwebui.log" },
  { name: "daemon", path: "C:/Users/msanc/mxre/daemon.log" },
];

const PROCESS_PATTERNS = [
  { name: "stable-ingest", match: /stable-ingest\.ts/, role: "ingest" },
  { name: "stable-ingest-v2", match: /stable-ingest-v2\.ts/, role: "ingest" },
  { name: "orchestrator", match: /orchestrator\.ts/, role: "orchestration" },
  { name: "ingest-fidlar", match: /ingest-fidlar/, role: "recorder" },
  { name: "mundox-llama", match: /llama-server.*18791/, role: "llm" },
  { name: "open-webui", match: /open-webui.*serve/, role: "ui" },
  { name: "mxre-agent", match: /mundox-agent\.ts/, role: "agent" },
];

// ─── Supabase ───────────────────────────────────────────────────────

const supabase =
  process.env.SUPABASE_URL && process.env.SUPABASE_SERVICE_KEY
    ? createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_KEY, {
        auth: { persistSession: false },
      })
    : null;

let lastCounts = null;
let lastCountsAt = null;
let cachedCounts = null;
let cachedCountsAt = 0;

async function getCounts() {
  // 15-second cache so the dashboard polling doesn't hammer the DB
  if (cachedCounts && Date.now() - cachedCountsAt < 15_000) {
    return cachedCounts;
  }
  if (!supabase) {
    return { error: "SUPABASE_URL or SUPABASE_SERVICE_KEY not set" };
  }
  try {
    const tables = [
      "properties",
      "mortgage_records",
      "rent_snapshots",
      "mls_listings",
      "property_sales_history",
    ];
    const counts = {};
    for (const t of tables) {
      try {
        const { count } = await supabase.from(t).select("*", { count: "exact", head: true });
        counts[t] = count ?? 0;
      } catch {
        counts[t] = null; // table might not exist yet (e.g., before migration 005)
      }
    }
    try {
      const { count } = await supabase
        .from("mortgage_records")
        .select("*", { count: "exact", head: true })
        .not("loan_amount", "is", null)
        .gt("loan_amount", 0);
      counts.mortgage_records_with_amount = count ?? 0;
    } catch {
      counts.mortgage_records_with_amount = null;
    }

    // Compute deltas vs prior poll
    let deltas = null;
    let ratePerMin = null;
    if (lastCounts && lastCountsAt) {
      const elapsedMin = (Date.now() - lastCountsAt) / 60_000;
      deltas = {};
      ratePerMin = {};
      for (const [k, v] of Object.entries(counts)) {
        if (v !== null && lastCounts[k] !== null && lastCounts[k] !== undefined) {
          deltas[k] = v - lastCounts[k];
          ratePerMin[k] = elapsedMin > 0 ? deltas[k] / elapsedMin : 0;
        }
      }
    }
    lastCounts = counts;
    lastCountsAt = Date.now();

    cachedCounts = { counts, deltas, ratePerMin, fetchedAt: new Date().toISOString() };
    cachedCountsAt = Date.now();
    return cachedCounts;
  } catch (err) {
    return { error: String(err?.message || err) };
  }
}

// ─── Process detection ──────────────────────────────────────────────

function getProcesses() {
  try {
    // Use PowerShell to get python.exe and node.exe processes with command lines
    const ps = execSync(
      `powershell -NoProfile -Command "Get-CimInstance Win32_Process -Filter \\"Name='node.exe' OR Name='python.exe' OR Name='llama-server.exe' OR Name='tsx.exe'\\" | Select-Object ProcessId, Name, CommandLine, CreationDate | ConvertTo-Json -Compress"`,
      { encoding: "utf8", timeout: 8000 }
    );
    let raw;
    try {
      raw = JSON.parse(ps);
    } catch {
      return [];
    }
    if (!Array.isArray(raw)) raw = [raw];

    const matched = [];
    for (const p of raw) {
      const cmd = String(p.CommandLine || "");
      for (const pat of PROCESS_PATTERNS) {
        if (pat.match.test(cmd)) {
          // Parse CreationDate (CIM format: /Date(1234567890000)/)
          let startedAt = null;
          if (p.CreationDate) {
            const m = String(p.CreationDate).match(/(\d+)/);
            if (m) startedAt = new Date(Number(m[1])).toISOString();
          }
          matched.push({
            name: pat.name,
            role: pat.role,
            pid: p.ProcessId,
            exe: p.Name,
            startedAt,
            cmd: cmd.length > 140 ? cmd.slice(0, 140) + "…" : cmd,
          });
          break;
        }
      }
    }
    return matched;
  } catch (err) {
    return [{ error: String(err?.message || err) }];
  }
}

// ─── Listening ports ────────────────────────────────────────────────

function getListeningPorts() {
  try {
    const out = execSync("netstat -ano", { encoding: "utf8", timeout: 5000 });
    const ports = [];
    for (const line of out.split("\n")) {
      if (!line.includes("LISTENING")) continue;
      const m = line.match(/\s+TCP\s+([\d.:]+)\s+([\d.:]+)\s+LISTENING\s+(\d+)/);
      if (!m) continue;
      const local = m[1];
      const pid = Number(m[3]);
      const portMatch = local.match(/:(\d+)$/);
      if (!portMatch) continue;
      const port = Number(portMatch[1]);
      // Only the ports we care about
      if ([18791, 18792, 3000, 3333, 3334, 18789].includes(port)) {
        ports.push({ port, pid, local });
      }
    }
    return ports;
  } catch {
    return [];
  }
}

// ─── GPU ────────────────────────────────────────────────────────────

function getGpu() {
  try {
    const out = execSync(
      "nvidia-smi --query-gpu=memory.used,memory.free,memory.total,utilization.gpu,temperature.gpu,name --format=csv,noheader,nounits",
      { encoding: "utf8", timeout: 5000 }
    );
    const [used, free, total, util, temp, name] = out.trim().split(",").map((s) => s.trim());
    return {
      name,
      memUsedMb: Number(used),
      memFreeMb: Number(free),
      memTotalMb: Number(total),
      utilPct: Number(util),
      tempC: Number(temp),
    };
  } catch (err) {
    return { error: String(err?.message || err) };
  }
}

// ─── Logs ───────────────────────────────────────────────────────────

function tailFile(path, lines = 20) {
  try {
    if (!existsSync(path)) return { error: "not found" };
    const content = readFileSync(path, "utf8");
    const all = content.split(/\r?\n/);
    const tail = all.slice(-lines).filter((l) => l.length > 0);
    const stat = statSync(path);
    return { tail, sizeBytes: stat.size, mtime: stat.mtime.toISOString() };
  } catch (err) {
    return { error: String(err?.message || err) };
  }
}

function getRecentErrors() {
  const errors = [];
  for (const lf of LOG_FILES) {
    const t = tailFile(lf.path, 200);
    if (t.error) continue;
    for (const line of t.tail || []) {
      if (/error|fail|exception|fatal|404|500|timeout|refused/i.test(line)) {
        errors.push({ source: lf.name, line: line.length > 240 ? line.slice(0, 240) + "…" : line });
      }
    }
  }
  return errors.slice(-30);
}

// ─── Countdown ──────────────────────────────────────────────────────

function getCountdown(currentProperties) {
  const now = Date.now();
  const totalMs = COUNTDOWN_END.getTime() - COUNTDOWN_START.getTime();
  const elapsedMs = now - COUNTDOWN_START.getTime();
  const remainingMs = COUNTDOWN_END.getTime() - now;
  const dayOf = Math.floor(elapsedMs / 86_400_000) + 1;
  const totalDays = Math.round(totalMs / 86_400_000);
  const pctTime = Math.max(0, Math.min(100, (elapsedMs / totalMs) * 100));
  const pctData =
    currentProperties != null ? Math.max(0, Math.min(100, (currentProperties / TARGET_PROPERTIES) * 100)) : null;
  return {
    startedAt: COUNTDOWN_START.toISOString(),
    endsAt: COUNTDOWN_END.toISOString(),
    dayOf,
    totalDays,
    daysRemaining: Math.max(0, Math.ceil(remainingMs / 86_400_000)),
    pctTime,
    pctData,
    targetProperties: TARGET_PROPERTIES,
  };
}

// ─── Status aggregator ──────────────────────────────────────────────

async function getStatus() {
  const [counts, processes, ports, gpu] = await Promise.all([
    getCounts(),
    Promise.resolve(getProcesses()),
    Promise.resolve(getListeningPorts()),
    Promise.resolve(getGpu()),
  ]);

  const currentProps = counts?.counts?.properties || null;
  const countdown = getCountdown(currentProps);

  // ETA calculation: use property rate/min, project to 150M
  let eta = null;
  if (counts?.ratePerMin?.properties && counts.ratePerMin.properties > 0 && currentProps) {
    const remaining = TARGET_PROPERTIES - currentProps;
    const minutesToFinish = remaining / counts.ratePerMin.properties;
    eta = {
      minutesToFinish,
      etaIso: new Date(Date.now() + minutesToFinish * 60_000).toISOString(),
      ratePerMin: counts.ratePerMin.properties,
    };
  }

  const errors = getRecentErrors();

  return {
    timestamp: new Date().toISOString(),
    countdown,
    counts,
    processes,
    ports,
    gpu,
    eta,
    errors,
  };
}

// ─── HTTP server ────────────────────────────────────────────────────

const app = new Hono();

app.get("/api/status", async (c) => {
  const status = await getStatus();
  return c.json(status);
});

app.get("/api/logs/:name", (c) => {
  const name = c.req.param("name");
  const linesParam = c.req.query("lines");
  const lines = linesParam ? Number(linesParam) : 50;
  const lf = LOG_FILES.find((l) => l.name === name);
  if (!lf) return c.json({ error: "unknown log" }, 404);
  return c.json(tailFile(lf.path, lines));
});

app.get("/", (c) => c.html(HTML));

// ─── Dashboard HTML ─────────────────────────────────────────────────

const HTML = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<title>MXRE Control Center</title>
<meta name="viewport" content="width=device-width, initial-scale=1" />
<style>
:root {
  --bg: #0b0f17;
  --bg2: #131a26;
  --bg3: #1a2333;
  --text: #e6edf3;
  --muted: #8b95a5;
  --accent: #7dd3fc;
  --good: #4ade80;
  --warn: #fbbf24;
  --bad: #f87171;
  --border: #233148;
}
* { box-sizing: border-box; margin: 0; padding: 0; }
body {
  font-family: ui-monospace, "SF Mono", Menlo, Consolas, monospace;
  background: var(--bg);
  color: var(--text);
  font-size: 13px;
  line-height: 1.5;
  padding: 16px;
}
h1 {
  font-size: 18px;
  font-weight: 600;
  margin-bottom: 16px;
  display: flex;
  align-items: center;
  gap: 12px;
}
h1 .pulse {
  width: 8px; height: 8px;
  background: var(--good);
  border-radius: 50%;
  animation: pulse 1.5s infinite;
}
@keyframes pulse {
  0%, 100% { opacity: 1; }
  50% { opacity: 0.3; }
}
.grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(380px, 1fr));
  gap: 12px;
}
.card {
  background: var(--bg2);
  border: 1px solid var(--border);
  border-radius: 6px;
  padding: 14px;
}
.card h2 {
  font-size: 11px;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.5px;
  color: var(--muted);
  margin-bottom: 10px;
  display: flex;
  justify-content: space-between;
  align-items: center;
}
.metric {
  font-size: 28px;
  font-weight: 600;
  color: var(--accent);
}
.metric-sub {
  color: var(--muted);
  font-size: 11px;
  margin-top: 2px;
}
.row {
  display: flex;
  justify-content: space-between;
  padding: 6px 0;
  border-bottom: 1px solid var(--bg3);
}
.row:last-child { border-bottom: none; }
.row .k { color: var(--muted); }
.row .v { color: var(--text); font-weight: 500; }
.row .delta { color: var(--good); font-size: 11px; margin-left: 6px; }
.row .delta.zero { color: var(--muted); }
.process {
  display: grid;
  grid-template-columns: 14px 1fr auto;
  gap: 8px;
  padding: 5px 0;
  align-items: center;
  border-bottom: 1px solid var(--bg3);
}
.process:last-child { border-bottom: none; }
.dot { width: 8px; height: 8px; border-radius: 50%; }
.dot.alive { background: var(--good); }
.dot.dead { background: var(--bad); }
.process .name { font-weight: 500; }
.process .meta { color: var(--muted); font-size: 11px; }
.process .pid { color: var(--muted); font-size: 11px; }
.bar {
  height: 6px;
  background: var(--bg3);
  border-radius: 3px;
  overflow: hidden;
  margin-top: 4px;
}
.bar > span { display: block; height: 100%; background: var(--accent); }
.bar.warn > span { background: var(--warn); }
.bar.bad > span { background: var(--bad); }
.error-line {
  font-size: 11px;
  padding: 4px 0;
  border-bottom: 1px solid var(--bg3);
  color: var(--bad);
  word-break: break-all;
}
.error-line .src { color: var(--muted); margin-right: 6px; }
.empty { color: var(--muted); font-style: italic; padding: 8px 0; }
.countdown {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 12px;
}
.big {
  font-size: 36px;
  font-weight: 700;
  color: var(--accent);
}
.status-text {
  font-size: 11px;
  color: var(--muted);
  margin-top: 12px;
  text-align: right;
}
.gpu-mem {
  display: flex;
  justify-content: space-between;
  margin-top: 4px;
  font-size: 11px;
}
button {
  background: var(--bg3);
  border: 1px solid var(--border);
  color: var(--text);
  padding: 4px 10px;
  border-radius: 4px;
  cursor: pointer;
  font-family: inherit;
  font-size: 11px;
}
button:hover { background: var(--accent); color: var(--bg); }
.role {
  display: inline-block;
  padding: 1px 6px;
  border-radius: 3px;
  background: var(--bg3);
  color: var(--muted);
  font-size: 10px;
  text-transform: uppercase;
  margin-left: 6px;
}
</style>
</head>
<body>
<h1><span class="pulse"></span> MXRE Control Center <span style="color:var(--muted);font-size:12px;font-weight:400">Day <span id="day-of">–</span> of 30</span></h1>

<div class="grid">

  <div class="card" style="grid-column: span 2;">
    <h2>30-Day Sprint <span id="last-update" style="font-weight:400">–</span></h2>
    <div class="countdown">
      <div>
        <div class="metric"><span id="properties">–</span></div>
        <div class="metric-sub">Properties / 150M target</div>
        <div class="bar"><span id="bar-data" style="width:0%"></span></div>
        <div id="rate" class="metric-sub" style="margin-top:8px">– records/min</div>
        <div id="eta" class="metric-sub">ETA: –</div>
      </div>
      <div>
        <div class="big"><span id="days-remaining">–</span><span style="font-size:14px;color:var(--muted)"> days left</span></div>
        <div class="bar"><span id="bar-time" style="width:0%"></span></div>
        <div class="metric-sub" id="time-status">–</div>
      </div>
    </div>
  </div>

  <div class="card">
    <h2>Supabase Counts</h2>
    <div id="counts"><div class="empty">loading…</div></div>
  </div>

  <div class="card">
    <h2>Live Processes</h2>
    <div id="processes"><div class="empty">loading…</div></div>
  </div>

  <div class="card">
    <h2>GPU (RTX 3090)</h2>
    <div id="gpu"><div class="empty">loading…</div></div>
  </div>

  <div class="card">
    <h2>Listening Ports</h2>
    <div id="ports"><div class="empty">loading…</div></div>
  </div>

  <div class="card" style="grid-column: 1 / -1;">
    <h2>Recent Errors <button onclick="refresh()">refresh</button></h2>
    <div id="errors"><div class="empty">loading…</div></div>
  </div>

</div>

<script>
const fmt = (n) => n == null ? '–' : n.toLocaleString();
const fmtRate = (n) => n == null ? '–' : (n >= 1 ? n.toFixed(0) : n.toFixed(2));
const fmtDelta = (n) => n == null ? '' : (n > 0 ? '+' + fmt(n) : fmt(n));

async function refresh() {
  try {
    const r = await fetch('/api/status');
    const s = await r.json();
    render(s);
  } catch (e) {
    console.error('refresh failed', e);
  }
}

function render(s) {
  document.getElementById('last-update').textContent = new Date(s.timestamp).toLocaleTimeString();

  // Countdown
  const cd = s.countdown;
  document.getElementById('day-of').textContent = cd.dayOf;
  document.getElementById('days-remaining').textContent = cd.daysRemaining;
  document.getElementById('bar-time').style.width = cd.pctTime.toFixed(1) + '%';
  document.getElementById('time-status').textContent = cd.pctTime.toFixed(1) + '% of time elapsed';

  // Properties metric
  const props = s.counts?.counts?.properties;
  document.getElementById('properties').textContent = fmt(props);
  if (cd.pctData != null) {
    document.getElementById('bar-data').style.width = cd.pctData.toFixed(1) + '%';
  }

  // Rate / ETA
  const rate = s.counts?.ratePerMin?.properties;
  if (rate != null) {
    document.getElementById('rate').textContent = fmtRate(rate) + ' properties/min';
  }
  if (s.eta) {
    const days = s.eta.minutesToFinish / 1440;
    document.getElementById('eta').textContent = 'ETA: ~' + days.toFixed(1) + ' days at current rate';
  } else {
    document.getElementById('eta').textContent = 'ETA: gathering rate data...';
  }

  // Counts
  const c = s.counts?.counts || {};
  const d = s.counts?.deltas || {};
  const labels = {
    properties: 'Properties',
    mortgage_records: 'Mortgages',
    mortgage_records_with_amount: '↳ with $ amount',
    rent_snapshots: 'Rent snapshots',
    mls_listings: 'MLS listings',
    property_sales_history: 'Sales history',
  };
  let countsHtml = '';
  for (const [k, label] of Object.entries(labels)) {
    if (c[k] == null) continue;
    const delta = d[k];
    const deltaClass = delta == null || delta === 0 ? ' zero' : '';
    const deltaStr = delta != null ? '<span class="delta' + deltaClass + '">' + fmtDelta(delta) + '</span>' : '';
    countsHtml += '<div class="row"><span class="k">' + label + '</span><span class="v">' + fmt(c[k]) + deltaStr + '</span></div>';
  }
  document.getElementById('counts').innerHTML = countsHtml || '<div class="empty">no data</div>';

  // Processes
  const procs = s.processes || [];
  if (procs.length === 0 || procs[0]?.error) {
    document.getElementById('processes').innerHTML = '<div class="empty">' + (procs[0]?.error || 'no processes detected') + '</div>';
  } else {
    let pHtml = '';
    for (const p of procs) {
      const uptime = p.startedAt ? Math.floor((Date.now() - new Date(p.startedAt)) / 60000) : null;
      pHtml += '<div class="process"><div class="dot alive"></div><div class="name">' + p.name + ' <span class="role">' + p.role + '</span><div class="meta">' + (uptime != null ? uptime + 'min' : '') + ' · pid ' + p.pid + '</div></div></div>';
    }
    document.getElementById('processes').innerHTML = pHtml;
  }

  // GPU
  const g = s.gpu || {};
  if (g.error) {
    document.getElementById('gpu').innerHTML = '<div class="empty">' + g.error + '</div>';
  } else {
    const usedGb = (g.memUsedMb / 1024).toFixed(1);
    const totalGb = (g.memTotalMb / 1024).toFixed(0);
    const memPct = (g.memUsedMb / g.memTotalMb) * 100;
    const memBarClass = memPct > 90 ? 'bar bad' : memPct > 75 ? 'bar warn' : 'bar';
    document.getElementById('gpu').innerHTML =
      '<div class="row"><span class="k">' + (g.name || 'GPU') + '</span><span class="v">' + g.tempC + '°C</span></div>' +
      '<div class="row"><span class="k">Utilization</span><span class="v">' + g.utilPct + '%</span></div>' +
      '<div class="row"><span class="k">VRAM</span><span class="v">' + usedGb + ' / ' + totalGb + ' GB</span></div>' +
      '<div class="' + memBarClass + '"><span style="width:' + memPct.toFixed(1) + '%"></span></div>';
  }

  // Ports
  const ports = s.ports || [];
  let portsHtml = '';
  const portLabels = {
    18791: 'MundoX (llama.cpp)',
    18792: '(legacy worker)',
    3000: 'Open WebUI',
    3333: 'Control Center',
    3334: 'MundoX Chat UI',
    18789: 'MundoX OS gateway',
  };
  for (const p of ports) {
    portsHtml += '<div class="row"><span class="k">' + (portLabels[p.port] || 'port ' + p.port) + '</span><span class="v">:' + p.port + ' (pid ' + p.pid + ')</span></div>';
  }
  if (!portsHtml) portsHtml = '<div class="empty">no ports detected</div>';
  document.getElementById('ports').innerHTML = portsHtml;

  // Errors
  const errs = s.errors || [];
  if (errs.length === 0) {
    document.getElementById('errors').innerHTML = '<div class="empty">no recent errors 🎉</div>';
  } else {
    document.getElementById('errors').innerHTML = errs
      .map((e) => '<div class="error-line"><span class="src">[' + e.source + ']</span>' + escape(e.line) + '</div>')
      .join('');
  }
}

function escape(s) {
  return String(s).replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}

refresh();
setInterval(refresh, 3000);
</script>
</body>
</html>`;

// ─── Boot ───────────────────────────────────────────────────────────

console.log(`MXRE Control Center starting on http://127.0.0.1:${PORT}`);
serve({ fetch: app.fetch, port: PORT });
console.log(`Dashboard:  http://127.0.0.1:${PORT}/`);
console.log(`API:        http://127.0.0.1:${PORT}/api/status`);
