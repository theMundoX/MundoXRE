#!/usr/bin/env tsx
/**
 * MXRE Orchestrator - Single stable process that runs all night
 * - Manages ingest subprocess
 * - Auto-restarts on crash
 * - Reports status every 60 seconds
 * - Writes status to file for dashboard polling
 */
import { spawn, execSync } from "child_process";
import * as fs from "fs";
import * as path from "path";

const STATUS_FILE = "/tmp/mxre-status.json";
const LOG_FILE = "/tmp/orchestrator.log";

let ingestProcess: any = null;
let lastPropertyCount = 0;
let lastWriteTime = Date.now();
let startTime = Date.now();
let restartCount = 0;

function log(msg: string) {
  const timestamp = new Date().toLocaleTimeString();
  const line = `[${timestamp}] ${msg}`;
  console.log(line);
  fs.appendFileSync(LOG_FILE, line + "\n");
}

function writeStatus(status: any) {
  fs.writeFileSync(STATUS_FILE, JSON.stringify(status, null, 2));
}

function startIngest() {
  log(`Starting ingest (attempt ${++restartCount})...`);

  ingestProcess = spawn("bash", ["-c", 'cd /c/Users/msanc/mxre && NODE_OPTIONS="--max-old-space-size=4096" npx tsx run-parallel-ingest.ts --concurrency 50'], {
    env: { ...process.env },
    stdio: ["ignore", "pipe", "pipe"],
  });

  ingestProcess.stdout.on("data", (data: any) => {
    const lines = data.toString().split("\n");
    for (const line of lines) {
      if (line.includes("Progress:")) {
        process.stdout.write(".");
      }
    }
  });

  ingestProcess.on("error", (err: any) => {
    log(`❌ Ingest error: ${err.message}`);
  });

  ingestProcess.on("exit", (code: any) => {
    log(`⚠️  Ingest exited with code ${code}`);
    ingestProcess = null;
    setTimeout(startIngest, 3000);
  });
}

async function checkStatus() {
  const uptime = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
  const isRunning = ingestProcess && !ingestProcess.killed;

  // Try to get property count
  let propCount = lastPropertyCount;
  try {
    const output = execSync(
      `curl -s -m 3 -H "apikey: eyJhbGciOiAiSFMyNTYiLCAidHlwIjogInNlcnZpY2Vfcm9sZSIsICJpc3MiOiAic3VwYWJhc2UiLCAiaWF0IjogMTc3NDUyNDk2MiwgImV4cCI6IDIwODk4ODQ5NjJ9.Ex_u9UIYpPmJ0G8H3deic-zRulLOmgNZJS3hw7azoKU" "http://207.244.225.239:8000/rest/v1/properties?limit=1" 2>/dev/null | wc -c`,
      { encoding: "utf-8", stdio: ["pipe", "pipe", "pipe"] }
    );
    propCount = parseInt(output.trim()) || lastPropertyCount;
    if (propCount > lastPropertyCount) {
      lastPropertyCount = propCount;
      lastWriteTime = Date.now();
    }
  } catch {}

  const status = {
    timestamp: new Date().toISOString(),
    uptime_minutes: uptime,
    ingest_running: isRunning,
    restart_count: restartCount,
    last_write_seconds_ago: ((Date.now() - lastWriteTime) / 1000).toFixed(0),
    property_data_size_bytes: propCount,
    dashboard_url: "http://localhost:3334",
  };

  writeStatus(status);

  const statusStr = `✓ UP ${uptime}m | Ingest: ${isRunning ? "✓" : "⚠️"} | Properties: ${propCount} bytes | Restarts: ${restartCount}`;
  log(statusStr);
}

log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
log("🚀 MXRE ORCHESTRATOR STARTED");
log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

startIngest();
checkStatus();
setInterval(checkStatus, 60000);

// Graceful shutdown
process.on("SIGINT", () => {
  log("Shutting down gracefully...");
  if (ingestProcess) ingestProcess.kill();
  process.exit(0);
});

process.on("uncaughtException", (err: any) => {
  log(`❌ UNCAUGHT EXCEPTION: ${err.message}`);
  process.exit(1);
});
