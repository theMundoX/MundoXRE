#!/usr/bin/env tsx
/**
 * Real-time ingest monitor that shows progress, rate, and ETA
 */
import * as fs from "fs";

const LOG_FILE = "/tmp/stable-ingest-v2.log";
let lastCount = 0;
let lastTime = Date.now();
let firstTime = Date.now();

function parseProgress(line: string): number {
  const match = line.match(/Progress: ([\d,]+) processed/);
  if (!match) return 0;
  return parseInt(match[1].replace(/,/g, ""), 10);
}

async function monitor() {
  try {
    const data = fs.readFileSync(LOG_FILE, "utf-8");
    const lines = data.split("\n");

    // Find the most recent Progress line
    let latestProgress = 0;
    for (let i = lines.length - 1; i >= 0; i--) {
      const count = parseProgress(lines[i]);
      if (count > 0) {
        latestProgress = count;
        break;
      }
    }

    if (latestProgress === 0) {
      console.log("⏳ Ingest starting up...");
      return;
    }

    const now = Date.now();
    const elapsed = (now - firstTime) / 1000; // seconds
    const delta = latestProgress - lastCount;
    const timeDelta = (now - lastTime) / 1000;
    const rate = delta / timeDelta; // records/second

    const hoursRemaining = ((40_000_000 - latestProgress) / rate) / 3600;
    const etaTime = new Date(now + hoursRemaining * 3600 * 1000);

    console.log(`\n📊 MXRE INGEST MONITOR — ${new Date().toLocaleTimeString()}`);
    console.log(`━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━`);
    console.log(`✓ Total Records: ${latestProgress.toLocaleString()}`);
    console.log(`  Elapsed: ${(elapsed / 60).toFixed(1)} minutes`);
    console.log(`  Rate: ${rate.toFixed(0)} records/second`);
    console.log(`\n🎯 Target: 40,000,000 records`);
    console.log(`  Progress: ${((latestProgress / 40_000_000) * 100).toFixed(2)}%`);
    console.log(`  Remaining: ${(40_000_000 - latestProgress).toLocaleString()}`);
    console.log(`  ETA: ${etaTime.toLocaleTimeString()} (${hoursRemaining.toFixed(1)} hours)`);
    console.log(`━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n`);

    lastCount = latestProgress;
    lastTime = now;
  } catch (err) {
    console.error("Monitor error:", err);
  }
}

console.log("🚀 MXRE Ingest Monitor (updates every 30 seconds)\n");
monitor();
setInterval(monitor, 30000);
