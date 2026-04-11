#!/usr/bin/env node
/**
 * Start Autonomous MXRE Agent
 *
 * Launches:
 * 1. Parallel ingest pipeline (raw property ingestion)
 * 2. Autonomous agent (monitors failures, fixes adapters)
 * 3. Background enrichment (MundoX analyzes properties)
 */

import "dotenv/config";
import { spawn } from "child_process";
import { execSync } from "child_process";

async function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function main() {
  console.log("========================================");
  console.log("AUTONOMOUS MXRE SYSTEM - STARTUP");
  console.log("========================================\n");

  // Check MundoX is running
  console.log("[1/3] Verifying MundoX...");
  try {
    const response = await fetch("http://127.0.0.1:18792/health");
    const data = await response.json();
    console.log(`✓ MundoX ready: ${JSON.stringify(data)}\n`);
  } catch (err) {
    console.error("✗ MundoX not responding on port 18792");
    console.error("Start it: powershell C:\\Users\\msanc\\mundox-services\\start-mundox-worker.ps1");
    process.exit(1);
  }

  // Start parallel ingest
  console.log("[2/3] Starting parallel ingest pipeline...");
  const ingestCmd = process.platform === "win32" ? "npx.cmd" : "npx";
  const ingest = spawn(ingestCmd, ["tsx", "run-parallel-ingest.ts", "--concurrency", "50"], {
    cwd: process.cwd(),
    stdio: "inherit",
    detached: true,
    shell: true,
  });
  ingest.unref();
  console.log("✓ Ingest pipeline running in background\n");

  // Wait for ingest to establish
  await sleep(5000);

  // Start autonomous agent
  console.log("[3/3] Starting autonomous agent...");
  const agent = spawn(ingestCmd, ["tsx", "autonomous-agent.ts"], {
    cwd: process.cwd(),
    stdio: "inherit",
    shell: true,
  });

  agent.on("exit", (code) => {
    console.log(`\nAgent exited with code ${code}`);
    process.exit(code);
  });
}

main().catch(console.error);
