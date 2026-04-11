#!/usr/bin/env node
/**
 * FULL MXRE INGEST PIPELINE
 *
 * Three-layer data ingestion:
 * 1. Assessor/parcel data (fast parallel — 182 counties)
 * 2. Rental data (actual listing rates from RentCafe/property websites)
 * 3. Mortgage/lien data (actual county recorder filings)
 *
 * All layers feed to Supabase autonomously via MundoX orchestration
 */

import "dotenv/config";
import { spawn } from "child_process";

async function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function main() {
  console.log("========================================");
  console.log("FULL MXRE PIPELINE - 3 LAYERS");
  console.log("========================================\n");

  // Check MundoX
  console.log("[Health Check] Verifying MundoX...");
  try {
    const response = await fetch("http://127.0.0.1:18792/health");
    const data = await response.json();
    console.log(`✓ MundoX: ${JSON.stringify(data)}\n`);
  } catch (err) {
    console.error("✗ MundoX offline. Start: powershell C:\\Users\\msanc\\mundox-services\\start-mundox-worker.ps1");
    process.exit(1);
  }

  console.log("========================================");
  console.log("LAYER 1: ASSESSOR/PARCEL DATA");
  console.log("========================================");
  console.log("Starting: 182 counties in parallel (50 concurrent)\n");

  const ingest = spawn("npx", ["tsx", "run-parallel-ingest.ts", "--concurrency", "50"], {
    cwd: process.cwd(),
    stdio: "inherit",
    detached: true,
  });
  ingest.unref();

  // Wait for ingest to establish
  await sleep(10000);

  console.log("\n========================================");
  console.log("LAYER 2: RENTAL DATA");
  console.log("========================================");
  console.log("Starting: Scrape actual RentCafe/property rates\n");

  const rentals = spawn("npx", ["tsx", "scripts/scrape-rents.ts", "--discover"], {
    cwd: process.cwd(),
    stdio: "inherit",
    detached: true,
  });
  rentals.unref();

  // Wait for rental scraper to start
  await sleep(5000);

  console.log("\n========================================");
  console.log("LAYER 3: MORTGAGE/LIEN DATA");
  console.log("========================================");
  console.log("Starting: Link actual county recorder mortgages\n");

  const mortgages = spawn("npx", ["tsx", "scripts/link-mortgages-v3.ts"], {
    cwd: process.cwd(),
    stdio: "inherit",
    detached: true,
  });
  mortgages.unref();

  // Wait for mortgage processor to start
  await sleep(5000);

  console.log("\n========================================");
  console.log("AUTONOMOUS AGENT");
  console.log("========================================");
  console.log("Starting: MundoX monitors all layers, fixes failures\n");

  const agent = spawn("npx", ["tsx", "autonomous-agent.ts"], {
    cwd: process.cwd(),
    stdio: "inherit",
  });

  agent.on("exit", (code) => {
    console.log(`\nAgent exited with code ${code}`);
    process.exit(code);
  });

  console.log("\n========================================");
  console.log("PIPELINE RUNNING");
  console.log("========================================");
  console.log(`Assessor: 182 counties (50 concurrent)`);
  console.log(`Rentals: RentCafe + property websites (background)`);
  console.log(`Mortgages: County recorder filings (background)`);
  console.log(`Agent: MundoX orchestrating all layers`);
  console.log(`\nMonitor: http://localhost:3333 (command center)`);
  console.log(`Chat: http://localhost:3334 (MundoX)\n`);
  console.log("Everything runs autonomously overnight.");
}

main().catch(console.error);
