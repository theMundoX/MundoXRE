/**
 * MXRE Health Check — System health monitor
 *
 * Checks: DB connectivity, property counts by state, listing signal freshness,
 * mortgage records (actual vs estimated), rent snapshot freshness, proxy health,
 * disk cache size.
 *
 * Usage: npx tsx scripts/health-check.ts
 */

import "dotenv/config";
import { getDb } from "../src/db/client.js";
import { existsSync, statSync, readdirSync } from "node:fs";
import { join } from "node:path";

// ─── Helpers ────────────────────────────────────────────────────────

function pad(s: string, len: number, align: "left" | "right" = "left"): string {
  if (align === "right") return s.padStart(len);
  return s.padEnd(len);
}

function printTable(headers: string[], rows: string[][], colWidths?: number[]) {
  const widths =
    colWidths ??
    headers.map((h, i) =>
      Math.max(h.length, ...rows.map((r) => (r[i] ?? "").length)),
    );
  const sep = widths.map((w) => "-".repeat(w + 2)).join("+");

  const fmtRow = (cells: string[]) =>
    cells.map((c, i) => ` ${pad(c, widths[i])} `).join("|");

  console.log(fmtRow(headers));
  console.log(sep);
  rows.forEach((r) => console.log(fmtRow(r)));
}

function statusIcon(ok: boolean): string {
  return ok ? "[OK]" : "[!!]";
}

function formatNumber(n: number): string {
  return n.toLocaleString("en-US");
}

function dirSizeBytes(dir: string): number {
  if (!existsSync(dir)) return 0;
  let total = 0;
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    const full = join(dir, entry.name);
    if (entry.isDirectory()) {
      total += dirSizeBytes(full);
    } else {
      try {
        total += statSync(full).size;
      } catch {
        // skip unreadable files
      }
    }
  }
  return total;
}

function formatBytes(bytes: number): string {
  if (bytes === 0) return "0 B";
  const units = ["B", "KB", "MB", "GB"];
  const i = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1);
  const val = bytes / Math.pow(1024, i);
  return `${val.toFixed(1)} ${units[i]}`;
}

// ─── Checks ─────────────────────────────────────────────────────────

async function checkDatabase(): Promise<boolean> {
  console.log("\n=== Database Connectivity ===\n");
  try {
    const db = getDb();
    const { count, error } = await db
      .from("counties")
      .select("id", { count: "exact", head: true });
    if (error) throw error;
    console.log(`  ${statusIcon(true)} Connected — ${formatNumber(count ?? 0)} counties in database`);
    return true;
  } catch (e: any) {
    console.log(`  ${statusIcon(false)} FAILED — ${e.message}`);
    return false;
  }
}

async function checkPropertiesByState() {
  console.log("\n=== Properties by State ===\n");
  const db = getDb();

  // Get all states with counties
  const { data: counties } = await db
    .from("counties")
    .select("state_code")
    .eq("active", true);
  const expectedStates = [...new Set((counties ?? []).map((c: any) => c.state_code))].sort();

  // Count properties per state
  const { data: propCounts } = await db.rpc("get_property_counts_by_state").select("*");

  // Fallback: manual query if RPC doesn't exist
  let stateMap: Record<string, number> = {};
  if (!propCounts) {
    // Query state by state
    for (const st of expectedStates) {
      const { count } = await db
        .from("properties")
        .select("id", { count: "exact", head: true })
        .eq("state_code", st);
      stateMap[st] = count ?? 0;
    }
  } else {
    for (const row of propCounts as any[]) {
      stateMap[row.state_code] = row.count;
    }
  }

  // Also catch states in properties not in counties
  if (Object.keys(stateMap).length === 0) {
    for (const st of expectedStates) {
      const { count } = await db
        .from("properties")
        .select("id", { count: "exact", head: true })
        .eq("state_code", st);
      stateMap[st] = count ?? 0;
    }
  }

  const rows: string[][] = [];
  let total = 0;
  const gaps: string[] = [];

  for (const st of expectedStates) {
    const cnt = stateMap[st] ?? 0;
    total += cnt;
    const status = cnt === 0 ? "[GAP]" : "";
    rows.push([st, formatNumber(cnt), status]);
    if (cnt === 0) gaps.push(st);
  }

  // Check for states in properties but not in counties
  for (const [st, cnt] of Object.entries(stateMap)) {
    if (!expectedStates.includes(st)) {
      total += cnt;
      rows.push([st, formatNumber(cnt), "[no county]"]);
    }
  }

  rows.push(["---", "---", ""]);
  rows.push(["TOTAL", formatNumber(total), ""]);

  printTable(["State", "Properties", "Status"], rows);

  if (gaps.length > 0) {
    console.log(`\n  ${statusIcon(false)} States with 0 properties: ${gaps.join(", ")}`);
  } else {
    console.log(`\n  ${statusIcon(true)} All active states have properties`);
  }
}

async function checkListingSignals() {
  console.log("\n=== Listing Signals Freshness ===\n");
  const db = getDb();

  const { count: totalCount } = await db
    .from("listing_signals")
    .select("id", { count: "exact", head: true });

  console.log(`  Total signals: ${formatNumber(totalCount ?? 0)}`);

  if ((totalCount ?? 0) === 0) {
    console.log(`  ${statusIcon(false)} No listing signals found`);
    return;
  }

  // Check for stale signals (last_seen_at older than 7 days)
  const sevenDaysAgo = new Date();
  sevenDaysAgo.setDate(sevenDaysAgo.getDate() - 7);
  const cutoff = sevenDaysAgo.toISOString();

  const { count: staleCount } = await db
    .from("listing_signals")
    .select("id", { count: "exact", head: true })
    .eq("is_on_market", true)
    .lt("last_seen_at", cutoff);

  const { count: onMarket } = await db
    .from("listing_signals")
    .select("id", { count: "exact", head: true })
    .eq("is_on_market", true);

  const { count: delisted } = await db
    .from("listing_signals")
    .select("id", { count: "exact", head: true })
    .eq("is_on_market", false);

  console.log(`  On-market:  ${formatNumber(onMarket ?? 0)}`);
  console.log(`  Delisted:   ${formatNumber(delisted ?? 0)}`);
  console.log(`  Stale (>7d): ${formatNumber(staleCount ?? 0)}`);

  if ((staleCount ?? 0) > 0) {
    console.log(`  ${statusIcon(false)} ${formatNumber(staleCount ?? 0)} on-market signals not updated in 7+ days`);
  } else {
    console.log(`  ${statusIcon(true)} All on-market signals are fresh`);
  }
}

async function checkMortgageRecords() {
  console.log("\n=== Mortgage Records ===\n");
  const db = getDb();

  const { count: totalCount } = await db
    .from("mortgage_records")
    .select("id", { count: "exact", head: true });

  console.log(`  Total records: ${formatNumber(totalCount ?? 0)}`);

  if ((totalCount ?? 0) === 0) {
    console.log(`  ${statusIcon(false)} No mortgage records found`);
    return;
  }

  // Check for estimated fields — any record with estimated_monthly_payment or estimated_current_balance
  const { count: estimatedPayment } = await db
    .from("mortgage_records")
    .select("id", { count: "exact", head: true })
    .not("estimated_monthly_payment", "is", null);

  const { count: estimatedBalance } = await db
    .from("mortgage_records")
    .select("id", { count: "exact", head: true })
    .not("estimated_current_balance", "is", null);

  const hasEstimated = (estimatedPayment ?? 0) > 0 || (estimatedBalance ?? 0) > 0;

  console.log(`  With estimated_monthly_payment: ${formatNumber(estimatedPayment ?? 0)}`);
  console.log(`  With estimated_current_balance: ${formatNumber(estimatedBalance ?? 0)}`);

  if (hasEstimated) {
    console.log(`  ${statusIcon(false)} WARNING: Estimated mortgage data exists — only actual recorder filings should be stored`);
  } else {
    console.log(`  ${statusIcon(true)} No estimated mortgage data found (good)`);
  }

  // Count by document_type
  const { data: byType } = await db
    .from("mortgage_records")
    .select("document_type")
    .limit(1000);

  if (byType && byType.length > 0) {
    const typeCounts: Record<string, number> = {};
    for (const r of byType as any[]) {
      const t = r.document_type ?? "unknown";
      typeCounts[t] = (typeCounts[t] ?? 0) + 1;
    }
    console.log("\n  By document type (sample):");
    for (const [type, cnt] of Object.entries(typeCounts).sort((a, b) => b[1] - a[1])) {
      console.log(`    ${type}: ${formatNumber(cnt)}`);
    }
  }
}

async function checkRentSnapshots() {
  console.log("\n=== Rent Snapshots Freshness ===\n");
  const db = getDb();

  const { count: totalCount } = await db
    .from("rent_snapshots")
    .select("id", { count: "exact", head: true });

  console.log(`  Total snapshots: ${formatNumber(totalCount ?? 0)}`);

  if ((totalCount ?? 0) === 0) {
    console.log(`  ${statusIcon(false)} No rent snapshots found`);
    return;
  }

  // Latest snapshot date
  const { data: latest } = await db
    .from("rent_snapshots")
    .select("observed_at")
    .order("observed_at", { ascending: false })
    .limit(1);

  if (latest && latest.length > 0) {
    const latestDate = (latest[0] as any).observed_at;
    const daysSince = Math.floor(
      (Date.now() - new Date(latestDate).getTime()) / (1000 * 60 * 60 * 24),
    );
    console.log(`  Latest snapshot: ${latestDate} (${daysSince} days ago)`);

    if (daysSince > 7) {
      console.log(`  ${statusIcon(false)} Rent data is stale (${daysSince} days old)`);
    } else {
      console.log(`  ${statusIcon(true)} Rent data is fresh`);
    }
  }
}

async function checkProxy() {
  console.log("\n=== Proxy Health ===\n");
  const proxyUrl = process.env.PROXY_URL;

  if (!proxyUrl) {
    console.log("  [--] PROXY_URL not set, skipping proxy check");
    return;
  }

  try {
    const start = Date.now();
    const resp = await fetch("https://httpbin.org/ip", {
      signal: AbortSignal.timeout(10000),
    });
    const elapsed = Date.now() - start;

    if (resp.ok) {
      const body = await resp.json();
      console.log(`  ${statusIcon(true)} Proxy responding — IP: ${body.origin} (${elapsed}ms)`);
    } else {
      console.log(`  ${statusIcon(false)} Proxy returned HTTP ${resp.status}`);
    }
  } catch (e: any) {
    console.log(`  ${statusIcon(false)} Proxy unreachable: ${e.message}`);
  }
}

function checkDiskCache() {
  console.log("\n=== Disk Cache ===\n");
  const cacheDir = join(process.cwd(), ".cache");

  if (!existsSync(cacheDir)) {
    console.log("  [--] No .cache directory found");
    return;
  }

  const bytes = dirSizeBytes(cacheDir);
  const entries = readdirSync(cacheDir).length;
  console.log(`  Cache directory: ${cacheDir}`);
  console.log(`  Size: ${formatBytes(bytes)}`);
  console.log(`  Entries: ${formatNumber(entries)}`);

  if (bytes > 1024 * 1024 * 500) {
    console.log(`  ${statusIcon(false)} Cache exceeds 500 MB — consider cleanup`);
  } else {
    console.log(`  ${statusIcon(true)} Cache size is reasonable`);
  }
}

// ─── Main ───────────────────────────────────────────────────────────

async function main() {
  console.log("============================================");
  console.log("  MXRE Health Check");
  console.log(`  ${new Date().toISOString()}`);
  console.log("============================================");

  const dbOk = await checkDatabase();
  if (!dbOk) {
    console.log("\nDatabase unreachable — cannot continue further checks.");
    process.exit(1);
  }

  await checkPropertiesByState();
  await checkListingSignals();
  await checkMortgageRecords();
  await checkRentSnapshots();
  await checkProxy();
  checkDiskCache();

  console.log("\n============================================");
  console.log("  Health check complete");
  console.log("============================================\n");
}

main().catch((err) => {
  console.error("Health check failed:", err);
  process.exit(1);
});
