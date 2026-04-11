/**
 * MXRE Data Coverage Report
 *
 * Comprehensive report: properties by state/county, field completeness,
 * listing signals, mortgage records, agent licenses, rent snapshots.
 *
 * Usage: npx tsx scripts/data-coverage-report.ts
 */

import "dotenv/config";
import { getDb } from "../src/db/client.js";

// ─── Helpers ────────────────────────────────────────────────────────

function pad(s: string, len: number): string {
  return s.padEnd(len);
}

function padR(s: string, len: number): string {
  return s.padStart(len);
}

function formatNumber(n: number): string {
  return n.toLocaleString("en-US");
}

function pct(part: number, total: number): string {
  if (total === 0) return "N/A";
  return `${((part / total) * 100).toFixed(1)}%`;
}

function printTable(headers: string[], rows: string[][], alignRight?: Set<number>) {
  const widths = headers.map((h, i) =>
    Math.max(h.length, ...rows.map((r) => (r[i] ?? "").length)),
  );
  const sep = widths.map((w) => "-".repeat(w + 2)).join("+");

  const fmtRow = (cells: string[]) =>
    cells
      .map((c, i) => {
        const val = alignRight?.has(i) ? padR(c, widths[i]) : pad(c, widths[i]);
        return ` ${val} `;
      })
      .join("|");

  console.log(fmtRow(headers));
  console.log(sep);
  rows.forEach((r) => console.log(fmtRow(r)));
}

function section(title: string) {
  console.log(`\n${"=".repeat(60)}`);
  console.log(`  ${title}`);
  console.log(`${"=".repeat(60)}\n`);
}

// ─── Reports ────────────────────────────────────────────────────────

async function propertiesByState() {
  section("Properties by State");
  const db = getDb();

  // Get all distinct states from properties
  const states: Record<string, number> = {};
  const { data: counties } = await db
    .from("counties")
    .select("state_code")
    .eq("active", true);

  const allStates = [...new Set((counties ?? []).map((c: any) => c.state_code))].sort();

  for (const st of allStates) {
    const { count } = await db
      .from("properties")
      .select("id", { count: "exact", head: true })
      .eq("state_code", st);
    states[st] = count ?? 0;
  }

  let total = 0;
  const rows: string[][] = [];
  for (const [st, cnt] of Object.entries(states).sort((a, b) => b[1] - a[1])) {
    total += cnt;
    rows.push([st, formatNumber(cnt)]);
  }
  rows.push(["---", "---"]);
  rows.push(["TOTAL", formatNumber(total)]);

  printTable(["State", "Count"], rows, new Set([1]));
}

async function propertiesByCounty() {
  section("Properties by County (Top 20)");
  const db = getDb();

  // Join properties with counties, group by county
  const { data: counties } = await db
    .from("counties")
    .select("id, county_name, state_code")
    .eq("active", true);

  if (!counties || counties.length === 0) {
    console.log("  No counties found.");
    return;
  }

  const countyCounts: { name: string; state: string; count: number }[] = [];

  for (const c of counties as any[]) {
    const { count } = await db
      .from("properties")
      .select("id", { count: "exact", head: true })
      .eq("county_id", c.id);
    countyCounts.push({
      name: c.county_name,
      state: c.state_code,
      count: count ?? 0,
    });
  }

  countyCounts.sort((a, b) => b.count - a.count);
  const top20 = countyCounts.slice(0, 20);

  const rows = top20.map((c) => [c.name, c.state, formatNumber(c.count)]);
  printTable(["County", "State", "Properties"], rows, new Set([2]));

  console.log(`\n  Showing top 20 of ${countyCounts.length} counties`);
}

async function fieldCompleteness() {
  section("Property Field Completeness");
  const db = getDb();

  const { count: totalProps } = await db
    .from("properties")
    .select("id", { count: "exact", head: true });
  const total = totalProps ?? 0;

  if (total === 0) {
    console.log("  No properties in database.");
    return;
  }

  console.log(`  Total properties: ${formatNumber(total)}\n`);

  const fields = [
    { name: "zip", col: "zip" },
    { name: "year_built", col: "year_built" },
    { name: "assessed_value", col: "assessed_value" },
    { name: "lat/lng", col: "lat" },
    { name: "total_units", col: "total_units" },
    { name: "total_sqft", col: "total_sqft" },
    { name: "property_type", col: "property_type" },
    { name: "owner_name", col: "owner_name" },
    { name: "market_value", col: "market_value" },
    { name: "last_sale_price", col: "last_sale_price" },
  ];

  const rows: string[][] = [];

  for (const f of fields) {
    // Count where field is NOT null and NOT empty string
    const { count: hasField } = await db
      .from("properties")
      .select("id", { count: "exact", head: true })
      .not(f.col, "is", null);

    const has = hasField ?? 0;
    const missing = total - has;
    rows.push([
      f.name,
      formatNumber(has),
      formatNumber(missing),
      pct(has, total),
    ]);
  }

  printTable(["Field", "Has Value", "Missing", "Coverage"], rows, new Set([1, 2, 3]));
}

async function listingSignalReport() {
  section("Listing Signals");
  const db = getDb();

  const { count: totalCount } = await db
    .from("listing_signals")
    .select("id", { count: "exact", head: true });
  const total = totalCount ?? 0;

  console.log(`  Total signals: ${formatNumber(total)}`);

  if (total === 0) return;

  const { count: onMarket } = await db
    .from("listing_signals")
    .select("id", { count: "exact", head: true })
    .eq("is_on_market", true);

  const { count: delisted } = await db
    .from("listing_signals")
    .select("id", { count: "exact", head: true })
    .eq("is_on_market", false);

  console.log(`  On-market:   ${formatNumber(onMarket ?? 0)}`);
  console.log(`  Delisted:    ${formatNumber(delisted ?? 0)}`);

  // By source
  console.log("\n  By source:");
  const sources = ["zillow", "redfin", "realtor"];
  const rows: string[][] = [];

  for (const src of sources) {
    const { count: srcCount } = await db
      .from("listing_signals")
      .select("id", { count: "exact", head: true })
      .eq("listing_source", src);
    const { count: srcOnMarket } = await db
      .from("listing_signals")
      .select("id", { count: "exact", head: true })
      .eq("listing_source", src)
      .eq("is_on_market", true);
    rows.push([src, formatNumber(srcCount ?? 0), formatNumber(srcOnMarket ?? 0)]);
  }

  // Check for other sources
  const { count: otherCount } = await db
    .from("listing_signals")
    .select("id", { count: "exact", head: true })
    .not("listing_source", "in", `(${sources.join(",")})`);

  if ((otherCount ?? 0) > 0) {
    rows.push(["other", formatNumber(otherCount ?? 0), "?"]);
  }

  printTable(["Source", "Total", "On-Market"], rows, new Set([1, 2]));
}

async function mortgageReport() {
  section("Mortgage Records");
  const db = getDb();

  const { count: totalCount } = await db
    .from("mortgage_records")
    .select("id", { count: "exact", head: true });
  const total = totalCount ?? 0;

  console.log(`  Total records: ${formatNumber(total)}`);

  if (total === 0) return;

  // By document_type — sample up to 5000 to get distribution
  const { data: sample } = await db
    .from("mortgage_records")
    .select("document_type")
    .limit(5000);

  if (sample && sample.length > 0) {
    const typeCounts: Record<string, number> = {};
    for (const r of sample as any[]) {
      const t = r.document_type ?? "unknown";
      typeCounts[t] = (typeCounts[t] ?? 0) + 1;
    }

    console.log(`\n  By document type (from ${formatNumber(sample.length)} sample):\n`);
    const rows = Object.entries(typeCounts)
      .sort((a, b) => b[1] - a[1])
      .map(([type, cnt]) => [type, formatNumber(cnt), pct(cnt, sample.length)]);

    printTable(["Document Type", "Count", "Share"], rows, new Set([1, 2]));
  }
}

async function agentLicenseReport() {
  section("Agent Licenses");
  const db = getDb();

  const { count: totalCount } = await db
    .from("agent_licenses")
    .select("id", { count: "exact", head: true });
  const total = totalCount ?? 0;

  console.log(`  Total licenses: ${formatNumber(total)}`);

  if (total === 0) return;

  // By state — sample
  const { data: sample } = await db
    .from("agent_licenses")
    .select("license_state")
    .limit(5000);

  if (sample && sample.length > 0) {
    const stateCounts: Record<string, number> = {};
    for (const r of sample as any[]) {
      const st = r.license_state ?? "??";
      stateCounts[st] = (stateCounts[st] ?? 0) + 1;
    }

    console.log(`\n  By state (from ${formatNumber(sample.length)} sample):\n`);
    const rows = Object.entries(stateCounts)
      .sort((a, b) => b[1] - a[1])
      .map(([st, cnt]) => [st, formatNumber(cnt)]);

    printTable(["State", "Count"], rows, new Set([1]));
  }
}

async function rentSnapshotReport() {
  section("Rent Snapshots");
  const db = getDb();

  const { count: totalCount } = await db
    .from("rent_snapshots")
    .select("id", { count: "exact", head: true });
  const total = totalCount ?? 0;

  console.log(`  Total snapshots: ${formatNumber(total)}`);

  if (total === 0) return;

  // Latest date
  const { data: latest } = await db
    .from("rent_snapshots")
    .select("observed_at")
    .order("observed_at", { ascending: false })
    .limit(1);

  if (latest && latest.length > 0) {
    console.log(`  Latest date:    ${(latest[0] as any).observed_at}`);
  }

  // Earliest date
  const { data: earliest } = await db
    .from("rent_snapshots")
    .select("observed_at")
    .order("observed_at", { ascending: true })
    .limit(1);

  if (earliest && earliest.length > 0) {
    console.log(`  Earliest date:  ${(earliest[0] as any).observed_at}`);
  }

  // Distinct properties with rent data
  const { data: propSample } = await db
    .from("rent_snapshots")
    .select("property_id")
    .limit(5000);

  if (propSample) {
    const uniqueProps = new Set((propSample as any[]).map((r) => r.property_id));
    console.log(`  Distinct properties (from sample): ${formatNumber(uniqueProps.size)}`);
  }
}

// ─── Main ───────────────────────────────────────────────────────────

async function main() {
  console.log("============================================================");
  console.log("  MXRE Data Coverage Report");
  console.log(`  Generated: ${new Date().toISOString()}`);
  console.log("============================================================");

  await propertiesByState();
  await propertiesByCounty();
  await fieldCompleteness();
  await listingSignalReport();
  await mortgageReport();
  await agentLicenseReport();
  await rentSnapshotReport();

  console.log(`\n${"=".repeat(60)}`);
  console.log("  Report complete");
  console.log(`${"=".repeat(60)}\n`);
}

main().catch((err) => {
  console.error("Report generation failed:", err);
  process.exit(1);
});
