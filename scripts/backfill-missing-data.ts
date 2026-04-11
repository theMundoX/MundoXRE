/**
 * MXRE Backfill Missing Data — Gap identification and prioritized action list
 *
 * Identifies:
 *  - Properties without zip codes (queue geocoding)
 *  - Properties without listing signals (queue listing scan)
 *  - Counties with <1000 properties (flag for re-ingest)
 *  - States with 0 mortgage records (flag for recorder pull)
 *
 * Usage: npx tsx scripts/backfill-missing-data.ts
 */

import "dotenv/config";
import { getDb } from "../src/db/client.js";

// ─── Helpers ────────────────────────────────────────────────────────

function formatNumber(n: number): string {
  return n.toLocaleString("en-US");
}

function pad(s: string, len: number): string {
  return s.padEnd(len);
}

function padR(s: string, len: number): string {
  return s.padStart(len);
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

interface ActionItem {
  priority: number; // 1 = highest
  category: string;
  description: string;
  count: number;
  action: string;
}

// ─── Gap Finders ────────────────────────────────────────────────────

async function findMissingZips(): Promise<ActionItem[]> {
  section("Properties Without Zip Codes");
  const db = getDb();

  const { count: totalProps } = await db
    .from("properties")
    .select("id", { count: "exact", head: true });

  const { count: missingZip } = await db
    .from("properties")
    .select("id", { count: "exact", head: true })
    .or("zip.is.null,zip.eq.");

  const missing = missingZip ?? 0;
  const total = totalProps ?? 0;

  console.log(`  Total properties: ${formatNumber(total)}`);
  console.log(`  Missing zip:      ${formatNumber(missing)}`);

  if (missing === 0) {
    console.log("  [OK] All properties have zip codes");
    return [];
  }

  // Break down by state
  const { data: counties } = await db
    .from("counties")
    .select("state_code")
    .eq("active", true);
  const states = [...new Set((counties ?? []).map((c: any) => c.state_code))].sort();

  const rows: string[][] = [];
  for (const st of states) {
    const { count: stMissing } = await db
      .from("properties")
      .select("id", { count: "exact", head: true })
      .eq("state_code", st)
      .or("zip.is.null,zip.eq.");

    if ((stMissing ?? 0) > 0) {
      rows.push([st, formatNumber(stMissing ?? 0)]);
    }
  }

  if (rows.length > 0) {
    console.log("\n  Missing zips by state:\n");
    printTable(["State", "Missing Zips"], rows, new Set([1]));
  }

  return [
    {
      priority: 2,
      category: "Geocoding",
      description: `${formatNumber(missing)} properties missing zip codes`,
      count: missing,
      action: "Run: npm run geocode:zips",
    },
  ];
}

async function findMissingListingSignals(): Promise<ActionItem[]> {
  section("Properties Without Listing Signals");
  const db = getDb();

  const { count: totalProps } = await db
    .from("properties")
    .select("id", { count: "exact", head: true });
  const total = totalProps ?? 0;

  // Get property IDs that have listing signals
  // Since we can't do a left join easily, count listing_signals distinct property_ids
  const { data: signalSample } = await db
    .from("listing_signals")
    .select("property_id")
    .not("property_id", "is", null)
    .limit(10000);

  const propsWithSignals = new Set(
    (signalSample ?? []).map((s: any) => s.property_id),
  );

  // This is approximate since we're limited to sample size
  const { count: totalSignalProps } = await db
    .from("listing_signals")
    .select("property_id", { count: "exact", head: true })
    .not("property_id", "is", null);

  const withSignals = totalSignalProps ?? propsWithSignals.size;
  const withoutSignals = Math.max(0, total - withSignals);

  console.log(`  Total properties:        ${formatNumber(total)}`);
  console.log(`  With listing signals:    ${formatNumber(withSignals)}`);
  console.log(`  Without listing signals: ${formatNumber(withoutSignals)}`);

  if (withoutSignals === 0) {
    console.log("  [OK] All properties have listing signal coverage");
    return [];
  }

  return [
    {
      priority: 3,
      category: "Listing Scan",
      description: `~${formatNumber(withoutSignals)} properties lack listing signals`,
      count: withoutSignals,
      action: "Run: npm run scan:daily (to scan unlinked properties)",
    },
  ];
}

async function findThinCounties(): Promise<ActionItem[]> {
  section("Counties With <1000 Properties");
  const db = getDb();

  const { data: counties } = await db
    .from("counties")
    .select("id, county_name, state_code")
    .eq("active", true);

  if (!counties || counties.length === 0) {
    console.log("  No active counties found.");
    return [];
  }

  const thin: { name: string; state: string; count: number }[] = [];

  for (const c of counties as any[]) {
    const { count } = await db
      .from("properties")
      .select("id", { count: "exact", head: true })
      .eq("county_id", c.id);
    const cnt = count ?? 0;
    if (cnt < 1000) {
      thin.push({ name: c.county_name, state: c.state_code, count: cnt });
    }
  }

  if (thin.length === 0) {
    console.log("  [OK] All counties have 1000+ properties");
    return [];
  }

  thin.sort((a, b) => a.count - b.count);
  console.log(`  ${thin.length} counties have fewer than 1000 properties:\n`);

  const rows = thin.map((c) => [c.name, c.state, formatNumber(c.count)]);
  printTable(["County", "State", "Properties"], rows, new Set([2]));

  return [
    {
      priority: 1,
      category: "Re-Ingest",
      description: `${thin.length} counties have <1000 properties — likely incomplete ingest`,
      count: thin.length,
      action: "Re-run ingest for flagged counties (see list above)",
    },
  ];
}

async function findMissingMortgageStates(): Promise<ActionItem[]> {
  section("States With 0 Mortgage Records");
  const db = getDb();

  // Get all active states
  const { data: counties } = await db
    .from("counties")
    .select("state_code")
    .eq("active", true);
  const allStates = [...new Set((counties ?? []).map((c: any) => c.state_code))].sort();

  // Sample mortgage records to get states represented
  const { data: mortgageSample } = await db
    .from("mortgage_records")
    .select("property_id")
    .limit(1);

  const totalMortgages = mortgageSample?.length ?? 0;

  if (totalMortgages === 0) {
    // If no mortgages at all, check if table exists by trying count
    const { count } = await db
      .from("mortgage_records")
      .select("id", { count: "exact", head: true });

    if ((count ?? 0) === 0) {
      console.log(`  No mortgage records in database at all.`);
      console.log(`  States needing recorder pulls: ${allStates.join(", ")}`);
      return [
        {
          priority: 1,
          category: "Recorder Pull",
          description: `All ${allStates.length} active states have 0 mortgage records`,
          count: allStates.length,
          action: "Begin county recorder pulls for all active states",
        },
      ];
    }
  }

  // For each state, check if any properties in that state have mortgage records
  // We do this by joining through properties
  const statesWithMortgages = new Set<string>();
  const statesWithoutMortgages: string[] = [];

  for (const st of allStates) {
    // Get property IDs for this state (sample)
    const { data: props } = await db
      .from("properties")
      .select("id")
      .eq("state_code", st)
      .limit(100);

    if (!props || props.length === 0) continue;

    const propIds = (props as any[]).map((p) => p.id);

    const { count: mortgageCount } = await db
      .from("mortgage_records")
      .select("id", { count: "exact", head: true })
      .in("property_id", propIds);

    if ((mortgageCount ?? 0) > 0) {
      statesWithMortgages.add(st);
    } else {
      // Double check with a broader sample
      const { data: allProps } = await db
        .from("properties")
        .select("id")
        .eq("state_code", st)
        .limit(500);

      if (allProps && allProps.length > 0) {
        const allPropIds = (allProps as any[]).map((p) => p.id);
        const { count: broadCount } = await db
          .from("mortgage_records")
          .select("id", { count: "exact", head: true })
          .in("property_id", allPropIds);

        if ((broadCount ?? 0) > 0) {
          statesWithMortgages.add(st);
        } else {
          statesWithoutMortgages.push(st);
        }
      }
    }
  }

  console.log(`  States with mortgage data:    ${statesWithMortgages.size}`);
  console.log(`  States without mortgage data: ${statesWithoutMortgages.length}`);

  if (statesWithoutMortgages.length === 0) {
    console.log("  [OK] All states have mortgage records");
    return [];
  }

  console.log(`\n  Missing states: ${statesWithoutMortgages.join(", ")}`);

  return [
    {
      priority: 1,
      category: "Recorder Pull",
      description: `${statesWithoutMortgages.length} states have 0 mortgage records: ${statesWithoutMortgages.join(", ")}`,
      count: statesWithoutMortgages.length,
      action: "Begin county recorder pulls for listed states",
    },
  ];
}

// ─── Main ───────────────────────────────────────────────────────────

async function main() {
  console.log("============================================================");
  console.log("  MXRE Backfill — Missing Data Identification");
  console.log(`  ${new Date().toISOString()}`);
  console.log("============================================================");

  const actions: ActionItem[] = [];

  actions.push(...(await findThinCounties()));
  actions.push(...(await findMissingZips()));
  actions.push(...(await findMissingListingSignals()));
  actions.push(...(await findMissingMortgageStates()));

  // ─── Prioritized Action List ─────────────────────────────────────

  section("Prioritized Action List");

  if (actions.length === 0) {
    console.log("  No gaps found — data coverage is complete!");
    return;
  }

  actions.sort((a, b) => a.priority - b.priority);

  const priorityLabels: Record<number, string> = {
    1: "HIGH",
    2: "MEDIUM",
    3: "LOW",
  };

  const rows = actions.map((a, i) => [
    String(i + 1),
    priorityLabels[a.priority] ?? `P${a.priority}`,
    a.category,
    a.description,
    a.action,
  ]);

  printTable(["#", "Priority", "Category", "Gap", "Action"], rows);

  console.log(`\n  Total action items: ${actions.length}`);
  console.log(
    `  High priority:     ${actions.filter((a) => a.priority === 1).length}`,
  );
  console.log(
    `  Medium priority:   ${actions.filter((a) => a.priority === 2).length}`,
  );
  console.log(
    `  Low priority:      ${actions.filter((a) => a.priority === 3).length}`,
  );

  console.log(`\n${"=".repeat(60)}`);
  console.log("  Backfill analysis complete");
  console.log(`${"=".repeat(60)}\n`);
}

main().catch((err) => {
  console.error("Backfill analysis failed:", err);
  process.exit(1);
});
