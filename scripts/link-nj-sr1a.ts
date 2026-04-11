#!/usr/bin/env tsx
/**
 * MXRE — NJ SR1A Linker
 *
 * Builds a full parcel_id → property_id index from all NJ properties,
 * then matches SR1A deed records and updates mortgage_records.property_id.
 *
 * Key fix: PostgREST caps at 1000 rows — uses proper 1000-page pagination.
 */

import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import fs from "node:fs";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const PAGE = 1000; // PostgREST hard cap

const NJ_COUNTIES: Record<string, string> = {
  "01": "Atlantic", "02": "Bergen", "03": "Burlington", "04": "Camden",
  "05": "Cape May", "06": "Cumberland", "07": "Essex", "08": "Gloucester",
  "09": "Hudson", "10": "Hunterdon", "11": "Mercer", "12": "Middlesex",
  "13": "Monmouth", "14": "Morris", "15": "Ocean", "16": "Passaic",
  "17": "Salem", "18": "Somerset", "19": "Sussex", "20": "Union", "21": "Warren",
};

async function buildParcelIndex(): Promise<Map<string, number>> {
  const index = new Map<string, number>();
  const countyIdMap: Record<string, number> = {};

  // Load county IDs
  const { data: counties } = await db.from("counties")
    .select("id, county_name")
    .eq("state_code", "NJ");
  for (const c of counties || []) {
    const code = Object.entries(NJ_COUNTIES).find(([, name]) => name === c.county_name)?.[0];
    if (code) countyIdMap[code] = c.id;
  }

  console.log(`Loading parcel index for ${Object.keys(countyIdMap).length} NJ counties...`);

  for (const [code, name] of Object.entries(NJ_COUNTIES)) {
    const countyId = countyIdMap[code];
    if (!countyId) { process.stdout.write(`${name}(skip) `); continue; }

    let offset = 0;
    let countyCount = 0;

    while (true) {
      const { data, error } = await db.from("properties")
        .select("id, parcel_id")
        .eq("county_id", countyId)
        .not("parcel_id", "is", null)
        .neq("parcel_id", "")
        .range(offset, offset + PAGE - 1);

      if (error) { console.error(`\n  Error loading ${name}: ${error.message}`); break; }
      if (!data || data.length === 0) break;

      for (const p of data as { id: number; parcel_id: string }[]) {
        index.set(p.parcel_id, p.id);
      }

      countyCount += data.length;
      offset += PAGE;
      if (data.length < PAGE) break;
    }

    process.stdout.write(`${name}(${countyCount.toLocaleString()}) `);
  }

  console.log(`\nIndex total: ${index.size.toLocaleString()} entries`);
  return index;
}

function parseSr1aLine(line: string): { parcelId: string; deedBook: string; deedPage: string; cc: string } | null {
  if (line.length < 370) return null;

  const cc = line.substring(0, 2).trim();
  const dd = line.substring(2, 4).trim();
  const block = line.substring(350, 355).trim().replace(/^0+/, "") || "0";
  const lot = line.substring(359, 364).trim().replace(/^0+/, "") || "0";
  const deedBook = line.substring(328, 333).trim();
  const deedPage = line.substring(333, 338).trim();

  if (!NJ_COUNTIES[cc]) return null;

  return {
    cc,
    parcelId: `${cc}${dd}_${block}_${lot}`,
    deedBook,
    deedPage,
  };
}

async function flushUpdates(updates: Array<{ propId: number; docNum: string }>) {
  for (const u of updates) {
    await db.from("mortgage_records")
      .update({ property_id: u.propId })
      .eq("document_number", u.docNum)
      .ilike("source_url", "%sr1a%")
      .is("property_id", null);
  }
}

async function main() {
  console.log("MXRE — NJ SR1A Linker (full pagination)");
  console.log("=".repeat(50));

  const parcelIndex = await buildParcelIndex();

  const dataFiles = [
    "data/sr1a-cache/nj-sr1a-2023/Sales2023.txt",
    "data/sr1a-cache/nj-sr1a-2024/Sales2024.txt",
    "data/sr1a-cache/nj-sr1a-2025/Sales2025.txt",
    "data/sr1a-cache/nj-sr1a-2026/YTDSR1A2026.txt",
  ].filter(fs.existsSync);

  console.log(`\nFiles to process: ${dataFiles.map(f => f.split("/").pop()).join(", ")}`);

  let totalParsed = 0;
  let totalMatched = 0;
  let updates: Array<{ propId: number; docNum: string }> = [];

  for (const file of dataFiles) {
    const fname = file.split("/").pop();
    console.log(`\nParsing ${fname}...`);
    const lines = fs.readFileSync(file, "utf8").split(/\r?\n/);
    let fileParsed = 0;
    let fileMatched = 0;

    for (const line of lines) {
      const parsed = parseSr1aLine(line);
      if (!parsed) continue;
      fileParsed++;
      totalParsed++;

      const propId = parcelIndex.get(parsed.parcelId);
      if (propId) {
        updates.push({ propId, docNum: `${parsed.deedBook}-${parsed.deedPage}` });
        fileMatched++;
        totalMatched++;

        if (updates.length >= 200) {
          await flushUpdates(updates);
          updates = [];
        }

        if (totalMatched % 5000 === 0) {
          process.stdout.write(`\r  ${fname}: ${fileMatched.toLocaleString()} matched / ${fileParsed.toLocaleString()} parsed (${Math.round(fileMatched/fileParsed*100)}%)`);
        }
      }
    }

    if (updates.length > 0) {
      await flushUpdates(updates);
      updates = [];
    }

    console.log(`\r  ${fname}: ${fileMatched.toLocaleString()} matched / ${fileParsed.toLocaleString()} parsed (${fileParsed > 0 ? Math.round(fileMatched/fileParsed*100) : 0}%)`);
  }

  console.log(`\n${"=".repeat(50)}`);
  console.log(`TOTAL: ${totalMatched.toLocaleString()} matched / ${totalParsed.toLocaleString()} parsed (${totalParsed > 0 ? Math.round(totalMatched/totalParsed*100) : 0}%)`);

  // Refresh materialized views
  console.log("\nRefreshing materialized views...");
  const url = process.env.SUPABASE_URL!;
  const key = process.env.SUPABASE_SERVICE_KEY!;
  for (const v of ["county_lien_counts", "county_stats_mv"]) {
    const res = await fetch(`${url}/pg/query`, {
      method: "POST",
      headers: { apikey: key, Authorization: `Bearer ${key}`, "Content-Type": "application/json" },
      body: JSON.stringify({ query: `REFRESH MATERIALIZED VIEW ${v}` }),
    });
    console.log(`  REFRESH ${v}: ${res.ok ? "OK" : "FAILED"}`);
  }

  console.log("\nDone.");
}

main().catch(err => { console.error("Fatal:", err); process.exit(1); });
