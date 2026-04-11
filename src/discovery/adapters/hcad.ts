/**
 * Harris County (HCAD / Houston) Bulk Adapter
 *
 * Imports property data from HCAD's free bulk tab-delimited downloads.
 * Source: https://hcad.org/pdata/pdata-property-downloads.html
 *
 * Files used:
 *   real_acct.txt — account info, address, owner, values (tab-delimited)
 *   building_res.txt — residential building details (tab-delimited)
 *   building_other.txt — commercial building details (tab-delimited)
 *   deeds.txt — deed transfer dates (tab-delimited)
 *
 * All files are tab-delimited with headers. Join on `acct` field.
 * HCAD does NOT publish sale prices — only deed dates.
 */

import { createReadStream } from "node:fs";
import { createInterface } from "node:readline";
import { AssessorAdapter, type CountyConfig, type RawPropertyRecord, type AdapterProgress } from "./base.js";

// ─── Tab-Delimited Parsing ───────────────────────────────────────────

async function loadTSVIndex<T>(
  filePath: string,
  keyField: string,
  mapper: (row: Record<string, string>) => T,
): Promise<Map<string, T>> {
  const index = new Map<string, T>();
  const stream = createReadStream(filePath, { encoding: "utf-8" });
  const rl = createInterface({ input: stream, crlfDelay: Infinity });

  let headers: string[] = [];
  let lineNum = 0;

  for await (const line of rl) {
    lineNum++;
    if (lineNum === 1) {
      headers = line.split("\t").map((h) => h.replace(/^\uFEFF/, "").trim());
      continue;
    }

    const fields = line.split("\t");
    if (fields.length < 3) continue;

    const row: Record<string, string> = {};
    for (let i = 0; i < headers.length && i < fields.length; i++) {
      row[headers[i]] = fields[i]?.trim() || "";
    }

    const key = row[keyField]?.trim();
    if (key) {
      index.set(key, mapper(row));
    }
  }

  return index;
}

// ─── Data Types ──────────────────────────────────────────────────────

interface HCADBuilding {
  yearBuilt: number;
  totalSqft: number;
  stories: number;
}

interface HCADDeed {
  saleDate: string;
}

// ─── Adapter ─────────────────────────────────────────────────────────

export class HCADAdapter extends AssessorAdapter {
  readonly platform = "hcad";

  canHandle(config: CountyConfig): boolean {
    return config.platform === "hcad";
  }

  async *fetchProperties(
    config: CountyConfig,
    onProgress?: (progress: AdapterProgress) => void,
  ): AsyncGenerator<RawPropertyRecord> {
    const dataDir = config.search_params?.data_dir;
    if (!dataDir) {
      console.error("  HCAD adapter requires search_params.data_dir");
      return;
    }

    const progress: AdapterProgress = {
      county: config.name,
      total_found: 0,
      total_processed: 0,
      errors: 0,
      started_at: new Date(),
    };

    // Phase 1: Load residential building details
    console.log("  Loading residential buildings...");
    const resBuildings = await loadTSVIndex<HCADBuilding>(
      `${dataDir}/building_res.txt`,
      "acct",
      (row) => ({
        yearBuilt: parseInt(row.date_erected) || 0,
        totalSqft: parseInt(row.act_ar) || parseInt(row.im_sq_ft) || 0,
        stories: 0,
      }),
    );
    console.log(`  Loaded ${resBuildings.size} residential buildings`);

    // Phase 2: Load commercial building details
    console.log("  Loading commercial buildings...");
    const comBuildings = await loadTSVIndex<HCADBuilding>(
      `${dataDir}/building_other.txt`,
      "acct",
      (row) => ({
        yearBuilt: parseInt(row.date_erected) || 0,
        totalSqft: parseInt(row.im_sq_ft) || parseInt(row.nra) || 0,
        stories: parseInt(row.stories) || 0,
      }),
    );
    console.log(`  Loaded ${comBuildings.size} commercial buildings`);

    // Phase 3: Load deed dates (most recent per account)
    console.log("  Loading deeds...");
    const deeds = await loadTSVIndex<HCADDeed>(
      `${dataDir}/deeds.txt`,
      "acct",
      (row) => ({
        saleDate: row.dos || "",
      }),
    );
    console.log(`  Loaded ${deeds.size} deed records`);

    // Phase 4: Stream through real_acct.txt and yield merged records
    console.log("  Streaming accounts and merging...");
    const stream = createReadStream(`${dataDir}/real_acct.txt`, { encoding: "utf-8" });
    const rl = createInterface({ input: stream, crlfDelay: Infinity });

    let headers: string[] = [];
    let lineNum = 0;

    for await (const line of rl) {
      lineNum++;
      if (lineNum === 1) {
        headers = line.split("\t").map((h) => h.replace(/^\uFEFF/, "").trim());
        continue;
      }

      const fields = line.split("\t");
      if (fields.length < 10) continue;

      const row: Record<string, string> = {};
      for (let i = 0; i < headers.length && i < fields.length; i++) {
        row[headers[i]] = fields[i]?.trim() || "";
      }

      const acct = row.acct?.trim();
      if (!acct) continue;

      // Skip if no site address
      const address = row.site_addr_1?.trim() || "";
      if (!address) continue;

      progress.total_found++;

      // Merge building data
      const resBldg = resBuildings.get(acct);
      const comBldg = comBuildings.get(acct);
      const bldg = resBldg || comBldg;
      const deed = deeds.get(acct);

      // Parse values — HCAD stores as strings with possible decimals
      const totMktVal = parseFloat(row.tot_mkt_val) || 0;
      const assessedVal = parseFloat(row.assessed_val) || 0;
      const bldAr = parseInt(row.bld_ar) || 0;

      // Parse deed date (MM/DD/YYYY format)
      let lastSaleDate: string | undefined;
      if (deed?.saleDate) {
        const parts = deed.saleDate.split("/");
        if (parts.length === 3) {
          lastSaleDate = `${parts[2]}-${parts[0].padStart(2, "0")}-${parts[1].padStart(2, "0")}`;
        }
      }

      const record: RawPropertyRecord = {
        parcel_id: acct,
        address,
        city: row.site_addr_2?.trim() || "HOUSTON",
        state: "TX",
        zip: row.site_addr_3?.trim() || "",
        owner_name: row.mailto?.trim() || undefined,
        property_type: row.state_class?.trim() || undefined,
        assessed_value: assessedVal > 0 ? assessedVal : totMktVal > 0 ? totMktVal : undefined,
        market_value: totMktVal > 0 ? totMktVal : undefined,
        year_built: bldg?.yearBuilt || undefined,
        total_sqft: bldAr > 0 ? bldAr : bldg?.totalSqft || undefined,
        stories: bldg?.stories || undefined,
        last_sale_date: lastSaleDate,
        legal_description: [row.lgl_1, row.lgl_2].filter(Boolean).join(" ").trim() || undefined,
        assessor_url: `https://public.hcad.org/records/details.asp?cession_yr=2025&acct=${acct}`,
        raw: {
          stateClass: row.state_class,
          neighborhoodCode: row.Neighborhood_Code,
          landVal: row.land_val,
          bldVal: row.bld_val,
          acreage: row.acreage,
        },
      };

      progress.total_processed++;
      if (progress.total_processed % 10000 === 0) {
        console.log(`  Progress: ${progress.total_processed.toLocaleString()} processed`);
        onProgress?.(progress);
      }

      yield record;
    }

    console.log(
      `  ${config.name}: ${progress.total_found.toLocaleString()} found, ${progress.total_processed.toLocaleString()} processed`,
    );
  }
}
