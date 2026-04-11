/**
 * Denton County (DCAD - Denton Central Appraisal District) Adapter
 * Single CSV: nightly_appraisals.csv
 * Direct download from dentoncad.com
 */

import { createReadStream } from "node:fs";
import { createInterface } from "node:readline";
import { readdirSync } from "node:fs";
import { join } from "node:path";
import { AssessorAdapter, type CountyConfig, type RawPropertyRecord, type AdapterProgress } from "./base.js";

function parseCSVLine(line: string): string[] {
  const fields: string[] = [];
  let current = "";
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') {
      if (inQuotes && i + 1 < line.length && line[i + 1] === '"') { current += '"'; i++; }
      else inQuotes = !inQuotes;
    } else if (ch === "," && !inQuotes) { fields.push(current.trim()); current = ""; }
    else current += ch;
  }
  fields.push(current.trim());
  return fields;
}

export class DentonAdapter extends AssessorAdapter {
  readonly platform = "denton";

  canHandle(config: CountyConfig): boolean {
    return config.platform === "denton";
  }

  async *fetchProperties(
    config: CountyConfig,
    onProgress?: (progress: AdapterProgress) => void,
  ): AsyncGenerator<RawPropertyRecord> {
    const dataDir = config.search_params?.data_dir;
    if (!dataDir) { console.error("  Denton adapter requires search_params.data_dir"); return; }

    const files = readdirSync(dataDir).filter((f) => f.endsWith(".csv"));
    if (files.length === 0) { console.error(`  No CSV files found in ${dataDir}`); return; }

    const progress: AdapterProgress = {
      county: config.name, total_found: 0, total_processed: 0, errors: 0, started_at: new Date(),
    };

    const filePath = join(dataDir, files[0]);
    console.log(`  Processing: ${files[0]}`);

    const stream = createReadStream(filePath, { encoding: "utf-8" });
    const rl = createInterface({ input: stream, crlfDelay: Infinity });
    let headers: string[] = [];
    let lineNum = 0;

    for await (const line of rl) {
      lineNum++;
      if (lineNum === 1) { headers = parseCSVLine(line).map((h) => h.replace(/^\uFEFF/, "").trim()); continue; }

      const fields = parseCSVLine(line);
      if (fields.length < 20) continue;

      const row: Record<string, string> = {};
      for (let i = 0; i < headers.length && i < fields.length; i++) row[headers[i]] = fields[i];

      const address = row.situs_street_address?.trim() || row.situs_full_address?.trim() || "";
      if (!address) continue;

      progress.total_found++;

      const marketValue = parseInt(row.ownerMarketValue) || 0;
      const imprValue = parseInt(row.improvementValue) || 0;
      const yearBuilt = parseInt(row.imprvActualYearBuilt) || 0;
      const mainArea = parseInt(row.imprvMainArea) || 0;
      const totalArea = parseInt(row.imprvTotalArea) || 0;
      const propType = row.propType?.trim() || "";
      const useCd = row.useCd?.trim() || "";

      // Classify apartments
      const isApt = propType.toUpperCase().includes("MULTI") ||
                    propType.toUpperCase().includes("APART") ||
                    useCd.startsWith("A") ||
                    (mainArea > 5000 && propType.toUpperCase().includes("COM"));

      const record: RawPropertyRecord = {
        parcel_id: row.pid?.trim() || "",
        address,
        city: row.situsCity?.trim() || "DENTON",
        state: "TX",
        zip: row.situsZip?.trim().slice(0, 5) || "",
        owner_name: row.name?.trim() || undefined,
        property_type: propType || useCd || undefined,
        assessed_value: marketValue > 0 ? marketValue : imprValue > 0 ? imprValue : undefined,
        market_value: marketValue > 0 ? marketValue : undefined,
        year_built: yearBuilt > 1800 ? yearBuilt : undefined,
        total_sqft: mainArea > 0 ? mainArea : totalArea > 0 ? totalArea : undefined,
        last_sale_date: row.deedDt?.trim() || undefined,
        legal_description: row.legalDescription?.trim() || undefined,
        assessor_url: row.property_url?.trim() || undefined,
        raw: {
          propType,
          useCd,
          stateCodes: row.stateCodes?.trim(),
          deedType: row.deedType?.trim(),
          isApartment: isApt,
          isSfr: propType.toUpperCase().includes("SINGLE") || propType.toUpperCase().includes("RES"),
          isCondo: propType.toUpperCase().includes("CONDO"),
        },
      };

      progress.total_processed++;
      if (progress.total_processed % 10000 === 0) {
        console.log(`  Progress: ${progress.total_processed.toLocaleString()} processed`);
        onProgress?.(progress);
      }
      yield record;
    }

    console.log(`  ${config.name}: ${progress.total_found.toLocaleString()} found, ${progress.total_processed.toLocaleString()} processed`);
  }
}
