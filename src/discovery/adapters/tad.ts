/**
 * Tarrant County (TAD / Fort Worth) Bulk Adapter
 *
 * Imports from TAD's pipe-delimited flat file.
 * Source: https://www.tad.org/resources/data-downloads
 *
 * Single file: PropertyData_{year}.txt
 * Pipe-delimited (|) with header row.
 * Contains all property types in one file.
 */

import { createReadStream } from "node:fs";
import { createInterface } from "node:readline";
import { readdirSync } from "node:fs";
import { join } from "node:path";
import { AssessorAdapter, type CountyConfig, type RawPropertyRecord, type AdapterProgress } from "./base.js";

export class TADAdapter extends AssessorAdapter {
  readonly platform = "tad";

  canHandle(config: CountyConfig): boolean {
    return config.platform === "tad";
  }

  async *fetchProperties(
    config: CountyConfig,
    onProgress?: (progress: AdapterProgress) => void,
  ): AsyncGenerator<RawPropertyRecord> {
    const dataDir = config.search_params?.data_dir;
    if (!dataDir) {
      console.error("  TAD adapter requires search_params.data_dir");
      return;
    }

    // Find the PropertyData txt file
    const files = readdirSync(dataDir).filter((f) => f.includes("PropertyData") && f.endsWith(".txt"));
    if (files.length === 0) {
      console.error(`  No PropertyData files found in ${dataDir}`);
      return;
    }

    const progress: AdapterProgress = {
      county: config.name,
      total_found: 0,
      total_processed: 0,
      errors: 0,
      started_at: new Date(),
    };

    const filePath = join(dataDir, files[0]);
    console.log(`  Processing: ${files[0]}`);

    const stream = createReadStream(filePath, { encoding: "utf-8" });
    const rl = createInterface({ input: stream, crlfDelay: Infinity });

    let headers: string[] = [];
    let lineNum = 0;

    for await (const line of rl) {
      lineNum++;
      if (lineNum === 1) {
        headers = line.split("|").map((h) => h.replace(/^\uFEFF/, "").trim());
        continue;
      }

      const fields = line.split("|");
      if (fields.length < 20) continue;

      const row: Record<string, string> = {};
      for (let i = 0; i < headers.length && i < fields.length; i++) {
        row[headers[i]] = fields[i]?.trim() || "";
      }

      // Skip non-real property records
      const recordType = row.Record_Type || "";
      if (recordType !== "AAAA" && recordType !== "") continue;

      const address = row.Situs_Address?.trim() || "";
      if (!address) continue;

      // Skip personal property
      const propClass = row.Property_Class?.trim() || "";
      if (propClass.startsWith("L") || propClass.startsWith("BPP")) continue;

      progress.total_found++;

      const totalValue = parseInt(row.Total_Value) || 0;
      const yearBuilt = parseInt(row.Year_Built) || 0;
      const livingArea = parseInt(row.Living_Area) || 0;
      const landValue = parseInt(row.Land_Value) || 0;
      const imprValue = parseInt(row.Improvement_Value) || 0;

      // Extract city from the City code — TAD uses numeric city codes
      // The Situs_Address often contains the city info
      const cityCode = row.City?.trim() || "";

      const record: RawPropertyRecord = {
        parcel_id: row.Account_Num?.trim() || row.PIDN?.trim() || "",
        address,
        city: "FORT WORTH", // Default — TAD uses city codes, will refine
        state: "TX",
        zip: "", // TAD flat file doesn't include property zip — needs geocoding
        owner_name: row.Owner_Name?.trim() || undefined,
        property_type: propClass || undefined,
        assessed_value: totalValue > 0 ? totalValue : undefined,
        market_value: totalValue > 0 ? totalValue : undefined,
        year_built: yearBuilt > 1800 ? yearBuilt : undefined,
        total_sqft: livingArea > 0 ? livingArea : undefined,
        legal_description: row.LegalDescription?.trim() || undefined,
        assessor_url: `https://www.tad.org/property/${row.Account_Num?.trim()}`,
        raw: {
          propertyClass: propClass,
          cityCode,
          landValue: landValue > 0 ? landValue : undefined,
          imprValue: imprValue > 0 ? imprValue : undefined,
          pidn: row.PIDN?.trim(),
          stateUseCode: row.State_Use_Code?.trim(),
          exemptionCode: row.Exemption_Code?.trim(),
          garageCapacity: parseInt(row.Garage_Capacity) || undefined,
          landAcres: parseFloat(row.Land_Acres) || undefined,
          appraisedValue: parseInt(row.Appraised_Value) || undefined,
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
