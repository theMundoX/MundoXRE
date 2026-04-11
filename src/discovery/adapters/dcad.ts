/**
 * Dallas County (DCAD) Bulk CSV Adapter
 *
 * Imports property data from DCAD's free bulk CSV downloads.
 * Source: https://www.dallascad.org/DataProducts.aspx
 *
 * Files used:
 *   ACCOUNT_INFO.CSV — owner, address, legal description
 *   ACCOUNT_APPRL_YEAR.CSV — assessed values, property type
 *   RES_DETAIL.CSV — residential year built, sqft, bedrooms, units
 *   COM_DETAIL.CSV — commercial year built, sqft, units
 *
 * All CSV files are comma-delimited with headers.
 * Join on ACCOUNT_NUM field.
 */

import { createReadStream } from "node:fs";
import { createInterface } from "node:readline";
import { AssessorAdapter, type CountyConfig, type RawPropertyRecord, type AdapterProgress } from "./base.js";

// ─── CSV Parsing ─────────────────────────────────────────────────────

function parseCSVLine(line: string): string[] {
  const fields: string[] = [];
  let current = "";
  let inQuotes = false;

  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') {
      if (inQuotes && i + 1 < line.length && line[i + 1] === '"') {
        current += '"';
        i++;
      } else {
        inQuotes = !inQuotes;
      }
    } else if (ch === "," && !inQuotes) {
      fields.push(current.trim());
      current = "";
    } else {
      current += ch;
    }
  }
  fields.push(current.trim());
  return fields;
}

async function loadCSVIndex<T>(
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
      headers = parseCSVLine(line).map((h) => h.replace(/^\uFEFF/, "").trim());
      continue;
    }

    const fields = parseCSVLine(line);
    if (fields.length < headers.length * 0.5) continue; // skip malformed rows

    const row: Record<string, string> = {};
    for (let i = 0; i < headers.length && i < fields.length; i++) {
      row[headers[i]] = fields[i];
    }

    const key = row[keyField];
    if (key) {
      index.set(key, mapper(row));
    }
  }

  return index;
}

// ─── Data Types ──────────────────────────────────────────────────────

interface DCADAccountInfo {
  accountNum: string;
  ownerName: string;
  bizName: string;
  streetNum: string;
  streetName: string;
  unitId: string;
  city: string;
  zip: string;
  deedDate: string;
  legal: string;
}

interface DCADValues {
  totalValue: number;
  landValue: number;
  imprValue: number;
  divisionCd: string;
  sptdCode: string;
  bldgClassDesc: string;
  gisParcelId: string;
}

interface DCADBuilding {
  yearBuilt: number;
  totalSqft: number;
  numUnits: number;
  numStories: string;
  numBedrooms: number;
  numBaths: number;
  propertyName: string;
}

// ─── Adapter ─────────────────────────────────────────────────────────

export class DCADAdapter extends AssessorAdapter {
  readonly platform = "dcad";

  canHandle(config: CountyConfig): boolean {
    return config.platform === "dcad";
  }

  async *fetchProperties(
    config: CountyConfig,
    onProgress?: (progress: AdapterProgress) => void,
  ): AsyncGenerator<RawPropertyRecord> {
    const dataDir = config.search_params?.data_dir;
    if (!dataDir) {
      console.error("  DCAD adapter requires search_params.data_dir pointing to extracted CSV directory");
      return;
    }

    const progress: AdapterProgress = {
      county: config.name,
      total_found: 0,
      total_processed: 0,
      errors: 0,
      started_at: new Date(),
    };

    // Phase 1: Load values index (ACCOUNT_APPRL_YEAR.CSV)
    console.log("  Loading appraisal values...");
    const values = await loadCSVIndex<DCADValues>(
      `${dataDir}/ACCOUNT_APPRL_YEAR.CSV`,
      "ACCOUNT_NUM",
      (row) => ({
        totalValue: parseInt(row.TOT_VAL) || 0,
        landValue: parseInt(row.LAND_VAL) || 0,
        imprValue: parseInt(row.IMPR_VAL) || 0,
        divisionCd: row.DIVISION_CD || "",
        sptdCode: row.SPTD_CODE || "",
        bldgClassDesc: row["BLDG CLASS CD"] || row.BLDG_CLASS_CD || "",
        gisParcelId: row.GIS_PARCEL_ID || "",
      }),
    );
    console.log(`  Loaded ${values.size} appraisal records`);

    // Phase 2: Load residential building details
    console.log("  Loading residential details...");
    const resBuildings = await loadCSVIndex<DCADBuilding>(
      `${dataDir}/RES_DETAIL.CSV`,
      "ACCOUNT_NUM",
      (row) => ({
        yearBuilt: parseInt(row.YR_BUILT) || 0,
        totalSqft: parseInt(row.TOT_LIVING_AREA_SF) || parseInt(row.TOT_MAIN_SF) || 0,
        numUnits: parseInt(row.NUM_UNITS) || 0,
        numStories: row.NUM_STORIES_DESC || "",
        numBedrooms: parseInt(row.NUM_BEDROOMS) || 0,
        numBaths: parseInt(row.NUM_FULL_BATHS) || 0,
        propertyName: "",
      }),
    );
    console.log(`  Loaded ${resBuildings.size} residential building records`);

    // Phase 3: Load commercial building details
    console.log("  Loading commercial details...");
    const comBuildings = await loadCSVIndex<DCADBuilding>(
      `${dataDir}/COM_DETAIL.CSV`,
      "ACCOUNT_NUM",
      (row) => ({
        yearBuilt: parseInt(row.YEAR_BUILT) || 0,
        totalSqft: parseInt(row.GROSS_BLDG_AREA) || 0,
        numUnits: parseInt(row.NUM_UNITS) || 0,
        numStories: row.NUM_STORIES || "",
        numBedrooms: 0,
        numBaths: 0,
        propertyName: row.PROPERTY_NAME || "",
      }),
    );
    console.log(`  Loaded ${comBuildings.size} commercial building records`);

    // Phase 4: Stream through ACCOUNT_INFO.CSV and yield merged records
    console.log("  Streaming account info and merging...");
    const stream = createReadStream(`${dataDir}/ACCOUNT_INFO.CSV`, { encoding: "utf-8" });
    const rl = createInterface({ input: stream, crlfDelay: Infinity });

    let headers: string[] = [];
    let lineNum = 0;

    for await (const line of rl) {
      lineNum++;
      if (lineNum === 1) {
        headers = parseCSVLine(line).map((h) => h.replace(/^\uFEFF/, "").trim());
        continue;
      }

      const fields = parseCSVLine(line);
      if (fields.length < 5) continue;

      const row: Record<string, string> = {};
      for (let i = 0; i < headers.length && i < fields.length; i++) {
        row[headers[i]] = fields[i];
      }

      const acctNum = row.ACCOUNT_NUM;
      if (!acctNum) continue;

      // Skip Business Personal Property — only import real property
      const rowDivCd = row.DIVISION_CD?.trim() || "";
      if (rowDivCd === "BPP" || rowDivCd === "P") continue;

      // Merge with values and building data
      const val = values.get(acctNum);

      progress.total_found++;
      const resBldg = resBuildings.get(acctNum);
      const comBldg = comBuildings.get(acctNum);
      const bldg = resBldg || comBldg;

      // Build address
      const streetNum = row.STREET_NUM?.trim() || "";
      const streetName = row.FULL_STREET_NAME?.trim() || "";
      const unitId = row.UNIT_ID?.trim() || "";
      let address = `${streetNum} ${streetName}`.trim();
      if (unitId) address += ` ${unitId}`;

      if (!address || address.includes("LEASED EQUIPMENT") || !streetName) continue;

      // Determine property type from SPTD code and division
      const divCd = val?.divisionCd || rowDivCd;
      const sptdCode = val?.sptdCode || "";

      const record: RawPropertyRecord = {
        parcel_id: acctNum,
        address,
        city: (row.PROPERTY_CITY?.trim() || "DALLAS").replace(/\s*\(DALLAS CO\)\s*/i, ""),
        state: "TX",
        zip: row.PROPERTY_ZIPCODE?.trim() || "",
        owner_name: row.OWNER_NAME1?.trim() || undefined,
        property_type: sptdCode || divCd || undefined,
        assessed_value: val?.totalValue || undefined,
        market_value: val?.totalValue || undefined,
        year_built: bldg?.yearBuilt || undefined,
        total_sqft: bldg?.totalSqft || undefined,
        total_units: bldg && bldg.numUnits > 0 ? bldg.numUnits : undefined,
        stories: bldg?.numStories ? parseInt(bldg.numStories) || undefined : undefined,
        last_sale_date: row.DEED_TXFR_DATE?.trim() || undefined,
        legal_description: [row.LEGAL1, row.LEGAL2, row.LEGAL3]
          .filter(Boolean)
          .join(" ")
          .trim() || undefined,
        assessor_url: `https://www.dallascad.org/AcctDetailRes.aspx?ID=${acctNum}`,
        raw: {
          divisionCd: divCd,
          sptdCode,
          bldgClassDesc: val?.bldgClassDesc,
          gisParcelId: val?.gisParcelId,
          bizName: row.BIZ_NAME?.trim(),
          bedrooms: bldg?.numBedrooms,
          baths: bldg?.numBaths,
          propertyName: bldg?.propertyName,
          isApartment: !!(
            bldg?.propertyName?.toUpperCase().includes("APARTMENT") ||
            val?.bldgClassDesc?.toUpperCase().includes("APARTMENT") ||
            (bldg && bldg.numUnits >= 5)
          ),
          isSfr: divCd === "RES" && (!bldg || bldg.numUnits <= 1),
          isCondo: !!(val?.bldgClassDesc?.toUpperCase().includes("CONDO")),
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
