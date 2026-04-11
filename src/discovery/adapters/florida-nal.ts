/**
 * Florida NAL (Name-Address-Legal) Bulk CSV Adapter
 *
 * Imports property data from Florida Dept of Revenue standardized assessment files.
 * Source: https://floridarevenue.com/property/dataportal/Pages/default.aspx
 *
 * Format: Comma-delimited CSV with headers. One file per county per year.
 * Standardized across all 67 Florida counties (165 columns).
 *
 * Key fields: PHY_ADDR1 (address), PHY_CITY (city), PHY_ZIPCD (zip),
 * OWN_NAME (owner), DOR_UC (land use code), JV (just value),
 * ACT_YR_BLT (year built), TOT_LVG_AREA (sqft), NO_RES_UNTS (units),
 * SALE_PRC1/YR1/MO1 (last sale), TV_SD (taxable value), CENSUS_BK (FIPS)
 */

import { createReadStream } from "node:fs";
import { createInterface } from "node:readline";
import { readdirSync } from "node:fs";
import { join } from "node:path";
import { AssessorAdapter, type CountyConfig, type RawPropertyRecord, type AdapterProgress } from "./base.js";

// ─── DOR Land Use Code Mapping ───────────────────────────────────────

const DOR_USE_CODES: Record<string, { type: string; isApartment: boolean; isSfr: boolean; isCondo: boolean }> = {
  // Residential
  "001": { type: "single_family", isApartment: false, isSfr: true, isCondo: false },
  "002": { type: "mobile_home", isApartment: false, isSfr: true, isCondo: false },
  "003": { type: "multifamily", isApartment: true, isSfr: false, isCondo: false },   // 2-9 units
  "004": { type: "condo", isApartment: false, isSfr: false, isCondo: true },
  "005": { type: "cooperatives", isApartment: false, isSfr: false, isCondo: false },
  "006": { type: "retirement_home", isApartment: false, isSfr: false, isCondo: false },
  "007": { type: "miscellaneous_residential", isApartment: false, isSfr: false, isCondo: false },
  "008": { type: "multifamily", isApartment: true, isSfr: false, isCondo: false },   // 10+ units
  "009": { type: "single_family", isApartment: false, isSfr: true, isCondo: false }, // Non-homestead residential
  "010": { type: "vacant_residential", isApartment: false, isSfr: false, isCondo: false },
  // Commercial
  "011": { type: "commercial_store", isApartment: false, isSfr: false, isCondo: false },
  "012": { type: "commercial_mixed_use", isApartment: false, isSfr: false, isCondo: false },
  "013": { type: "commercial_dept_store", isApartment: false, isSfr: false, isCondo: false },
  "014": { type: "commercial_supermarket", isApartment: false, isSfr: false, isCondo: false },
  "015": { type: "commercial_regional_mall", isApartment: false, isSfr: false, isCondo: false },
  "016": { type: "commercial_community_center", isApartment: false, isSfr: false, isCondo: false },
  "017": { type: "commercial_office", isApartment: false, isSfr: false, isCondo: false },
  "018": { type: "commercial_office_multi_story", isApartment: false, isSfr: false, isCondo: false },
  "019": { type: "commercial_medical_office", isApartment: false, isSfr: false, isCondo: false },
  "020": { type: "commercial_transit_terminal", isApartment: false, isSfr: false, isCondo: false },
  "021": { type: "commercial_restaurant", isApartment: false, isSfr: false, isCondo: false },
  "022": { type: "commercial_fast_food", isApartment: false, isSfr: false, isCondo: false },
  "023": { type: "commercial_hotel", isApartment: false, isSfr: false, isCondo: false },
  "024": { type: "commercial_financial", isApartment: false, isSfr: false, isCondo: false },
  "025": { type: "commercial_service_station", isApartment: false, isSfr: false, isCondo: false },
  "026": { type: "commercial_auto_repair", isApartment: false, isSfr: false, isCondo: false },
  "027": { type: "commercial_auto_sales", isApartment: false, isSfr: false, isCondo: false },
  "028": { type: "commercial_parking", isApartment: false, isSfr: false, isCondo: false },
  "029": { type: "commercial_wholesale", isApartment: false, isSfr: false, isCondo: false },
  "030": { type: "commercial_florist_greenhouse", isApartment: false, isSfr: false, isCondo: false },
  "031": { type: "commercial_drive_in_theater", isApartment: false, isSfr: false, isCondo: false },
  "032": { type: "commercial_theater", isApartment: false, isSfr: false, isCondo: false },
  "033": { type: "commercial_nightclub", isApartment: false, isSfr: false, isCondo: false },
  "034": { type: "commercial_bowling", isApartment: false, isSfr: false, isCondo: false },
  "035": { type: "commercial_tourist_attraction", isApartment: false, isSfr: false, isCondo: false },
  "036": { type: "commercial_camp", isApartment: false, isSfr: false, isCondo: false },
  "037": { type: "commercial_race_track", isApartment: false, isSfr: false, isCondo: false },
  "038": { type: "commercial_golf_course", isApartment: false, isSfr: false, isCondo: false },
  "039": { type: "commercial_hotel_motel", isApartment: false, isSfr: false, isCondo: false },
  // Industrial
  "040": { type: "industrial_light", isApartment: false, isSfr: false, isCondo: false },
  "041": { type: "industrial_heavy", isApartment: false, isSfr: false, isCondo: false },
  "042": { type: "industrial_chemical", isApartment: false, isSfr: false, isCondo: false },
  "043": { type: "industrial_lumber", isApartment: false, isSfr: false, isCondo: false },
  "044": { type: "industrial_fruit_packing", isApartment: false, isSfr: false, isCondo: false },
  "045": { type: "industrial_cattle_feed", isApartment: false, isSfr: false, isCondo: false },
  "046": { type: "industrial_other_food", isApartment: false, isSfr: false, isCondo: false },
  "047": { type: "industrial_mineral_processing", isApartment: false, isSfr: false, isCondo: false },
  "048": { type: "industrial_warehouse", isApartment: false, isSfr: false, isCondo: false },
  "049": { type: "industrial_open_storage", isApartment: false, isSfr: false, isCondo: false },
  // Agricultural
  "050": { type: "agricultural_improved", isApartment: false, isSfr: false, isCondo: false },
  "051": { type: "agricultural_cropland", isApartment: false, isSfr: false, isCondo: false },
  "052": { type: "agricultural_timberland", isApartment: false, isSfr: false, isCondo: false },
  "053": { type: "agricultural_timberland_2", isApartment: false, isSfr: false, isCondo: false },
  "054": { type: "agricultural_timberland_3", isApartment: false, isSfr: false, isCondo: false },
  "060": { type: "agricultural_grazing", isApartment: false, isSfr: false, isCondo: false },
  "061": { type: "agricultural_ornamental", isApartment: false, isSfr: false, isCondo: false },
  "066": { type: "agricultural_orchard", isApartment: false, isSfr: false, isCondo: false },
  "067": { type: "agricultural_poultry", isApartment: false, isSfr: false, isCondo: false },
  "068": { type: "agricultural_dairy", isApartment: false, isSfr: false, isCondo: false },
  "069": { type: "agricultural_other", isApartment: false, isSfr: false, isCondo: false },
  // Institutional / Government
  "070": { type: "institutional_vacant", isApartment: false, isSfr: false, isCondo: false },
  "071": { type: "institutional_church", isApartment: false, isSfr: false, isCondo: false },
  "072": { type: "institutional_private_school", isApartment: false, isSfr: false, isCondo: false },
  "073": { type: "institutional_private_hospital", isApartment: false, isSfr: false, isCondo: false },
  "074": { type: "institutional_home_for_aged", isApartment: false, isSfr: false, isCondo: false },
  "075": { type: "institutional_orphanage", isApartment: false, isSfr: false, isCondo: false },
  "076": { type: "institutional_mortuary", isApartment: false, isSfr: false, isCondo: false },
  "077": { type: "institutional_club_lodge", isApartment: false, isSfr: false, isCondo: false },
  "078": { type: "institutional_sanitarium", isApartment: false, isSfr: false, isCondo: false },
  "079": { type: "institutional_cultural", isApartment: false, isSfr: false, isCondo: false },
  "080": { type: "government_undefined", isApartment: false, isSfr: false, isCondo: false },
  "081": { type: "government_military", isApartment: false, isSfr: false, isCondo: false },
  "082": { type: "government_forest_park", isApartment: false, isSfr: false, isCondo: false },
  "083": { type: "government_public_school", isApartment: false, isSfr: false, isCondo: false },
  "084": { type: "government_public_college", isApartment: false, isSfr: false, isCondo: false },
  "085": { type: "government_public_hospital", isApartment: false, isSfr: false, isCondo: false },
  "086": { type: "government_county", isApartment: false, isSfr: false, isCondo: false },
  "087": { type: "government_state", isApartment: false, isSfr: false, isCondo: false },
  "088": { type: "government_federal", isApartment: false, isSfr: false, isCondo: false },
  "089": { type: "government_municipal", isApartment: false, isSfr: false, isCondo: false },
  // Miscellaneous
  "090": { type: "misc_leasehold", isApartment: false, isSfr: false, isCondo: false },
  "091": { type: "misc_utility", isApartment: false, isSfr: false, isCondo: false },
  "092": { type: "misc_mining", isApartment: false, isSfr: false, isCondo: false },
  "093": { type: "misc_subsurface_rights", isApartment: false, isSfr: false, isCondo: false },
  "094": { type: "misc_right_of_way", isApartment: false, isSfr: false, isCondo: false },
  "095": { type: "misc_rivers_lakes", isApartment: false, isSfr: false, isCondo: false },
  "096": { type: "misc_sewage", isApartment: false, isSfr: false, isCondo: false },
  "097": { type: "misc_outdoor_recreation", isApartment: false, isSfr: false, isCondo: false },
  "098": { type: "misc_centrally_assessed", isApartment: false, isSfr: false, isCondo: false },
  "099": { type: "misc_non_agricultural_acreage", isApartment: false, isSfr: false, isCondo: false },
};

function classifyDorCode(code: string): { type: string; isApartment: boolean; isSfr: boolean; isCondo: boolean } {
  const mapped = DOR_USE_CODES[code];
  if (mapped) return mapped;

  const num = parseInt(code);
  if (num >= 1 && num <= 9) return { type: "residential", isApartment: false, isSfr: true, isCondo: false };
  if (num >= 10 && num <= 19) return { type: "commercial", isApartment: false, isSfr: false, isCondo: false };
  if (num >= 20 && num <= 39) return { type: "commercial", isApartment: false, isSfr: false, isCondo: false };
  if (num >= 40 && num <= 49) return { type: "industrial", isApartment: false, isSfr: false, isCondo: false };
  if (num >= 50 && num <= 69) return { type: "agricultural", isApartment: false, isSfr: false, isCondo: false };
  if (num >= 70 && num <= 79) return { type: "institutional", isApartment: false, isSfr: false, isCondo: false };
  if (num >= 80 && num <= 89) return { type: "government", isApartment: false, isSfr: false, isCondo: false };
  return { type: "other", isApartment: false, isSfr: false, isCondo: false };
}

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

// ─── County Number → FIPS Mapping ────────────────────────────────────

/** DOR county number (CO_NO) → { name, fips (3-digit county FIPS) } */
export const FL_COUNTY_MAP: Record<string, { name: string; fips: string }> = {
  "01": { name: "Alachua", fips: "001" },
  "02": { name: "Baker", fips: "003" },
  "03": { name: "Bay", fips: "005" },
  "04": { name: "Bradford", fips: "007" },
  "05": { name: "Brevard", fips: "009" },
  "06": { name: "Broward", fips: "011" },
  "07": { name: "Calhoun", fips: "013" },
  "08": { name: "Charlotte", fips: "015" },
  "09": { name: "Citrus", fips: "017" },
  "10": { name: "Clay", fips: "019" },
  "11": { name: "Columbia", fips: "023" },
  "12": { name: "DeSoto", fips: "027" },
  "13": { name: "Dixie", fips: "029" },
  "14": { name: "Duval", fips: "031" },
  "15": { name: "Escambia", fips: "033" },
  "16": { name: "Flagler", fips: "035" },
  "17": { name: "Franklin", fips: "037" },
  "18": { name: "Gadsden", fips: "039" },
  "19": { name: "Gilchrist", fips: "041" },
  "20": { name: "Glades", fips: "043" },
  "21": { name: "Gulf", fips: "045" },
  "22": { name: "Hamilton", fips: "047" },
  "23": { name: "Hardee", fips: "049" },
  "24": { name: "Hendry", fips: "051" },
  "25": { name: "Hernando", fips: "053" },
  "26": { name: "Highlands", fips: "055" },
  "27": { name: "Hillsborough", fips: "057" },
  "28": { name: "Holmes", fips: "059" },
  "29": { name: "Indian River", fips: "061" },
  "30": { name: "Jackson", fips: "063" },
  "31": { name: "Jefferson", fips: "065" },
  "32": { name: "Lafayette", fips: "067" },
  "33": { name: "Lake", fips: "069" },
  "34": { name: "Lee", fips: "071" },
  "35": { name: "Leon", fips: "073" },
  "36": { name: "Levy", fips: "075" },
  "37": { name: "Liberty", fips: "077" },
  "38": { name: "Madison", fips: "079" },
  "39": { name: "Manatee", fips: "081" },
  "40": { name: "Marion", fips: "083" },
  "41": { name: "Martin", fips: "085" },
  "42": { name: "Miami-Dade", fips: "086" },
  "43": { name: "Monroe", fips: "087" },
  "44": { name: "Nassau", fips: "089" },
  "45": { name: "Okaloosa", fips: "091" },
  "46": { name: "Okeechobee", fips: "093" },
  "47": { name: "Orange", fips: "095" },
  "48": { name: "Osceola", fips: "097" },
  "49": { name: "Palm Beach", fips: "099" },
  "50": { name: "Pasco", fips: "101" },
  "51": { name: "Pinellas", fips: "103" },
  "52": { name: "Polk", fips: "105" },
  "53": { name: "Putnam", fips: "107" },
  "54": { name: "Santa Rosa", fips: "113" },
  "55": { name: "Sarasota", fips: "115" },
  "56": { name: "Seminole", fips: "117" },
  "57": { name: "St. Johns", fips: "109" },
  "58": { name: "St. Lucie", fips: "111" },
  "59": { name: "Sumter", fips: "119" },
  "60": { name: "Suwannee", fips: "121" },
  "61": { name: "Taylor", fips: "123" },
  "62": { name: "Union", fips: "125" },
  "63": { name: "Volusia", fips: "127" },
  "64": { name: "Wakulla", fips: "129" },
  "65": { name: "Walton", fips: "131" },
  "66": { name: "Washington", fips: "133" },
  "67": { name: "Monroe", fips: "087" },
};

// ─── Adapter ─────────────────────────────────────────────────────────

export class FloridaNALAdapter extends AssessorAdapter {
  readonly platform = "florida_nal";

  canHandle(config: CountyConfig): boolean {
    return config.platform === "florida_nal";
  }

  async *fetchProperties(
    config: CountyConfig,
    onProgress?: (progress: AdapterProgress) => void,
  ): AsyncGenerator<RawPropertyRecord> {
    const dataDir = config.search_params?.data_dir;
    if (!dataDir) {
      console.error("  Florida NAL adapter requires search_params.data_dir");
      return;
    }

    // Find NAL CSV file(s) in the data directory
    const files = readdirSync(dataDir).filter(
      (f) => f.toUpperCase().includes("NAL") && f.endsWith(".csv"),
    );

    if (files.length === 0) {
      // Try to find any CSV file
      const allCsvs = readdirSync(dataDir).filter((f) => f.endsWith(".csv") || f.endsWith(".CSV"));
      if (allCsvs.length > 0) {
        files.push(...allCsvs);
      } else {
        console.error(`  No NAL CSV files found in ${dataDir}`);
        return;
      }
    }

    const progress: AdapterProgress = {
      county: config.name,
      total_found: 0,
      total_processed: 0,
      errors: 0,
      started_at: new Date(),
    };

    for (const file of files) {
      console.log(`  Processing: ${file}`);
      const filePath = join(dataDir, file);
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
        if (fields.length < 20) continue;

        const row: Record<string, string> = {};
        for (let i = 0; i < headers.length && i < fields.length; i++) {
          row[headers[i]] = fields[i];
        }

        // Skip if no physical address
        const address = row.PHY_ADDR1?.trim() || "";
        if (!address) continue;

        progress.total_found++;

        // Classify property type from DOR use code
        const dorCode = row.DOR_UC?.trim().padStart(3, "0") || "000";
        const classification = classifyDorCode(dorCode);

        // Parse values
        const justValue = parseFloat(row.JV) || 0;
        const landValue = parseFloat(row.LND_VAL) || 0;
        const taxableValue = parseFloat(row.TV_SD) || 0;
        const yearBuilt = parseInt(row.ACT_YR_BLT) || 0;
        const effYearBuilt = parseInt(row.EFF_YR_BLT) || 0;
        const totalSqft = parseInt(row.TOT_LVG_AREA) || 0;
        const numUnits = parseInt(row.NO_RES_UNTS) || 0;
        const numBldgs = parseInt(row.NO_BULDNG) || 0;

        // ─── Sale history extraction ─────────────────────────────
        const salePrice1 = parseFloat(row.SALE_PRC1) || 0;
        const saleYear1 = parseInt(row.SALE_YR1) || 0;
        const saleMonth1 = parseInt(row.SALE_MO1) || 0;
        const qualCode1 = row.QUAL_CD1?.trim() || "";

        const salePrice2 = parseFloat(row.SALE_PRC2) || 0;
        const saleYear2 = parseInt(row.SALE_YR2) || 0;
        const saleMonth2 = parseInt(row.SALE_MO2) || 0;

        // Build last sale date from year + month
        let lastSaleDate: string | undefined;
        let lastSalePrice: number | undefined;
        if (salePrice1 > 0 && saleYear1 > 1900) {
          const mm = saleMonth1 > 0 ? String(saleMonth1).padStart(2, "0") : "01";
          lastSaleDate = `${saleYear1}-${mm}-01`;
          lastSalePrice = salePrice1;
        }

        // Build second sale date for metadata
        let prevSaleDate: string | undefined;
        if (salePrice2 > 0 && saleYear2 > 1900) {
          const mm2 = saleMonth2 > 0 ? String(saleMonth2).padStart(2, "0") : "01";
          prevSaleDate = `${saleYear2}-${mm2}-01`;
        }

        // Extract FIPS county code from CENSUS_BK if available (positions 3-5, 0-indexed 2-4)
        const censusBk = row.CENSUS_BK?.trim() || "";
        let countyFips: string | undefined;
        if (censusBk.length >= 5) {
          countyFips = censusBk.substring(2, 5);
        }

        // Determine best year built: prefer ACT_YR_BLT, fall back to EFF_YR_BLT
        const bestYearBuilt = yearBuilt > 1800 ? yearBuilt : effYearBuilt > 1800 ? effYearBuilt : undefined;

        const record: RawPropertyRecord = {
          parcel_id: row.PARCEL_ID?.trim() || "",
          address: address + (row.PHY_ADDR2?.trim() ? ` ${row.PHY_ADDR2.trim()}` : ""),
          city: row.PHY_CITY?.trim() || "",
          state: "FL",
          zip: row.PHY_ZIPCD?.trim() || "",
          owner_name: row.OWN_NAME?.trim() || undefined,
          property_type: classification.type,
          assessed_value: justValue > 0 ? justValue : undefined,
          market_value: justValue > 0 ? justValue : undefined, // JV = Just/Market Value in FL
          year_built: bestYearBuilt,
          total_sqft: totalSqft > 0 ? totalSqft : undefined,
          total_units: numUnits > 0 ? numUnits : undefined,
          last_sale_price: lastSalePrice,
          last_sale_date: lastSaleDate,
          legal_description: row.S_LEGAL?.trim() || undefined,
          assessor_url: undefined,
          raw: {
            dorCode,
            coNo: row.CO_NO?.trim() || undefined,
            stateParId: row.STATE_PAR_ID?.trim() || undefined,
            countyFips,
            censusBk: censusBk || undefined,
            landValue: landValue > 0 ? landValue : undefined,
            taxableValue: taxableValue > 0 ? taxableValue : undefined,
            numBldgs: numBldgs > 0 ? numBldgs : undefined,
            impQuality: row.IMP_QUAL?.trim() || undefined,
            constClass: row.CONST_CLASS?.trim() || undefined,
            effYearBuilt: effYearBuilt > 1800 ? effYearBuilt : undefined,
            landSqft: parseInt(row.LND_SQFOOT) || undefined,
            specFeatVal: parseFloat(row.SPEC_FEAT_VAL) || undefined,
            taxAuthCode: row.TAX_AUTH_CD?.trim() || undefined,
            township: row.TWN?.trim() || undefined,
            range: row.RNG?.trim() || undefined,
            section: row.SEC?.trim() || undefined,
            isApartment: classification.isApartment,
            isSfr: classification.isSfr,
            isCondo: classification.isCondo,
            // Sale 1 metadata
            saleQualCode1: qualCode1 || undefined,
            saleViCode1: row.VI_CD1?.trim() || undefined,
            // Sale 2 (previous) metadata
            prevSalePrice: salePrice2 > 0 ? salePrice2 : undefined,
            prevSaleDate,
            saleQualCode2: row.QUAL_CD2?.trim() || undefined,
            // Ownership mailing address
            ownerAddr1: row.OWN_ADDR1?.trim() || undefined,
            ownerAddr2: row.OWN_ADDR2?.trim() || undefined,
            ownerCity: row.OWN_CITY?.trim() || undefined,
            ownerState: row.OWN_STATE?.trim() || undefined,
            ownerZip: row.OWN_ZIPCD?.trim() || undefined,
          },
        };

        progress.total_processed++;
        if (progress.total_processed % 10000 === 0) {
          console.log(`  Progress: ${progress.total_processed.toLocaleString()} processed`);
          onProgress?.(progress);
        }

        yield record;
      }
    }

    console.log(
      `  ${config.name}: ${progress.total_found.toLocaleString()} found, ${progress.total_processed.toLocaleString()} processed`,
    );
  }
}
