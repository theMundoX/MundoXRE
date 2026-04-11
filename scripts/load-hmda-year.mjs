/**
 * Stream an HMDA LAR CSV (originated-only filter from CFPB data-browser-api)
 * directly into hmda_lar via COPY FROM STDIN. Picks only the columns we need.
 *
 * Usage:  node scripts/load-hmda-year.mjs 2023
 *         node scripts/load-hmda-year.mjs data/hmda/hmda_2023_originated.csv 2023
 */
import pkg from "pg";
import { from as copyFrom } from "pg-copy-streams";
import { createReadStream, statSync } from "fs";
import { pipeline } from "stream/promises";
import { Transform } from "stream";
import { parse } from "csv-parse";

const { Pool } = pkg;

const pool = new Pool({
  host: "207.244.225.239",
  port: 5432,
  database: "postgres",
  user: "postgres.your-tenant-id",
  password: "d6168ff6e8d9559d62642418bafb3d17",
  max: 2,
  connectionTimeoutMillis: 10000,
});

// Source CSV -> DB column. Everything else dropped.
const COLS = [
  "activity_year",
  "lei",
  "derived_msa_md",
  "state_code",
  "county_code",
  "census_tract",
  "conforming_loan_limit",
  "action_taken",
  "loan_type",
  "loan_purpose",
  "lien_status",
  "occupancy_type",
  "loan_amount",
  "interest_rate",
  "rate_spread",
  "loan_term",
  "property_value",
  "total_loan_costs",
  "origination_charges",
  "discount_points",
  "debt_to_income_ratio",
  "combined_loan_to_value",
  "applicant_credit_score_type",
  "hoepa_status",
  "preapproval",
  "construction_method",
  "manufactured_home_secured_property_type",
  "total_units",
  "reverse_mortgage",
  "open_end_line_of_credit",
  "business_or_commercial_purpose",
];

// HMDA uses slightly different header names (hyphens, different cased)
const CSV_HEADER_MAP = {
  derived_msa_md: "derived_msa-md",
  combined_loan_to_value: "combined_loan_to_value_ratio",
  open_end_line_of_credit: "open-end_line_of_credit",
};

// Numeric columns that may contain "Exempt" / "NA" / "" — rewrite to \N
const NUMERIC_COLS = new Set([
  "derived_msa_md",
  "action_taken",
  "loan_type",
  "loan_purpose",
  "lien_status",
  "occupancy_type",
  "loan_amount",
  "interest_rate",
  "rate_spread",
  "loan_term",
  "property_value",
  "total_loan_costs",
  "origination_charges",
  "discount_points",
  "combined_loan_to_value",
  "applicant_credit_score_type",
  "hoepa_status",
  "preapproval",
  "construction_method",
  "manufactured_home_secured_property_type",
  "total_units",
  "reverse_mortgage",
  "open_end_line_of_credit",
  "business_or_commercial_purpose",
  "activity_year",
]);

// HMDA placeholder values per https://ffiec.cfpb.gov/documentation/publications/loan-level-datasets
// Real interest rates are 0-30%. Real rate spreads are -10 to +10. Anything outside is a code.
const HMDA_NUMERIC_LIMITS = {
  interest_rate: { min: 0, max: 30 },
  rate_spread: { min: -50, max: 50 },
  combined_loan_to_value: { min: 0, max: 200 },
  loan_term: { min: 1, max: 600 },
};

function cleanNumeric(v, colName) {
  if (v == null) return "\\N";
  const s = String(v).trim();
  if (
    s === "" ||
    s === "NA" ||
    s === "Exempt" ||
    s === "Not Applicable" ||
    s === "..." ||
    s === "-"
  )
    return "\\N";
  const n = Number(s);
  if (!Number.isFinite(n)) return "\\N";
  // Clamp known placeholder/exempt codes (1111, 9999, -2620 etc)
  const limit = HMDA_NUMERIC_LIMITS[colName];
  if (limit && (n < limit.min || n > limit.max)) return "\\N";
  return s;
}

function tsvEscape(v) {
  if (v == null) return "\\N";
  const s = String(v);
  if (s === "") return "\\N";
  return s.replace(/\\/g, "\\\\").replace(/\t/g, " ").replace(/\n/g, " ").replace(/\r/g, "");
}

async function main() {
  const args = process.argv.slice(2);
  let csvPath, year;
  if (args.length === 1) {
    year = parseInt(args[0]);
    csvPath = `C:/Users/msanc/mxre/data/hmda/hmda_${year}_originated.csv`;
  } else if (args.length === 2) {
    csvPath = args[0];
    year = parseInt(args[1]);
  } else {
    console.error("usage: load-hmda-year.mjs <year>   OR   <csvPath> <year>");
    process.exit(1);
  }

  const size = statSync(csvPath).size;
  console.log(`Loading ${csvPath} (${(size / 1024 / 1024).toFixed(1)} MB) for year ${year}`);

  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const copySQL = `COPY hmda_lar (${COLS.join(",")}) FROM STDIN WITH (FORMAT text, DELIMITER E'\\t', NULL '\\N')`;
    const ingestStream = client.query(copyFrom(copySQL));

    let rowCount = 0;
    const startedAt = Date.now();

    const transform = new Transform({
      objectMode: true,
      transform(record, _enc, cb) {
        rowCount++;
        if (rowCount % 250000 === 0) {
          const secs = (Date.now() - startedAt) / 1000;
          const rate = Math.round(rowCount / secs);
          console.log(
            `  ${rowCount.toLocaleString()} rows  ${secs.toFixed(0)}s  ${rate}/s`
          );
        }
        const cells = COLS.map((dbCol) => {
          const csvKey = CSV_HEADER_MAP[dbCol] || dbCol;
          const raw = record[csvKey];
          if (NUMERIC_COLS.has(dbCol)) return cleanNumeric(raw, dbCol);
          return tsvEscape(raw);
        });
        cb(null, cells.join("\t") + "\n");
      },
    });

    await pipeline(
      createReadStream(csvPath),
      parse({ columns: true, skip_empty_lines: true, relax_quotes: true, relax_column_count: true }),
      transform,
      ingestStream
    );

    await client.query("COMMIT");
    const secs = (Date.now() - startedAt) / 1000;
    console.log(
      `\nLoaded ${rowCount.toLocaleString()} rows for ${year} in ${secs.toFixed(0)}s`
    );
  } catch (e) {
    await client.query("ROLLBACK").catch(() => {});
    throw e;
  } finally {
    client.release();
    await pool.end();
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
