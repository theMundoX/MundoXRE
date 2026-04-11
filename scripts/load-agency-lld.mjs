/**
 * Stream a FHFA PUDB Single-Family Census Tract File CSV into agency_lld
 * via COPY FROM STDIN.
 *
 * Source: https://www.fhfa.gov/data/pudb  (free, no registration)
 *
 * Usage:
 *   node scripts/load-agency-lld.mjs data/agency-lld/2024_PUDB_SF_CTF/2024_pudb_sf_ctf_fnma.csv 2024 fannie
 *   node scripts/load-agency-lld.mjs data/agency-lld/2024_PUDB_SF_CTF/2024_pudb_sf_ctf_fhlmc.csv 2024 freddie
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

// DB columns we insert into
const DB_COLS = [
  "source",
  "vintage_year",
  "vintage_month",
  "loan_id",
  "channel",
  "origination_rate",
  "original_upb",
  "original_loan_term",
  "original_ltv",
  "number_of_borrowers",
  "dti",
  "credit_score",
  "first_time_buyer",
  "loan_purpose",
  "property_type",
  "number_of_units",
  "occupancy_status",
  "property_state",
  "msa",
];

// FIPS state code (2-digit string) -> USPS 2-letter
const FIPS_TO_STATE = {
  "01": "AL", "02": "AK", "04": "AZ", "05": "AR", "06": "CA", "08": "CO",
  "09": "CT", "10": "DE", "11": "DC", "12": "FL", "13": "GA", "15": "HI",
  "16": "ID", "17": "IL", "18": "IN", "19": "IA", "20": "KS", "21": "KY",
  "22": "LA", "23": "ME", "24": "MD", "25": "MA", "26": "MI", "27": "MN",
  "28": "MS", "29": "MO", "30": "MT", "31": "NE", "32": "NV", "33": "NH",
  "34": "NJ", "35": "NM", "36": "NY", "37": "NC", "38": "ND", "39": "OH",
  "40": "OK", "41": "OR", "42": "PA", "44": "RI", "45": "SC", "46": "SD",
  "47": "TN", "48": "TX", "49": "UT", "50": "VT", "51": "VA", "53": "WA",
  "54": "WV", "55": "WI", "56": "WY", "60": "AS", "66": "GU", "69": "MP",
  "72": "PR", "78": "VI",
};

// FHFA sentinel codes for "missing" in numeric fields.
// Per PUDB data dictionary:
//   rate_orig / ltv / upb_orig / term_orig use 9, 99, 999, 999999 as missing depending on column.
// We treat any row where the numeric parse fails or matches a sentinel as NULL.
function cleanNum(raw, sentinels = []) {
  if (raw == null) return "\\N";
  const s = String(raw).trim();
  if (s === "" || s === "NA" || s === "." || s === "-") return "\\N";
  if (sentinels.includes(s)) return "\\N";
  const n = Number(s);
  if (!Number.isFinite(n)) return "\\N";
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
  if (args.length !== 3) {
    console.error(
      "usage: load-agency-lld.mjs <csvPath> <vintageYear> <source=fannie|freddie>"
    );
    process.exit(1);
  }
  const [csvPath, yearStr, source] = args;
  const vintageYear = parseInt(yearStr);
  if (source !== "fannie" && source !== "freddie") {
    console.error(`source must be 'fannie' or 'freddie', got ${source}`);
    process.exit(1);
  }

  const size = statSync(csvPath).size;
  console.log(
    `Loading ${csvPath} (${(size / 1024 / 1024).toFixed(1)} MB) year=${vintageYear} source=${source}`
  );

  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const copySQL = `COPY agency_lld (${DB_COLS.join(",")}) FROM STDIN WITH (FORMAT text, DELIMITER E'\\t', NULL '\\N')`;
    const ingestStream = client.query(copyFrom(copySQL));

    let rowCount = 0;
    const startedAt = Date.now();

    const transform = new Transform({
      objectMode: true,
      transform(r, _enc, cb) {
        rowCount++;
        if (rowCount % 100000 === 0) {
          const secs = (Date.now() - startedAt) / 1000;
          console.log(
            `  ${rowCount.toLocaleString()} rows  ${secs.toFixed(0)}s  ${Math.round(rowCount / secs)}/s`
          );
        }

        // FHFA zero-pads some, not others. state_fips arrives as "1".."78".
        const fips = r.state_fips ? String(r.state_fips).padStart(2, "0") : null;
        const stateAbbr = fips ? FIPS_TO_STATE[fips] || null : null;

        const cells = [
          source,                                                  // source
          String(vintageYear),                                     // vintage_year
          "\\N",                                                    // vintage_month (annual)
          tsvEscape(r.record_num_sf_ctf),                          // loan_id
          tsvEscape(r.channel_apply),                              // channel
          cleanNum(r.rate_orig, ["99.000", "99", "9"]),            // origination_rate
          cleanNum(r.upb_orig, ["999999", "9999999"]),             // original_upb
          cleanNum(r.term_orig, ["999", "9999"]),                  // original_loan_term
          cleanNum(r.ltv, ["999", "9999"]),                        // original_ltv
          cleanNum(r.borr_num, ["9"]),                             // number_of_borrowers
          cleanNum(r.dti_cat, ["99"]),                             // dti (bucket code)
          cleanNum(r.score_borr_model, ["9"]),                     // credit_score (bucket code)
          tsvEscape(r.fthb),                                       // first_time_buyer
          tsvEscape(r.purpose_ctf),                                // loan_purpose
          tsvEscape(r.property_type),                              // property_type
          cleanNum(r.units_num, ["9"]),                            // number_of_units
          tsvEscape(r.occupancy_sf_ctf),                           // occupancy_status
          stateAbbr ? stateAbbr : "\\N",                           // property_state
          tsvEscape(r.cbsa_metro_code),                            // msa
        ];
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
      `\nLoaded ${rowCount.toLocaleString()} rows from ${source} ${vintageYear} in ${secs.toFixed(0)}s`
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
