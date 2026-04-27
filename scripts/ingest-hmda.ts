#!/usr/bin/env tsx
/**
 * MXRE — CFPB HMDA originations ingest
 *
 * Downloads Home Mortgage Disclosure Act (HMDA) loan origination data from the
 * CFPB Data Browser API. Free, no auth required. Covers all mortgage originations
 * nationwide since 2018.
 *
 * IMPORTANT LIMITATION: HMDA public data does NOT include property street address
 * (redacted for privacy). Records link to census tract, not individual parcels.
 * This data powers census-tract-level analytics (lender market share, loan volume
 * by neighbourhood, FHA/VA/conventional mix, investment property %) rather than
 * individual property lien history.
 *
 * Creates hmda_originations table on first run (via pg/query).
 *
 * Usage:
 *   npx tsx scripts/ingest-hmda.ts                        # Indiana, 2018–present
 *   npx tsx scripts/ingest-hmda.ts --states=IN,OH,IL      # multiple states
 *   npx tsx scripts/ingest-hmda.ts --year=2023            # single year
 *   npx tsx scripts/ingest-hmda.ts --from-year=2018 --to-year=2024
 *   npx tsx scripts/ingest-hmda.ts --dry-run
 */

import "dotenv/config";

// ─── CLI args ──────────────────────────────────────────────────────────────

const argv    = process.argv.slice(2);
const getArg  = (n: string) => argv.find(a => a.startsWith(`--${n}=`))?.split("=")[1];
const hasFlag = (n: string) => argv.includes(`--${n}`);

const STATES_ARG = getArg("states") ?? "IN";
const STATES     = STATES_ARG.split(",").map(s => s.trim().toUpperCase()).filter(Boolean);
const YEAR_ARG   = getArg("year");
const FROM_YEAR  = YEAR_ARG ? parseInt(YEAR_ARG) : parseInt(getArg("from-year") ?? "2018");
const TO_YEAR    = YEAR_ARG ? parseInt(YEAR_ARG) : parseInt(getArg("to-year") ?? String(new Date().getFullYear() - 1));
const DRY_RUN    = hasFlag("dry-run");

// ─── DB ────────────────────────────────────────────────────────────────────

const PG_URL = (process.env.SUPABASE_URL ?? "").replace(/\/$/, "") + "/pg/query";
const PG_KEY = process.env.SUPABASE_SERVICE_KEY ?? "";

async function pgQuery(query: string): Promise<void> {
  const res = await fetch(PG_URL, {
    method: "POST",
    headers: { apikey: PG_KEY, Authorization: `Bearer ${PG_KEY}`, "Content-Type": "application/json" },
    body: JSON.stringify({ query }),
  });
  if (!res.ok) throw new Error(`pg ${res.status}: ${await res.text()}`);
}


// ─── Schema ────────────────────────────────────────────────────────────────

async function ensureTable() {
  await pgQuery(`
    CREATE TABLE IF NOT EXISTS hmda_originations (
      id              BIGSERIAL PRIMARY KEY,
      year            SMALLINT NOT NULL,
      state_code      CHAR(2)  NOT NULL,
      county_fips     CHAR(5),
      census_tract    VARCHAR(20),
      lei             VARCHAR(25),
      loan_type       SMALLINT,
      loan_purpose    SMALLINT,
      lien_status     SMALLINT,
      occupancy_type  SMALLINT,
      loan_amount     INTEGER,
      property_value  INTEGER,
      loan_term       SMALLINT,
      interest_rate   NUMERIC(6,3),
      action_taken    SMALLINT,
      created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS hmda_orig_state_year ON hmda_originations (state_code, year);
    CREATE INDEX IF NOT EXISTS hmda_orig_tract ON hmda_originations (census_tract) WHERE census_tract IS NOT NULL;
    CREATE UNIQUE INDEX IF NOT EXISTS hmda_orig_dedup ON hmda_originations (year, state_code, census_tract, lei, loan_amount, loan_purpose, lien_status);
  `);
}

// ─── CFPB API ──────────────────────────────────────────────────────────────

const CFPB_API = "https://ffiec.cfpb.gov/v2/data-browser-api/view/csv";

/**
 * Stream one year of HMDA originations for a state, yielding rows in batches.
 * Avoids loading the full CSV into memory (CA can be 200MB+).
 */
async function* streamHmdaYear(state: string, year: number): AsyncGenerator<Record<string, string>[]> {
  const url = `${CFPB_API}?states=${state}&years=${year}&actions_taken=1`;
  console.log(`  Fetching ${state} ${year}... ${url}`);

  const res = await fetch(url, {
    headers: { "User-Agent": "MXRE-Ingest/1.0" },
    signal: AbortSignal.timeout(300_000),
  });

  if (!res.ok) {
    if (res.status === 400 || res.status === 404) {
      console.log(`  No data for ${state} ${year} (HTTP ${res.status})`);
      return;
    }
    throw new Error(`CFPB API ${res.status} for ${state} ${year}`);
  }

  const reader = res.body!.getReader();
  const decoder = new TextDecoder("utf-8");
  let remainder = "";
  let header: string[] | null = null;
  let batch: Record<string, string>[] = [];
  let total = 0;
  const STREAM_BATCH = 500;

  while (true) {
    const { done, value } = await reader.read();
    const chunk = done ? "" : decoder.decode(value, { stream: true });
    const text = remainder + chunk;
    const lines = text.split("\n");
    remainder = done ? "" : (lines.pop() ?? "");

    for (const line of lines) {
      const trimmed = line.trim();
      if (!trimmed) continue;
      if (!header) {
        header = trimmed.split(",").map(h => h.trim().replace(/^"|"$/g, ""));
        continue;
      }
      const cols = trimmed.split(",");
      const row: Record<string, string> = {};
      for (let j = 0; j < header.length; j++) {
        row[header[j]] = (cols[j] ?? "").replace(/^"|"$/g, "").trim();
      }
      batch.push(row);
      if (batch.length >= STREAM_BATCH) {
        yield batch;
        total += batch.length;
        batch = [];
      }
    }

    if (done) break;
  }

  if (batch.length > 0) {
    yield batch;
    total += batch.length;
  }
  console.log(`  → ${total.toLocaleString()} originations`);
}

/** Map a raw HMDA row to our DB record shape */
function mapRow(row: Record<string, string>, state: string, year: number): Record<string, unknown> | null {
  const tract = row["census_tract"] || row["Census Tract"] || "";
  const lei   = row["lei"] || row["LEI"] || "";

  // loan_amount_ is the rounded midpoint field (in thousands); convert to dollars
  const loanAmt = parseFloat(row["loan_amount_"] || row["loan_amount"] || "0");
  const propVal = parseFloat(row["property_value_"] || row["property_value"] || "0");
  const term    = parseInt(row["loan_term"] || "0", 10);
  const rate    = parseFloat(row["interest_rate"] || "0");

  // county: HMDA uses 5-digit FIPS "SSCCC" — state 2-digit + county 3-digit
  const countyFips = row["county_code"] || row["County Code"] || null;

  return {
    year,
    state_code:     state,
    county_fips:    countyFips || null,
    census_tract:   tract || null,
    lei:            lei || null,
    loan_type:      parseInt(row["loan_type"] || "0", 10) || null,
    loan_purpose:   parseInt(row["loan_purpose"] || "0", 10) || null,
    lien_status:    parseInt(row["lien_status"] || "0", 10) || null,
    occupancy_type: parseInt(row["occupancy_type"] || "0", 10) || null,
    // HMDA sentinel values (Exempt=9999999, NA=1111111, etc) are > 100,000 in thousands
    // Cap to avoid INTEGER overflow and drop sentinel values
    loan_amount:    loanAmt > 0 && loanAmt < 100_000 ? Math.round(loanAmt * 1000) : null,
    property_value: propVal > 0 && propVal < 100_000 ? Math.round(propVal * 1000) : null,
    loan_term:      term > 0 && term < 1200 ? term : null,
    interest_rate:  rate > 0 && rate < 100 ? rate : null,
    action_taken:   1,
  };
}

// ─── Main ──────────────────────────────────────────────────────────────────

async function main() {
  console.log("\nMXRE — CFPB HMDA originations ingest");
  console.log("═".repeat(60));
  console.log(`States   : ${STATES.join(", ")}`);
  console.log(`Years    : ${FROM_YEAR}–${TO_YEAR}`);
  console.log(`Dry run  : ${DRY_RUN}`);
  console.log();

  if (!DRY_RUN) {
    console.log("Ensuring hmda_originations table exists...");
    await ensureTable();
  }

  let totalInserted = 0, totalSkipped = 0;

  for (const state of STATES) {
    for (let year = FROM_YEAR; year <= TO_YEAR; year++) {
      console.log(`\n── ${state} ${year} ──`);
      const esc = (v: unknown) => v == null ? "NULL" : typeof v === "number" ? String(v) : `'${String(v).replace(/'/g, "''")}'`;
      const COLS = "year,state_code,county_fips,census_tract,lei,loan_type,loan_purpose,lien_status,occupancy_type,loan_amount,property_value,loan_term,interest_rate,action_taken";
      const KEYS = COLS.split(",") as (keyof Record<string, unknown>)[];
      const BATCH = 200;

      let yearCount = 0;
      try {
        for await (const rawBatch of streamHmdaYear(state, year)) {
          const records = rawBatch.map(r => mapRow(r, state, year)).filter(Boolean) as Record<string, unknown>[];
          if (DRY_RUN) { totalInserted += records.length; yearCount += records.length; continue; }

          for (let i = 0; i < records.length; i += BATCH) {
            const chunk = records.slice(i, i + BATCH);
            const vals = chunk.map(r => `(${KEYS.map(k => esc(r[k])).join(",")})`).join(",");
            try {
              await pgQuery(`INSERT INTO hmda_originations (${COLS}) VALUES ${vals} ON CONFLICT DO NOTHING`);
              totalInserted += chunk.length;
              yearCount += chunk.length;
            } catch (e: any) {
              console.error(`  Batch insert error: ${(e as Error).message.slice(0, 120)}`);
              totalSkipped += chunk.length;
            }
          }
          process.stdout.write(`\r  Inserted ${totalInserted.toLocaleString()}   `);
        }
      } catch (err: any) {
        console.error(`  Error: ${err.message}`);
        continue;
      }
      if (DRY_RUN) console.log(`  [dry-run] would insert ${yearCount.toLocaleString()} records`);
      else console.log(`\n  Done: ${yearCount.toLocaleString()} records processed`);
    }
  }

  console.log(`\n${"═".repeat(60)}`);
  console.log(`TOTAL inserted: ${totalInserted.toLocaleString()} | dupes skipped: ${totalSkipped.toLocaleString()}`);
  console.log("Done.\n");
}

main().catch(err => { console.error("Fatal:", err); process.exit(1); });
