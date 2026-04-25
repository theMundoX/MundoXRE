/**
 * Demo: rate lookup for a property by address or parcel.
 *
 * Shows the BBC member experience: query a property, get back the mortgage(s)
 * with rate + source provenance + confidence score.
 *
 * Usage:
 *   node scripts/demo-rate-lookup.mjs <address fragment>
 *
 * Examples:
 *   node scripts/demo-rate-lookup.mjs "13114 OAKMERE"
 *   node scripts/demo-rate-lookup.mjs "1815 TWIN HOUSE"
 *
 * Output: human-readable summary of the property, its mortgages, and
 * how each rate was determined.
 */
import pkg from "pg";
const { Pool } = pkg;
const pool = new Pool({
  host: (process.env.MXRE_PG_HOST ?? ""),
  port: 5432,
  database: "postgres",
  user: "postgres.your-tenant-id",
  password: "${process.env.MXRE_PG_PASSWORD}",
  max: 1,
  statement_timeout: 30000,
});

const SOURCE_LABELS = {
  hmda_match: "HMDA Match (CFPB Public Loan Application Register)",
  agency_match: "Agency Loan-Level Disclosure (Fannie Mae / Freddie Mac)",
  pmms_weekly: "Freddie Mac PMMS (weekly survey average)",
  estimated: "Historical Average",
};

function fmt(n) {
  if (n == null) return "—";
  if (typeof n === "string") n = parseFloat(n);
  return n.toLocaleString();
}

function fmtPct(n) {
  if (n == null) return "—";
  return `${parseFloat(n).toFixed(3)}%`;
}

async function main() {
  const query = process.argv.slice(2).join(" ");
  if (!query) {
    console.error("usage: demo-rate-lookup.mjs <address fragment>");
    process.exit(1);
  }

  const c = await pool.connect();
  try {
    console.log(`\n=== MXRE Property Rate Lookup ===\n`);
    console.log(`Query: "${query}"\n`);

    // 1. Find matching properties
    const props = await c.query(
      `SELECT id, address, city, state_code, zip, county_id, parcel_id, owner_name, year_built, total_sqft, last_sale_price, last_sale_date
         FROM properties
        WHERE address ILIKE $1
        LIMIT 5`,
      [`%${query}%`]
    );

    if (!props.rows.length) {
      console.log("No properties found.");
      return;
    }

    for (const p of props.rows) {
      console.log(`────────────────────────────────────────────────────────`);
      console.log(`PROPERTY #${p.id}`);
      console.log(`  Address:    ${p.address}`);
      console.log(`  City/State: ${p.city}, ${p.state_code} ${p.zip || ""}`);
      console.log(`  Parcel:     ${p.parcel_id || "—"}`);
      console.log(`  Owner:      ${p.owner_name || "—"}`);
      console.log(`  Built:      ${p.year_built || "—"}    Sqft: ${p.total_sqft || "—"}`);
      if (p.last_sale_price) {
        console.log(`  Last sale:  $${fmt(p.last_sale_price)} on ${p.last_sale_date}`);
      }

      // 2. Find mortgages for this property
      const mortgages = await c.query(
        `SELECT id, document_type, recording_date, loan_amount, lender_name,
                interest_rate, term_months, rate_source, rate_match_confidence,
                document_number
           FROM mortgage_records
          WHERE property_id = $1
            AND loan_amount IS NOT NULL
          ORDER BY recording_date DESC
          LIMIT 10`,
        [p.id]
      );

      if (!mortgages.rows.length) {
        console.log(`  Mortgages:  (none linked — see note below)`);
      } else {
        console.log(`  Mortgages:  (${mortgages.rows.length})`);
        for (const m of mortgages.rows) {
          console.log(``);
          console.log(`    ┌── Recorded: ${m.recording_date}  Doc#: ${m.document_number || "—"}`);
          console.log(`    │  Type:     ${m.document_type}`);
          console.log(`    │  Lender:   ${m.lender_name || "—"}`);
          console.log(`    │  Amount:   $${fmt(m.loan_amount)}`);
          console.log(`    │  Rate:     ${fmtPct(m.interest_rate)}`);
          console.log(`    │  Term:     ${m.term_months || "—"} months`);
          console.log(`    │  Source:   ${SOURCE_LABELS[m.rate_source] || m.rate_source || "—"}`);
          if (m.rate_match_confidence != null) {
            console.log(`    │  Confidence: ${m.rate_match_confidence}/100`);
          }
          console.log(`    └─`);
        }
      }
      console.log(``);
    }

    console.log(`────────────────────────────────────────────────────────`);
    console.log(`\nProvenance legend:`);
    console.log(`  hmda_match (conf 70-95):  Cross-matched to CFPB HMDA Loan Application Register`);
    console.log(`  agency_match (conf 60-80): Matched to Fannie/Freddie loan-level disclosure`);
    console.log(`  pmms_weekly (conf 40):    Freddie PMMS weekly survey average for the recording week`);
    console.log(`  estimated (conf 20):       Historical average fallback`);
    console.log(`\nNothing else in the industry shows you the source. ATTOM/CoreLogic/RealEstateAPI sell you a number and pretend it's gospel.`);
  } finally {
    c.release();
    await pool.end();
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
