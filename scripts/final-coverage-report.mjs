import pkg from "pg";
import { writeFileSync } from "fs";
const { Pool } = pkg;
const pool = new Pool({ host: (process.env.MXRE_PG_HOST ?? ""), port: 5432, database: "postgres", user: "postgres.your-tenant-id", password: "${process.env.MXRE_PG_PASSWORD}", max: 1 });
const c = await pool.connect();

const report = { generated_at: new Date().toISOString() };

console.log("=== FINAL RATE COVERAGE REPORT ===\n");

// 1. Headline
const headline = await c.query(`
  SELECT
    COUNT(*) AS total,
    COUNT(*) FILTER (WHERE loan_amount IS NOT NULL) AS has_amount,
    COUNT(*) FILTER (WHERE rate_source = 'pmms_weekly') AS pmms,
    COUNT(*) FILTER (WHERE rate_source = 'hmda_match') AS hmda,
    COUNT(*) FILTER (WHERE rate_source = 'agency_match') AS agency,
    COUNT(*) FILTER (WHERE interest_rate IS NULL) AS no_rate
  FROM mortgage_records`);
report.headline = headline.rows[0];
console.log("1. HEADLINE:");
console.log(`   Total records:       ${headline.rows[0].total.toLocaleString()}`);
console.log(`   With loan_amount:    ${headline.rows[0].has_amount.toLocaleString()}`);
console.log(`   PMMS rate:           ${headline.rows[0].pmms.toLocaleString()}`);
console.log(`   HMDA rate (high):    ${headline.rows[0].hmda.toLocaleString()}`);
console.log(`   Agency rate:         ${headline.rows[0].agency.toLocaleString()}`);
console.log(`   No rate:             ${headline.rows[0].no_rate.toLocaleString()}`);

// 2. Eligible universe analysis
const eligible = await c.query(`
  SELECT COUNT(*) AS eligible,
         COUNT(*) FILTER (WHERE rate_source = 'hmda_match') AS matched
    FROM mortgage_records
   WHERE loan_amount IS NOT NULL
     AND county_fips IS NOT NULL
     AND recording_date IS NOT NULL
     AND EXTRACT(YEAR FROM recording_date) BETWEEN 2018 AND 2023`);
report.eligible_universe = eligible.rows[0];
const eligPct = ((eligible.rows[0].matched / eligible.rows[0].eligible) * 100).toFixed(1);
console.log(`\n2. ELIGIBLE UNIVERSE (loan_amount + county_fips + year 2018-2023):`);
console.log(`   Eligible:            ${eligible.rows[0].eligible.toLocaleString()}`);
console.log(`   HMDA matched:        ${eligible.rows[0].matched.toLocaleString()}  (${eligPct}%)`);

// 3. By year
console.log(`\n3. MATCH RATE BY YEAR (mortgages with loan_amount):`);
const byYear = await c.query(`
  SELECT EXTRACT(YEAR FROM recording_date)::int AS yr,
         COUNT(*) FILTER (WHERE loan_amount IS NOT NULL) AS mortgages,
         COUNT(*) FILTER (WHERE rate_source = 'hmda_match') AS matched
    FROM mortgage_records
   WHERE recording_date IS NOT NULL
     AND EXTRACT(YEAR FROM recording_date) BETWEEN 2015 AND 2026
   GROUP BY yr
   ORDER BY yr DESC`);
report.by_year = byYear.rows;
for (const r of byYear.rows) {
  const pct = r.mortgages > 0 ? ((r.matched / r.mortgages) * 100).toFixed(1) : "n/a";
  console.log(`   ${r.yr}:  mortgages=${String(r.mortgages).padStart(8)}  matched=${String(r.matched).padStart(8)}  ${pct}%`);
}

// 4. rate_source breakdown
const sources = await c.query(`
  SELECT rate_source, COUNT(*), AVG(rate_match_confidence)::int AS avg_conf
    FROM mortgage_records
   WHERE rate_source IS NOT NULL
   GROUP BY rate_source
   ORDER BY COUNT(*) DESC`);
report.by_source = sources.rows;
console.log(`\n4. RATE SOURCE BREAKDOWN:`);
for (const r of sources.rows) {
  console.log(`   ${(r.rate_source||"(null)").padEnd(15)}  ${String(r.count).padStart(10)}  avg_conf=${r.avg_conf || "—"}`);
}

// 5. Coverage on records that ACTUALLY have a loan
console.log(`\n5. COVERAGE ON REAL MORTGAGES (loan_amount IS NOT NULL):`);
const realCoverage = await c.query(`
  SELECT
    COUNT(*) AS total,
    COUNT(*) FILTER (WHERE rate_source = 'hmda_match') AS hmda,
    COUNT(*) FILTER (WHERE rate_source = 'pmms_weekly') AS pmms,
    COUNT(*) FILTER (WHERE interest_rate IS NULL) AS no_rate
  FROM mortgage_records
  WHERE loan_amount IS NOT NULL`);
report.real_mortgage_coverage = realCoverage.rows[0];
const tot = Number(realCoverage.rows[0].total);
console.log(`   Total real mortgages: ${tot.toLocaleString()}`);
console.log(`     HMDA-matched:       ${realCoverage.rows[0].hmda.toLocaleString()}  (${((realCoverage.rows[0].hmda/tot)*100).toFixed(1)}%)`);
console.log(`     PMMS-fallback:      ${realCoverage.rows[0].pmms.toLocaleString()}  (${((realCoverage.rows[0].pmms/tot)*100).toFixed(1)}%)`);
console.log(`     No rate:            ${realCoverage.rows[0].no_rate.toLocaleString()}  (${((realCoverage.rows[0].no_rate/tot)*100).toFixed(1)}%)`);

writeFileSync("C:/Users/msanc/mxre/data/rate-coverage-final.json", JSON.stringify(report, null, 2));
console.log(`\nReport written to data/rate-coverage-final.json`);

c.release();
await pool.end();
