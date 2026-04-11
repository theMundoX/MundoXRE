import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

async function main() {
  const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
    auth: { persistSession: false },
  });

  console.log("=== INTEREST RATE REALISM CHECK ===\n");

  // Pull 200 sample records with interest_rate populated
  const { data, error } = await db
    .from("mortgage_records")
    .select("id, recording_date, original_amount, interest_rate, term_months, lender_name, source_url")
    .not("interest_rate", "is", null)
    .limit(200);
  if (error) {
    console.error(error);
    process.exit(1);
  }

  if (!data || data.length === 0) {
    console.log("No records found.");
    return;
  }

  // Distribution analysis
  const rates = data.map((r) => Number(r.interest_rate));
  const distinctRates = new Set(rates);
  const min = Math.min(...rates);
  const max = Math.max(...rates);
  const avg = rates.reduce((a, b) => a + b, 0) / rates.length;

  console.log(`Sample size:    ${rates.length}`);
  console.log(`Distinct rates: ${distinctRates.size}`);
  console.log(`Min rate:       ${min}`);
  console.log(`Max rate:       ${max}`);
  console.log(`Avg rate:       ${avg.toFixed(3)}`);
  console.log();

  // Round-number test: are rates "clean" market rates (4.5, 6.0, 6.875) or
  // suspiciously precise (6.723, 5.831)?
  let looksRound = 0;
  let looksPrecise = 0;
  for (const r of rates) {
    // A "real" lender rate is usually a multiple of 0.125 (eighth of a percent)
    // or 0.25 (quarter point). E.g. 6.875, 7.0, 6.75, 6.5
    const cents = Math.round(r * 1000);
    if (cents % 125 === 0 || cents % 250 === 0) looksRound++;
    else looksPrecise++;
  }

  console.log("Round-number test:");
  console.log(`  Multiple of 0.125 (real lender rate): ${looksRound} (${((looksRound / rates.length) * 100).toFixed(0)}%)`);
  console.log(`  NOT a multiple (probably estimated):  ${looksPrecise} (${((looksPrecise / rates.length) * 100).toFixed(0)}%)`);
  console.log();

  // Group rates by recording year and see if they cluster around Freddie Mac avg
  const byYear: Record<string, number[]> = {};
  for (const r of data) {
    if (!r.recording_date) continue;
    const year = r.recording_date.slice(0, 4);
    if (!byYear[year]) byYear[year] = [];
    byYear[year].push(Number(r.interest_rate));
  }
  console.log("Avg rate by recording year:");
  for (const year of Object.keys(byYear).sort()) {
    const vals = byYear[year];
    const yAvg = vals.reduce((a, b) => a + b, 0) / vals.length;
    const yMin = Math.min(...vals);
    const yMax = Math.max(...vals);
    console.log(`  ${year}: avg ${yAvg.toFixed(3)}, range ${yMin}-${yMax}, n=${vals.length}`);
  }
  console.log();

  // Sample 10
  console.log("First 10 records (eyeball test):");
  for (const r of data.slice(0, 10)) {
    console.log(
      `  rate=${String(r.interest_rate).padStart(6)} term=${r.term_months} amt=$${(r.original_amount || 0).toLocaleString().padStart(10)} ${r.recording_date} ${r.lender_name?.slice(0, 30) || ""}`,
    );
  }

  console.log("\n=== VERDICT ===");
  if (looksPrecise / rates.length > 0.5) {
    console.log("🔴 LIKELY ESTIMATED. >50% of rates are NOT clean lender rates.");
    console.log("   Per your feedback rule: these 464K rows should be flagged or purged.");
  } else if (looksRound / rates.length > 0.5) {
    console.log("✅ LIKELY REAL. Most rates are clean multiples of 1/8% — typical lender rate format.");
    console.log("   These probably came from real document extraction.");
  } else {
    console.log("⚠️  MIXED. Some real, some not. Need a closer look.");
  }
}
main();
