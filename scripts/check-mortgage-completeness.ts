import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

async function main() {
  const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
    auth: { persistSession: false },
  });

  const fields = [
    "loan_amount",
    "original_amount",
    "interest_rate",
    "term_months",
    "lender_name",
    "borrower_name",
    "maturity_date",
    "loan_type",
    "estimated_current_balance",
    "estimated_monthly_payment",
    "document_number",
    "recording_date",
  ];

  // Total
  const { count: total } = await db.from("mortgage_records").select("*", { count: "exact", head: true });
  console.log(`\nTotal mortgage_records: ${total?.toLocaleString()}`);
  console.log("\nField completeness:");
  console.log("─".repeat(60));

  for (const f of fields) {
    const { count } = await db
      .from("mortgage_records")
      .select("*", { count: "exact", head: true })
      .not(f, "is", null);
    const pct = total ? ((count! / total) * 100).toFixed(1) : "?";
    const bar = "█".repeat(Math.floor((count || 0) / total! * 30));
    console.log(`  ${f.padEnd(30)} ${count?.toString().padStart(10).toLocaleString()}  ${pct.padStart(5)}%  ${bar}`);
  }

  // Bonus: find one record with full data to see what good looks like
  console.log("\n─── Sample of a 'fully populated' record ───");
  const { data: best } = await db
    .from("mortgage_records")
    .select("*")
    .not("interest_rate", "is", null)
    .not("lender_name", "is", null)
    .not("term_months", "is", null)
    .limit(1);
  if (best && best.length > 0) {
    const r = best[0];
    for (const [k, v] of Object.entries(r)) {
      if (v !== null && v !== "" && k !== "raw") console.log(`  ${k.padEnd(30)} ${String(v).slice(0, 60)}`);
    }
  } else {
    console.log("  ❌ No records have all 3 of (interest_rate, lender_name, term_months)");
  }
}
main();
