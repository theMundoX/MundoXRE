import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

async function main() {
  const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
    auth: { persistSession: false },
  });
  const { data, error } = await db.from("mortgage_records").select("*").limit(1);
  if (error) {
    console.error(error);
    process.exit(1);
  }
  console.log("Columns:", Object.keys(data?.[0] || {}).sort().join(", "));
  try {
    const { count: withPath } = await db
      .from("mortgage_records")
      .select("*", { count: "exact", head: true })
      .not("document_path", "is", null);
    console.log("\nRecords with document_path populated:", withPath);
    const { data: sample } = await db
      .from("mortgage_records")
      .select("id, document_path, document_number, source_url, lender_name, loan_amount")
      .not("document_path", "is", null)
      .limit(5);
    console.log("\nSample:", JSON.stringify(sample, null, 2));
  } catch (e) {
    console.log("\ndocument_path query failed (column may not exist):", e instanceof Error ? e.message : e);
  }
}
main();
