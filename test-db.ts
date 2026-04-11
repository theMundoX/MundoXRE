import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

async function main() {
  const db = createClient(
    process.env.SUPABASE_URL!,
    process.env.SUPABASE_SERVICE_KEY!,
    { auth: { persistSession: false } }
  );

  try {
    const { count } = await db
      .from("properties")
      .select("*", { count: "exact", head: true });
    console.log(`✓ Properties in database: ${count?.toLocaleString() || "0"}`);
  } catch (err: any) {
    console.error(`✗ Error: ${err.message}`);
  }
}

main();
