import { createClient, type SupabaseClient } from "@supabase/supabase-js";

let readClient: SupabaseClient | null = null;
let writeClient: SupabaseClient | null = null;

/**
 * Read-only client using anon key (restricted by RLS policies).
 * Use this for MCP server queries.
 */
export function getDb(): SupabaseClient {
  if (readClient) return readClient;

  const url = process.env.SUPABASE_URL;
  const key = process.env.SUPABASE_SERVICE_KEY ?? process.env.SUPABASE_ANON_KEY;

  if (!url || !key) {
    throw new Error("Database connection not configured.");
  }

  readClient = createClient(url, key, {
    auth: { persistSession: false },
  });

  return readClient;
}

/**
 * Write client using service role key (bypasses RLS).
 * Use ONLY for seed scripts and scraper ingestion.
 */
export function getWriteDb(): SupabaseClient {
  if (writeClient) return writeClient;

  const url = process.env.SUPABASE_URL;
  const key = process.env.SUPABASE_SERVICE_KEY;

  if (!url || !key) {
    throw new Error("Write database connection not configured.");
  }

  writeClient = createClient(url, key, {
    auth: { persistSession: false },
  });

  return writeClient;
}
