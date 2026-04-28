import "dotenv/config";
import { parse } from "csv-parse/sync";
import fs from "node:fs";
import { getWriteDb } from "../src/db/client.js";

type CsvRow = {
  property_id?: string;
  address?: string;
  city?: string;
  state?: string;
  zip?: string;
  complex_name?: string;
  management_company?: string;
  website?: string;
  phone?: string;
  email?: string;
  source?: string;
  source_url?: string;
  unit_count?: string;
  year_built?: string;
  amenities?: string;
  description?: string;
  confidence?: string;
};

const file = process.argv[2];
if (!file) {
  console.error("Usage: npx tsx scripts/upsert-complex-profiles.ts path/to/complex-profiles.csv");
  process.exit(1);
}

const db = getWriteDb();
const csv = fs.readFileSync(file, "utf8");
const rows = parse(csv, { columns: true, skip_empty_lines: true, trim: true }) as CsvRow[];

function intOrNull(value: string | undefined): number | null {
  if (!value) return null;
  const parsed = parseInt(value, 10);
  return Number.isFinite(parsed) ? parsed : null;
}

function amenities(value: string | undefined): string[] {
  if (!value) return [];
  return value.split("|").map((item) => item.trim()).filter(Boolean);
}

async function resolvePropertyId(row: CsvRow): Promise<number | null> {
  const explicit = intOrNull(row.property_id);
  if (explicit) return explicit;
  if (!row.address || !row.state) return null;

  let query = db.from("properties")
    .select("id")
    .eq("state_code", row.state.toUpperCase())
    .like("address", `${row.address.toUpperCase()}%`)
    .limit(1);

  if (row.city) query = query.eq("city", row.city.toUpperCase());
  if (row.zip) query = query.eq("zip", row.zip);

  const { data, error } = await query;
  if (error) throw error;
  return data?.[0]?.id ?? null;
}

let upserted = 0;
let skipped = 0;

for (const row of rows) {
  const propertyId = await resolvePropertyId(row);
  if (!propertyId) {
    skipped++;
    console.warn(`Skipped unresolved profile: ${row.complex_name || row.address || "(missing name/address)"}`);
    continue;
  }

  const payload = {
    property_id: propertyId,
    complex_name: row.complex_name || null,
    management_company: row.management_company || null,
    website: row.website || null,
    phone: row.phone || null,
    email: row.email || null,
    source: row.source || "manual_research",
    source_url: row.source_url || null,
    unit_count: intOrNull(row.unit_count),
    year_built: intOrNull(row.year_built),
    amenities: amenities(row.amenities),
    description: row.description || null,
    confidence: row.confidence || "medium",
    last_seen_at: new Date().toISOString(),
    raw: row,
    updated_at: new Date().toISOString(),
  };

  const { error } = await db.from("property_complex_profiles").upsert(payload, { onConflict: "property_id" });
  if (error) throw error;
  upserted++;
}

console.log(JSON.stringify({ upserted, skipped, file }, null, 2));
