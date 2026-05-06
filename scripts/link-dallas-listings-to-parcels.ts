#!/usr/bin/env tsx
import "dotenv/config";

const PG_URL = `${(process.env.SUPABASE_URL ?? "").replace(/\/$/, "")}/pg/query`;
const PG_KEY = process.env.SUPABASE_SERVICE_KEY ?? "";
const DRY_RUN = process.argv.includes("--dry-run");
const LIMIT = Math.max(1, Number(process.argv.find(a => a.startsWith("--limit="))?.split("=")[1] ?? "5000"));

type Row = Record<string, any>;

async function pg<T extends Row = Row>(query: string): Promise<T[]> {
  const response = await fetch(PG_URL, {
    method: "POST",
    headers: { apikey: PG_KEY, Authorization: `Bearer ${PG_KEY}`, "Content-Type": "application/json" },
    body: JSON.stringify({ query }),
    signal: AbortSignal.timeout(120_000),
  });
  if (!response.ok) throw new Error(`pg/query ${response.status}: ${await response.text()}`);
  return response.json() as Promise<T[]>;
}

function sql(value: unknown): string {
  if (value == null || value === "") return "null";
  return `'${String(value).replace(/'/g, "''")}'`;
}

function normalize(value: string): string {
  return value
    .toUpperCase()
    .replace(/&/g, " AND ")
    .replace(/[.,]/g, " ")
    .replace(/#/g, " UNIT ")
    .replace(/\b(APARTMENT|APT|UNIT|STE|SUITE|BLDG|BUILDING)\b/g, " UNIT ")
    .replace(/\bHLS\b/g, "HILLS")
    .replace(/\bSTREET\b/g, "ST")
    .replace(/\bAVENUE\b/g, "AVE")
    .replace(/\bBOULEVARD\b/g, "BLVD")
    .replace(/\bDRIVE\b/g, "DR")
    .replace(/\bPLAZA\b/g, "PLZ")
    .replace(/\bSPRINGS\b/g, "SPGS")
    .replace(/\bLANE\b/g, "LN")
    .replace(/\bROAD\b/g, "RD")
    .replace(/\bCOURT\b/g, "CT")
    .replace(/\bCIRCLE\b/g, "CIR")
    .replace(/\bPLACE\b/g, "PL")
    .replace(/\bNORTH\b/g, "N")
    .replace(/\bSOUTH\b/g, "S")
    .replace(/\bEAST\b/g, "E")
    .replace(/\bWEST\b/g, "W")
    .replace(/\s+/g, " ")
    .trim();
}

function keys(address: string): string[] {
  const full = normalize(address);
  const unitMatch = full.match(/\s+UNIT\s+([A-Z0-9-]+)\s*$/);
  const unit = unitMatch?.[1];
  const base = full.replace(/\s+UNIT\s+\S+.*$/, "").trim();
  const withoutUnit = base.replace(/\s+\d+[A-Z]?$/, "").trim();
  const lotNormalized = full.replace(/\s+LOT\s+(\S+)/, " UNIT $1");
  const unitVariants: string[] = [];
  if (unit) {
    unitVariants.push(`${base} ${unit}`);
    if (/^\d+$/.test(unit)) unitVariants.push(`${base} ${unit.padStart(4, "0")}`);
    unitVariants.push(`${base} UNIT ${unit.replace(/^0+/, "")}`);
  }
  return [...new Set([full, lotNormalized, ...unitVariants, withoutUnit].filter(k => k.length >= 5))];
}

function streetNumber(address: string): string | null {
  return normalize(address).match(/^(\d+[A-Z]?)/)?.[1] ?? null;
}

async function main() {
  console.log("MXRE - Dallas listing to parcel linker");
  console.log(JSON.stringify({ dry_run: DRY_RUN, limit: LIMIT }, null, 2));

  const listings = await pg(`
    select id, address, zip
      from listing_signals
     where is_on_market = true
       and state_code = 'TX'
       and upper(coalesce(city,'')) = 'DALLAS'
       and property_id is null
       and nullif(address,'') is not null
     order by last_seen_at desc nulls last
     limit ${LIMIT};
  `);

  const zips = [...new Set(listings.map(row => String(row.zip ?? "").match(/\d{5}/)?.[0]).filter(Boolean))];
  const streetNumbers = [...new Set(listings.map(row => streetNumber(String(row.address ?? ""))).filter(Boolean))];
  const properties = await pg(`
    select id, address, zip
      from properties
     where county_id = 7
       and state_code = 'TX'
       and zip in (${zips.map(sql).join(",") || "null"})
       and split_part(upper(address), ' ', 1) in (${streetNumbers.map(sql).join(",") || "null"})
       and nullif(address,'') is not null;
  `);

  const byZipKey = new Map<string, number[]>();
  for (const property of properties) {
    const zip = String(property.zip ?? "");
    for (const key of keys(String(property.address ?? ""))) {
      const mapKey = `${zip}|${key}`;
      const ids = byZipKey.get(mapKey) ?? [];
      ids.push(Number(property.id));
      byZipKey.set(mapKey, ids);
    }
  }

  const matches: Array<{ listingId: number; propertyId: number }> = [];
  const ambiguous: Row[] = [];
  for (const listing of listings) {
    const zip = String(listing.zip ?? "");
    const candidates = new Set<number>();
    for (const key of keys(String(listing.address ?? ""))) {
      for (const id of byZipKey.get(`${zip}|${key}`) ?? []) candidates.add(id);
    }
    if (candidates.size === 1) {
      matches.push({ listingId: Number(listing.id), propertyId: [...candidates][0] });
    } else if (candidates.size > 1) {
      ambiguous.push({ listing_id: listing.id, address: listing.address, zip, candidates: [...candidates].slice(0, 8) });
    }
  }

  if (!DRY_RUN && matches.length > 0) {
    for (let i = 0; i < matches.length; i += 100) {
      const updates = matches.slice(i, i + 100)
        .map(match => `update listing_signals set property_id = ${match.propertyId}, updated_at = now() where id = ${match.listingId} and property_id is null;`)
        .join("\n");
      await pg(updates);
    }
  }

  console.log(JSON.stringify({
    scanned: listings.length,
    properties_loaded: properties.length,
    matched: matches.length,
    ambiguous: ambiguous.length,
    dry_run: DRY_RUN,
    ambiguous_samples: ambiguous.slice(0, 10),
  }, null, 2));
}

main().catch(error => {
  console.error("Fatal:", error instanceof Error ? error.message : error);
  process.exit(1);
});
