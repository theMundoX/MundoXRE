#!/usr/bin/env tsx
import "dotenv/config";

const PG_URL = `${(process.env.SUPABASE_URL ?? "").replace(/\/$/, "")}/pg/query`;
const PG_KEY = process.env.SUPABASE_SERVICE_KEY ?? "";
const LIMIT = Math.max(1, parseInt(process.argv.find(a => a.startsWith("--limit="))?.split("=")[1] ?? "500", 10));
const DRY_RUN = process.argv.includes("--dry-run");
const arg = (name: string) =>
  process.argv.find(a => a.startsWith(`--${name}=`))?.split("=").slice(1).join("=");
const STATE = arg("state")?.toUpperCase();
const CITY = arg("city")?.toUpperCase();

type ListingRow = {
  id: number;
  listing_agent_name: string | null;
  listing_agent_first_name: string | null;
  listing_agent_last_name: string | null;
  listing_brokerage: string | null;
  raw: Record<string, unknown> | null;
};

async function pg(query: string): Promise<Record<string, unknown>[]> {
  const response = await fetch(PG_URL, {
    method: "POST",
    headers: {
      apikey: PG_KEY,
      Authorization: `Bearer ${PG_KEY}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ query }),
    signal: AbortSignal.timeout(120_000),
  });
  if (!response.ok) throw new Error(`pg/query ${response.status}: ${await response.text()}`);
  return response.json();
}

function sqlString(value: string | null): string {
  if (value == null || value === "") return "null";
  return `'${value.replace(/'/g, "''")}'`;
}

function walk(value: unknown): unknown[] {
  if (value == null) return [];
  if (typeof value !== "object") return [value];
  if (Array.isArray(value)) return value.flatMap(walk);
  return Object.values(value as Record<string, unknown>).flatMap(walk);
}

function valuesByKey(raw: Record<string, unknown> | null, keyPattern: RegExp): string {
  const out: string[] = [];

  function visit(value: unknown, path = "") {
    if (value == null) return;
    if (typeof value !== "object") {
      if (keyPattern.test(path) && (typeof value === "string" || typeof value === "number")) out.push(String(value));
      return;
    }
    if (Array.isArray(value)) {
      value.forEach((item, index) => visit(item, `${path}[${index}]`));
      return;
    }
    for (const [key, child] of Object.entries(value as Record<string, unknown>)) {
      visit(child, path ? `${path}.${key}` : key);
    }
  }

  visit(raw);
  return out.join(" ");
}

function splitName(name: string | null): { first: string | null; last: string | null } {
  const clean = name?.replace(/\s+/g, " ").trim();
  if (!clean) return { first: null, last: null };
  const parts = clean.split(" ");
  return { first: parts[0] ?? null, last: parts.length > 1 ? parts.slice(1).join(" ") : null };
}

function extractContact(raw: Record<string, unknown> | null) {
  const emailText = valuesByKey(raw, /(^|\.)(email|agentEmail|contactEmail|brokerEmail|mail)$/i);
  const phoneText = valuesByKey(raw, /(^|\.)(phone|number|agentPhone|agentNumber|listingAgentNumber|contactPhone|brokerPhone|brokerNumber|listingBrokerNumber|officePhone|mobile|cell|tel|telephone)$/i);
  const email = emailText.match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i)?.[0]?.toLowerCase() ?? null;
  const phone = phoneText.match(/(?:\+?1[\s.-]?)?(?:\(?\d{3}\)?[\s.-]?)\d{3}[\s.-]?\d{4}/)?.[0] ?? null;
  return { email, phone: phone?.replace(/[^\d+]/g, "").replace(/^1(?=\d{10}$)/, "") ?? null };
}

function extractBrokerage(raw: Record<string, unknown> | null): string | null {
  const text = valuesByKey(raw, /(^|\.)(listingBrokerage|brokerageName|brokerName|broker|officeName)$/i);
  const value = text
    .split(/\s{2,}|\|/)
    .map(part => part.replace(/\s+/g, " ").trim())
    .find(part => part.length >= 3 && !/^(ntreis|mls|mls grid|actris)$/i.test(part));
  return value ?? null;
}

async function main() {
  console.log("MXRE - Listing agent contact enrichment");
  console.log(`Dry run: ${DRY_RUN}`);
  console.log(`Limit: ${LIMIT}`);
  if (STATE || CITY) console.log(`Market filter: ${CITY ?? "all cities"}, ${STATE ?? "all states"}`);

  const filters = [
    STATE ? `state_code = ${sqlString(STATE)}` : null,
    CITY ? `upper(coalesce(city,'')) = ${sqlString(CITY)}` : null,
  ].filter(Boolean).join("\n      and ");

  const rows = await pg(`
    select id, listing_agent_name, listing_agent_first_name, listing_agent_last_name, listing_brokerage, raw
    from listing_signals
    where is_on_market = true
      ${filters ? `and ${filters}` : ""}
      and (
        listing_agent_first_name is null
        or listing_agent_last_name is null
        or listing_agent_email is null
        or listing_agent_phone is null
      )
    order by last_seen_at desc nulls last
    limit ${LIMIT};
  `) as ListingRow[];

  let nameBackfilled = 0;
  let emails = 0;
  let phones = 0;
  const updates: string[] = [];

  for (const row of rows) {
    const name = splitName(row.listing_agent_name);
    const contact = extractContact(row.raw);
    const brokerage = extractBrokerage(row.raw);
    const first = row.listing_agent_first_name ?? name.first;
    const last = row.listing_agent_last_name ?? name.last;
    if ((!row.listing_agent_first_name || !row.listing_agent_last_name) && (first || last)) nameBackfilled++;
    if (contact.email) emails++;
    if (contact.phone) phones++;
    const shouldBackfillBrokerage = brokerage && (!row.listing_brokerage || /^(ntreis|mls|mls grid|actris)$/i.test(row.listing_brokerage));
    if (!first && !last && !contact.email && !contact.phone && !shouldBackfillBrokerage) continue;

    updates.push(`
      update listing_signals
         set listing_agent_first_name = coalesce(listing_agent_first_name, ${sqlString(first)}),
             listing_agent_last_name = coalesce(listing_agent_last_name, ${sqlString(last)}),
             listing_agent_email = coalesce(listing_agent_email, ${sqlString(contact.email)}),
             listing_agent_phone = coalesce(listing_agent_phone, ${sqlString(contact.phone)}),
             listing_brokerage = case
               when ${sqlString(brokerage)} is not null and (listing_brokerage is null or listing_brokerage ~* '^(ntreis|mls|mls grid|actris)$') then ${sqlString(brokerage)}
               else listing_brokerage
             end,
             agent_contact_source = case
               when ${sqlString(contact.email)} is not null or ${sqlString(contact.phone)} is not null then coalesce(agent_contact_source, 'listing_raw_payload')
               else agent_contact_source
             end,
             agent_contact_confidence = case
               when ${sqlString(contact.email)} is not null or ${sqlString(contact.phone)} is not null then coalesce(agent_contact_confidence, 'raw_extracted')
               else agent_contact_confidence
             end,
             updated_at = now()
       where id = ${row.id};
    `);
  }

  if (!DRY_RUN) {
    for (let i = 0; i < updates.length; i += 100) await pg(updates.slice(i, i + 100).join("\n"));
  }

  console.log(JSON.stringify({
    scanned: rows.length,
    name_backfilled: nameBackfilled,
    raw_emails_found: emails,
    raw_phones_found: phones,
    raw_brokerages_found: rows.filter(row => extractBrokerage(row.raw)).length,
    updated: DRY_RUN ? 0 : updates.length,
  }, null, 2));
}

main().catch(error => {
  console.error("Fatal:", error instanceof Error ? error.message : error);
  process.exit(1);
});
