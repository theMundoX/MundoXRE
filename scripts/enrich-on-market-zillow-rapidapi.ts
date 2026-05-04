#!/usr/bin/env tsx
import "dotenv/config";
import { Client } from "pg";

type Candidate = {
  property_id: number;
  address: string;
  city: string;
  state_code: string;
  zip: string | null;
  mls_list_price: number | null;
  listing_url: string | null;
};

type Contact = {
  name: string | null;
  firstName: string | null;
  lastName: string | null;
  email: string | null;
  phone: string | null;
  brokerage: string | null;
  screenName: string | null;
  sourceEndpoint: string;
};

const args = process.argv.slice(2);
const valueArg = (name: string) => {
  const prefix = `--${name}=`;
  return args.find((arg) => arg.startsWith(prefix))?.slice(prefix.length) ?? null;
};

const dryRun = args.includes("--dry-run");
const force = args.includes("--force");
const city = (valueArg("city") ?? "Indianapolis").toUpperCase();
const state = (valueArg("state") ?? "IN").toUpperCase();
const limit = Math.min(Math.max(Number(valueArg("limit") ?? "10"), 1), 500);
const maxCalls = Math.min(Math.max(Number(valueArg("max-calls") ?? String(limit * 3)), 0), limit * 5);
const rapidApiKey = process.env.RAPIDAPI_KEY ?? process.env.ZILLOW_RAPIDAPI_KEY;
const host = process.env.ZILLOW_RAPIDAPI_HOST ?? "zillow-real-estate-api.p.rapidapi.com";
const provider = process.env.ZILLOW_RAPIDAPI_PROVIDER ?? "jdtpnjtp";
const databaseUrl = process.env.MXRE_DIRECT_PG_URL
  ?? process.env.MXRE_PG_URL
  ?? process.env.DATABASE_URL
  ?? process.env.POSTGRES_URL;

if (!databaseUrl) throw new Error("Set MXRE_DIRECT_PG_URL, MXRE_PG_URL, DATABASE_URL, or POSTGRES_URL.");
if (!dryRun && maxCalls > 0 && !rapidApiKey) {
  throw new Error("Set RAPIDAPI_KEY before making Zillow RapidAPI calls. Use --dry-run to preview.");
}

const client = new Client({ connectionString: databaseUrl });
await client.connect();

const stats = {
  city,
  state,
  provider: `zillow_api_${provider}`,
  host,
  dryRun,
  candidates: 0,
  apiCalls: 0,
  updated: 0,
  foundName: 0,
  foundEmail: 0,
  foundPhone: 0,
  foundBrokerage: 0,
  failed: 0,
};

try {
  const candidates = await loadCandidates();
  stats.candidates = candidates.length;

  for (const candidate of candidates) {
    if (!dryRun && stats.apiCalls >= maxCalls) break;
    try {
      if (dryRun) {
        console.log(JSON.stringify({
          wouldLookup: requestLabel(candidate),
          property_id: candidate.property_id,
          listing_url: candidate.listing_url,
        }));
        continue;
      }

      const contact = await lookupContact(candidate);
      if (!contact) continue;
      if (contact.name) stats.foundName++;
      if (contact.email) stats.foundEmail++;
      if (contact.phone) stats.foundPhone++;
      if (contact.brokerage) stats.foundBrokerage++;

      if (contact.name || contact.email || contact.phone || contact.brokerage) {
        await updateListing(candidate.property_id, contact);
        stats.updated++;
      }
      await sleep(250);
    } catch (error) {
      stats.failed++;
      console.error(`Failed Zillow lookup for ${candidate.property_id}:`, error instanceof Error ? error.message : error);
    }
  }
} finally {
  await client.end();
}

console.log(JSON.stringify(stats, null, 2));

async function loadCandidates(): Promise<Candidate[]> {
  const { rows } = await client.query<Candidate>(`
    select distinct on (p.id)
      p.id as property_id,
      p.address,
      p.city,
      p.state_code,
      p.zip,
      l.mls_list_price,
      l.listing_url
    from listing_signals l
    join properties p on p.id = l.property_id
    where l.is_on_market = true
      and p.state_code = $1
      and upper(coalesce(p.city,'')) = $2
      and (
        $3::boolean = true
        or nullif(l.listing_agent_email,'') is null
        or nullif(l.listing_agent_phone,'') is null
        or nullif(l.listing_agent_name,'') is null
      )
    order by p.id, l.last_seen_at desc nulls last, l.updated_at desc nulls last
    limit $4
  `, [state, city, force, limit]);
  return rows;
}

async function lookupContact(candidate: Candidate): Promise<Contact | null> {
  const detail = await propertyLookup(candidate);
  const zpid = findFirstString(detail, ["zpid", "property.zpid", "data.zpid", "homeInfo.zpid"]);
  const baseContact = extractContact(detail, "property_lookup");

  let contact = baseContact;
  if (zpid && stats.apiCalls < maxCalls) {
    const property = await rapidGet(`/v1/property/${encodeURIComponent(zpid)}`, { include: "agent" });
    contact = mergeContact(contact, extractContact(property, "property_include_agent"));
  }

  const screenName = contact?.screenName ?? findAgentScreenName(detail);
  if (screenName && stats.apiCalls < maxCalls) {
    const profile = await rapidGet(`/v1/agents/${encodeURIComponent(screenName)}/contact`, {});
    contact = mergeContact(contact, extractContact(profile, "agent_contact"));
  }

  return contact;
}

async function propertyLookup(candidate: Candidate): Promise<Record<string, unknown>> {
  if (candidate.listing_url && candidate.listing_url.includes("zillow.com")) {
    return rapidGet("/v1/search/url", { url: candidate.listing_url });
  }
  return rapidGet("/v1/property/lookup", {
    address: requestLabel(candidate),
    include: "agent",
  });
}

async function rapidGet(path: string, params: Record<string, string>): Promise<Record<string, unknown>> {
  if (stats.apiCalls >= maxCalls) throw new Error(`max-calls reached (${maxCalls})`);
  const url = new URL(`https://${host}${path}`);
  for (const [key, value] of Object.entries(params)) {
    if (value) url.searchParams.set(key, value);
  }

  const response = await fetch(url, {
    method: "GET",
    headers: {
      "x-rapidapi-key": rapidApiKey ?? "",
      "x-rapidapi-host": host,
      accept: "application/json",
    },
    signal: AbortSignal.timeout(45_000),
  });
  stats.apiCalls++;

  const text = await response.text();
  if (!response.ok) throw new Error(`Zillow RapidAPI ${response.status} ${path}: ${text.slice(0, 500)}`);
  return JSON.parse(text) as Record<string, unknown>;
}

async function updateListing(propertyId: number, contact: Contact) {
  await client.query(`
    update listing_signals
    set
      listing_agent_name = coalesce(nullif(listing_agent_name,''), $2),
      listing_agent_first_name = coalesce(nullif(listing_agent_first_name,''), $3),
      listing_agent_last_name = coalesce(nullif(listing_agent_last_name,''), $4),
      listing_agent_email = coalesce(nullif(listing_agent_email,''), $5),
      listing_agent_phone = coalesce(nullif(listing_agent_phone,''), $6),
      listing_brokerage = coalesce(nullif(listing_brokerage,''), $7),
      agent_contact_source = case
        when $5::text is not null or $6::text is not null then $8
        else agent_contact_source
      end,
      agent_contact_confidence = case
        when $5::text is not null or $6::text is not null then 'medium'
        else agent_contact_confidence
      end,
      raw = jsonb_set(coalesce(raw, '{}'::jsonb), '{zillow_rapidapi_contact}', $9::jsonb, true),
      updated_at = now()
    where property_id = $1
      and is_on_market = true
  `, [
    propertyId,
    contact.name,
    contact.firstName,
    contact.lastName,
    contact.email,
    contact.phone,
    contact.brokerage,
    `zillow_api_${provider}`,
    JSON.stringify({
      provider: `zillow_api_${provider}`,
      sourceEndpoint: contact.sourceEndpoint,
      screenName: contact.screenName,
      observedAt: new Date().toISOString(),
    }),
  ]);
}

function extractContact(payload: unknown, sourceEndpoint: string): Contact | null {
  const hits = collectObjects(payload);
  let best: Contact | null = null;
  for (const obj of hits) {
    const name = firstString(obj, ["agentName", "name", "displayName", "fullName", "listingAgentName", "attributionTitle"]);
    const email = firstEmail(obj);
    const phone = firstPhone(obj);
    const brokerage = firstString(obj, ["brokerage", "brokerageName", "brokerName", "officeName", "agentOffice", "businessName"]);
    const screenName = firstString(obj, ["screenName", "screen_name", "profileScreenName", "zillowScreenName"]);
    if (!name && !email && !phone && !brokerage && !screenName) continue;
    const split = splitName(name);
    const candidate: Contact = { name, firstName: split.firstName, lastName: split.lastName, email, phone, brokerage, screenName, sourceEndpoint };
    if (!best || scoreContact(candidate) > scoreContact(best)) best = candidate;
  }
  return best;
}

function mergeContact(a: Contact | null, b: Contact | null): Contact | null {
  if (!a) return b;
  if (!b) return a;
  const merged: Contact = {
    name: a.name ?? b.name,
    firstName: a.firstName ?? b.firstName,
    lastName: a.lastName ?? b.lastName,
    email: a.email ?? b.email,
    phone: a.phone ?? b.phone,
    brokerage: a.brokerage ?? b.brokerage,
    screenName: a.screenName ?? b.screenName,
    sourceEndpoint: `${a.sourceEndpoint}+${b.sourceEndpoint}`,
  };
  return merged;
}

function scoreContact(contact: Contact): number {
  return (contact.email ? 10 : 0) + (contact.phone ? 5 : 0) + (contact.name ? 3 : 0) + (contact.brokerage ? 2 : 0);
}

function collectObjects(value: unknown, depth = 0): Record<string, unknown>[] {
  if (!value || depth > 8) return [];
  if (Array.isArray(value)) return value.flatMap((item) => collectObjects(item, depth + 1));
  if (typeof value !== "object") return [];
  const obj = value as Record<string, unknown>;
  return [obj, ...Object.values(obj).flatMap((child) => collectObjects(child, depth + 1))];
}

function firstString(obj: Record<string, unknown>, keys: string[]): string | null {
  for (const key of keys) {
    const value = obj[key];
    if (typeof value === "string" && value.trim()) return value.trim();
  }
  return null;
}

function firstEmail(obj: Record<string, unknown>): string | null {
  for (const [key, value] of Object.entries(obj)) {
    if (!/email/i.test(key) || typeof value !== "string") continue;
    const email = value.match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i)?.[0];
    if (email) return email;
  }
  return null;
}

function firstPhone(obj: Record<string, unknown>): string | null {
  for (const [key, value] of Object.entries(obj)) {
    if (!/(phone|mobile|cell)/i.test(key) || typeof value !== "string") continue;
    const digits = value.replace(/\D/g, "");
    if (digits.length >= 10) return digits.length === 11 && digits.startsWith("1") ? digits.slice(1) : digits;
  }
  return null;
}

function findFirstString(payload: unknown, paths: string[]): string | null {
  for (const path of paths) {
    const value = path.split(".").reduce<unknown>((acc, key) => {
      if (!acc || typeof acc !== "object" || Array.isArray(acc)) return undefined;
      return (acc as Record<string, unknown>)[key];
    }, payload);
    if (typeof value === "string" || typeof value === "number") return String(value);
  }
  return null;
}

function findAgentScreenName(payload: unknown): string | null {
  for (const obj of collectObjects(payload)) {
    const screenName = firstString(obj, ["screenName", "screen_name", "profileScreenName", "zillowScreenName"]);
    if (screenName) return screenName;
  }
  return null;
}

function requestLabel(candidate: Candidate): string {
  return [candidate.address, candidate.city, candidate.state_code, candidate.zip].filter(Boolean).join(", ");
}

function splitName(name: string | null) {
  if (!name) return { firstName: null, lastName: null };
  const parts = name.trim().split(/\s+/);
  if (parts.length === 1) return { firstName: null, lastName: parts[0] };
  return { firstName: parts[0], lastName: parts.slice(1).join(" ") };
}

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
