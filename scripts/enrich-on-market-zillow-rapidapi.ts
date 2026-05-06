#!/usr/bin/env tsx
import "dotenv/config";
import { Client } from "pg";

type Candidate = {
  property_id: number;
  address: string;
  city: string;
  state_code: string;
  zip: string | null;
  listing_id: number | null;
  search_address: string | null;
  search_city: string | null;
  search_zip: string | null;
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

type DetailSummary = {
  zpid: string | null;
  price: number | null;
  zestimate: number | null;
  rentZestimate: number | null;
  homeStatus: string | null;
  homeType: string | null;
  daysOnZillow: number | null;
  bedrooms: number | null;
  bathrooms: number | null;
  livingArea: number | null;
  yearBuilt: number | null;
  lastSoldPrice: number | null;
  raw: unknown;
};

type Enrichment = {
  contact: Contact | null;
  detail: DetailSummary | null;
};

const args = process.argv.slice(2);
const valueArg = (name: string) => {
  const prefix = `--${name}=`;
  return args.find((arg) => arg.startsWith(prefix))?.slice(prefix.length) ?? null;
};

const dryRun = args.includes("--dry-run");
const force = args.includes("--force");
const descriptionsOnly = args.includes("--descriptions-only");
const contactsOnly = args.includes("--contacts-only");
const city = (valueArg("city") ?? "Indianapolis").toUpperCase();
const state = (valueArg("state") ?? "IN").toUpperCase();
const limit = Math.min(Math.max(Number(valueArg("limit") ?? "10"), 1), 2500);
const maxCalls = Math.min(Math.max(Number(valueArg("max-calls") ?? String(limit)), 0), limit * 5);
const concurrency = Math.min(Math.max(Number(valueArg("concurrency") ?? "1"), 1), 10);
const rapidApiKey = process.env.RAPIDAPI_KEY ?? process.env.ZILLOW_RAPIDAPI_KEY;
const provider = process.env.ZILLOW_RAPIDAPI_PROVIDER ?? "realestate101";
const host = process.env.ZILLOW_RAPIDAPI_HOST ?? defaultHost(provider);
const databaseUrl = process.env.MXRE_DIRECT_PG_URL
  ?? process.env.MXRE_PG_URL
  ?? process.env.DATABASE_URL
  ?? process.env.POSTGRES_URL;

if (!databaseUrl) throw new Error("Set MXRE_DIRECT_PG_URL, MXRE_PG_URL, DATABASE_URL, or POSTGRES_URL.");
if (!dryRun && maxCalls > 0 && !rapidApiKey) {
  throw new Error("Set RAPIDAPI_KEY before making Zillow RapidAPI calls. Use --dry-run to preview.");
}
if (!dryRun && maxCalls > 0 && rapidApiKey && !isValidHeaderValue(rapidApiKey)) {
  throw new Error("RAPIDAPI_KEY/ZILLOW_RAPIDAPI_KEY is present but is not a valid HTTP header value. Reset the environment variable; do not run paid calls with this value.");
}

type Queryable = {
  query<T = Record<string, unknown>>(query: string, params?: unknown[]): Promise<{ rows: T[] }>;
  end(): Promise<void>;
};

function sqlLiteral(value: unknown): string {
  if (value == null) return "null";
  if (Array.isArray(value)) return `array[${value.map(sqlLiteral).join(",")}]`;
  if (typeof value === "number") return Number.isFinite(value) ? String(value) : "null";
  if (typeof value === "boolean") return value ? "true" : "false";
  return dollarQuotedString(String(value));
}

function dollarQuotedString(value: string): string {
  let tag = "mxre";
  while (value.includes(`$${tag}$`)) tag = `${tag}x`;
  return `$${tag}$${value}$${tag}$`;
}

function bindSql(query: string, params: unknown[] = []): string {
  const templated = params.reduceRight((sql, _value, index) => {
    const token = new RegExp(`\\$${index + 1}(?!\\d)`, "g");
    return sql.replace(token, `__MXRE_PARAM_${index + 1}__`);
  }, query);
  return params.reduce((sql, value, index) =>
    sql.replaceAll(`__MXRE_PARAM_${index + 1}__`, sqlLiteral(value)), templated);
}

function makeClient(): Queryable {
  if (/^https?:\/\//i.test(databaseUrl ?? "")) {
    const endpoint = databaseUrl.replace(/\/$/, "");
    const key = process.env.SUPABASE_SERVICE_KEY ?? "";
    return {
      async query<T = Record<string, unknown>>(query: string, params: unknown[] = []) {
        const response = await fetch(endpoint, {
          method: "POST",
          headers: { apikey: key, Authorization: `Bearer ${key}`, "Content-Type": "application/json" },
          body: JSON.stringify({ query: bindSql(query, params) }),
          signal: AbortSignal.timeout(120_000),
        });
        if (!response.ok) throw new Error(`pg/query ${response.status}: ${await response.text()}`);
        const body = await response.json();
        return { rows: Array.isArray(body) ? body as T[] : [] };
      },
      async end() {},
    };
  }
  return new Client({ connectionString: databaseUrl }) as unknown as Queryable;
}

function isValidHeaderValue(value: string): boolean {
  return value.trim().length >= 10 && !/[\u0000-\u001f\u007f]/.test(value);
}

const client = makeClient();
if (!/^https?:\/\//i.test(databaseUrl ?? "")) {
  await (client as unknown as Client).connect();
}

const stats = {
  city,
  state,
  provider: `zillow_api_${provider}`,
  host,
  dryRun,
  concurrency,
  candidates: 0,
  apiCalls: 0,
  updated: 0,
  foundName: 0,
  foundEmail: 0,
  foundPhone: 0,
  foundBrokerage: 0,
  foundDescription: 0,
  failed: 0,
};

try {
  const candidates = await loadCandidates();
  stats.candidates = candidates.length;

  let nextIndex = 0;
  const workerCount = dryRun ? 1 : Math.min(concurrency, candidates.length || 1);
  await Promise.all(Array.from({ length: workerCount }, async () => {
    while (nextIndex < candidates.length) {
      if (!dryRun && stats.apiCalls >= maxCalls) break;
      const candidate = candidates[nextIndex++];
      if (!candidate) break;
      await processCandidate(candidate);
    }
  }));
} finally {
  await client.end();
}

console.log(JSON.stringify(stats, null, 2));

async function processCandidate(candidate: Candidate) {
    try {
      if (dryRun) {
        console.log(JSON.stringify({
          wouldLookup: requestLabel(candidate),
          property_id: candidate.property_id,
          listing_url: candidate.listing_url,
        }));
        return;
      }

      const enrichment = await lookupContact(candidate);
      if (!enrichment) return;
      const contact = enrichment.contact;
      if (contact?.name) stats.foundName++;
      if (contact?.email) stats.foundEmail++;
      if (contact?.phone) stats.foundPhone++;
      if (contact?.brokerage) stats.foundBrokerage++;
      if (listingDescription(enrichment.detail?.raw)) stats.foundDescription++;

      if (contact?.name || contact?.email || contact?.phone || contact?.brokerage || enrichment.detail) {
        await updateListing(candidate.property_id, enrichment);
        stats.updated++;
      }
      await sleep(250);
    } catch (error) {
      stats.failed++;
      console.error(`Failed Zillow lookup for ${candidate.property_id}:`, error instanceof Error ? error.message : error);
    }
}

async function loadCandidates(): Promise<Candidate[]> {
  const { rows } = await client.query<Candidate>(`
    select distinct on (p.id)
      p.id as property_id,
      p.address,
      p.city,
      p.state_code,
      p.zip,
      l.id as listing_id,
      coalesce(nullif(l.address,''), p.address) as search_address,
      coalesce(nullif(l.city,''), p.city) as search_city,
      coalesce(nullif(l.zip,''), p.zip) as search_zip,
      l.mls_list_price,
      l.listing_url
    from listing_signals l
    join properties p on p.id = l.property_id
    where l.is_on_market = true
      and l.state_code = $1
      and upper(coalesce(l.city,'')) = $2
      and p.state_code = $1
      and (
        $3::boolean = true
        or (
          $5::boolean = false
          and coalesce(l.raw, '{}'::jsonb)->'zillow_rapidapi_detail' is null
        )
        or (
          $5::boolean = true
          and nullif(coalesce(
            l.raw #>> '{description}',
            l.raw #>> '{publicRemarks}',
            l.raw #>> '{public_remarks}',
            l.raw #>> '{remarks}',
            l.raw #>> '{listingRemarks}',
            l.raw #>> '{marketingRemarks}',
            l.raw #>> '{propertyDescription}',
            l.raw #>> '{redfinDetail,publicRemarks}',
            l.raw #>> '{redfinDetail,description}',
            l.raw #>> '{zillow_rapidapi_detail,raw,property,description}',
            l.raw #>> '{zillow_rapidapi_detail,raw,description}',
            l.raw #>> '{zillow_rapidapi_detail,raw,data,description}',
            l.raw #>> '{zillow_rapidapi_detail,raw,homeInfo,description}',
            l.raw #>> '{mls,remarks}',
            l.raw #>> '{mls,description}'
          ), '') is null
        )
      )
      and (
        (
          $5::boolean = true
          and nullif(coalesce(
            l.raw #>> '{description}',
            l.raw #>> '{publicRemarks}',
            l.raw #>> '{public_remarks}',
            l.raw #>> '{remarks}',
            l.raw #>> '{listingRemarks}',
            l.raw #>> '{marketingRemarks}',
            l.raw #>> '{propertyDescription}',
            l.raw #>> '{redfinDetail,publicRemarks}',
            l.raw #>> '{redfinDetail,description}',
            l.raw #>> '{zillow_rapidapi_detail,raw,property,description}',
            l.raw #>> '{zillow_rapidapi_detail,raw,description}',
            l.raw #>> '{zillow_rapidapi_detail,raw,data,description}',
            l.raw #>> '{zillow_rapidapi_detail,raw,homeInfo,description}',
            l.raw #>> '{mls,remarks}',
            l.raw #>> '{mls,description}'
          ), '') is null
        )
        or (
          $6::boolean = false
          and (
            $3::boolean = true
            or nullif(l.listing_agent_email,'') is null
            or nullif(l.listing_agent_phone,'') is null
            or nullif(l.listing_agent_name,'') is null
            or nullif(l.listing_brokerage,'') is null
          )
        )
      )
    order by p.id, l.last_seen_at desc nulls last, l.updated_at desc nulls last
    limit $4
  `, [state, city, force, limit, descriptionsOnly, contactsOnly]);
  return rows;
}

async function lookupContact(candidate: Candidate): Promise<Enrichment | null> {
  if (provider === "realestate101") {
    const detail = await rapidGet("/api/property-details/byaddress", { address: requestLabel(candidate) });
    return {
      contact: extractContact(detail, "property_details_byaddress"),
      detail: summarizeRealEstate101(detail),
    };
  }

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

  return { contact, detail: summarizeGenericDetail(detail) };
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

async function updateListing(propertyId: number, enrichment: Enrichment) {
  const contact = descriptionsOnly ? null : enrichment.contact;
  const detail = enrichment.detail;
  await client.query(`
    update listing_signals
    set
      listing_agent_name = coalesce(nullif(listing_agent_name,''), $2),
      listing_agent_first_name = coalesce(nullif(listing_agent_first_name,''), $3),
      listing_agent_last_name = coalesce(nullif(listing_agent_last_name,''), $4),
      listing_agent_email = coalesce(nullif(listing_agent_email,''), $5),
      listing_agent_phone = coalesce(nullif(listing_agent_phone,''), $6),
      listing_brokerage = coalesce(nullif(listing_brokerage,''), $7),
      mls_list_price = coalesce(mls_list_price, $10),
      days_on_market = coalesce(days_on_market, $11),
      agent_contact_source = case
        when $5::text is not null or $6::text is not null then $8
        else agent_contact_source
      end,
      agent_contact_confidence = case
        when $5::text is not null or $6::text is not null then 'medium'
        else agent_contact_confidence
      end,
      raw = jsonb_set(
        jsonb_set(coalesce(raw, '{}'::jsonb), '{zillow_rapidapi_contact}', $9::jsonb, true),
        '{zillow_rapidapi_detail}',
        $12::jsonb,
        true
      ),
      updated_at = now()
    where property_id = $1
      and is_on_market = true
  `, [
    propertyId,
    contact?.name ?? null,
    contact?.firstName ?? null,
    contact?.lastName ?? null,
    contact?.email ?? null,
    contact?.phone ?? null,
    contact?.brokerage ?? null,
    `zillow_api_${provider}`,
    JSON.stringify({
      provider: `zillow_api_${provider}`,
      sourceEndpoint: contact?.sourceEndpoint ?? null,
      screenName: contact?.screenName ?? null,
      observedAt: new Date().toISOString(),
    }),
    detail?.price ?? null,
    detail?.daysOnZillow ?? null,
    JSON.stringify({
      provider: `zillow_api_${provider}`,
      sourceEndpoint: detail ? "property_details_byaddress" : null,
      observedAt: new Date().toISOString(),
      ...detail,
      raw: detail?.raw ?? null,
    }),
  ]);

  if (detail && !descriptionsOnly) {
    await client.query(`
      update properties
      set
        bedrooms = coalesce(bedrooms, $2),
        bathrooms_full = coalesce(bathrooms_full, $3),
        living_sqft = coalesce(living_sqft, $4),
        year_built = coalesce(year_built, $5),
        updated_at = now()
      where id = $1
    `, [
      propertyId,
      detail.bedrooms,
      detail.bathrooms,
      detail.livingArea,
      detail.yearBuilt,
    ]);
  }
}

function listingDescription(payload: unknown): string | null {
  return firstString(payload, [
    "property.description",
    "description",
    "data.description",
    "homeInfo.description",
    "publicRemarks",
    "remarks",
    "listingRemarks",
    "marketingRemarks",
    "propertyDescription",
  ]);
}

function extractContact(payload: unknown, sourceEndpoint: string): Contact | null {
  const hits = collectObjects(payload);
  let best: Contact | null = null;
  for (const obj of hits) {
    const name = firstString(obj, ["agentName", "name", "displayName", "fullName", "listingAgentName", "attributionTitle"]);
    const email = firstEmail(obj);
    const phone = firstPhone(obj);
    const brokerage = firstString(obj, ["brokerage", "brokerageName", "brokerName", "broker", "officeName", "agentOffice", "businessName"]);
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

function summarizeRealEstate101(payload: unknown): DetailSummary | null {
  if (!payload || typeof payload !== "object") return null;
  const root = payload as Record<string, unknown>;
  const property = root.property && typeof root.property === "object"
    ? root.property as Record<string, unknown>
    : root;
  return {
    zpid: findFirstString(payload, ["zpid", "property.zpid"]),
    price: numberOrNull(property.price),
    zestimate: numberOrNull(property.zestimate),
    rentZestimate: numberOrNull(property.rentZestimate),
    homeStatus: stringOrNull(property.homeStatus),
    homeType: stringOrNull(property.homeType),
    daysOnZillow: numberOrNull(property.daysOnZillow),
    bedrooms: numberOrNull(property.bedrooms),
    bathrooms: numberOrNull(property.bathrooms),
    livingArea: numberOrNull((property.livingArea as Record<string, unknown> | undefined)?.value ?? property.livingArea),
    yearBuilt: numberOrNull(property.yearBuilt),
    lastSoldPrice: numberOrNull(property.lastSoldPrice),
    raw: payload,
  };
}

function summarizeGenericDetail(payload: unknown): DetailSummary | null {
  if (!payload || typeof payload !== "object") return null;
  return {
    zpid: findFirstString(payload, ["zpid", "property.zpid", "data.zpid", "homeInfo.zpid"]),
    price: numberOrNull(findFirstValue(payload, ["price", "data.price", "property.price"])),
    zestimate: numberOrNull(findFirstValue(payload, ["zestimate", "data.financials.zestimate", "property.zestimate"])),
    rentZestimate: numberOrNull(findFirstValue(payload, ["rentZestimate", "data.financials.rent_zestimate", "property.rentZestimate"])),
    homeStatus: stringOrNull(findFirstValue(payload, ["homeStatus", "data.status", "property.homeStatus"])),
    homeType: stringOrNull(findFirstValue(payload, ["homeType", "property.homeType", "data.home_type"])),
    daysOnZillow: numberOrNull(findFirstValue(payload, ["daysOnZillow", "data.days_on_zillow", "property.daysOnZillow"])),
    bedrooms: numberOrNull(findFirstValue(payload, ["bedrooms", "data.bedrooms", "property.bedrooms"])),
    bathrooms: numberOrNull(findFirstValue(payload, ["bathrooms", "data.bathrooms", "property.bathrooms"])),
    livingArea: numberOrNull(findFirstValue(payload, ["livingArea", "data.living_area", "property.livingArea"])),
    yearBuilt: numberOrNull(findFirstValue(payload, ["yearBuilt", "data.year_built", "property.yearBuilt"])),
    lastSoldPrice: numberOrNull(findFirstValue(payload, ["lastSoldPrice", "data.financials.last_sold_price", "property.lastSoldPrice"])),
    raw: payload,
  };
}

function scoreContact(contact: Contact): number {
  return (contact.email ? 10 : 0) + (contact.phone ? 5 : 0) + (contact.name ? 3 : 0) + (contact.brokerage ? 2 : 0);
}

function stringOrNull(value: unknown): string | null {
  if (value == null) return null;
  const text = String(value).trim();
  return text.length > 0 ? text : null;
}

function numberOrNull(value: unknown): number | null {
  if (value == null || value === "") return null;
  const num = Number(value);
  return Number.isFinite(num) ? Math.round(num) : null;
}

function collectObjects(value: unknown, depth = 0): Record<string, unknown>[] {
  if (!value || depth > 8) return [];
  if (Array.isArray(value)) return value.flatMap((item) => collectObjects(item, depth + 1));
  if (typeof value !== "object") return [];
  const obj = value as Record<string, unknown>;
  return [obj, ...Object.values(obj).flatMap((child) => collectObjects(child, depth + 1))];
}

function firstString(obj: unknown, keys: string[]): string | null {
  for (const key of keys) {
    const value = getPath(obj, key);
    if (typeof value === "string" && value.trim()) return value.trim();
  }
  return null;
}

function getPath(obj: unknown, path: string): unknown {
  let current = obj;
  for (const part of path.split(".")) {
    if (!current || typeof current !== "object" || Array.isArray(current)) return null;
    current = (current as Record<string, unknown>)[part];
  }
  return current;
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
  const value = findFirstValue(payload, paths);
  if (typeof value === "string" || typeof value === "number") return String(value);
  return null;
}

function findFirstValue(payload: unknown, paths: string[]): unknown {
  for (const path of paths) {
    const value = path.split(".").reduce<unknown>((acc, key) => {
      if (!acc || typeof acc !== "object" || Array.isArray(acc)) return undefined;
      return (acc as Record<string, unknown>)[key];
    }, payload);
    if (value != null) return value;
  }
  return undefined;
}

function findAgentScreenName(payload: unknown): string | null {
  for (const obj of collectObjects(payload)) {
    const screenName = firstString(obj, ["screenName", "screen_name", "profileScreenName", "zillowScreenName"]);
    if (screenName) return screenName;
  }
  return null;
}

function requestLabel(candidate: Candidate): string {
  return [
    candidate.search_address ?? candidate.address,
    candidate.search_city ?? candidate.city,
    candidate.state_code,
    candidate.search_zip ?? candidate.zip,
  ].filter(Boolean).join(", ");
}

function defaultHost(providerName: string) {
  if (providerName === "realestate101") return "real-estate101.p.rapidapi.com";
  if (providerName === "oneapi") return "zllw-working-api.p.rapidapi.com";
  return "zillow-real-estate-api.p.rapidapi.com";
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
