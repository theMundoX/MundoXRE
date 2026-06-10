#!/usr/bin/env tsx
import "dotenv/config";
import { mkdir, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { firstEnv, hydrateWindowsUserEnv } from "./lib/env.ts";

hydrateWindowsUserEnv();

const PG_URL = firstEnv("MXRE_PG_URL")
  ?? `${(firstEnv("SUPABASE_URL") ?? "").replace(/\/$/, "")}/pg/query`;
const PG_KEY = firstEnv("SUPABASE_SERVICE_KEY", "SUPABASE_SERVICE_ROLE_KEY") ?? "";
const OUT_DIR = join(process.cwd(), "logs", "market-refresh");

type TargetMarket = {
  id: string;
  label: string;
  city: string;
  state: string;
  county?: string;
  zips: string[];
};

const TARGETS: TargetMarket[] = [
  { id: "nashville-tn", label: "Nashville, TN", city: "NASHVILLE", state: "TN", county: "Davidson", zips: ["37201", "37203", "37204", "37205", "37206", "37207", "37208", "37209", "37210", "37211", "37212", "37213", "37214", "37215", "37216", "37217", "37218", "37219", "37220", "37221", "37228", "37240", "37246"] },
  { id: "orlando-fl", label: "Orlando, FL", city: "ORLANDO", state: "FL", county: "Orange", zips: ["32801", "32803", "32804", "32805", "32806", "32807", "32808", "32809", "32810", "32811", "32812", "32814", "32817", "32819", "32821", "32822", "32824", "32827", "32829", "32832", "32835", "32836", "32837", "32839"] },
  { id: "gilchrist-tx", label: "Gilchrist, TX", city: "GILCHRIST", state: "TX", county: "Galveston", zips: ["77617"] },
  { id: "austin-tx", label: "Austin, TX", city: "AUSTIN", state: "TX", county: "Travis", zips: ["78701", "78702", "78703", "78704", "78705", "78721", "78722", "78723", "78724", "78727", "78728", "78729", "78731", "78732", "78733", "78735", "78736", "78737", "78738", "78739", "78741", "78742", "78744", "78745", "78746", "78747", "78748", "78749", "78750", "78751", "78752", "78753", "78754", "78756", "78757", "78758", "78759"] },
  { id: "scottsdale-az", label: "Scottsdale, AZ", city: "SCOTTSDALE", state: "AZ", county: "Maricopa", zips: ["85250", "85251", "85254", "85255", "85257", "85258", "85259", "85260", "85262", "85266"] },
  { id: "galveston-tx", label: "Galveston, TX", city: "GALVESTON", state: "TX", county: "Galveston", zips: ["77550", "77551", "77554"] },
  { id: "blue-ridge-ga", label: "Blue Ridge, GA", city: "BLUE RIDGE", state: "GA", county: "Fannin", zips: ["30513"] },
  { id: "bentonville-ar", label: "Bentonville, AR", city: "BENTONVILLE", state: "AR", county: "Benton", zips: ["72712", "72713"] },
  { id: "panama-city-beach-fl", label: "Panama City Beach, FL", city: "PANAMA CITY BEACH", state: "FL", county: "Bay", zips: ["32407", "32408", "32413"] },
  { id: "destin-fl", label: "Destin, FL", city: "DESTIN", state: "FL", county: "Okaloosa", zips: ["32541"] },
];

const PUBLISHABILITY_MIN_CORE_PCT = 80;
const PUBLISHABILITY_MIN_LISTING_LINK_PCT = 95;
const PUBLISHABILITY_MIN_AGENT_EMAIL_PCT = 50;

async function main() {
  await mkdir(OUT_DIR, { recursive: true });
  const generatedAt = new Date().toISOString();
  const markets = [];
  for (const target of TARGETS) {
    markets.push(await summarizeTarget(target));
  }

  const payload = { generated_at: generatedAt, markets };
  const jsonPath = join(OUT_DIR, "publishability-targets.json");
  const mdPath = join(OUT_DIR, "publishability-targets.md");
  await writeFile(jsonPath, JSON.stringify(payload, null, 2));
  await writeFile(mdPath, toMarkdown(payload));
  console.log(JSON.stringify({ wrote: [jsonPath, mdPath], markets: markets.map((row) => ({ id: row.id, active_listings: row.active_listing_count, publishable_now: row.publishable_now, gaps: row.blocking_gaps })) }, null, 2));
}

async function summarizeTarget(target: TargetMarket) {
  const city = sql(target.city);
  const state = sql(target.state);
  const zips = target.zips.map(sql).join(",");
  const listingWhere = `is_on_market = true and state_code = ${state} and (upper(coalesce(city,'')) = ${city} or zip = any(array[${zips}]))`;
  const propertyIds = `select distinct property_id from listing_signals where ${listingWhere} and property_id is not null`;

  const [listings] = await pg(`
    select count(*)::int as active_listing_count,
           count(*) filter (where property_id is not null)::int as linked_listing_count,
           count(distinct property_id)::int as active_property_count,
           count(*) filter (where nullif(listing_agent_name,'') is not null)::int as agent_name_count,
           count(*) filter (where nullif(listing_agent_email,'') is not null)::int as agent_email_count,
           count(*) filter (where nullif(listing_agent_phone,'') is not null)::int as agent_phone_count,
           count(*) filter (where nullif(listing_brokerage,'') is not null)::int as brokerage_count,
           count(*) filter (where nullif(listing_url,'') is not null)::int as listing_url_count,
           count(*) filter (where creative_finance_status = 'positive')::int as creative_positive_count,
           count(*) filter (where creative_finance_observed_at is not null or creative_finance_status is not null)::int as creative_reviewed_count,
           count(*) filter (where creative_finance_status is not null and creative_finance_status <> 'no_data')::int as creative_signal_count
      from listing_signals
     where ${listingWhere};
  `);

  const [properties] = await pg(`
    select count(*)::int as property_count,
           count(*) filter (where nullif(owner_name,'') is not null)::int as owner_name_count,
           count(*) filter (where nullif(mailing_address,'') is not null)::int as owner_mailing_count,
           count(*) filter (where nullif(asset_type,'') is not null or nullif(property_type,'') is not null or nullif(property_use,'') is not null)::int as classified_count,
           count(*) filter (where coalesce(lat, latitude) is not null and coalesce(lng, longitude) is not null)::int as coordinate_count
      from properties
     where id in (${propertyIds});
  `);

  const [rents] = await pg(`
    select count(distinct property_id)::int as properties_with_rent,
           count(*)::int as rent_snapshot_count,
           max(observed_at) as latest_rent_observed
      from rent_snapshots
     where property_id in (${propertyIds});
  `);

  const [debt] = await pg(`
    with active_properties as (${propertyIds}),
    covered as (
      select distinct property_id from mortgage_records where property_id in (select property_id from active_properties)
      union
      select distinct property_id
        from realestateapi_property_details
       where property_id in (select property_id from active_properties)
         and status = 'ok'
         and response_body <> '{}'::jsonb
    )
    select count(distinct property_id)::int as properties_with_debt_status
      from covered;
  `);

  const active = num(listings.active_listing_count);
  const props = num(properties.property_count);
  const completion = {
    listing_link_pct: pct(listings.linked_listing_count, active),
    listing_url_pct: pct(listings.listing_url_count, active),
    owner_name_pct: pct(properties.owner_name_count, props),
    owner_mailing_pct: pct(properties.owner_mailing_count, props),
    debt_status_pct: pct(debt.properties_with_debt_status, props),
    rent_estimate_pct: pct(rents.properties_with_rent, props),
    agent_name_pct: pct(listings.agent_name_count, active),
    agent_email_pct: pct(listings.agent_email_count, active),
    agent_phone_pct: pct(listings.agent_phone_count, active),
    asset_type_pct: pct(properties.classified_count, props),
    coordinate_pct: pct(properties.coordinate_count, props),
    creative_reviewed_pct: pct(listings.creative_reviewed_count, active),
    creative_signal_pct: pct(listings.creative_signal_count, active),
  };

  const blockingGaps = [
    active === 0 ? "no active listing inventory loaded" : null,
    completion.listing_link_pct < PUBLISHABILITY_MIN_LISTING_LINK_PCT ? `listing linking ${completion.listing_link_pct}%` : null,
    completion.owner_name_pct < PUBLISHABILITY_MIN_CORE_PCT ? `owner names ${completion.owner_name_pct}%` : null,
    completion.owner_mailing_pct < PUBLISHABILITY_MIN_CORE_PCT ? `owner mailing ${completion.owner_mailing_pct}%` : null,
    completion.debt_status_pct < PUBLISHABILITY_MIN_CORE_PCT ? `debt/free-clear ${completion.debt_status_pct}%` : null,
    completion.rent_estimate_pct < PUBLISHABILITY_MIN_CORE_PCT ? `rent estimates ${completion.rent_estimate_pct}%` : null,
    completion.agent_email_pct < PUBLISHABILITY_MIN_AGENT_EMAIL_PCT ? `agent emails ${completion.agent_email_pct}%` : null,
    completion.coordinate_pct < PUBLISHABILITY_MIN_CORE_PCT ? `coordinates ${completion.coordinate_pct}%` : null,
  ].filter((value): value is string => Boolean(value));

  return {
    ...target,
    active_listing_count: active,
    active_property_count: num(listings.active_property_count),
    property_count: props,
    completion,
    raw_counts: { listings, properties, rents, debt },
    publishable_now: blockingGaps.length === 0,
    blocking_gaps: blockingGaps,
  };
}

async function pg(query: string): Promise<Record<string, unknown>[]> {
  const response = await fetch(PG_URL, {
    method: "POST",
    headers: { apikey: PG_KEY, Authorization: `Bearer ${PG_KEY}`, "Content-Type": "application/json" },
    body: JSON.stringify({ query }),
    signal: AbortSignal.timeout(120_000),
  });
  if (!response.ok) throw new Error(`pg/query ${response.status}: ${await response.text()}`);
  return response.json() as Promise<Record<string, unknown>[]>;
}

function toMarkdown(payload: { generated_at: string; markets: Awaited<ReturnType<typeof summarizeTarget>>[] }) {
  const rows = payload.markets.map((m) => `| ${m.label} | ${m.active_listing_count} | ${m.completion.listing_link_pct}% | ${m.completion.listing_url_pct}% | ${m.completion.owner_name_pct}% | ${m.completion.owner_mailing_pct}% | ${m.completion.debt_status_pct}% | ${m.completion.rent_estimate_pct}% | ${m.completion.agent_name_pct}% | ${m.completion.agent_email_pct}% | ${m.completion.agent_phone_pct}% | ${m.completion.asset_type_pct}% | ${m.completion.coordinate_pct}% | ${m.completion.creative_reviewed_pct}% | ${m.completion.creative_signal_pct}% | ${m.publishable_now ? "yes" : "no"} | ${m.blocking_gaps.join("; ")} |`);
  return `# MXRE Publishability Targets

Generated: ${payload.generated_at}

| Market | Active listings | Linked | URLs | Owner | Mailing | Debt | Rents | Agent names | Agent emails | Agent phones | Asset type | Coordinates | Creative reviewed | Creative signal | Publishable | Gaps |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|
${rows.join("\n")}
`;
}

function sql(value: string) {
  return `'${value.replace(/'/g, "''")}'`;
}

function num(value: unknown) {
  return Number(value ?? 0);
}

function pct(value: unknown, total: unknown) {
  const n = num(value);
  const d = num(total);
  return d > 0 ? Math.round((n / d) * 10000) / 100 : 0;
}

main().catch((error) => {
  console.error("Fatal publishability target report error:", error instanceof Error ? error.message : error);
  process.exit(1);
});
