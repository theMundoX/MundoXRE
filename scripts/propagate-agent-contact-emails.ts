#!/usr/bin/env tsx
import "dotenv/config";

const PG_URL = `${(process.env.SUPABASE_URL ?? "").replace(/\/$/, "")}/pg/query`;
const PG_KEY = process.env.SUPABASE_SERVICE_KEY ?? "";
const DRY_RUN = process.argv.includes("--dry-run");

const arg = (name: string) =>
  process.argv.find(a => a.startsWith(`--${name}=`))?.split("=").slice(1).join("=");
const STATE = arg("state")?.toUpperCase();
const CITY = arg("city")?.toUpperCase();

function sql(value: unknown): string {
  if (value == null || value === "") return "null";
  return `'${String(value).replace(/'/g, "''")}'`;
}

async function pg(query: string): Promise<Record<string, unknown>[]> {
  const response = await fetch(PG_URL, {
    method: "POST",
    headers: { apikey: PG_KEY, Authorization: `Bearer ${PG_KEY}`, "Content-Type": "application/json" },
    body: JSON.stringify({ query }),
    signal: AbortSignal.timeout(120_000),
  });
  if (!response.ok) throw new Error(`pg/query ${response.status}: ${await response.text()}`);
  return response.json();
}

const filters = [
  STATE ? `state_code = ${sql(STATE)}` : null,
  CITY ? `upper(coalesce(city,'')) = ${sql(CITY)}` : null,
].filter(Boolean).join("\n      and ");

const marketWhere = `
  is_on_market = true
  ${filters ? `and ${filters}` : ""}
`;

const exactIdentity = `
  lower(trim(coalesce(listing_agent_first_name, split_part(listing_agent_name, ' ', 1)))) as first_name_key,
  lower(trim(coalesce(listing_agent_last_name, regexp_replace(listing_agent_name, '^\\S+\\s+', '')))) as last_name_key,
  right(regexp_replace(coalesce(listing_agent_phone, ''), '\\D', '', 'g'), 10) as phone_key
`;

async function main() {
  console.log("MXRE - Propagate verified agent emails by exact identity");
  if (STATE || CITY) console.log(`Market filter: ${CITY ?? "all cities"}, ${STATE ?? "all states"}`);
  console.log(`Dry run: ${DRY_RUN}`);

  const preview = await pg(`
    with base as (
      select id, listing_agent_email, agent_contact_source, agent_contact_confidence, raw,
             ${exactIdentity}
      from listing_signals
      where ${marketWhere}
    ),
    verified_sources as (
      select first_name_key, last_name_key, phone_key,
             min(lower(trim(listing_agent_email))) as email,
             count(distinct lower(trim(listing_agent_email))) as email_count,
             count(*) as source_rows
      from base
      where nullif(trim(listing_agent_email), '') is not null
        and first_name_key <> ''
        and last_name_key <> ''
        and phone_key ~ '^\\d{10}$'
        and (agent_contact_source = 'realestateapi' or agent_contact_confidence = 'public_profile_verified')
        and lower(trim(listing_agent_email)) !~ '^(info|support|offers|admin|office|contact|hello|team|sales|leads|noreply)@'
      group by 1,2,3
      having count(distinct lower(trim(listing_agent_email))) = 1
    ),
    targets as (
      select b.id, v.email, v.source_rows
      from base b
      join verified_sources v using (first_name_key, last_name_key, phone_key)
      where nullif(trim(coalesce(b.listing_agent_email, '')), '') is null
    )
    select count(*)::int as target_rows,
           count(distinct email)::int as unique_emails,
           count(distinct (email, source_rows))::int as source_groups
    from targets;
  `);
  console.log("Preview:", JSON.stringify(preview[0] ?? {}, null, 2));
  if (DRY_RUN) return;

  const updated = await pg(`
    with base as (
      select id, listing_agent_email, agent_contact_source, agent_contact_confidence, raw,
             ${exactIdentity}
      from listing_signals
      where ${marketWhere}
    ),
    verified_sources as (
      select first_name_key, last_name_key, phone_key,
             min(lower(trim(listing_agent_email))) as email,
             count(distinct lower(trim(listing_agent_email))) as email_count,
             count(*) as source_rows
      from base
      where nullif(trim(listing_agent_email), '') is not null
        and first_name_key <> ''
        and last_name_key <> ''
        and phone_key ~ '^\\d{10}$'
        and (agent_contact_source = 'realestateapi' or agent_contact_confidence = 'public_profile_verified')
        and lower(trim(listing_agent_email)) !~ '^(info|support|offers|admin|office|contact|hello|team|sales|leads|noreply)@'
      group by 1,2,3
      having count(distinct lower(trim(listing_agent_email))) = 1
    ),
    targets as (
      select b.id, v.email, v.source_rows
      from base b
      join verified_sources v using (first_name_key, last_name_key, phone_key)
      where nullif(trim(coalesce(b.listing_agent_email, '')), '') is null
    ),
    updated as (
      update listing_signals l
         set listing_agent_email = t.email,
             agent_contact_source = 'verified_identity_propagation',
             agent_contact_confidence = 'public_profile_verified',
             raw = coalesce(l.raw, '{}'::jsonb) || jsonb_build_object(
               'agentEmailPropagation',
               jsonb_build_object(
                 'email', t.email,
                 'matchedOn', 'exact_agent_name_and_phone',
                 'sourceRows', t.source_rows,
                 'observedAt', now()
               )
             ),
             updated_at = now()
        from targets t
       where l.id = t.id
       returning l.id
    )
    select count(*)::int as updated from updated;
  `);
  console.log("Updated:", JSON.stringify(updated[0] ?? {}, null, 2));
}

main().catch(error => {
  console.error("Fatal:", error instanceof Error ? error.message : error);
  process.exit(1);
});
