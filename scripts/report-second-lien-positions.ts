#!/usr/bin/env tsx
import "dotenv/config";
import { mkdir, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { firstEnv, hydrateWindowsUserEnv } from "./lib/env.ts";
import { makeDbClient, type DbClient } from "./lib/db.ts";

hydrateWindowsUserEnv();

const OUT_DIR = join(process.cwd(), "logs", "debt-reports");

type Row = Record<string, unknown>;

const APPROVED_MARKETS = [
  { id: "indianapolis-in", label: "Indianapolis, IN", city: "INDIANAPOLIS", state: "IN" },
  { id: "dallas-tx", label: "Dallas, TX", city: "DALLAS", state: "TX" },
  { id: "columbus-oh", label: "Columbus, OH", city: "COLUMBUS", state: "OH" },
  { id: "dayton-oh", label: "Dayton, OH", city: "DAYTON", state: "OH" },
  { id: "toledo-oh", label: "Toledo, OH", city: "TOLEDO", state: "OH" },
  { id: "akron-oh", label: "Akron, OH", city: "AKRON", state: "OH" },
  { id: "peoria-il", label: "Peoria, IL", city: "PEORIA", state: "IL" },
  { id: "cleveland-oh", label: "Cleveland, OH", city: "CLEVELAND", state: "OH" },
  { id: "cincinnati-oh", label: "Cincinnati, OH", city: "CINCINNATI", state: "OH" },
  { id: "birmingham-al", label: "Birmingham, AL", city: "BIRMINGHAM", state: "AL" },
  { id: "memphis-tn", label: "Memphis, TN", city: "MEMPHIS", state: "TN" },
  { id: "detroit-mi", label: "Detroit, MI", city: "DETROIT", state: "MI" },
  { id: "pigeon-forge-tn", label: "Pigeon Forge, TN", city: "PIGEON FORGE", state: "TN" },
  { id: "sevierville-tn", label: "Sevierville, TN", city: "SEVIERVILLE", state: "TN" },
  { id: "gatlinburg-tn", label: "Gatlinburg, TN", city: "GATLINBURG", state: "TN" },
];

const STATE_BATCHES = [
  "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
  "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
  "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
  "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
  "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
  "DC",
];

const args = new Set(process.argv.slice(2));
const approvedOnly = args.has("--approved-markets-only");
const includeHistorical = args.has("--include-historical");
const individualLendersOnly = args.has("--individual-lenders-only");
const stateArg = process.argv.find((arg) => arg.startsWith("--state="))?.split("=").slice(1).join("=").trim().toUpperCase();
const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
let db: DbClient;
let activeStateFilter: string | null = stateArg || null;
let activeIdLower: number | null = null;
let activeIdUpper: number | null = null;
const ID_BATCH_SIZE = 500_000;

const COLUMNS = [
  "target_name",
  "target_role",
  "target_email",
  "target_phone",
  "target_contact_status",
  "is_likely_individual_lender",
  "property_id",
  "mortgage_record_id",
  "market_label",
  "property_address",
  "property_city",
  "property_state",
  "property_zip",
  "parcel_id",
  "apn_formatted",
  "owner_name",
  "owner_mailing_address",
  "owner_mailing_city",
  "owner_mailing_state",
  "owner_mailing_zip",
  "lien_position",
  "position_source",
  "document_type",
  "loan_type",
  "recording_date",
  "document_number",
  "book_page",
  "original_amount",
  "estimated_current_balance",
  "interest_rate",
  "interest_rate_type",
  "rate_source",
  "rate_match_confidence",
  "estimated_monthly_payment",
  "maturity_date",
  "lender_name",
  "lender_type",
  "lender_code",
  "borrower_name",
  "open",
  "source_url",
  "listing_agent_name",
  "listing_agent_email",
  "listing_agent_phone",
  "listing_brokerage",
  "listing_url",
  "mls_list_price",
] as const;

async function main() {
  firstEnv("MXRE_DIRECT_PG_URL", "DATABASE_URL", "POSTGRES_URL", "MXRE_PG_URL");
  db = await makeDbClient();
  await mkdir(OUT_DIR, { recursive: true });

  try {
    const rows = await fetchDetailRows();
    const summaryRows = canSummarizeInSql() ? await pg(buildSummaryQuery()) : summarizeByMarket(rows);
    const lenderRows = canSummarizeInSql() ? await pg(buildLenderSummaryQuery()) : summarizeByLender(rows);

  const scope = [
    approvedOnly ? "approved-markets" : "all-data",
    individualLendersOnly ? "individual-lenders" : null,
  ].filter(Boolean).join("-");
  const base = `second-lien-positions-${scope}-${timestamp}`;
  const csvPath = join(OUT_DIR, `${base}.csv`);
  const jsonPath = join(OUT_DIR, `${base}.json`);
  const mdPath = join(OUT_DIR, `${base}.md`);

  const payload = {
    generated_at: new Date().toISOString(),
    scope,
    include_historical: includeHistorical,
    individual_lenders_only: individualLendersOnly,
    state_filter: stateArg ?? null,
    row_count: rows.length,
    notes: [
      "Second lien rows use explicit mortgage_records.position = 2 when present.",
      "When explicit position is absent, open/current lien rows are ranked by existing position, recording date, and document number; computed rank 2 is included.",
      "Likely individual lender mode uses conservative lender-name heuristics and excludes common institutional/entity terms.",
      "MXRE currently stores lender name/type/code but not lender phone/email/address in mortgage_records; owner mailing and listing-agent contact fields are context only and are not treated as lienholder contact info.",
      "Interest rates may be recorded, matched, estimated, unknown, or null; use rate_source/rate_match_confidence before treating a rate as lender-stated.",
    ],
    summary_by_market: summaryRows,
    summary_by_lender: lenderRows,
    rows,
  };

  await writeFile(csvPath, toCsv(rows));
  await writeFile(jsonPath, JSON.stringify(payload, null, 2));
  await writeFile(mdPath, toMarkdown(payload));

    console.log(JSON.stringify({
      wrote: [csvPath, jsonPath, mdPath],
      row_count: rows.length,
      scope,
      include_historical: includeHistorical,
      individual_lenders_only: individualLendersOnly,
      state_filter: stateArg ?? null,
      top_markets: summaryRows.slice(0, 10),
      top_lenders: lenderRows.slice(0, 10),
    }, null, 2));
  } finally {
    await db.end();
  }
}

function baseLienCte() {
  const marketJoin = approvedOnly
    ? `join approved_markets am on am.state_code = p.state_code and am.city_upper = upper(coalesce(p.city, ''))`
    : `left join approved_markets am on am.state_code = p.state_code and am.city_upper = upper(coalesce(p.city, ''))`;
  const openFilter = includeHistorical ? "true" : "coalesce(m.open, true) = true";
  const stateFilter = activeStateFilter ? `and p.state_code = ${sql(activeStateFilter)}` : "";
  const idFilter = activeIdLower != null && activeIdUpper != null ? `and p.id >= ${activeIdLower} and p.id < ${activeIdUpper}` : "";
  const candidateStateFilter = activeStateFilter ? `and cp.state_code = ${sql(activeStateFilter)}` : "";
  const candidateIdFilter = activeIdLower != null && activeIdUpper != null ? `and cp.id >= ${activeIdLower} and cp.id < ${activeIdUpper}` : "";
  const useCandidatePropertyCte = individualLendersOnly && !approvedOnly;

  return `
    with approved_markets(id, label, city_upper, state_code) as (
      values ${APPROVED_MARKETS.map((m) => `(${sql(m.id)}, ${sql(m.label)}, ${sql(m.city)}, ${sql(m.state)})`).join(",\n             ")}
    ),
    latest_listing as (
      select distinct on (property_id)
        property_id,
        listing_agent_name,
        listing_agent_email,
        listing_agent_phone,
        listing_brokerage,
        listing_url,
        mls_list_price
      from listing_signals
      where property_id is not null
      order by property_id, is_on_market desc nulls last, last_seen_at desc nulls last, updated_at desc nulls last, id desc
    ),
    ${useCandidatePropertyCte ? `candidate_property_ids as (
      select distinct cm.property_id
      from properties cp
      join mortgage_records cm on cm.property_id = cp.id
      where (${likelyIndividualLenderSql()})
        and (cm.position = 2 or cm.position is null)
        ${candidateStateFilter}
        ${candidateIdFilter}
    ),` : ""}
    current_lien_rows as (
      select
        m.*,
        p.address as property_address,
        p.city as property_city,
        p.state_code as property_state,
        p.zip as property_zip,
        p.parcel_id,
        p.apn_formatted,
        p.owner_name,
        coalesce(nullif(p.mailing_address, ''), nullif(p.mail_address, '')) as owner_mailing_address,
        coalesce(nullif(p.mailing_city, ''), nullif(p.mail_city, '')) as owner_mailing_city,
        coalesce(nullif(p.mailing_state, ''), nullif(p.mail_state, '')) as owner_mailing_state,
        coalesce(nullif(p.mailing_zip, ''), nullif(p.mail_zip, '')) as owner_mailing_zip,
        coalesce(am.label, concat_ws(', ', nullif(p.city, ''), nullif(p.state_code, ''))) as market_label,
        ll.listing_agent_name,
        ll.listing_agent_email,
        ll.listing_agent_phone,
        ll.listing_brokerage,
        ll.listing_url,
        ll.mls_list_price,
        row_number() over (
          partition by m.property_id
          order by
            coalesce(m.position, 999999),
            m.recording_date nulls last,
            m.document_number nulls last,
            m.id
        ) as computed_position
      from properties p
      join mortgage_records m on m.property_id = p.id
      ${marketJoin}
      left join latest_listing ll on ll.property_id = p.id
      where ${openFilter}
        ${stateFilter}
        ${idFilter}
        ${useCandidatePropertyCte ? "and m.property_id in (select property_id from candidate_property_ids)" : ""}
        and not (
          upper(coalesce(m.document_type, '')) like '%SATISFACTION%'
          or upper(coalesce(m.document_type, '')) like '%RELEASE%'
          or upper(coalesce(m.document_type, '')) like '%DISCHARGE%'
          or upper(coalesce(m.document_type, '')) like '%ASSIGNMENT%'
          or upper(coalesce(m.document_type, '')) like '%DEED%'
        )
    ),
    second_liens as (
      select *,
        case
          when position = 2 then 2
          else computed_position
        end as lien_position,
        case
          when position = 2 then 'explicit'
          else 'computed'
        end as position_source,
        ${likelyIndividualLenderSql()} as is_likely_individual_lender
      from current_lien_rows
      where position = 2 or (position is null and computed_position = 2)
    )
  `;
}

function secondLienWhereClause() {
  return individualLendersOnly ? "where is_likely_individual_lender = true" : "";
}

function canSummarizeInSql() {
  return !individualLendersOnly || approvedOnly;
}

async function fetchDetailRows() {
  if (individualLendersOnly && !approvedOnly && !stateArg) {
    const states = await fetchStatesForBatches();
    const rows: Row[] = [];
    for (const state of states) {
      activeStateFilter = state;
      const batch = await fetchStateBatch(state);
      rows.push(...batch);
      console.log(JSON.stringify({ state, rows: batch.length, total_rows: rows.length }));
    }
    activeStateFilter = null;
    return rows;
  }
  if (individualLendersOnly && !approvedOnly && stateArg) {
    return fetchStateBatch(stateArg);
  }
  return pg(buildDetailQuery());
}

async function fetchStateBatch(state: string) {
  activeStateFilter = state;
  activeIdLower = null;
  activeIdUpper = null;
  try {
    return await pg(buildDetailQuery());
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    if (!message.includes("408") && !message.toLowerCase().includes("timeout")) throw error;
    console.log(JSON.stringify({ state, fallback: "property_id_windows", reason: message.slice(0, 120) }));
    return fetchStateByIdWindows(state);
  } finally {
    activeIdLower = null;
    activeIdUpper = null;
  }
}

async function fetchStateByIdWindows(state: string) {
  activeStateFilter = state;
  const rows: Row[] = [];
  for (let lower = 0; lower <= 530_000_000; lower += ID_BATCH_SIZE) {
    activeIdLower = lower;
    activeIdUpper = lower + ID_BATCH_SIZE;
    const batch = await pg(buildDetailQuery());
    if (batch.length) {
      rows.push(...batch);
      console.log(JSON.stringify({ state, id_lower: lower, id_upper: activeIdUpper, rows: batch.length, state_rows: rows.length }));
    }
  }
  activeIdLower = null;
  activeIdUpper = null;
  return rows;
}

async function fetchStatesForBatches() {
  return STATE_BATCHES;
}

function likelyIndividualLenderSql() {
  return `(
          nullif(trim(coalesce(lender_name, '')), '') is not null
          and upper(trim(lender_name)) not in ('NONE', 'UNKNOWN', 'N/A', 'NA', 'NULL')
          and (nullif(trim(coalesce(lender_type, '')), '') is null or lender_type ~* '(Private Party|Individual)')
          and (nullif(trim(coalesce(lender_code, '')), '') is null or upper(lender_code) = 'P')
          and lender_name !~* '(BANK|\\mBK\\M|BANCORP|MORTGAGE|\\mMTG\\M|MERS|REGISTRATION|SAVINGS|CREDIT|\\mCU\\M|\\mFCU\\M|FEDERAL|NATIONAL|TRUST COMPANY|TITLE|INSURANCE|SECRETARY|HOUSING|HUD|AUTHORITY|AGENCY|DEPARTMENT|TREASURY|FINANCE|FINANCIAL|SERVICING|SERVICE|SERVICES|LOAN|LENDING|CAPITAL|FUND|INVEST|INVESTMENT|PARTNERS|HOLDINGS|PROPERTIES|PROPERTY|REALTY|REAL ESTATE|ASSOCIATION|ASSN|CORPORATION|CORP|INCORPORATED|INC|LLC|L\\.L\\.C|\\mLP\\M|LTD|LIMITED|\\mCO\\M|COMPANY|TRUST|TRUSTEE|ESTATE|CHURCH|MINISTRIES|UNIVERSITY|COLLEGE|COUNTY|CITY OF|STATE OF|TOWNSHIP|SCHOOL|BOARD|FOUNDATION|CO ATTY|ATTY|BUREAU|WORKERS|REVENUE|GOODLEAP|SOLAR|MOSAIC|HOMES|FSLA|FLCA|ENTERPRISES|LAND REUTILIZATION|PROGRAM|OFFICE|GROUP|RATE|GUARANTEED|SEWER|DISTRICT|REGIONAL|ECONOMIC|SOLUTIONS|LUMBER)'
          and upper(lender_name) ~ '^[A-Z][A-Z''.-]+( [A-Z][A-Z''.-]+){1,4}$'
        )`;
}

function buildDetailQuery() {
  return `
    ${baseLienCte()}
    select
      lender_name as target_name,
      'second_lien_lender'::text as target_role,
      null::text as target_email,
      null::text as target_phone,
      'no_lender_contact_in_mxre'::text as target_contact_status,
      is_likely_individual_lender,
      property_id,
      id as mortgage_record_id,
      market_label,
      property_address,
      property_city,
      property_state,
      property_zip,
      parcel_id,
      apn_formatted,
      owner_name,
      owner_mailing_address,
      owner_mailing_city,
      owner_mailing_state,
      owner_mailing_zip,
      lien_position,
      position_source,
      document_type,
      loan_type,
      recording_date,
      document_number,
      book_page,
      coalesce(original_amount, loan_amount) as original_amount,
      estimated_current_balance,
      interest_rate,
      interest_rate_type,
      rate_source,
      rate_match_confidence,
      estimated_monthly_payment,
      maturity_date,
      lender_name,
      lender_type,
      lender_code,
      borrower_name,
      open,
      source_url,
      listing_agent_name,
      listing_agent_email,
      listing_agent_phone,
      listing_brokerage,
      listing_url,
      mls_list_price
    from second_liens
    ${secondLienWhereClause()}
    order by coalesce(estimated_current_balance, original_amount, loan_amount, 0) desc nulls last,
      property_state, property_city, property_address, recording_date nulls last, mortgage_record_id;
  `;
}

function buildSummaryQuery() {
  return `
    ${baseLienCte()}
    select
      market_label,
      count(*)::int as second_lien_count,
      count(distinct property_id)::int as property_count,
      sum(coalesce(estimated_current_balance, original_amount, loan_amount, 0))::bigint as total_amount_or_balance,
      avg(nullif(interest_rate, 0))::numeric(8,3) as avg_interest_rate,
      count(*) filter (where interest_rate is not null)::int as rows_with_interest_rate,
      count(*) filter (where rate_source = 'recorded' or interest_rate_type = 'actual')::int as rows_with_recorded_rate_signal,
      count(*) filter (where nullif(lender_name, '') is not null)::int as rows_with_lender_name,
      count(*) filter (where nullif(listing_agent_email, '') is not null or nullif(listing_agent_phone, '') is not null)::int as rows_with_listing_agent_contact,
      count(*) filter (where nullif(owner_mailing_address, '') is not null)::int as rows_with_owner_mailing
    from second_liens
    ${secondLienWhereClause()}
    group by market_label
    order by second_lien_count desc, total_amount_or_balance desc nulls last, market_label;
  `;
}

function buildLenderSummaryQuery() {
  return `
    ${baseLienCte()}
    select
      coalesce(nullif(lender_name, ''), 'UNKNOWN') as lender_name,
      count(*)::int as second_lien_count,
      count(distinct property_id)::int as property_count,
      sum(coalesce(estimated_current_balance, original_amount, loan_amount, 0))::bigint as total_amount_or_balance,
      avg(nullif(interest_rate, 0))::numeric(8,3) as avg_interest_rate,
      count(*) filter (where interest_rate is not null)::int as rows_with_interest_rate,
      string_agg(distinct nullif(lender_type, ''), '; ' order by nullif(lender_type, '')) as lender_types,
      string_agg(distinct nullif(lender_code, ''), '; ' order by nullif(lender_code, '')) as lender_codes
    from second_liens
    ${secondLienWhereClause()}
    group by coalesce(nullif(lender_name, ''), 'UNKNOWN')
    order by second_lien_count desc, total_amount_or_balance desc nulls last, lender_name
    limit 100;
  `;
}

async function pg<T extends Row = Row>(query: string): Promise<T[]> {
  if (!db) throw new Error("DB client is not initialized.");
  const result = await db.query<T>(query);
  return result.rows;
}

function summarizeByMarket(rows: Row[]) {
  const grouped = new Map<string, Row[]>();
  for (const row of rows) {
    const key = String(row.market_label || "UNKNOWN");
    grouped.set(key, [...(grouped.get(key) ?? []), row]);
  }
  return [...grouped.entries()].map(([market_label, group]) => ({
    market_label,
    second_lien_count: group.length,
    property_count: new Set(group.map((row) => row.property_id).filter(Boolean)).size,
    total_amount_or_balance: sumAmount(group),
    avg_interest_rate: avgRate(group),
    rows_with_interest_rate: group.filter((row) => row.interest_rate != null).length,
    rows_with_recorded_rate_signal: group.filter((row) => row.rate_source === "recorded" || row.interest_rate_type === "actual").length,
    rows_with_lender_name: group.filter((row) => String(row.lender_name ?? "").trim()).length,
    rows_with_listing_agent_contact: group.filter((row) => String(row.listing_agent_email ?? "").trim() || String(row.listing_agent_phone ?? "").trim()).length,
    rows_with_owner_mailing: group.filter((row) => String(row.owner_mailing_address ?? "").trim()).length,
  })).sort((a, b) =>
    Number(b.second_lien_count) - Number(a.second_lien_count)
    || Number(b.total_amount_or_balance) - Number(a.total_amount_or_balance)
    || String(a.market_label).localeCompare(String(b.market_label))
  );
}

function summarizeByLender(rows: Row[]) {
  const grouped = new Map<string, Row[]>();
  for (const row of rows) {
    const key = String(row.lender_name || "UNKNOWN");
    grouped.set(key, [...(grouped.get(key) ?? []), row]);
  }
  return [...grouped.entries()].map(([lender_name, group]) => ({
    lender_name,
    second_lien_count: group.length,
    property_count: new Set(group.map((row) => row.property_id).filter(Boolean)).size,
    total_amount_or_balance: sumAmount(group),
    avg_interest_rate: avgRate(group),
    rows_with_interest_rate: group.filter((row) => row.interest_rate != null).length,
    lender_types: distinctJoined(group.map((row) => row.lender_type)),
    lender_codes: distinctJoined(group.map((row) => row.lender_code)),
  })).sort((a, b) =>
    Number(b.second_lien_count) - Number(a.second_lien_count)
    || Number(b.total_amount_or_balance) - Number(a.total_amount_or_balance)
    || String(a.lender_name).localeCompare(String(b.lender_name))
  ).slice(0, 100);
}

function sumAmount(rows: Row[]) {
  return rows.reduce((sum, row) => {
    const amount = Number(row.estimated_current_balance ?? row.original_amount ?? row.loan_amount ?? 0);
    return sum + (Number.isFinite(amount) ? amount : 0);
  }, 0);
}

function avgRate(rows: Row[]) {
  const rates = rows.map((row) => Number(row.interest_rate)).filter((rate) => Number.isFinite(rate) && rate !== 0);
  if (!rates.length) return null;
  return Math.round((rates.reduce((sum, rate) => sum + rate, 0) / rates.length) * 1000) / 1000;
}

function distinctJoined(values: unknown[]) {
  const items = [...new Set(values.map((value) => String(value ?? "").trim()).filter(Boolean))].sort();
  return items.length ? items.join("; ") : null;
}

function toCsv(rows: Row[]) {
  return [
    COLUMNS.join(","),
    ...rows.map((row) => COLUMNS.map((column) => csvCell(row[column])).join(",")),
  ].join("\n");
}

function toMarkdown(payload: {
  generated_at: string;
  scope: string;
  include_historical: boolean;
  individual_lenders_only: boolean;
  row_count: number;
  notes: string[];
  summary_by_market: Row[];
  summary_by_lender: Row[];
  rows: Row[];
}) {
  const marketRows = payload.summary_by_market.map((row) =>
    `| ${cell(row.market_label)} | ${cell(row.second_lien_count)} | ${cell(row.property_count)} | ${money(row.total_amount_or_balance)} | ${rate(row.avg_interest_rate)} | ${cell(row.rows_with_interest_rate)} | ${cell(row.rows_with_recorded_rate_signal)} | ${cell(row.rows_with_lender_name)} | ${cell(row.rows_with_owner_mailing)} | ${cell(row.rows_with_listing_agent_contact)} |`
  );
  const lenderRows = payload.summary_by_lender.slice(0, 25).map((row) =>
    `| ${cell(row.lender_name)} | ${cell(row.second_lien_count)} | ${cell(row.property_count)} | ${money(row.total_amount_or_balance)} | ${rate(row.avg_interest_rate)} | ${cell(row.rows_with_interest_rate)} | ${cell(row.lender_types)} | ${cell(row.lender_codes)} |`
  );
  const sampleRows = payload.rows.slice(0, 25).map((row) =>
    `| ${cell(row.market_label)} | ${cell(row.property_address)} | ${cell(row.target_name)} | ${cell(row.target_contact_status)} | ${money(row.original_amount)} | ${money(row.estimated_current_balance)} | ${rate(row.interest_rate)} | ${cell(row.rate_source ?? row.interest_rate_type)} | ${cell(row.owner_name)} | ${cell(row.listing_agent_email ?? row.listing_agent_phone)} |`
  );

  return `# MXRE Second Lien Position Report

Generated: ${payload.generated_at}

Scope: ${payload.scope}
Historical included: ${payload.include_historical ? "yes" : "no"}
Individual lenders only: ${payload.individual_lenders_only ? "yes" : "no"}
Rows: ${payload.row_count.toLocaleString()}

## Notes

${payload.notes.map((note) => `- ${note}`).join("\n")}

## Market Summary

| Market | 2nd liens | Properties | Amount / balance | Avg rate | Rows with rate | Recorded-rate signal | Lender name | Owner mailing | Listing-agent contact |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
${marketRows.join("\n")}

## Top Lenders

| Lender | 2nd liens | Properties | Amount / balance | Avg rate | Rows with rate | Types | Codes |
|---|---:|---:|---:|---:|---:|---|---|
${lenderRows.join("\n")}

## Sample Rows

| Market | Address | Target name | Target contact status | Original amount | Est. balance | Rate | Rate source | Owner | Context contact |
|---|---|---|---|---:|---:|---:|---|---|---|
${sampleRows.join("\n")}
`;
}

function csvCell(value: unknown) {
  if (value == null) return "";
  const text = value instanceof Date ? value.toISOString() : String(value);
  return /[",\n\r]/.test(text) ? `"${text.replace(/"/g, '""')}"` : text;
}

function sql(value: string) {
  return `'${value.replace(/'/g, "''")}'`;
}

function cell(value: unknown) {
  if (value == null || value === "") return "";
  return String(value).replace(/\|/g, "\\|").replace(/\n/g, " ");
}

function money(value: unknown) {
  const n = Number(value);
  if (!Number.isFinite(n) || n === 0) return "";
  return `$${Math.round(n).toLocaleString()}`;
}

function rate(value: unknown) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "";
  return `${n.toFixed(3)}%`;
}

main().catch((error) => {
  console.error("Fatal second-lien report error:", error instanceof Error ? error.message : error);
  process.exit(1);
});
