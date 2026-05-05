#!/usr/bin/env tsx
import "dotenv/config";
import { mkdir, writeFile } from "node:fs/promises";
import { join } from "node:path";

const PG_URL = `${(process.env.SUPABASE_URL ?? "").replace(/\/$/, "")}/pg/query`;
const PG_KEY = process.env.SUPABASE_SERVICE_KEY ?? "";
const OUT = process.argv.find(a => a.startsWith("--out="))?.split("=").slice(1).join("=")
  ?? "logs/market-refresh/dallas-coverage-dashboard.html";

type Row = Record<string, unknown>;

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

const n = (value: unknown) => Number(value ?? 0);
const fmt = (value: unknown) => Math.round(n(value)).toLocaleString();
const money = (value: unknown) => n(value) > 0 ? `$${Math.round(n(value)).toLocaleString()}` : "-";
const moneyOr = (value: unknown, fallback: string) => n(value) > 0 ? money(value) : fallback;
const pct = (value: unknown, total: unknown) => {
  const denominator = n(total);
  if (denominator <= 0) return "0.00%";
  return `${Math.round((n(value) / denominator) * 10000) / 100}%`;
};
const esc = (value: unknown) => String(value ?? "")
  .replace(/&/g, "&amp;")
  .replace(/</g, "&lt;")
  .replace(/>/g, "&gt;")
  .replace(/"/g, "&quot;");

function bar(label: string, value: unknown, total: unknown) {
  const p = total ? Math.min(100, Math.round((n(value) / n(total)) * 10000) / 100) : 0;
  const klass = p >= 85 ? "" : p >= 25 ? " warn" : " bad";
  return `<div class="row"><div>${esc(label)}</div><div class="track"><div class="fill${klass}" style="width:${p}%"></div></div><strong>${p}%</strong></div>`;
}

async function main() {
  const [listings] = await pg(`
    select count(*)::int as total,
           count(distinct property_id)::int as active_properties,
           count(*) filter (where property_id is null)::int as unlinked_listings,
           count(distinct listing_url)::int as distinct_listing_urls,
           count(*) filter (where mls_list_price is not null)::int as price,
           count(*) filter (where nullif(listing_agent_name,'') is not null)::int as agent_name,
           count(*) filter (where nullif(listing_agent_first_name,'') is not null and nullif(listing_agent_last_name,'') is not null)::int as first_last,
           count(*) filter (where nullif(listing_agent_phone,'') is not null)::int as phone,
           count(*) filter (
             where nullif(listing_agent_email,'') is not null
               and (
                 agent_contact_source = 'realestateapi'
                 or agent_contact_confidence = 'public_profile_verified'
               )
           )::int as email,
           count(*) filter (where nullif(listing_brokerage,'') is not null)::int as brokerage,
           count(*) filter (where raw ? 'redfinDetail')::int as redfin_detail,
           count(*) filter (where raw #>> '{redfinDetail,publicRemarks}' is not null)::int as mls_description,
           count(*) filter (where creative_finance_status is not null)::int as creative_evaluated,
           count(*) filter (where creative_finance_status = 'positive')::int as creative_positive,
           count(*) filter (where creative_finance_status = 'negative')::int as creative_negative,
           count(*) filter (where creative_finance_status = 'no_data')::int as creative_no_data
      from listing_signals
     where is_on_market = true
       and state_code = 'TX'
       and upper(coalesce(city,'')) = 'DALLAS';
  `);

  const [parcels] = await pg(`
    select count(*)::int as total,
           count(*) filter (where parcel_id is not null and parcel_id <> '')::int as parcel_id,
           count(*) filter (where asset_type is not null)::int as asset_type,
           count(*) filter (where owner_name is not null)::int as owner,
           count(*) filter (where coalesce(market_value, assessed_value, taxable_value, 0) > 0)::int as value,
           count(*) filter (where total_units is not null)::int as total_units,
           count(*) filter (where year_built is not null)::int as year_built,
           count(*) filter (where total_sqft is not null or living_sqft is not null)::int as sqft
      from properties
     where county_id = 7
       and state_code = 'TX'
       and upper(coalesce(city,'')) like '%DALLAS%';
  `);
  const [countyParcels] = await pg(`
    select count(*)::int as total,
           count(*) filter (where parcel_id is not null and parcel_id <> '')::int as parcel_id,
           count(distinct upper(coalesce(city,'')))::int as cities
      from properties
     where county_id = 7
       and state_code = 'TX';
  `);

  const [events] = await pg(`
    select count(*)::int as total,
           count(*) filter (where event_type = 'price_changed')::int as price_changed,
           count(*) filter (where event_type = 'listed')::int as listed,
           max(event_at)::text as latest_event
      from listing_signal_events
     where state_code = 'TX'
       and upper(coalesce(city,'')) = 'DALLAS';
  `);

  const [recorder] = await pg(`
    select count(*)::int as source_docs,
           count(*) filter (where document_type = 'mortgage')::int as mortgage_docs,
           count(*) filter (where document_type in ('lien','tax_lien','mechanics_lien','judgment_lien'))::int as lien_docs,
           count(*) filter (where document_type in ('mortgage','lien','tax_lien','mechanics_lien','judgment_lien'))::int as debt_docs,
           count(*) filter (where property_id is not null)::int as linked_docs,
           count(distinct property_id) filter (where property_id is not null)::int as linked_properties,
           count(*) filter (where coalesce(loan_amount, original_amount, estimated_current_balance, 0) > 0)::int as amount_docs,
           count(*) filter (where coalesce(estimated_monthly_payment, 0) > 0)::int as payment_docs,
           sum(coalesce(loan_amount, original_amount, estimated_current_balance, 0))::numeric as total_recorded_amount,
           sum(coalesce(estimated_monthly_payment, 0))::numeric as total_estimated_payment,
           max(recording_date)::text as latest_recording
      from mortgage_records
     where source_url ilike '%dallas.tx.publicsearch.us%';
  `);

  const [rent] = await pg(`
    select count(distinct pw.property_id)::int as website_properties
      from property_websites pw
      join properties p on p.id = pw.property_id
     where pw.active = true
       and p.county_id = 7
       and p.state_code = 'TX'
       and upper(coalesce(p.city,'')) like '%DALLAS%';
  `);
  const [floorplans] = await pg(`
    select count(distinct fp.property_id)::int as properties,
           count(*)::int as rows
      from floorplans fp
      join properties p on p.id = fp.property_id
     where p.county_id = 7
       and p.state_code = 'TX'
       and upper(coalesce(p.city,'')) like '%DALLAS%';
  `);
  const [rents] = await pg(`
    select count(distinct rs.property_id)::int as properties,
           count(*)::int as rows,
           count(*) filter (where rs.observed_at >= now() - interval '1 day')::int as fresh_rows,
           count(*) filter (where coalesce(rs.effective_rent, rs.asking_rent) > 0)::int as rent_amount_rows,
           count(*) filter (where rs.rent_per_door > 0)::int as rent_per_door_rows,
           count(*) filter (where rs.total_monthly_rent > 0)::int as total_monthly_rows,
           max(rs.observed_at)::text as latest_observed,
           percentile_cont(0.5) within group (order by coalesce(rs.effective_rent, rs.asking_rent))::numeric as median_rent,
           percentile_cont(0.5) within group (order by rs.rent_per_door)::numeric as median_rent_per_door,
           sum(coalesce(rs.total_monthly_rent, 0))::numeric as reported_total_monthly_rent
      from rent_snapshots rs
      join properties p on p.id = rs.property_id
     where p.county_id = 7
       and p.state_code = 'TX'
       and upper(coalesce(p.city,'')) like '%DALLAS%'
       and rs.floorplan_id is not null
       and coalesce(rs.raw->>'source', '') <> 'estimated'
       and coalesce(rs.raw->>'source', '') <> 'estimated_v2_fixed';
  `);

  const lienSamples = await pg(`
    select document_type, recording_date::text, borrower_name, lender_name,
           coalesce(loan_amount, original_amount, estimated_current_balance, 0)::numeric as amount,
           estimated_monthly_payment,
           open,
           source_url
      from mortgage_records
     where source_url ilike '%dallas.tx.publicsearch.us%'
       and document_type in ('mortgage','lien','tax_lien','mechanics_lien','judgment_lien')
     order by recording_date desc nulls last
     limit 12;
  `);

  const rentSamples = await pg(`
    select coalesce(cp.complex_name, p.address) as complex_name,
           p.address,
           fp.name as floorplan,
           rs.beds,
           rs.baths,
           rs.sqft,
           coalesce(rs.effective_rent, rs.asking_rent)::numeric as rent,
           rs.rent_per_door,
           rs.estimated_unit_count,
           rs.rent_unit_basis,
           rs.total_monthly_rent,
           rs.observed_at::text as observed_at
      from rent_snapshots rs
      join properties p on p.id = rs.property_id
      left join floorplans fp on fp.id = rs.floorplan_id
      left join property_complex_profiles cp on cp.property_id = p.id
     where p.county_id = 7
       and p.state_code = 'TX'
       and upper(coalesce(p.city,'')) like '%DALLAS%'
       and rs.floorplan_id is not null
       and coalesce(rs.raw->>'source', '') <> 'estimated'
       and coalesce(rs.raw->>'source', '') <> 'estimated_v2_fixed'
     order by rs.observed_at desc nulls last
     limit 12;
  `);

  const html = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>MXRE Dallas Coverage Dashboard</title>
  <style>
    :root { color-scheme: light; font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; background:#f5f7f9; color:#16212f; }
    * { box-sizing:border-box; } body { margin:0; background:#f5f7f9; } header { padding:26px 32px 18px; border-bottom:1px solid #d8e0e8; background:#fff; }
    .eyebrow { color:#5a6b7e; font-size:13px; font-weight:700; text-transform:uppercase; } h1 { margin:5px 0 4px; font-size:30px; letter-spacing:0; } .sub,.note { color:#5a6b7e; font-size:13px; line-height:1.35; }
    main { padding:24px 32px 36px; } .grid { display:grid; grid-template-columns:repeat(4,minmax(170px,1fr)); gap:14px; } .card,.panel { background:#fff; border:1px solid #d8e0e8; border-radius:8px; padding:16px; }
    .card { min-height:116px; } .label { color:#5a6b7e; font-size:13px; font-weight:700; } .metric { margin-top:9px; font-size:30px; line-height:1; font-weight:800; }
    section { margin-top:22px; } h2 { margin:0 0 12px; font-size:18px; letter-spacing:0; } .bars { display:grid; gap:12px; } .row { display:grid; grid-template-columns:210px 1fr 76px; gap:12px; align-items:center; font-size:14px; }
    .track { height:12px; border-radius:999px; background:#e7edf3; overflow:hidden; } .fill { height:100%; background:#2f7d70; border-radius:999px; } .fill.warn { background:#c27803; } .fill.bad { background:#b53b45; }
    .two { display:grid; grid-template-columns:minmax(0,1fr) minmax(0,1fr); gap:14px; } table { width:100%; border-collapse:collapse; font-size:14px; } th,td { padding:10px 8px; border-bottom:1px solid #e4eaf0; text-align:left; vertical-align:top; } th { color:#5a6b7e; font-size:12px; text-transform:uppercase; }
    code { font-family: ui-monospace, SFMono-Regular, Consolas, monospace; font-size:12px; } @media (max-width:960px){ .grid,.two{grid-template-columns:1fr;} .row{grid-template-columns:150px 1fr 62px;} header,main{padding-left:18px;padding-right:18px;} }
  </style>
</head>
<body>
  <header>
    <div class="eyebrow">MXRE Market Coverage</div>
    <h1>Dallas, TX</h1>
    <div class="sub">Live coverage snapshot generated ${esc(new Date().toLocaleString())}. This file is regenerated by <code>scripts/generate-dallas-coverage-dashboard.ts</code>.</div>
  </header>
  <main>
    <div class="grid">
      <div class="card"><div class="label">Active Listing Rows</div><div class="metric">${fmt(listings.total)}</div><div class="note">Redfin-derived rows: ${fmt(listings.active_properties)} linked properties; ${fmt(listings.unlinked_listings)} rows still need property matching. MLS/list price ${pct(listings.price, listings.total)}.</div></div>
      <div class="card"><div class="label">Parcel Universe</div><div class="metric">${fmt(parcels.total)}</div><div class="note">Dallas city situs rows. Full Dallas County loaded: ${fmt(countyParcels.total)} parcels across ${fmt(countyParcels.cities)} city labels.</div></div>
      <div class="card"><div class="label">Recorded Debt Signals</div><div class="metric">${fmt(recorder.debt_docs)}</div><div class="note">${fmt(recorder.mortgage_docs)} mortgages, ${fmt(recorder.lien_docs)} liens; ${fmt(recorder.linked_properties)} linked properties.</div></div>
      <div class="card"><div class="label">Fresh Rent/Floorplans</div><div class="metric">${fmt(floorplans.rows)}</div><div class="note">${fmt(rents.fresh_rows)} rent rows observed today; latest ${esc(rents.latest_observed)}.</div></div>
    </div>

    <section class="two">
      <div class="panel"><h2>Listing & Agent Coverage</h2><div class="bars">
        ${bar("MLS/list price", listings.price, listings.total)}
        ${bar("MLS description saved", listings.mls_description, listings.total)}
        ${bar("Agent name", listings.agent_name, listings.total)}
        ${bar("Agent first / last", listings.first_last, listings.total)}
        ${bar("Agent phone", listings.phone, listings.total)}
        ${bar("Brokerage", listings.brokerage, listings.total)}
        ${bar("Agent email", listings.email, listings.total)}
      </div></div>
      <div class="panel"><h2>Parcel & Property Coverage</h2><div class="bars">
        ${bar("Parcel id", parcels.parcel_id, parcels.total)}
        ${bar("Asset type", parcels.asset_type, parcels.total)}
        ${bar("Owner", parcels.owner, parcels.total)}
        ${bar("Assessed/market value", parcels.value, parcels.total)}
        ${bar("Unit count", parcels.total_units, parcels.total)}
        ${bar("Year built", parcels.year_built, parcels.total)}
        ${bar("Sqft", parcels.sqft, parcels.total)}
      </div></div>
    </section>

    <section class="panel">
      <h2>Coverage Detail</h2>
      <table>
        <tr><th>Metric</th><th>Count</th><th>Coverage / Yield</th><th>Notes</th></tr>
        <tr><td>Creative evaluated</td><td>${fmt(listings.creative_evaluated)} / ${fmt(listings.total)}</td><td>${pct(listings.creative_evaluated, listings.total)}</td><td>Every evaluated row stores <code>creative_finance_status</code>. <code>no_data</code> means description was checked and no clear signal was found.</td></tr>
        <tr><td>Creative hits</td><td>${fmt(listings.creative_positive)} positive / ${fmt(listings.creative_negative)} negative</td><td>Signal yield ${pct(n(listings.creative_positive) + n(listings.creative_negative), listings.total)}</td><td>Coverage is the evaluated row above. MLS descriptions saved: ${fmt(listings.mls_description)}.</td></tr>
        <tr><td>Price-change tracking</td><td>${fmt(events.price_changed)} price changes / ${fmt(events.total)} events</td><td>-</td><td>Latest event: ${esc(events.latest_event)}.</td></tr>
        <tr><td>Listing source scope</td><td>${fmt(listings.total)} active rows / ${fmt(listings.distinct_listing_urls)} distinct URLs</td><td>-</td><td>Current Dallas on-market coverage is Redfin-derived listing rows, not an authoritative full MLS inventory count.</td></tr>
        <tr><td>Listing-property matching</td><td>${fmt(n(listings.total) - n(listings.unlinked_listings))} linked rows / ${fmt(listings.unlinked_listings)} unlinked rows</td><td>${pct(n(listings.total) - n(listings.unlinked_listings), listings.total)}</td><td>Unlinked rows are active listings that have not been matched to an MXRE <code>property_id</code>; they should not be counted as unique properties.</td></tr>
        <tr><td>County parcel universe</td><td>${fmt(countyParcels.total)} Dallas County parcels</td><td>${pct(countyParcels.parcel_id, countyParcels.total)}</td><td>The headline parcel card is the Dallas city situs subset, not all Dallas County cities.</td></tr>
        <tr><td>Recorder source docs</td><td>${fmt(recorder.source_docs)}</td><td>-</td><td>Dallas County PublicSearch rows normalized into typed documents.</td></tr>
        <tr><td>Recorded liens/debt</td><td>${fmt(recorder.debt_docs)}</td><td>${pct(recorder.debt_docs, recorder.source_docs)}</td><td>${fmt(recorder.amount_docs)} rows include amount/balance data; ${fmt(recorder.payment_docs)} include estimated monthly payment.</td></tr>
        <tr><td>Recorder linked properties</td><td>${fmt(recorder.linked_properties)}</td><td>${pct(recorder.linked_properties, parcels.total)}</td><td>Still the biggest debt gap: linking needs legal/address-level matching beyond owner name.</td></tr>
        <tr><td>Apartment websites</td><td>${fmt(rent.website_properties)}</td><td>-</td><td>Public/free discovery only.</td></tr>
        <tr><td>Floorplans</td><td>${fmt(floorplans.rows)} rows / ${fmt(floorplans.properties)} properties</td><td>-</td><td>By bed type where source pages expose it.</td></tr>
        <tr><td>Rent snapshots</td><td>${fmt(rents.rent_amount_rows)} rent rows / ${fmt(rents.properties)} properties</td><td>${pct(rents.rent_amount_rows, rents.rows)}</td><td>Public apartment sites usually report per-unit floorplan rent. Whole-building monthly rent is only shown when the source reports unit counts: ${fmt(rents.total_monthly_rows)} rows, ${money(rents.reported_total_monthly_rent)} reported total/mo.</td></tr>
      </table>
    </section>

    <section class="two">
      <div class="panel">
        <h2>Recorded Lien / Debt Samples</h2>
        <table><tr><th>Type</th><th>Date</th><th>Borrower</th><th>Lender</th><th>Amount</th><th>Payment</th></tr>
          ${lienSamples.map(r => `<tr><td>${esc(r.document_type)}</td><td>${esc(r.recording_date)}</td><td>${esc(r.borrower_name)}</td><td>${esc(r.lender_name)}</td><td>${money(r.amount)}</td><td>${money(r.estimated_monthly_payment)}</td></tr>`).join("")}
        </table>
      </div>
      <div class="panel">
        <h2>Rent / Floorplan Samples</h2>
        <table><tr><th>Complex</th><th>Floorplan</th><th>Unit</th><th>Per-unit rent</th><th>Total/mo</th><th>Observed</th></tr>
          ${rentSamples.map(r => `<tr><td>${esc(r.complex_name)}<div class="note">${esc(r.address)}</div></td><td>${esc(r.floorplan)}</td><td>${esc(r.beds)} bd / ${esc(r.baths)} ba / ${esc(r.sqft)} sf</td><td>${money(r.rent)}<div class="note">${esc(r.rent_unit_basis || "per_unit")}</div></td><td>${moneyOr(r.total_monthly_rent, "Unit count not reported")}<div class="note">${n(r.estimated_unit_count) > 0 ? `${fmt(r.estimated_unit_count)} units` : ""}</div></td><td>${esc(r.observed_at)}</td></tr>`).join("")}
        </table>
      </div>
    </section>

    <section class="panel">
      <h2>Rerun Commands</h2>
      <table>
        <tr><th>Purpose</th><th>Command</th></tr>
        <tr><td>Full Dallas refresh</td><td><code>npm run market:dallas:refresh</code></td></tr>
        <tr><td>Regenerate dashboard only</td><td><code>npx tsx scripts/generate-dallas-coverage-dashboard.ts</code></td></tr>
        <tr><td>Recorder normalize</td><td><code>npx tsx scripts/normalize-dallas-publicsearch-recorder.ts</code></td></tr>
        <tr><td>Floorplan/rent scrape</td><td><code>npx tsx scripts/scrape-rents-bulk.ts --city=Dallas --state=TX --county_id=7 --limit=100</code></td></tr>
      </table>
    </section>
  </main>
</body>
</html>`;

  await mkdir(join(process.cwd(), "logs", "market-refresh"), { recursive: true });
  await writeFile(join(process.cwd(), OUT), html, "utf8");
  console.log(JSON.stringify({ wrote: OUT, generated_at: new Date().toISOString() }, null, 2));
}

main().catch(error => {
  console.error("Fatal:", error instanceof Error ? error.message : error);
  process.exit(1);
});
