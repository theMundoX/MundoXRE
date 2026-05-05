#!/usr/bin/env tsx
import "dotenv/config";

const PG_URL = `${(process.env.SUPABASE_URL ?? "").replace(/\/$/, "")}/pg/query`;
const PG_KEY = process.env.SUPABASE_SERVICE_KEY ?? "";
const DRY_RUN = process.argv.includes("--dry-run");

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

async function main() {
  console.log("MXRE - Normalize Dallas PublicSearch recorder rows");
  console.log(`Dry run: ${DRY_RUN}`);

  const [before] = await pg(`
    select count(*)::int as total,
           count(*) filter (where coalesce(document_type,'') = '')::int as blank_type,
           count(*) filter (where coalesce(document_type,'') = '' and nullif(book_page,'') is not null)::int as recoverable_type
      from mortgage_records
     where source_url ilike '%dallas.tx.publicsearch.us%';
  `);

  if (!DRY_RUN) {
    await pg(`
      update mortgage_records
         set raw = coalesce(raw, '{}'::jsonb) || jsonb_build_object(
               'dallasPublicSearchNormalize',
               jsonb_build_object(
                 'previousDocumentType', document_type,
                 'previousDocumentNumber', document_number,
                 'previousBookPage', book_page,
                 'observedAt', now()
               )
             ),
             document_type = case
               when upper(book_page) like '%DEED OF TRUST%' or upper(book_page) = 'DOT' then 'mortgage'
               when upper(book_page) like '%MORTGAGE%' and upper(book_page) not like '%RELEASE%' and upper(book_page) not like '%ASSIGNMENT%' then 'mortgage'
               when upper(book_page) like '%STATE TAX LIEN%' or upper(book_page) like '%TAX%LIEN%' then 'tax_lien'
               when upper(book_page) like '%MECHANIC%' or upper(book_page) like '%MATERIALMAN%' then 'mechanics_lien'
               when upper(book_page) like '%JUDG%LIEN%' or upper(book_page) like '%ABSTRACT OF JUDG%' then 'judgment_lien'
               when upper(book_page) like '%LIEN%' then 'lien'
               when upper(book_page) like '%RELEASE%' or upper(book_page) like '%SATISFACTION%' then 'satisfaction'
               when upper(book_page) like '%ASSIGN%' then 'assignment'
               when upper(book_page) like '%WARRANTY DEED%' then 'deed'
               when upper(book_page) like '%QUIT%CLAIM%' then 'deed'
               when upper(book_page) like '%DEED%' then 'deed'
               when upper(book_page) like '%FINANCING STATEMENT%' or upper(book_page) like 'UCC%' then 'financing_statement'
               else lower(book_page)
             end,
             loan_type = case
               when (upper(book_page) like '%DEED OF TRUST%' or upper(book_page) like '%MORTGAGE%') and loan_type is null then 'purchase'
               else loan_type
             end,
             open = case
               when upper(book_page) like '%RELEASE%' or upper(book_page) like '%SATISFACTION%' or upper(book_page) like '%ASSIGN%' then false
               when upper(book_page) like '%LIEN%' or upper(book_page) like '%DEED OF TRUST%' or upper(book_page) like '%MORTGAGE%' then true
               else open
             end,
             borrower_name = coalesce(nullif(borrower_name,''), nullif(document_number,'')),
             document_number = null,
             book_page = null
       where source_url ilike '%dallas.tx.publicsearch.us%'
         and coalesce(document_type,'') = ''
         and nullif(book_page,'') is not null;
    `);
  }

  const [after] = await pg(`
    select count(*)::int as total,
           count(*) filter (where coalesce(document_type,'') = '')::int as blank_type,
           count(*) filter (where document_type in ('mortgage','lien','tax_lien','mechanics_lien','judgment_lien'))::int as debt_signal_docs,
           count(*) filter (where property_id is not null)::int as linked_docs,
           count(distinct property_id) filter (where property_id is not null)::int as linked_properties
      from mortgage_records
     where source_url ilike '%dallas.tx.publicsearch.us%';
  `);

  console.log(JSON.stringify({ before, after }, null, 2));
}

main().catch(error => {
  console.error("Fatal:", error instanceof Error ? error.message : error);
  process.exit(1);
});
