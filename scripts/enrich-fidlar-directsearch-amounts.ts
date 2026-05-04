#!/usr/bin/env tsx
import "dotenv/config";
import { Client } from "pg";
import { computeMortgageFields } from "../src/utils/mortgage-calc.js";

const args = process.argv.slice(2);
const valueArg = (name: string) => {
  const prefix = `--${name}=`;
  return args.find((arg) => arg.startsWith(prefix))?.slice(prefix.length) ?? null;
};

const dryRun = args.includes("--dry-run");
const limit = Math.min(Math.max(Number(valueArg("limit") ?? "500"), 1), 5000);
const maxRunMs = Number(valueArg("max-run-ms") ?? "0");
const sourceUrl = valueArg("source-url") ?? "https://inmarion.fidlar.com/INMarion/DirectSearch/";
const databaseUrl = process.env.MXRE_DIRECT_PG_URL ?? process.env.DATABASE_URL ?? process.env.POSTGRES_URL;

if (!databaseUrl) throw new Error("Set MXRE_DIRECT_PG_URL, DATABASE_URL, or POSTGRES_URL.");

type MortgageRow = {
  id: number;
  document_number: string;
  recording_date: string | null;
  source_url: string;
};

type DirectSearchDoc = {
  ConsiderationAmount?: number;
  LegalSummary?: string;
  Legals?: Array<{ Description?: string }>;
};

async function getApiBase(baseUrl: string): Promise<string> {
  const resp = await fetch(`${baseUrl.replace(/\/?$/, "/")}appConfig.json`, { signal: AbortSignal.timeout(15_000) });
  if (!resp.ok) throw new Error(`appConfig fetch failed: ${resp.status}`);
  const config = await resp.json() as { webApiBase?: string };
  if (!config.webApiBase) throw new Error("DirectSearch appConfig missing webApiBase.");
  return config.webApiBase;
}

async function getToken(apiBase: string): Promise<string> {
  const resp = await fetch(`${apiBase}token`, {
    method: "POST",
    headers: { "content-type": "application/x-www-form-urlencoded" },
    body: "grant_type=password&username=anonymous&password=anonymous",
    signal: AbortSignal.timeout(15_000),
  });
  if (!resp.ok) throw new Error(`token failed: ${resp.status}`);
  const body = await resp.json() as { access_token?: string };
  if (!body.access_token) throw new Error("token response missing access_token.");
  return body.access_token;
}

async function lookupDocument(apiBase: string, token: string, documentNumber: string): Promise<DirectSearchDoc | null> {
  const resp = await fetch(`${apiBase}breeze/Search`, {
    method: "POST",
    headers: { "content-type": "application/json", authorization: `Bearer ${token}` },
    body: JSON.stringify({ DocumentName: documentNumber }),
    signal: AbortSignal.timeout(45_000),
  });
  if (!resp.ok) throw new Error(`document lookup failed ${resp.status}`);
  const body = await resp.json() as { DocResults?: DirectSearchDoc[] };
  return body.DocResults?.[0] ?? null;
}

function legalDescription(doc: DirectSearchDoc): string | null {
  const legal = doc.Legals?.map((item) => item.Description).filter(Boolean).join("; ") || doc.LegalSummary || "";
  return legal.trim() || null;
}

async function main() {
  const startedAt = Date.now();
  const client = new Client({ connectionString: databaseUrl });
  await client.connect();
  await client.query("set max_parallel_workers_per_gather = 0");

  try {
    const apiBase = await getApiBase(sourceUrl);
    let token = await getToken(apiBase);
    let tokenAt = Date.now();

    const { rows } = await client.query<MortgageRow>(
      `
        select id, document_number, recording_date, source_url
        from mortgage_records
        where source_url = $1
          and document_number is not null
          and lower(coalesce(document_type,'')) like '%mortgage%'
          and coalesce(nullif(loan_amount,0), nullif(original_amount,0)) is null
        order by id
        limit $2
      `,
      [sourceUrl, limit],
    );

    let processed = 0;
    let enriched = 0;
    let stillMissing = 0;
    let errors = 0;

    for (const row of rows) {
      if (maxRunMs > 0 && Date.now() - startedAt > maxRunMs) break;
      processed++;

      if (Date.now() - tokenAt > 8 * 60_000) {
        token = await getToken(apiBase);
        tokenAt = Date.now();
      }

      try {
        const doc = await lookupDocument(apiBase, token, row.document_number);
        const amount = Math.round(Number(doc?.ConsiderationAmount ?? 0));
        const legal = doc ? legalDescription(doc) : null;

        if (amount > 0) {
          const fields = computeMortgageFields({ originalAmount: amount, recordingDate: row.recording_date ?? undefined });
          if (!dryRun) {
            await client.query(
              `
                update mortgage_records
                set loan_amount = $2,
                    original_amount = $2,
                    legal_description = coalesce(legal_description, $3),
                    interest_rate = coalesce(interest_rate, $4),
                    term_months = coalesce(term_months, $5),
                    estimated_monthly_payment = $6,
                    estimated_current_balance = $7,
                    balance_as_of = $8,
                    maturity_date = $9,
                    rate_source = coalesce(rate_source, $10)
                where id = $1
              `,
              [
                row.id,
                amount,
                legal,
                fields.interest_rate,
                fields.term_months,
                fields.estimated_monthly_payment,
                fields.estimated_current_balance,
                fields.balance_as_of,
                fields.maturity_date,
                fields.rate_source,
              ],
            );
          }
          enriched++;
        } else {
          if (legal && !dryRun) {
            await client.query("update mortgage_records set legal_description = coalesce(legal_description, $2) where id = $1", [row.id, legal]);
          }
          stillMissing++;
        }

        if (processed % 25 === 0) {
          process.stdout.write(`\rprocessed=${processed} enriched=${enriched} still_missing=${stillMissing} errors=${errors}`);
        }
      } catch (error) {
        errors++;
        if (errors <= 5) console.error(`\n${row.document_number}: ${(error as Error).message}`);
      }
    }

    console.log();
    console.log(JSON.stringify({ dryRun, processed, enriched, stillMissing, errors }, null, 2));
  } finally {
    await client.end();
  }
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
