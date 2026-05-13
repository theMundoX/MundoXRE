#!/usr/bin/env tsx
import "dotenv/config";
import { computeMortgageFields } from "../src/utils/mortgage-calc.js";
import { makeDbClient } from "./lib/db.js";

const args = process.argv.slice(2);
const valueArg = (name: string) => {
  const prefix = `--${name}=`;
  return args.find((arg) => arg.startsWith(prefix))?.slice(prefix.length) ?? null;
};

const dryRun = args.includes("--dry-run");
const state = valueArg("state")?.toUpperCase() ?? null;
const city = valueArg("city")?.toUpperCase() ?? null;
const limit = Number(valueArg("limit") ?? "0");
const batchSize = Math.min(Math.max(Number(valueArg("batch-size") ?? "1000"), 1), 5000);
const client = await makeDbClient();

let updated = 0;
let scanned = 0;

try {
  while (true) {
    const remaining = limit > 0 ? Math.max(0, limit - updated) : batchSize;
    if (limit > 0 && remaining === 0) break;
    const take = Math.min(batchSize, remaining || batchSize);

    const rows = await client.query<{
      id: number;
      original_amount: number;
      recording_date: string;
      interest_rate: number | null;
      term_months: number | null;
    }>(`
      select
        m.id,
        coalesce(nullif(m.loan_amount,0), nullif(m.original_amount,0))::numeric as original_amount,
        m.recording_date,
        nullif(m.interest_rate,0)::numeric as interest_rate,
        nullif(m.term_months,0)::int as term_months
      from mortgage_records m
      join properties p on p.id = m.property_id
      where m.estimated_current_balance is null
        and coalesce(nullif(m.loan_amount,0), nullif(m.original_amount,0)) is not null
        and m.recording_date is not null
        and lower(coalesce(m.document_type,'')) not in ('deed', 'satisfaction', 'assignment')
        ${state ? "and p.state_code = $1" : ""}
        ${city ? `and upper(coalesce(p.city,'')) = $${state ? 2 : 1}` : ""}
      order by m.recording_date desc, m.id
      limit ${take}
    `, [state, city].filter(Boolean));

    if (rows.rowCount === 0) break;
    scanned += rows.rowCount;

    const updates = rows.rows.map((row) => {
      const fields = computeMortgageFields({
        originalAmount: Math.round(Number(row.original_amount)),
        recordingDate: row.recording_date,
        interestRate: row.interest_rate == null ? undefined : Number(row.interest_rate),
        termMonths: row.term_months ?? undefined,
      });
      return {
        id: row.id,
        interest_rate: fields.interest_rate,
        term_months: fields.term_months,
        estimated_monthly_payment: fields.estimated_monthly_payment,
        estimated_current_balance: fields.estimated_current_balance,
        balance_as_of: fields.balance_as_of,
        maturity_date: fields.maturity_date,
        rate_source: fields.rate_source,
      };
    });

    if (!dryRun) {
      await client.query("begin");
      try {
        for (const row of updates) {
          await client.query(`
            update mortgage_records
            set
              interest_rate = coalesce(interest_rate, $2),
              term_months = coalesce(term_months, $3),
              estimated_monthly_payment = $4,
              estimated_current_balance = $5,
              balance_as_of = $6,
              maturity_date = $7,
              rate_source = coalesce(rate_source, $8)
            where id = $1
          `, [
            row.id,
            row.interest_rate,
            row.term_months,
            row.estimated_monthly_payment,
            row.estimated_current_balance,
            row.balance_as_of,
            row.maturity_date,
            row.rate_source,
          ]);
        }
        await client.query("commit");
      } catch (error) {
        await client.query("rollback");
        throw error;
      }
    }

    updated += updates.length;
    console.log(`${dryRun ? "Would update" : "Updated"} ${updated.toLocaleString("en-US")} mortgage balance estimates...`);
  }
} finally {
  await client.end();
}

console.log(JSON.stringify({
  dryRun,
  state,
  city,
  scanned,
  updated,
}, null, 2));
