#!/usr/bin/env tsx
import "dotenv/config";
import { Client } from "pg";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname } from "node:path";

const args = process.argv.slice(2);
const valueArg = (name: string) => {
  const prefix = `--${name}=`;
  return args.find((arg) => arg.startsWith(prefix))?.slice(prefix.length) ?? null;
};

const limit = Number(valueArg("limit") ?? "1000");
const dryRun = args.includes("--dry-run");
const maxRunMs = Number(valueArg("max-run-ms") ?? "0");
const explicitAfterId = Number(valueArg("after-id") ?? "0");
const cursorFile = valueArg("cursor-file");
const onlyMortgageAmounts = args.includes("--only-mortgage-amounts");
const directPgUrl = process.env.MXRE_DIRECT_PG_URL ?? process.env.DATABASE_URL ?? process.env.POSTGRES_URL;
const MARION_COUNTY_ID = 797583;
const SOURCE_URL = "https://inmarion.fidlar.com/INMarion/DirectSearch/";

if (!directPgUrl) throw new Error("MXRE_DIRECT_PG_URL, DATABASE_URL, or POSTGRES_URL is required.");

type RecorderRow = {
  id: number;
  borrower_name: string | null;
  grantee_name: string | null;
  lender_name: string | null;
  document_type: string | null;
  recording_date: string | null;
  loan_amount: number | null;
  original_amount: number | null;
};

type PropertyRow = {
  id: number;
  owner_name: string | null;
  owner_name2: string | null;
  owner1_first: string | null;
  owner1_last: string | null;
  owner2_first: string | null;
  owner2_last: string | null;
  ownership_start_date: string | null;
  last_sale_date: string | null;
  last_sale_price: number | null;
};

type NameCandidate = {
  name: string;
  role: "borrower" | "grantee" | "lender";
};

function normalizeName(value: string): string {
  return value
    .toUpperCase()
    .replace(/[,.'"\-]/g, " ")
    .replace(/\b(LLC|INC|CORP|CORPORATION|LTD|LP|LLP|PLLC|PC|CO|COMPANY|TRUSTEE|TRUST|ET\s+AL|ETAL)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function usefulParts(value: string): string[] {
  return normalizeName(value)
    .split(/\s+/)
    .filter((part) => part.length >= 3 && !["THE", "AND", "FOR", "REVOCABLE", "AGREEMENT", "DATED"].includes(part));
}

function scoreNameMatch(recordName: string, ownerName: string): number {
  const recordParts = usefulParts(recordName);
  const owner = normalizeName(ownerName);
  if (recordParts.length === 0 || !owner) return 0;

  const recordNorm = normalizeName(recordName);
  if (recordNorm && recordNorm === owner) return 100;

  let matched = 0;
  for (const part of recordParts) {
    if (owner.includes(part)) matched++;
  }

  if (matched < Math.min(2, recordParts.length)) return 0;
  return Math.round((matched / recordParts.length) * 90);
}

function daysBetween(a: string | null, b: string | null): number | null {
  if (!a || !b) return null;
  const left = new Date(a).getTime();
  const right = new Date(b).getTime();
  if (!Number.isFinite(left) || !Number.isFinite(right)) return null;
  return Math.abs(left - right) / 86_400_000;
}

function propertyNames(property: PropertyRow): string[] {
  return [
    property.owner_name,
    property.owner_name2,
    [property.owner1_first, property.owner1_last].filter(Boolean).join(" "),
    [property.owner2_first, property.owner2_last].filter(Boolean).join(" "),
  ].filter((value): value is string => Boolean(value && value.trim().length >= 3));
}

function scoreProperty(record: RecorderRow, candidate: NameCandidate, property: PropertyRow): number {
  const nameScore = Math.max(0, ...propertyNames(property).map((name) => scoreNameMatch(candidate.name, name)));
  if (nameScore === 0) return 0;

  let score = nameScore;
  if (candidate.role === "borrower" || candidate.role === "grantee") score += 12;
  if (candidate.role === "lender") score -= 20;

  const ownershipDays = daysBetween(record.recording_date, property.ownership_start_date);
  const saleDays = daysBetween(record.recording_date, property.last_sale_date);
  const closestTransferDays = Math.min(ownershipDays ?? Number.POSITIVE_INFINITY, saleDays ?? Number.POSITIVE_INFINITY);
  if (closestTransferDays <= 14) score += 12;
  else if (closestTransferDays <= 60) score += 8;
  else if (closestTransferDays <= 180) score += 4;

  const amount = record.loan_amount ?? record.original_amount;
  if (amount && property.last_sale_price && closestTransferDays <= 90) {
    const ratio = amount / property.last_sale_price;
    if (ratio > 0.4 && ratio < 1.15) score += 6;
  }

  return score;
}

function namesToTry(record: RecorderRow): NameCandidate[] {
  const names = new Map<string, NameCandidate>();
  const add = (raw: string | null, role: NameCandidate["role"]) => {
    for (const part of (raw ?? "").split(";")) {
      const trimmed = part.trim();
      if (trimmed.length >= 3) names.set(`${role}:${normalizeName(trimmed)}`, { name: trimmed, role });
    }
  };

  add(record.borrower_name, "borrower");
  add(record.grantee_name, "grantee");
  if (!onlyMortgageAmounts) add(record.lender_name, "lender");

  return [...names.values()];
}

async function main() {
  let afterId = explicitAfterId;
  if (cursorFile) {
    try {
      const saved = Number((await readFile(cursorFile, "utf8")).trim());
      if (Number.isFinite(saved) && saved > afterId) afterId = saved;
    } catch {
      // Missing cursor is fine; the first run starts from the explicit cursor.
    }
  }

  console.log(`Fast Marion Fidlar owner linker | limit=${limit} | dry=${dryRun} | onlyMortgageAmounts=${onlyMortgageAmounts} | afterId=${afterId}`);
  const startedAt = Date.now();
  const client = new Client({ connectionString: directPgUrl });
  await client.connect();
  await client.query("set max_parallel_workers_per_gather = 0");

  try {
    const records = await client.query<RecorderRow>(
      `
        select id, borrower_name, grantee_name, lender_name, document_type, recording_date, loan_amount, original_amount
        from mortgage_records
        where source_url = $1
          and property_id is null
          and id > $3
          and (borrower_name is not null or lender_name is not null)
          ${onlyMortgageAmounts ? "and lower(coalesce(document_type,'')) like '%mortgage%' and coalesce(nullif(loan_amount,0), nullif(original_amount,0)) is not null" : ""}
        order by id
        limit $2
      `,
      [SOURCE_URL, limit, afterId],
    );

    let processed = 0;
    let linked = 0;
    let ambiguous = 0;
    let noMatch = 0;
    let lastProcessedId = afterId;

    for (const record of records.rows) {
      if (maxRunMs > 0 && Date.now() - startedAt > maxRunMs) {
        console.log(`\nReached max runtime ${maxRunMs}ms; stopping cleanly.`);
        break;
      }

      processed++;
      lastProcessedId = record.id;
      let best: { id: number; score: number } | null = null;
      let tied = false;

      for (const name of namesToTry(record)) {
        const firstPart = usefulParts(name.name)[0];
        if (!firstPart) continue;

        const candidates = await client.query<PropertyRow>(
          `
            select
              id,
              owner_name,
              owner_name2,
              owner1_first,
              owner1_last,
              owner2_first,
              owner2_last,
              ownership_start_date,
              last_sale_date,
              last_sale_price
            from properties
            where county_id = $1
              and owner_name ilike $2
            limit 75
          `,
          [MARION_COUNTY_ID, `%${firstPart}%`],
        );

        for (const property of candidates.rows) {
          const score = scoreProperty(record, name, property);
          if (score > (best?.score ?? 0)) {
            best = { id: property.id, score };
            tied = false;
          } else if (best && Math.abs(score - best.score) <= 2 && score >= 92 && property.id !== best.id) {
            tied = true;
          }
        }
      }

      if (best && best.score >= 92 && !tied) {
        if (!dryRun) {
          await client.query("update mortgage_records set property_id = $1 where id = $2", [best.id, record.id]);
        }
        linked++;
      } else if (tied) {
        ambiguous++;
      } else {
        noMatch++;
      }

      if (processed % 25 === 0) {
        process.stdout.write(`\rprocessed=${processed} linked=${linked} ambiguous=${ambiguous} no_match=${noMatch}`);
      }
    }

    if (cursorFile && !dryRun && lastProcessedId > afterId) {
      await mkdir(dirname(cursorFile), { recursive: true });
      await writeFile(cursorFile, `${lastProcessedId}\n`, "utf8");
    }

    console.log();
    console.log(JSON.stringify({ processed, linked, ambiguous, noMatch, dryRun, afterId, lastProcessedId, cursorFile }, null, 2));
  } finally {
    await client.end();
  }
}

main()
  .then(() => process.exit(0))
  .catch((error) => {
    console.error(error);
    process.exit(1);
  });
