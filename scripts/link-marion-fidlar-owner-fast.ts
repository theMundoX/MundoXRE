#!/usr/bin/env tsx
import "dotenv/config";
import { Client } from "pg";

const args = process.argv.slice(2);
const valueArg = (name: string) => {
  const prefix = `--${name}=`;
  return args.find((arg) => arg.startsWith(prefix))?.slice(prefix.length) ?? null;
};

const limit = Number(valueArg("limit") ?? "1000");
const dryRun = args.includes("--dry-run");
const maxRunMs = Number(valueArg("max-run-ms") ?? "0");
const afterId = Number(valueArg("after-id") ?? "0");
const directPgUrl = process.env.MXRE_DIRECT_PG_URL ?? process.env.DATABASE_URL ?? process.env.POSTGRES_URL;
const MARION_COUNTY_ID = 797583;
const SOURCE_URL = "https://inmarion.fidlar.com/INMarion/DirectSearch/";

if (!directPgUrl) throw new Error("MXRE_DIRECT_PG_URL, DATABASE_URL, or POSTGRES_URL is required.");

type RecorderRow = {
  id: number;
  borrower_name: string | null;
  lender_name: string | null;
  document_type: string | null;
};

type PropertyRow = {
  id: number;
  owner_name: string | null;
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

function scoreMatch(recordName: string, ownerName: string): number {
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

function namesToTry(record: RecorderRow): string[] {
  const names = new Set<string>();
  for (const raw of [record.borrower_name, record.lender_name]) {
    for (const part of (raw ?? "").split(";")) {
      const trimmed = part.trim();
      if (trimmed.length >= 3) names.add(trimmed);
    }
  }
  return [...names];
}

async function main() {
  console.log(`Fast Marion Fidlar owner linker | limit=${limit} | dry=${dryRun}`);
  const startedAt = Date.now();
  const client = new Client({ connectionString: directPgUrl });
  await client.connect();

  try {
    const records = await client.query<RecorderRow>(
      `
        select id, borrower_name, lender_name, document_type
        from mortgage_records
        where source_url = $1
          and property_id is null
          and id > $3
          and (borrower_name is not null or lender_name is not null)
        limit $2
      `,
      [SOURCE_URL, limit, afterId],
    );

    let processed = 0;
    let linked = 0;
    let ambiguous = 0;
    let noMatch = 0;

    for (const record of records.rows) {
      if (maxRunMs > 0 && Date.now() - startedAt > maxRunMs) {
        console.log(`\nReached max runtime ${maxRunMs}ms; stopping cleanly.`);
        break;
      }

      processed++;
      let best: { id: number; score: number } | null = null;
      let tied = false;

      for (const name of namesToTry(record)) {
        const firstPart = usefulParts(name)[0];
        if (!firstPart) continue;

        const candidates = await client.query<PropertyRow>(
          `
            select id, owner_name
            from properties
            where county_id = $1
              and owner_name ilike $2
            limit 75
          `,
          [MARION_COUNTY_ID, `%${firstPart}%`],
        );

        for (const property of candidates.rows) {
          if (!property.owner_name) continue;
          const score = scoreMatch(name, property.owner_name);
          if (score > (best?.score ?? 0)) {
            best = { id: property.id, score };
            tied = false;
          } else if (best && score === best.score && score >= 80 && property.id !== best.id) {
            tied = true;
          }
        }
      }

      if (best && best.score >= 80 && !tied) {
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

    console.log();
    console.log(JSON.stringify({ processed, linked, ambiguous, noMatch, dryRun }, null, 2));
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
