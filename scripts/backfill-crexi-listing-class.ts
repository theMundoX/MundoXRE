#!/usr/bin/env tsx
import "dotenv/config";
import { mkdir, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { normalizeCrexiBuildingClass } from "./lib/crexi-listing-class.ts";
import { makeDbClient } from "./lib/db.ts";
import { hydrateWindowsUserEnv } from "./lib/env.ts";

hydrateWindowsUserEnv();

const DRY_RUN = process.argv.includes("--dry-run");
const WRITE_LOG = !process.argv.includes("--no-write");
const arg = (name: string, fallback?: string) =>
  process.argv.find((item) => item.startsWith(`--${name}=`))?.split("=").slice(1).join("=") ?? fallback;
const LIMIT = Math.max(1, Number.parseInt(arg("limit", "5000") ?? "5000", 10));

type ExternalListingRow = {
  id: number;
  title: string | null;
  source_url: string | null;
  asset_class: string;
  raw: Record<string, unknown> | null;
};

function compactText(value: unknown): string | null {
  const text = String(value ?? "").replace(/\s+/g, " ").trim();
  return text || null;
}

function objectRecord(value: unknown): Record<string, unknown> | null {
  return value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : null;
}

function valueByCaseInsensitiveKey(value: unknown, label: string): unknown {
  const obj = objectRecord(value);
  if (!obj) return undefined;
  const target = label.toLowerCase();
  const entry = Object.entries(obj).find(([key]) => key.trim().toLowerCase() === target);
  return entry?.[1];
}

function detailValue(raw: Record<string, unknown>, label: string): unknown {
  const direct = valueByCaseInsensitiveKey(raw, label);
  if (direct !== undefined) return direct;

  const detail = objectRecord(raw.detail);
  const fromDetail = valueByCaseInsensitiveKey(detail, label);
  if (fromDetail !== undefined) return fromDetail;

  const details = objectRecord(detail?.details);
  const fromDetails = valueByCaseInsensitiveKey(details, label);
  if (fromDetails !== undefined) return fromDetails;

  const summaryDetails = Array.isArray(detail?.summaryDetails) ? detail.summaryDetails as Record<string, unknown>[] : [];
  const target = label.toLowerCase();
  const summaryMatch = summaryDetails.find((item) =>
    String(item.label ?? item.key ?? "").trim().toLowerCase() === target
  );
  return summaryMatch?.value ?? summaryMatch?.display;
}

async function main() {
  const db = await makeDbClient();
  const { rows } = await db.query<ExternalListingRow>(`
    select id, title, source_url, asset_class, raw
      from external_market_listings
     where source = 'crexi_rapidapi'
     order by id
     limit $1;
  `, [LIMIT]);

  let explicit = 0;
  let needsReview = 0;
  let noData = 0;
  let updated = 0;
  const examples: Array<Record<string, unknown>> = [];

  for (const row of rows) {
    const raw = row.raw ?? {};
    const parsed = normalizeCrexiBuildingClass(compactText(detailValue(raw, "class")));
    if (parsed.status === "explicit_value") explicit++;
    if (parsed.status === "needs_review") needsReview++;
    if (parsed.status === "no_data") noData++;

    const patch = {
      class: parsed.sourceClass,
      building_class: parsed.buildingClass,
      building_class_status: parsed.status,
      building_class_source: parsed.sourceClass ? "crexi_details.class" : "no_data",
      building_class_evidence: parsed.evidence,
    };

    const changed =
      raw.class !== patch.class ||
      raw.building_class !== patch.building_class ||
      raw.building_class_status !== patch.building_class_status ||
      raw.building_class_source !== patch.building_class_source ||
      raw.building_class_evidence !== patch.building_class_evidence;

    if (changed) {
      updated++;
      if (!DRY_RUN) {
        await db.query(`
          update external_market_listings
             set raw = coalesce(raw, '{}'::jsonb) || $1::jsonb,
                 updated_at = now()
           where id = $2;
        `, [JSON.stringify(patch), row.id]);
      }
    }

    if (parsed.status !== "no_data" && examples.length < 25) {
      examples.push({
        id: row.id,
        title: row.title,
        assetClass: row.asset_class,
        sourceUrl: row.source_url,
        sourceClass: parsed.sourceClass,
        buildingClass: parsed.buildingClass,
        status: parsed.status,
      });
    }
  }

  await db.end();

  const report = {
    schemaVersion: "mxre.crexiListingClassBackfill.v1",
    generatedAt: new Date().toISOString(),
    dryRun: DRY_RUN,
    scanned: rows.length,
    updated,
    explicit,
    needsReview,
    noData,
    examples,
  };

  if (WRITE_LOG) {
    const dir = join(process.cwd(), "logs", "crexi-listing-class");
    await mkdir(dir, { recursive: true });
    const stamp = new Date().toISOString().replace(/[:.]/g, "-");
    await writeFile(join(dir, `crexi-listing-class-${stamp}.json`), `${JSON.stringify(report, null, 2)}\n`);
  }

  console.log(JSON.stringify(report, null, 2));
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
