#!/usr/bin/env tsx
import "dotenv/config";
import { readdir, readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";

type Row = Record<string, unknown>;

const OUT_DIR = join(process.cwd(), "logs", "debt-reports");

const COLUMNS = [
  "target_name",
  "target_role",
  "target_email",
  "target_phone",
  "target_contact_status",
  "property_id",
  "mortgage_record_id",
  "market_label",
  "property_address",
  "property_city",
  "property_state",
  "property_zip",
  "parcel_id",
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
  "source_url",
] as const;

async function main() {
  const inputPath = process.argv.find((arg) => arg.startsWith("--input="))?.split("=").slice(1).join("=");
  const sourcePath = inputPath || await latestAllDataJson();
  const source = JSON.parse(await readFile(sourcePath, "utf8"));
  const sourceRows = Array.isArray(source.rows) ? source.rows as Row[] : [];
  const rows = sourceRows.filter(isLikelyIndividualLender).map(toTargetRow);
  const summaryByState = summarize(rows, "property_state");
  const summaryByMarket = summarize(rows, "market_label");

  const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
  const base = `second-lien-individual-lender-outreach-all-data-${timestamp}`;
  const csvPath = join(OUT_DIR, `${base}.csv`);
  const jsonPath = join(OUT_DIR, `${base}.json`);
  const mdPath = join(OUT_DIR, `${base}.md`);

  const payload = {
    generated_at: new Date().toISOString(),
    source_report: sourcePath,
    row_count: rows.length,
    notes: [
      "This is a post-processed outreach slice from the full all-data second-lien report.",
      "Target name is the second-lien lender/lienholder name, not the borrower, owner, or listing agent.",
      "MXRE does not currently store lender email or phone in mortgage_records, so target_email and target_phone are blank until lender-contact enrichment is added.",
      "Use CAN-SPAM compliant outreach and verify identity/contact details before sending investment solicitations.",
    ],
    summary_by_state: summaryByState,
    summary_by_market: summaryByMarket,
    rows,
  };

  await writeFile(csvPath, toCsv(rows));
  await writeFile(jsonPath, JSON.stringify(payload, null, 2));
  await writeFile(mdPath, toMarkdown(payload));

  console.log(JSON.stringify({
    source_report: sourcePath,
    wrote: [csvPath, jsonPath, mdPath],
    row_count: rows.length,
    top_states: summaryByState.slice(0, 10),
    top_markets: summaryByMarket.slice(0, 10),
  }, null, 2));
}

async function latestAllDataJson() {
  const files = await readdir(OUT_DIR);
  const candidates = files
    .filter((name) => /^second-lien-positions-all-data-\d{4}.*\.json$/.test(name))
    .sort()
    .reverse();
  if (!candidates.length) throw new Error(`No full all-data second-lien JSON found in ${OUT_DIR}.`);
  return join(OUT_DIR, candidates[0]);
}

function toTargetRow(row: Row): Row {
  return {
    target_name: row.lender_name,
    target_role: "second_lien_lender",
    target_email: null,
    target_phone: null,
    target_contact_status: "no_lender_contact_in_mxre",
    ...row,
  };
}

function isLikelyIndividualLender(row: Row) {
  const name = String(row.lender_name ?? "").trim();
  if (!name || ["NONE", "UNKNOWN", "N/A", "NA", "NULL"].includes(name.toUpperCase())) return false;

  const type = String(row.lender_type ?? "").trim();
  const code = String(row.lender_code ?? "").trim().toUpperCase();
  if (type && !/private party|individual/i.test(type)) return false;
  if (code && code !== "P") return false;

  const entityPattern = /\b(BANK|BK|BANCORP|MORTGAGE|MTG|MERS|REGISTRATION|SAVINGS|CREDIT|CU|FCU|FEDERAL|NATIONAL|TRUST COMPANY|TITLE|INSURANCE|SECRETARY|HOUSING|HUD|AUTHORITY|AGENCY|DEPARTMENT|TREASURY|FINANCE|FINANCIAL|SERVICING|SERVICE|SERVICES|LOAN|LENDING|CAPITAL|FUND|INVEST|INVESTMENT|PARTNERS|HOLDINGS|PROPERTIES|PROPERTY|REALTY|REAL ESTATE|ASSOCIATION|ASSN|CORPORATION|CORP|INCORPORATED|INC|LLC|L\.L\.C|LP|LTD|LIMITED|CO|COMPANY|TRUST|TRUSTEE|ESTATE|CHURCH|MINISTRIES|UNIVERSITY|COLLEGE|COUNTY|CITY OF|STATE OF|TOWNSHIP|SCHOOL|BOARD|FOUNDATION|ATTY|BUREAU|WORKERS|REVENUE|GOODLEAP|SOLAR|MOSAIC|HOMES|FSLA|FLCA|ENTERPRISES|PROGRAM|OFFICE|GROUP|RATE|GUARANTEED|SEWER|DISTRICT|REGIONAL|ECONOMIC|SOLUTIONS|LUMBER)\b/i;
  if (entityPattern.test(name)) return false;

  return /^[A-Z][A-Z'.-]+( [A-Z][A-Z'.-]+){1,4}$/i.test(name);
}

function summarize(rows: Row[], field: string) {
  const grouped = new Map<string, Row[]>();
  for (const row of rows) {
    const key = String(row[field] || "UNKNOWN");
    grouped.set(key, [...(grouped.get(key) ?? []), row]);
  }
  return [...grouped.entries()].map(([label, group]) => ({
    [field]: label,
    second_lien_count: group.length,
    property_count: new Set(group.map((row) => row.property_id).filter(Boolean)).size,
    total_amount_or_balance: sumAmount(group),
    rows_with_interest_rate: group.filter((row) => row.interest_rate != null).length,
  })).sort((a, b) =>
    Number(b.second_lien_count) - Number(a.second_lien_count)
    || Number(b.total_amount_or_balance) - Number(a.total_amount_or_balance)
    || String(a[field]).localeCompare(String(b[field]))
  );
}

function sumAmount(rows: Row[]) {
  return rows.reduce((sum, row) => {
    const amount = Number(row.estimated_current_balance ?? row.original_amount ?? 0);
    return sum + (Number.isFinite(amount) ? amount : 0);
  }, 0);
}

function toCsv(rows: Row[]) {
  return [
    COLUMNS.join(","),
    ...rows.map((row) => COLUMNS.map((column) => csvCell(row[column])).join(",")),
  ].join("\n");
}

function toMarkdown(payload: { generated_at: string; source_report: string; row_count: number; notes: string[]; summary_by_state: Row[]; summary_by_market: Row[]; rows: Row[] }) {
  const stateRows = payload.summary_by_state.slice(0, 25).map((row) =>
    `| ${cell(row.property_state)} | ${cell(row.second_lien_count)} | ${cell(row.property_count)} | ${money(row.total_amount_or_balance)} | ${cell(row.rows_with_interest_rate)} |`
  );
  const sampleRows = payload.rows.slice(0, 25).map((row) =>
    `| ${cell(row.target_name)} | ${cell(row.property_state)} | ${cell(row.market_label)} | ${cell(row.property_address)} | ${money(row.original_amount)} | ${money(row.estimated_current_balance)} | ${cell(row.interest_rate)} | ${cell(row.rate_source ?? row.interest_rate_type)} | ${cell(row.target_contact_status)} |`
  );

  return `# MXRE Individual Second-Lien Lender Outreach Report

Generated: ${payload.generated_at}

Source: ${payload.source_report}
Rows: ${payload.row_count.toLocaleString()}

## Notes

${payload.notes.map((note) => `- ${note}`).join("\n")}

## State Summary

| State | Target rows | Properties | Amount / balance | Rows with rate |
|---|---:|---:|---:|---:|
${stateRows.join("\n")}

## Sample Targets

| Target name | State | Market | Property | Original amount | Est. balance | Rate | Rate source | Contact status |
|---|---|---|---|---:|---:|---:|---|---|
${sampleRows.join("\n")}
`;
}

function csvCell(value: unknown) {
  if (value == null) return "";
  const text = String(value);
  return /[",\n\r]/.test(text) ? `"${text.replace(/"/g, '""')}"` : text;
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

main().catch((error) => {
  console.error("Fatal individual lender filter error:", error instanceof Error ? error.message : error);
  process.exit(1);
});
