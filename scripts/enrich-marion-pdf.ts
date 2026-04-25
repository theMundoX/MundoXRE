#!/usr/bin/env tsx
/**
 * MXRE — Enrich Marion (Indianapolis) properties from Assessor Property Card PDFs.
 *
 * Per-parcel data the ArcGIS layer doesn't expose:
 *   - year_built, year_remodeled, living_sqft, stories
 *   - bedrooms, bathrooms_full, bathrooms_half, total_rooms
 *   - heating, air_conditioning, basement, roof_type, construction_class
 *   - grade, condition_code
 *   - sale history → fills last_sale_date / last_sale_price / sale_year
 *
 * Source: maps.indy.gov/AssessorPropertyCards.Reports.Service/Service.svc/PropertyCard/{PARCEL_I}
 * The endpoint expects 7-digit PARCEL_I (or PARCEL_C, same value).
 * Rows with 18-digit STATEPARCELNUMBER as parcel_id are resolved via ArcGIS
 * lookup before the card fetch.
 *
 * Usage:
 *   npx tsx scripts/enrich-marion-pdf.ts                    # all Marion parcels needing enrichment
 *   npx tsx scripts/enrich-marion-pdf.ts --max=500          # cap
 *   npx tsx scripts/enrich-marion-pdf.ts --workers=4        # parallel HTTP requests
 *   npx tsx scripts/enrich-marion-pdf.ts --dry-run          # preview, no DB writes
 *   npx tsx scripts/enrich-marion-pdf.ts --resolve-legacy   # also resolve 18-digit parcels via ArcGIS
 */

import "dotenv/config";
import { getDocument } from "pdfjs-dist/legacy/build/pdf.mjs";

// ─── Config ────────────────────────────────────────────────────────

const args = process.argv.slice(2);
const getArg = (n: string) => args.find((a) => a.startsWith(`--${n}=`))?.split("=")[1];
const hasFlag = (n: string) => args.includes(`--${n}`);

const MAX = getArg("max") ? parseInt(getArg("max")!, 10) : Infinity;
const WORKERS = getArg("workers") ? parseInt(getArg("workers")!, 10) : 4;
const DRY_RUN = hasFlag("dry-run");
const RESOLVE_LEGACY = hasFlag("resolve-legacy");

const PG_URL = (process.env.SUPABASE_URL ?? "").replace(/\/$/, "") + "/pg/query";
const PG_KEY = process.env.SUPABASE_SERVICE_KEY ?? "";
if (!PG_URL || !PG_KEY) { console.error("SUPABASE_URL / SUPABASE_SERVICE_KEY missing"); process.exit(1); }

const CARD_BASE = "https://maps.indy.gov/AssessorPropertyCards.Reports.Service/Service.svc/PropertyCard";
const ARCGIS_PARCELS = "https://xmaps.indy.gov/arcgis/rest/services/Common/CommonlyUsedLayers/MapServer/0/query";
const FETCH_TIMEOUT_MS = 25_000;
const RETRY_DELAYS = [1000, 3000];

// ─── DB helpers (direct pg-meta, no supabase-js) ───────────────────

async function pg<T = any>(sql: string, params: any[] = []): Promise<T[]> {
  // pg-meta /pg/query takes raw SQL; we inline-quote params manually.
  let full = sql;
  if (params.length > 0) {
    let i = 0;
    full = sql.replace(/\$(\d+)/g, (_, n) => {
      const v = params[parseInt(n, 10) - 1];
      if (v === null || v === undefined) return "NULL";
      if (typeof v === "number") return String(v);
      if (typeof v === "boolean") return v ? "true" : "false";
      return `'${String(v).replace(/'/g, "''")}'`;
    });
  }
  const res = await fetch(PG_URL, {
    method: "POST",
    headers: { apikey: PG_KEY, Authorization: `Bearer ${PG_KEY}`, "Content-Type": "application/json" },
    body: JSON.stringify({ query: full }),
  });
  if (!res.ok) throw new Error(`pg ${res.status}: ${(await res.text()).slice(0, 200)}`);
  return res.json();
}

// ─── PDF text extractor (pdfjs-dist) ───────────────────────────────
// Marion's iTextSharp-generated PDFs have FlateDecode-compressed streams
// that the naive (...)Tj regex won't crack. pdfjs-dist properly decodes
// and yields ordered text items per page.
async function extractPdfText(buffer: Buffer): Promise<string | null> {
  try {
    const u8 = new Uint8Array(buffer.buffer, buffer.byteOffset, buffer.byteLength);
    const doc = await getDocument({
      data: u8,
      useWorkerFetch: false,
      isEvalSupported: false,
      useSystemFonts: false,
      verbosity: 0,
    } as any).promise;
    let out = "";
    for (let i = 1; i <= doc.numPages; i++) {
      const page = await doc.getPage(i);
      const content = await page.getTextContent();
      // Use "|" between items so tabular layouts produce predictable separators.
      out += content.items.map((it: any) => it.str ?? "").join("|") + "\n";
    }
    await doc.destroy();
    return out.length > 0 ? out : null;
  } catch { return null; }
}

// ─── Building parser ───────────────────────────────────────────────

interface Building {
  year_built?: number; year_remodeled?: number; living_sqft?: number;
  bedrooms?: number; total_rooms?: number;
  bathrooms_full?: number; bathrooms_half?: number;
  construction_class?: string; grade?: string; condition_code?: string;
  heating?: string; air_conditioning?: string; stories?: number;
  basement?: string; roof_type?: string;
}

function parseBuilding(pdfText: string): Building | null {
  try {
    const t = pdfText;
    // The "SUMMARY OF IMPROVEMENTS" row for the dwelling has a very stable shape:
    //   Dwelling| |Frame| |D++| |1960| |1960| |A| |1558| |1.00| |172530| ...
    //   Use   Const  Grade   YearC EfftvY  Cnd  Sqft  LCM   Replace
    // Match flexibly across the pipe/space separators.
    const dwell = t.match(
      /Dwelling\b[\s|]*\w[\w\s]*?[\s|]+(?:[A-Da-d][+\-]*)[\s|]+(\d{4})[\s|]+(\d{4})[\s|]+([A-Za-z])[\s|]+(\d[\d,]*)[\s|]+([\d.]+)[\s|]+(\d[\d,]*)/
    );
    const year_built     = dwell ? parseInt(dwell[1], 10) : undefined;
    const year_remodeled = dwell && dwell[2] !== dwell[1] ? parseInt(dwell[2], 10) : undefined;
    const condition_code = dwell ? dwell[3].toUpperCase() : undefined;
    const living_sqft    = dwell ? parseInt(dwell[4].replace(/,/g, ""), 10) : undefined;

    // Grade lives on the same dwelling row, just earlier; capture the standalone grade token.
    const gradeMatch = t.match(/Dwelling\b[\s|]+\w[\w\s]*?[\s|]+([A-Da-d][+\-]*)[\s|]+\d{4}[\s|]+\d{4}/);
    const grade = gradeMatch ? gradeMatch[1].toUpperCase() : undefined;

    // Construction type — token preceding grade in the dwelling row.
    const constMatch = t.match(/Dwelling\b[\s|]+(\w[\w\s\/]+?)[\s|]+[A-Da-d][+\-]*[\s|]+\d{4}/);
    const construction_class = constMatch ? constMatch[1].trim() : undefined;

    // Heating — these are listed as named columns; "Central Warm Air|Hot Water...|Heat Pump|Central Air Cond"
    // and the user's selection appears after them in flag positions. Conservative heuristic: look for the
    // primary phrase in proximity to "Heating" header.
    const heatingZone = (t.match(/Heating\s*\/?\s*Air\s*Conditioning[\s\S]{0,400}/i) ?? [""])[0];
    const heating =
      /Central\s*Warm\s*Air[\s|]*[1Y]/i.test(heatingZone) ? "Central Warm Air" :
      /Hot\s*Water[\s|]*[1Y]/i.test(heatingZone)         ? "Hot Water/Steam"  :
      /Heat\s*Pump[\s|]*[1Y]/i.test(heatingZone)         ? "Heat Pump"        :
      /No\s*Heat[\s|]*[1Y]/i.test(heatingZone)           ? "None"             : undefined;
    const air_conditioning =
      /Central\s*Air\s*Cond\.?[\s|]*[1Y]/i.test(heatingZone) ? "Central" : undefined;

    // Bedrooms / bathrooms — in the "Accommodations" zone, values follow column headers.
    // Header sequence we observed:
    //   Total # Rooms|Bedrooms|Formal Dining Room|Family Room|Rec Room Type|Area|Fireplace
    // followed by a values row like "9|4|1|0|1|1"  (rooms, beds, dining, family, recroom, area)
    const accomZone = (t.match(/Accommodations[\s\S]{0,1500}/i) ?? [""])[0];
    const accomMatch = accomZone.match(
      /Total\s*#?\s*Rooms[\s|]*Bedrooms[\s|]*Formal\s*Dining[^\n]*?\|((?:\s*\d+\s*\|){4,})/i
    );
    let total_rooms: number | undefined;
    let bedrooms: number | undefined;
    if (accomMatch) {
      const vals = accomMatch[1].split("|").map((s) => s.trim()).filter(Boolean).map((s) => parseInt(s, 10));
      if (!isNaN(vals[0])) total_rooms = vals[0];
      if (!isNaN(vals[1])) bedrooms = vals[1];
    }
    // Plumbing zone — Full Baths, Half Baths header followed by values.
    //   Full Baths|Half Baths|Water Heater|Kitchen Sink||#||0|1|1|1
    const plumbZone = (t.match(/Plumbing[\s\S]{0,800}/i) ?? [""])[0];
    const plumbMatch = plumbZone.match(/Full\s*Baths[\s|]*Half\s*Baths[^\n]*?\|((?:\s*\d+\s*\|){3,})/i);
    let bathrooms_full: number | undefined;
    let bathrooms_half: number | undefined;
    if (plumbMatch) {
      const vals = plumbMatch[1].split("|").map((s) => s.trim()).filter(Boolean).map((s) => parseInt(s, 10));
      if (!isNaN(vals[0])) bathrooms_full = vals[0];
      if (!isNaN(vals[1])) bathrooms_half = vals[1];
    }

    // Story height appears as a 2-digit code or float adjacent to "Story Height".
    const stMatch = t.match(/Story\s*Height[\s|]*([\d.]+)/i);
    const stories = stMatch ? parseFloat(stMatch[1]) : undefined;

    // Roofing
    const roof =
      /Asphalt\s*Shingles[\s|]*[1X]/i.test(t) ? "Asphalt Shingles" :
      /Slate\s*or\s*Tile[\s|]*[1X]/i.test(t) ? "Slate/Tile" :
      /Roofing[\s\S]{0,200}Metal[\s|]*[1X]/i.test(t) ? "Metal" : undefined;

    // Basement
    const bsmt =
      /Bsmt[\s\S]{0,80}Full[\s|]*[1X]/i.test(t) ? "Full" :
      /Bsmt[\s\S]{0,80}3\/4[\s|]*[1X]/i.test(t) ? "3/4" :
      /Bsmt[\s\S]{0,80}1\/2[\s|]*[1X]/i.test(t) ? "1/2" :
      /Bsmt[\s\S]{0,80}1\/4[\s|]*[1X]/i.test(t) ? "1/4" :
      /Crawl[\s|]*[1X]/i.test(t) ? "Crawl" :
      /Slab[\s|]*[1X]/i.test(t) ? "Slab" : undefined;

    return {
      year_built, year_remodeled, living_sqft,
      bedrooms, total_rooms, bathrooms_full, bathrooms_half,
      construction_class, grade, condition_code,
      heating, air_conditioning, stories,
      basement: bsmt, roof_type: roof,
    };
  } catch { return null; }
}

// ─── Sale history parser ───────────────────────────────────────────

interface Transfer { date: string; iso: string; grantor: string; valid: boolean; amount: number; type: string; }

function toIso(s: string): string | null {
  const m = s.match(/(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{1,2}),\s+(\d{4})/);
  if (!m) return null;
  const months: Record<string, string> = { Jan:"01",Feb:"02",Mar:"03",Apr:"04",May:"05",Jun:"06",Jul:"07",Aug:"08",Sep:"09",Oct:"10",Nov:"11",Dec:"12" };
  return `${m[3]}-${months[m[1]]}-${m[2].padStart(2,"0")}`;
}

function parseTransfers(pdfText: string): Transfer[] {
  const out: Transfer[] = [];
  for (const line of pdfText.split("\n")) {
    const dm = line.match(/(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},\s+\d{4}/);
    if (!dm) continue;
    const iso = toIso(dm[0]); if (!iso) continue;
    const after = line.slice(dm.index! + dm[0].length).trim();
    if (!after) continue;
    const parts = after.split(/\s{2,}|\t|\|/);
    let grantor = "", valid = true, amount = 0, type = "";
    const vIdx = parts.findIndex((p) => /^[YN]$/.test(p.trim()));
    if (vIdx >= 0) {
      grantor = parts.slice(0, vIdx).join(" ").trim();
      valid = parts[vIdx].trim() === "Y";
      for (let i = vIdx + 1; i < parts.length; i++) {
        const n = parseFloat(parts[i].replace(/[,$]/g, "").trim());
        if (!isNaN(n) && n > 0) { amount = n; if (i + 1 < parts.length) type = parts[i + 1].trim(); break; }
      }
    } else { grantor = after; }
    grantor = grantor.replace(/\s*-\s*(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec).*$/, "").trim();
    if (grantor || amount > 0) out.push({ date: dm[0], iso, grantor, valid, amount, type: type || "Unknown" });
  }
  return out;
}

// ─── HTTP fetchers ─────────────────────────────────────────────────

async function fetchCard(parcelI: string): Promise<Buffer | null> {
  const url = `${CARD_BASE}/${encodeURIComponent(parcelI)}`;
  for (let attempt = 0; attempt <= RETRY_DELAYS.length; attempt++) {
    try {
      const ctrl = new AbortController();
      const timer = setTimeout(() => ctrl.abort(), FETCH_TIMEOUT_MS);
      const res = await fetch(url, {
        headers: { Accept: "*/*", "User-Agent": "MXRE/1.0 (Property Research)" },
        signal: ctrl.signal,
      });
      clearTimeout(timer);
      if (!res.ok) return null;
      const ct = res.headers.get("content-type") || "";
      if (!ct.includes("octet-stream") && !ct.includes("pdf")) return null;
      const buf = Buffer.from(await res.arrayBuffer());
      if (buf.length < 500) return null;
      return buf;
    } catch {
      if (attempt < RETRY_DELAYS.length) { await new Promise((r) => setTimeout(r, RETRY_DELAYS[attempt])); continue; }
      return null;
    }
  }
  return null;
}

async function resolvePARCEL_I(stateParcelDashed: string): Promise<string | null> {
  // 18-digit -> dashed: "490818117007000401" -> "49-08-18-117-007.000-401"
  const digits = stateParcelDashed.replace(/\D/g, "");
  if (digits.length < 18) return null;
  const dashed = `${digits.slice(0,2)}-${digits.slice(2,4)}-${digits.slice(4,6)}-${digits.slice(6,9)}-${digits.slice(9,12)}.${digits.slice(12,15)}-${digits.slice(15,18)}`;
  const url = `${ARCGIS_PARCELS}?where=${encodeURIComponent(`STATEPARCELNUMBER='${dashed}'`)}&outFields=PARCEL_I&f=json`;
  try {
    const r = await fetch(url, { signal: AbortSignal.timeout(10_000) });
    if (!r.ok) return null;
    const j: any = await r.json();
    return j?.features?.[0]?.attributes?.PARCEL_I != null ? String(j.features[0].attributes.PARCEL_I) : null;
  } catch { return null; }
}

// ─── Worker ────────────────────────────────────────────────────────

interface PropertyRow { id: number; parcel_id: string; }

const stats = { processed: 0, enriched: 0, skipped: 0, resolved: 0, errors: 0 };

async function enrichOne(p: PropertyRow): Promise<void> {
  stats.processed++;
  let parcelI = p.parcel_id;
  // 18-digit STATEPARCELNUMBER → resolve via ArcGIS
  if (parcelI.length >= 18) {
    if (!RESOLVE_LEGACY) { stats.skipped++; return; }
    const r = await resolvePARCEL_I(parcelI);
    if (!r) { stats.skipped++; return; }
    parcelI = r;
    stats.resolved++;
  }
  // PARCEL_I should be ≤ 7 digits. Validate.
  if (!/^\d{1,7}$/.test(parcelI)) { stats.skipped++; return; }

  const buf = await fetchCard(parcelI);
  if (!buf) { stats.skipped++; return; }
  const text = await extractPdfText(buf);
  if (!text) { stats.skipped++; return; }
  const building = parseBuilding(text);
  const transfers = parseTransfers(text);

  const validSales = transfers.filter((t) => t.valid && t.amount > 1000)
                              .sort((a, b) => (b.iso > a.iso ? 1 : -1));
  const lastSale = validSales[0];

  const sets: string[] = [];
  const add = (col: string, val: any) => {
    if (val === undefined || val === null) return;
    if (typeof val === "number") sets.push(`${col}=${val}`);
    else sets.push(`${col}='${String(val).replace(/'/g, "''")}'`);
  };
  if (building) {
    add("year_built", building.year_built);
    add("year_remodeled", building.year_remodeled);
    add("living_sqft", building.living_sqft);
    add("bedrooms", building.bedrooms);
    add("total_rooms", building.total_rooms);
    add("bathrooms_full", building.bathrooms_full);
    add("bathrooms_half", building.bathrooms_half);
    add("construction_class", building.construction_class);
    add("improvement_quality", building.grade);
    add("condition_code", building.condition_code);
    add("heating", building.heating);
    add("air_conditioning", building.air_conditioning);
    add("stories", building.stories);
    add("basement", building.basement);
    add("roof_type", building.roof_type);
  }
  if (lastSale) {
    add("last_sale_date", lastSale.iso);
    add("last_sale_price", Math.round(lastSale.amount));
    add("sale_year", parseInt(lastSale.iso.slice(0, 4), 10));
  }

  if (sets.length === 0) { stats.skipped++; return; }
  sets.push(`updated_at=now()`);

  if (!DRY_RUN) {
    try { await pg(`UPDATE properties SET ${sets.join(", ")} WHERE id=${p.id}`); }
    catch (e) { stats.errors++; console.error(`  err id=${p.id}: ${(e as Error).message.slice(0, 100)}`); return; }
  }
  stats.enriched++;

  if (stats.processed % 25 === 0) {
    const pct = stats.enriched / stats.processed * 100;
    console.log(`  ${stats.processed} processed | ${stats.enriched} enriched (${pct.toFixed(0)}%) | skip ${stats.skipped} | resolved ${stats.resolved} | err ${stats.errors}`);
  }
}

async function runPool(items: PropertyRow[], n: number): Promise<void> {
  let idx = 0;
  await Promise.all(Array.from({ length: n }, async () => {
    while (idx < items.length) {
      const p = items[idx++];
      try { await enrichOne(p); } catch (e) { stats.errors++; console.error(`  err: ${(e as Error).message.slice(0, 100)}`); }
    }
  }));
}

// ─── Main ──────────────────────────────────────────────────────────

async function main() {
  console.log("MXRE — Marion Property Card PDF Enricher");
  console.log("═".repeat(55));
  console.log(`  Workers: ${WORKERS}  |  Max: ${MAX === Infinity ? "all" : MAX}  |  Dry-run: ${DRY_RUN}  |  Resolve-legacy: ${RESOLVE_LEGACY}`);
  console.log();

  // Find Marion county_id
  const cr = await pg<{ id: number }>(`SELECT id FROM counties WHERE state_fips='18' AND county_fips='097' LIMIT 1`);
  if (cr.length === 0) { console.error("Marion not in counties"); process.exit(1); }
  const countyId = cr[0].id;
  console.log(`  Marion county_id: ${countyId}`);

  // Pull targets in pages to avoid the read-timeout on the gateway.
  // The gateway times out at 60s and a full SELECT of 237K rows blows that.
  // Strategy: page through by id ascending, oversampling so the runtime
  // skip filter (parcel length / resolve-legacy) still hits MAX targets.
  console.log(`  Querying targets (paged, ${RESOLVE_LEGACY ? "all parcels" : "7-digit only"})...`);
  const PAGE_SIZE = 5000;
  const targets: PropertyRow[] = [];
  let lastId = 0;
  while (targets.length < MAX) {
    const rows = await pg<PropertyRow>(`
      SELECT id, parcel_id FROM properties
      WHERE county_id=${countyId}
        AND year_built IS NULL
        AND parcel_id IS NOT NULL
        AND id > ${lastId}
      ORDER BY id ASC
      LIMIT ${PAGE_SIZE}
    `);
    if (rows.length === 0) break;
    for (const r of rows) {
      const pid = String(r.parcel_id);
      // Skip legacy 18-digit unless --resolve-legacy is set
      if (!RESOLVE_LEGACY && pid.length > 7) continue;
      targets.push({ id: r.id, parcel_id: pid });
      if (targets.length >= MAX) break;
    }
    lastId = rows[rows.length - 1].id;
    if (rows.length < PAGE_SIZE) break;  // last page
  }
  console.log(`  Found ${targets.length.toLocaleString()} parcels needing enrichment.\n`);
  if (targets.length === 0) { console.log("Nothing to do."); return; }

  const t0 = Date.now();
  await runPool(targets, WORKERS);
  const sec = Math.max(1, Math.round((Date.now() - t0) / 1000));
  console.log("\n" + "═".repeat(55));
  console.log(`  Processed: ${stats.processed.toLocaleString()}`);
  console.log(`  Enriched:  ${stats.enriched.toLocaleString()} (${(stats.enriched/Math.max(1,stats.processed)*100).toFixed(1)}%)`);
  console.log(`  Skipped:   ${stats.skipped.toLocaleString()}`);
  console.log(`  Resolved:  ${stats.resolved.toLocaleString()} (legacy 18-digit -> 7-digit)`);
  console.log(`  Errors:    ${stats.errors.toLocaleString()}`);
  console.log(`  Duration:  ${sec}s (${(stats.processed / sec).toFixed(1)} parcels/sec)`);
}

main().catch((e) => { console.error(e); process.exit(1); });
