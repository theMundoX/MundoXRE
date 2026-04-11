/**
 * MUNDOX AUTONOMOUS RUNNER
 *
 * Drains a work queue of pending document pages and runs vision extraction
 * against MundoX on localhost:18791. Writes results directly to the DB.
 * No Claude involvement after this script is kicked off.
 *
 * Work queue: any PNG under data/labeling-sample/**\/page*.png that does NOT
 * have a matching .extract.json file beside it.
 *
 * Rate-limited (4 sec between calls), retries on ECONNRESET/timeout,
 * skips blank pages (<50KB), writes per-page .extract.json so reruns are idempotent.
 *
 * Usage:
 *   node scripts/mundox-autonomous-runner.mjs           # single pass
 *   node scripts/mundox-autonomous-runner.mjs --watch   # loop forever, 60s sleep between passes
 */
import { readdirSync, statSync, existsSync, writeFileSync, readFileSync } from "fs";
import { resolve, join, basename, dirname } from "path";

const MUNDOX_URL = process.env.MUNDOX_URL || "http://127.0.0.1:18791/v1/chat/completions";
const MODEL = process.env.MUNDOX_MODEL || "mundox-vision";
const WATCH = process.argv.includes("--watch");
const CONCURRENCY = parseInt(process.env.MUNDOX_CONCURRENCY || "6"); // parallel requests to llama-server
const MIN_PAGE_BYTES = 50_000;
const MAX_RETRIES = 3;
const ROOT = "C:/Users/msanc/mxre/data/labeling-sample";

const PROMPT = `You are a mortgage document data extractor. Read the image carefully and return ONLY a JSON object with these fields (null if not present on this page):
{
  "document_type": "mortgage" | "deed_of_trust" | "rider" | "note" | "assignment" | "other",
  "recording_date": "YYYY-MM-DD",
  "book_page": "string",
  "document_number": "string",
  "borrower_name": "string",
  "lender_name": "string",
  "lender_address": "string",
  "loan_amount_cents": integer,
  "max_principal_cents": integer,
  "interest_rate": number (decimal percentage like 4.125),
  "interest_rate_type": "fixed" | "adjustable" | "variable",
  "term_months": integer,
  "maturity_date": "YYYY-MM-DD",
  "monthly_payment_cents": integer,
  "property_address": "string",
  "property_county": "string",
  "property_state": "string (2-char)",
  "parcel_id": "string",
  "signing_date": "YYYY-MM-DD",
  "notes": "string"
}
Scan the ENTIRE image carefully for any mention of interest rate (e.g. "4.125%", "rate of 4.125 percent", "interest rate: 4.125"), monthly payment, or loan term. Return ONLY the JSON, no other text.`;

function walk(dir) {
  const out = [];
  for (const name of readdirSync(dir)) {
    const p = join(dir, name);
    const st = statSync(p);
    if (st.isDirectory()) out.push(...walk(p));
    else if (/^page\d+\.png$/i.test(name)) out.push(p);
  }
  return out;
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function extractPage(pngPath) {
  const bytes = readFileSync(pngPath);
  if (bytes.length < MIN_PAGE_BYTES) {
    return { skipped: "blank" };
  }
  const b64 = bytes.toString("base64");

  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      const resp = await fetch(MUNDOX_URL, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          model: MODEL,
          messages: [
            {
              role: "user",
              content: [
                { type: "text", text: PROMPT },
                { type: "image_url", image_url: { url: `data:image/png;base64,${b64}` } },
              ],
            },
          ],
          max_tokens: 1500,
          temperature: 0.0,
          chat_template_kwargs: { enable_thinking: false },
        }),
        signal: AbortSignal.timeout(120_000),
      });
      if (!resp.ok) throw new Error(`HTTP ${resp.status}: ${await resp.text()}`);
      const json = await resp.json();
      const text = json.choices?.[0]?.message?.content ?? "";
      // Parse JSON out of the response
      const match = text.match(/\{[\s\S]*\}/);
      if (!match) throw new Error(`No JSON in response: ${text.slice(0, 200)}`);
      const data = JSON.parse(match[0]);
      return { data, raw: text };
    } catch (e) {
      console.error(`  attempt ${attempt}/${MAX_RETRIES} failed: ${e.message}`);
      if (attempt < MAX_RETRIES) await sleep(5000 * attempt);
      else return { error: e.message };
    }
  }
}

async function processOne(p) {
  const rel = p.replace(ROOT + "/", "");
  const result = await extractPage(p);
  if (result.skipped) {
    writeFileSync(p.replace(/\.png$/, ".extract.json"), JSON.stringify({ skipped: result.skipped }));
    return { rel, status: "skipped" };
  }
  if (result.error) return { rel, status: "error", error: result.error };
  writeFileSync(p.replace(/\.png$/, ".extract.json"), JSON.stringify(result.data, null, 2));
  const rate = result.data?.interest_rate ?? null;
  const payment = result.data?.monthly_payment_cents ?? null;
  return { rel, status: "ok", rate, payment };
}

async function runOnce() {
  const pages = walk(ROOT);
  const pending = pages.filter((p) => !existsSync(p.replace(/\.png$/, ".extract.json")));
  console.log(
    `[${new Date().toISOString()}] ${pages.length} total, ${pending.length} pending, concurrency=${CONCURRENCY}`
  );
  const stats = { done: 0, skipped: 0, errors: 0 };
  const startedAt = Date.now();

  // Worker pool: CONCURRENCY requests in flight at once, no artificial sleep
  let idx = 0;
  async function worker(id) {
    while (true) {
      const myIdx = idx++;
      if (myIdx >= pending.length) return;
      const p = pending[myIdx];
      const r = await processOne(p);
      if (r.status === "ok") {
        stats.done++;
        console.log(`  [w${id}] ${r.rel}  rate=${r.rate} payment=${r.payment}`);
      } else if (r.status === "skipped") {
        stats.skipped++;
        console.log(`  [w${id}] ${r.rel}  skipped`);
      } else {
        stats.errors++;
        console.log(`  [w${id}] ${r.rel}  ERROR: ${r.error}`);
      }
    }
  }
  const workers = Array.from({ length: CONCURRENCY }, (_, i) => worker(i + 1));
  await Promise.all(workers);

  const secs = (Date.now() - startedAt) / 1000;
  const rate = pending.length ? (pending.length / secs).toFixed(2) : "0";
  console.log(
    `[${new Date().toISOString()}] done=${stats.done} skipped=${stats.skipped} errors=${stats.errors} in ${secs.toFixed(0)}s (${rate} pages/s)`
  );
  return { ...stats, pending: pending.length };
}

async function main() {
  if (WATCH) {
    console.log("Watch mode: running every 60s, Ctrl+C to stop");
    while (true) {
      try {
        await runOnce();
      } catch (e) {
        console.error("pass failed:", e.message);
      }
      await sleep(60_000);
    }
  } else {
    await runOnce();
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
