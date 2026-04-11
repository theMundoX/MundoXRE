#!/usr/bin/env tsx
/**
 * Pull a small sample of real mortgage document PDFs for the labeling pipeline.
 *
 * Approach: grab Freddie Mac uniform mortgage note samples (publicly published
 * standardized forms used across US counties). These are real mortgage
 * instruments with real field structure — interest rate, amount, term, parties,
 * maturity date. Perfect for validating the extraction pipeline without fighting
 * county scrapers.
 *
 * For PRODUCTION training data we'll later pull actual scanned county filings
 * once a working scraper is in place, but this lets us prove the end-to-end
 * pipeline (download → pre-extract → labeling UI → fine-tune) today.
 *
 * Saves PDFs + first-page PNGs to data/labeling-sample/
 */
import { writeFileSync, mkdirSync, existsSync } from "node:fs";
import { join } from "node:path";

const OUT = "C:/Users/msanc/mxre/data/labeling-sample";
if (!existsSync(OUT)) mkdirSync(OUT, { recursive: true });

// Publicly published Fannie Mae / Freddie Mac uniform mortgage instruments.
// These are the actual standardized forms used by most US lenders.
const SAMPLES = [
  {
    id: "sample-fnma-3200",
    name: "Fannie Mae Uniform Note (Fixed Rate) 3200",
    url: "https://singlefamily.fanniemae.com/media/document/pdf/multistate-fixedrate-note-form-3200",
  },
  {
    id: "sample-fnma-3510",
    name: "Fannie Mae Uniform Mortgage (Deed of Trust) 3510",
    url: "https://singlefamily.fanniemae.com/media/document/pdf/multistate-deed-trust-form-3510",
  },
  {
    id: "sample-fnma-3502",
    name: "Fannie Mae Uniform Instrument 3502",
    url: "https://singlefamily.fanniemae.com/media/document/pdf/multistate-one-to-four-family-rider-form-3502",
  },
];

async function main() {
  console.log("MXRE — sample PDF puller\n");
  let ok = 0;
  let failed = 0;

  for (const s of SAMPLES) {
    const docDir = join(OUT, s.id);
    const pdfPath = join(docDir, `${s.id}.pdf`);
    mkdirSync(docDir, { recursive: true });

    if (existsSync(pdfPath)) {
      console.log(`  ${s.id}: already present, skipping`);
      ok++;
      continue;
    }

    try {
      console.log(`  fetching ${s.id} ...`);
      const r = await fetch(s.url, {
        headers: { "User-Agent": "Mozilla/5.0 MXRE-SamplePuller/1.0" },
        redirect: "follow",
      });
      if (!r.ok) {
        console.log(`    ✗ HTTP ${r.status}`);
        failed++;
        continue;
      }
      const ct = r.headers.get("content-type") || "";
      const buf = Buffer.from(await r.arrayBuffer());
      if (buf.length < 2000) {
        console.log(`    ✗ too small (${buf.length} b, ct=${ct})`);
        failed++;
        continue;
      }
      writeFileSync(pdfPath, buf);

      // Write a tiny stub metadata so pre-extract knows what this is
      writeFileSync(
        join(docDir, "meta.json"),
        JSON.stringify({
          doc_number: s.id,
          label: s.name,
          source: "fannie_mae_public_sample",
          source_url: s.url,
          pdf_path: pdfPath,
          status: "draft",
          note: "Public sample instrument, not a real county filing.",
        }, null, 2),
      );

      console.log(`    ✓ saved ${(buf.length/1024).toFixed(0)} KB → ${pdfPath}`);
      ok++;
    } catch (e: any) {
      console.log(`    ✗ ${e?.message || e}`);
      failed++;
    }
  }

  console.log(`\nDone. ok=${ok} failed=${failed}`);
  console.log(`Output: ${OUT}`);
  console.log(`\nNext: run PDF→PNG conversion, then pre-extract with MundoX.`);
}

main().catch((e) => { console.error(e); process.exit(1); });
