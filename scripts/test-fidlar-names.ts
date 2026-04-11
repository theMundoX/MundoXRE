#!/usr/bin/env tsx
/**
 * Figure out why Names are empty in direct API calls.
 * Might need a different endpoint or header.
 */

const BASE = "https://ava.fidlar.com/OHFairfield/ScrapRelay.WebService.Ava/";

async function main() {
  // Get token
  const tokenResp = await fetch(BASE + "token", {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: "grant_type=password&username=anonymous&password=anonymous",
  });
  const { access_token: token } = await tokenResp.json();

  // Search with full details
  const searchResp = await fetch(BASE + "breeze/Search", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Authorization": `Bearer ${token}`,
    },
    body: JSON.stringify({
      FirstName: "",
      LastBusinessName: "",
      StartDate: "2026-03-25",
      EndDate: "2026-03-26",
      DocumentName: "",
      DocumentType: "",
      SubdivisionName: "",
      SubdivisionLot: "",
      SubdivisionBlock: "",
      MunicipalityName: "",
      TractSection: "",
      TractTownship: "",
      TractRange: "",
      TractQuarter: "",
      TractQuarterQuarter: "",
      Book: "",
      Page: "",
      LotOfRecord: "",
      BlockOfRecord: "",
      AddressNumber: "",
      AddressDirection: "",
      AddressStreetName: "",
      TaxId: "",
    }),
  });
  const data = await searchResp.json();
  console.log(`Results: ${data.TotalResults}`);

  // Show first 3 with all fields
  for (const doc of (data.DocResults || []).slice(0, 3)) {
    console.log(`\nDoc ${doc.Id}: ${doc.DocumentType}`);
    console.log(`  ConsiderationAmount: $${doc.ConsiderationAmount}`);
    console.log(`  Names: ${JSON.stringify(doc.Names)}`);
    console.log(`  RecordedDateTime: ${doc.RecordedDateTime}`);
    console.log(`  DocumentName: ${doc.DocumentName}`);
    console.log(`  Legals: ${JSON.stringify(doc.Legals)}`);
    console.log(`  Book: ${doc.Book} Page: ${doc.Page}`);
    console.log(`  All keys: ${Object.keys(doc).join(", ")}`);
  }

  // Try getting document details
  if (data.DocResults?.length > 0) {
    const docId = data.DocResults[0].Id;
    console.log(`\n=== Trying document detail for ID ${docId} ===`);

    for (const ep of ["breeze/Document", "breeze/DocumentDetail", "breeze/GetDocument"]) {
      const r = await fetch(BASE + ep, {
        method: "POST",
        headers: { "Content-Type": "application/json", "Authorization": `Bearer ${token}` },
        body: JSON.stringify({ Id: docId, DocumentId: docId }),
      });
      if (r.ok) {
        const d = await r.json();
        console.log(`${ep}: ${JSON.stringify(d).slice(0, 500)}`);
      } else {
        console.log(`${ep}: ${r.status}`);
      }
    }

    // Also try GET
    for (const ep of [`breeze/Document/${docId}`, `breeze/DocumentDetail/${docId}`]) {
      const r = await fetch(BASE + ep, {
        headers: { "Authorization": `Bearer ${token}` },
      });
      if (r.ok) {
        const d = await r.json();
        console.log(`GET ${ep}: ${JSON.stringify(d).slice(0, 500)}`);
      }
    }
  }
}

main().catch(console.error);
