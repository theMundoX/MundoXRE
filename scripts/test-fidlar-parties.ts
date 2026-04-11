#!/usr/bin/env tsx
const BASE = "https://ava.fidlar.com/OHFairfield/ScrapRelay.WebService.Ava/";

async function main() {
  const { access_token: token } = await (await fetch(BASE + "token", {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: "grant_type=password&username=anonymous&password=anonymous",
  })).json();

  const data = await (await fetch(BASE + "breeze/Search", {
    method: "POST",
    headers: { "Content-Type": "application/json", "Authorization": `Bearer ${token}` },
    body: JSON.stringify({
      FirstName: "", LastBusinessName: "", StartDate: "2026-03-25", EndDate: "2026-03-26",
      DocumentName: "", DocumentType: "", SubdivisionName: "", SubdivisionLot: "", SubdivisionBlock: "",
      MunicipalityName: "", TractSection: "", TractTownship: "", TractRange: "", TractQuarter: "",
      TractQuarterQuarter: "", Book: "", Page: "", LotOfRecord: "", BlockOfRecord: "",
      AddressNumber: "", AddressDirection: "", AddressStreetName: "", TaxId: "",
    }),
  })).json();

  // Show mortgage records with Party1/Party2/Parties and CanViewImage
  for (const doc of data.DocResults.filter((d: any) => d.DocumentType.includes("MORTGAGE")).slice(0, 5)) {
    console.log(`\n${doc.DocumentType} — $${doc.ConsiderationAmount}`);
    console.log(`  Doc#: ${doc.DocumentName}`);
    console.log(`  Date: ${doc.RecordedDateTime}`);
    console.log(`  Party1: ${JSON.stringify(doc.Party1)}`);
    console.log(`  Party2: ${JSON.stringify(doc.Party2)}`);
    console.log(`  Parties: ${JSON.stringify(doc.Parties)?.slice(0, 300)}`);
    console.log(`  CanViewImage: ${doc.CanViewImage}`);
    console.log(`  ImagePageCount: ${doc.ImagePageCount}`);
    console.log(`  TapestryLink: ${doc.TapestryLink}`);
    console.log(`  Legal: ${doc.LegalSummary}`);
  }
}

main().catch(console.error);
