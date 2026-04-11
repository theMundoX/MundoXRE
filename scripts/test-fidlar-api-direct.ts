#!/usr/bin/env tsx
/**
 * Call Fidlar AVA API directly — no browser needed.
 * Auth: anonymous/anonymous bearer token
 */

const BASE = "https://ava.fidlar.com/OHFairfield/ScrapRelay.WebService.Ava/";

async function getToken(): Promise<string> {
  const resp = await fetch(BASE + "token", {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: "grant_type=password&username=anonymous&password=anonymous",
  });
  const data = await resp.json();
  return data.access_token;
}

async function search(token: string, startDate: string, endDate: string, page = 0): Promise<any> {
  const body = {
    FirstName: "",
    LastBusinessName: "",
    StartDate: startDate,
    EndDate: endDate,
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
    // Pagination
    ...(page > 0 ? { PageNumber: page } : {}),
  };

  const resp = await fetch(BASE + "breeze/Search", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Authorization": `Bearer ${token}`,
    },
    body: JSON.stringify(body),
  });

  if (!resp.ok) {
    console.log(`Search failed: ${resp.status} ${resp.statusText}`);
    const text = await resp.text();
    console.log(text.slice(0, 300));
    return null;
  }

  return await resp.json();
}

async function main() {
  console.log("Testing Fidlar AVA direct API...\n");

  const token = await getToken();
  console.log(`Token: ${token.slice(0, 50)}...\n`);

  // Search
  const data = await search(token, "2026-02-01", "2026-03-27");
  if (!data) return;

  console.log(`Total results: ${data.TotalResults}`);
  console.log(`Viewable results: ${data.ViewableResults}`);
  console.log(`DocResults returned: ${data.DocResults?.length}`);
  console.log(`ResultAccessCode: ${data.ResultAccessCode?.slice(0, 40)}`);
  console.log(`ResultId: ${data.ResultId}\n`);

  // Show first 3 results
  for (const doc of (data.DocResults || []).slice(0, 3)) {
    const grantors = doc.Names?.filter((n: any) => n.Type === "Grantor").map((n: any) => n.Name) || [];
    const grantees = doc.Names?.filter((n: any) => n.Type === "Grantee").map((n: any) => n.Name) || [];
    console.log(`  ${doc.DocumentType} | ${doc.RecordedDateTime} | $${doc.ConsiderationAmount}`);
    console.log(`    Grantor: ${grantors.join(", ").slice(0, 60)}`);
    console.log(`    Grantee: ${grantees.join(", ").slice(0, 60)}`);
    console.log(`    Doc#: ${doc.DocumentName} | Legal: ${doc.LegalSummary?.slice(0, 50)}`);
    console.log();
  }

  // Try to get page 2 using ResultAccessCode
  if (data.TotalResults > data.DocResults?.length) {
    console.log("\n=== Trying pagination ===\n");

    // Try PageNumber approach
    const page2 = await search(token, "2026-02-01", "2026-03-27", 1);
    if (page2) {
      console.log(`Page 2: ${page2.DocResults?.length} results`);
      if (page2.DocResults?.[0]) {
        console.log(`  First: ${page2.DocResults[0].DocumentType} | ${page2.DocResults[0].RecordedDateTime}`);
      }
    }

    // Try ResultAccessCode approach
    const resp2 = await fetch(BASE + "breeze/SearchPage", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${token}`,
      },
      body: JSON.stringify({
        ResultAccessCode: data.ResultAccessCode,
        ResultId: data.ResultId,
        PageNumber: 2,
      }),
    });
    if (resp2.ok) {
      const page2b = await resp2.json();
      console.log(`SearchPage endpoint: ${page2b.DocResults?.length || 0} results`);
    } else {
      console.log(`SearchPage: ${resp2.status} ${resp2.statusText}`);
      // Try other endpoint names
      for (const ep of ["breeze/Results", "breeze/GetPage", "breeze/NextPage"]) {
        const r = await fetch(BASE + ep, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "Authorization": `Bearer ${token}`,
          },
          body: JSON.stringify({
            ResultAccessCode: data.ResultAccessCode,
            ResultId: data.ResultId,
            PageNumber: 2,
          }),
        });
        if (r.ok) {
          const d = await r.json();
          console.log(`${ep}: ${JSON.stringify(d).slice(0, 200)}`);
        }
      }
    }
  }
}

main().catch(console.error);
