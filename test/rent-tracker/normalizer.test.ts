import { describe, it, expect } from "vitest";
import {
  normalizeListing,
  normalizeListings,
  crossReferenceListings,
  listingDedupeKey,
} from "../../src/rent-tracker/normalizer.js";
import type { OnMarketRecord } from "../../src/rent-tracker/adapters/base.js";

function makeRecord(overrides: Partial<OnMarketRecord> = {}): OnMarketRecord {
  return {
    address: "123 Main St",
    city: "Dallas",
    state: "TX",
    zip: "75201",
    is_on_market: true,
    mls_list_price: 350000,
    listing_source: "zillow",
    observed_at: "2026-03-27T00:00:00Z",
    raw: {},
    ...overrides,
  };
}

// ─── Address Normalization ──────────────────────────────────────────

describe("normalizeListing — address normalization", () => {
  it("collapses extra whitespace", () => {
    const result = normalizeListing(makeRecord({ address: "123   Main   St" }));
    expect(result).not.toBeNull();
    expect(result!.address).toBe("123 MAIN ST");
  });

  it("converts to uppercase", () => {
    const result = normalizeListing(makeRecord({ address: "456 elm avenue" }));
    expect(result!.address).toBe("456 ELM AVENUE");
  });

  it("normalizes spacing after house number", () => {
    const result = normalizeListing(makeRecord({ address: "789  Oak Blvd" }));
    expect(result!.address).toBe("789 OAK BLVD");
  });

  it("trims leading and trailing whitespace", () => {
    const result = normalizeListing(makeRecord({ address: "  100 Pine Rd  " }));
    expect(result!.address).toBe("100 PINE RD");
  });
});

// ─── ZIP Extraction ─────────────────────────────────────────────────

describe("normalizeListing — ZIP extraction", () => {
  it("extracts 5-digit ZIP from longer string", () => {
    const result = normalizeListing(makeRecord({ zip: "75201-1234" }));
    expect(result!.zip).toBe("75201");
  });

  it("passes through a clean 5-digit ZIP", () => {
    const result = normalizeListing(makeRecord({ zip: "90210" }));
    expect(result!.zip).toBe("90210");
  });

  it("returns undefined for empty ZIP", () => {
    const result = normalizeListing(makeRecord({ zip: "" }));
    expect(result!.zip).toBeUndefined();
  });

  it("returns undefined for undefined ZIP", () => {
    const result = normalizeListing(makeRecord({ zip: undefined as unknown as string }));
    expect(result!.zip).toBeUndefined();
  });
});

// ─── Price Validation ───────────────────────────────────────────────

describe("normalizeListing — price validation", () => {
  it("rejects negative price", () => {
    const result = normalizeListing(makeRecord({ mls_list_price: -50000 }));
    expect(result!.mls_list_price).toBeUndefined();
  });

  it("rejects price above 500M cap", () => {
    const result = normalizeListing(makeRecord({ mls_list_price: 600_000_000 }));
    expect(result!.mls_list_price).toBeUndefined();
  });

  it("rejects zero price", () => {
    const result = normalizeListing(makeRecord({ mls_list_price: 0 }));
    expect(result!.mls_list_price).toBeUndefined();
  });

  it("passes valid price through", () => {
    const result = normalizeListing(makeRecord({ mls_list_price: 275000 }));
    expect(result!.mls_list_price).toBe(275000);
  });

  it("accepts price just under 500M", () => {
    const result = normalizeListing(makeRecord({ mls_list_price: 499_999_999 }));
    expect(result!.mls_list_price).toBe(499_999_999);
  });
});

// ─── Invalid Record Filtering ───────────────────────────────────────

describe("normalizeListing — invalid record filtering", () => {
  it("returns null for missing address", () => {
    const result = normalizeListing(makeRecord({ address: "" }));
    expect(result).toBeNull();
  });

  it("returns null for address too short", () => {
    const result = normalizeListing(makeRecord({ address: "Hi" }));
    expect(result).toBeNull();
  });

  it("returns null for address with no digits", () => {
    const result = normalizeListing(makeRecord({ address: "Main Street" }));
    expect(result).toBeNull();
  });

  it("returns null for missing city", () => {
    const result = normalizeListing(makeRecord({ city: "" }));
    expect(result).toBeNull();
  });

  it("returns null for missing state", () => {
    const result = normalizeListing(makeRecord({ state: "" }));
    expect(result).toBeNull();
  });
});

describe("normalizeListings — batch filtering", () => {
  it("filters out invalid records and keeps valid ones", () => {
    const records = [
      makeRecord({ address: "100 Good St" }),
      makeRecord({ address: "" }), // invalid
      makeRecord({ address: "200 Also Good Ave" }),
      makeRecord({ city: "" }), // invalid
    ];
    const results = normalizeListings(records);
    expect(results).toHaveLength(2);
    expect(results[0].address).toBe("100 GOOD ST");
    expect(results[1].address).toBe("200 ALSO GOOD AVE");
  });
});

// ─── Cross-Reference Confidence Scoring ─────────────────────────────

describe("crossReferenceListings", () => {
  it("upgrades confidence to 'high' for same address from 2 sources with matching price", () => {
    const signals = [
      normalizeListing(makeRecord({
        address: "500 Test Dr",
        city: "Dallas",
        state: "TX",
        mls_list_price: 300000,
        listing_source: "zillow",
      }))!,
      normalizeListing(makeRecord({
        address: "500 Test Dr",
        city: "Dallas",
        state: "TX",
        mls_list_price: 300000,
        listing_source: "redfin",
      }))!,
    ];

    const result = crossReferenceListings(signals);
    expect(result[0].confidence).toBe("high");
    expect(result[1].confidence).toBe("high");
  });

  it("stays 'single' when prices differ by more than 5%", () => {
    const signals = [
      normalizeListing(makeRecord({
        address: "600 Price Diff Ln",
        city: "Houston",
        state: "TX",
        mls_list_price: 200000,
        listing_source: "zillow",
      }))!,
      normalizeListing(makeRecord({
        address: "600 Price Diff Ln",
        city: "Houston",
        state: "TX",
        mls_list_price: 250000, // 25% higher
        listing_source: "redfin",
      }))!,
    ];

    const result = crossReferenceListings(signals);
    expect(result[0].confidence).toBe("single");
    expect(result[1].confidence).toBe("single");
  });

  it("stays 'single' when same source appears twice (not cross-referenced)", () => {
    const signals = [
      normalizeListing(makeRecord({
        address: "700 Dup Ave",
        city: "Austin",
        state: "TX",
        mls_list_price: 400000,
        listing_source: "zillow",
      }))!,
      normalizeListing(makeRecord({
        address: "700 Dup Ave",
        city: "Austin",
        state: "TX",
        mls_list_price: 400000,
        listing_source: "zillow",
      }))!,
    ];

    const result = crossReferenceListings(signals);
    expect(result[0].confidence).toBe("single");
  });

  it("upgrades to 'high' when prices agree within 5%", () => {
    const signals = [
      normalizeListing(makeRecord({
        address: "800 Close Price Ct",
        city: "Miami",
        state: "FL",
        mls_list_price: 100000,
        listing_source: "zillow",
      }))!,
      normalizeListing(makeRecord({
        address: "800 Close Price Ct",
        city: "Miami",
        state: "FL",
        mls_list_price: 104000, // 4% higher — within 5%
        listing_source: "realtor",
      }))!,
    ];

    const result = crossReferenceListings(signals);
    expect(result[0].confidence).toBe("high");
    expect(result[1].confidence).toBe("high");
  });
});

// ─── Dedupe Key ─────────────────────────────────────────────────────

describe("listingDedupeKey", () => {
  it("generates a pipe-delimited key from normalized fields", () => {
    const key = listingDedupeKey(makeRecord({
      address: "  123  Main St  ",
      city: "dallas",
      state: "tx",
      listing_source: "zillow",
    }));
    expect(key).toBe("123 MAIN ST|DALLAS|TX|zillow");
  });

  it("different sources produce different keys", () => {
    const base = { address: "100 Oak Ave", city: "Houston", state: "TX" };
    const k1 = listingDedupeKey(makeRecord({ ...base, listing_source: "zillow" }));
    const k2 = listingDedupeKey(makeRecord({ ...base, listing_source: "redfin" }));
    expect(k1).not.toBe(k2);
  });

  it("same record produces the same key", () => {
    const record = makeRecord();
    expect(listingDedupeKey(record)).toBe(listingDedupeKey(record));
  });
});
