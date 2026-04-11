import { describe, it, expect } from "vitest";
import {
  validateUrlForListings,
  validateUrlBeforeScrape,
  isDomainBlocked,
  isDomainAllowed,
} from "../../src/utils/allowlist.js";

// ─── validateUrlForListings ─────────────────────────────────────────

describe("validateUrlForListings", () => {
  it("allows zillow.com", () => {
    const result = validateUrlForListings("https://zillow.com/homes/dallas");
    expect(result.allowed).toBe(true);
  });

  it("allows www.zillow.com", () => {
    const result = validateUrlForListings("https://www.zillow.com/homes/dallas");
    expect(result.allowed).toBe(true);
  });

  it("allows redfin.com", () => {
    const result = validateUrlForListings("https://redfin.com/city/123/TX/Dallas");
    expect(result.allowed).toBe(true);
  });

  it("allows realtor.com", () => {
    const result = validateUrlForListings("https://realtor.com/realestateandhomes-search/Dallas_TX");
    expect(result.allowed).toBe(true);
  });

  it("allows .gov sites (via standard allowlist fallback)", () => {
    const result = validateUrlForListings("https://assessor.county.gov/records");
    expect(result.allowed).toBe(true);
  });

  it("blocks apartments.com (CoStar)", () => {
    const result = validateUrlForListings("https://apartments.com/search");
    expect(result.allowed).toBe(false);
  });

  it("blocks unknown domains (default deny)", () => {
    const result = validateUrlForListings("https://random-site.com/listings");
    expect(result.allowed).toBe(false);
  });

  it("rejects invalid URLs", () => {
    const result = validateUrlForListings("not-a-url");
    expect(result.allowed).toBe(false);
  });
});

// ─── validateUrlBeforeScrape — existing behavior unchanged ──────────

describe("validateUrlBeforeScrape", () => {
  it("blocks zillow.com for general scraping", () => {
    const result = validateUrlBeforeScrape("https://zillow.com/homes/dallas");
    expect(result.allowed).toBe(false);
    expect(result.reason).toContain("blocklist");
  });

  it("blocks redfin.com for general scraping", () => {
    const result = validateUrlBeforeScrape("https://redfin.com/city/123");
    expect(result.allowed).toBe(false);
  });

  it("blocks realtor.com for general scraping", () => {
    const result = validateUrlBeforeScrape("https://realtor.com/search");
    expect(result.allowed).toBe(false);
  });

  it("allows .gov sites", () => {
    const result = validateUrlBeforeScrape("https://assessor.county.gov/records");
    expect(result.allowed).toBe(true);
  });

  it("blocks unknown domains (default deny)", () => {
    const result = validateUrlBeforeScrape("https://random-site.com/listings");
    expect(result.allowed).toBe(false);
    expect(result.reason).toContain("allowlist");
  });
});

// ─── isDomainBlocked ────────────────────────────────────────────────

describe("isDomainBlocked", () => {
  it("blocks apartments.com", () => {
    expect(isDomainBlocked("https://apartments.com/foo")).toBe(true);
  });

  it("blocks costar.com", () => {
    expect(isDomainBlocked("https://costar.com/data")).toBe(true);
  });

  it("blocks subdomains of blocked domains", () => {
    expect(isDomainBlocked("https://www.apartments.com/foo")).toBe(true);
  });

  it("does not block gov sites", () => {
    expect(isDomainBlocked("https://county.gov/records")).toBe(false);
  });

  it("blocks unparseable URLs", () => {
    expect(isDomainBlocked("garbage")).toBe(true);
  });
});

// ─── isDomainAllowed ────────────────────────────────────────────────

describe("isDomainAllowed", () => {
  it("allows .gov domains", () => {
    expect(isDomainAllowed("https://tax.county.gov/records")).toBe(true);
  });

  it("allows .org domains", () => {
    expect(isDomainAllowed("https://some-data.org/page")).toBe(true);
  });

  it("does not allow unknown commercial domains", () => {
    expect(isDomainAllowed("https://random-site.com/page")).toBe(false);
  });
});
