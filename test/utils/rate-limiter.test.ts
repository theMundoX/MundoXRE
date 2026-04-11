import { describe, it, expect } from "vitest";
import {
  addRandomJitter,
  waitForListingSlot,
  waitForSlot,
  setRateLimit,
  backoffDomain,
  resetDomainRate,
} from "../../src/utils/rate-limiter.js";

// ─── addRandomJitter ────────────────────────────────────────────────

describe("addRandomJitter", () => {
  it("returns a value greater than or equal to base + minJitter", () => {
    // Default minJitter = 500
    for (let i = 0; i < 20; i++) {
      const result = addRandomJitter(1000);
      expect(result).toBeGreaterThanOrEqual(1000 + 500);
    }
  });

  it("returns a value less than base + maxJitter + minJitter", () => {
    // Default maxJitter = 2000, minJitter = 500
    // Formula: base + Math.floor(Math.random() * (maxJitter - minJitter)) + minJitter
    // Max possible: base + (maxJitter - minJitter - 1) + minJitter = base + maxJitter - 1
    for (let i = 0; i < 20; i++) {
      const result = addRandomJitter(1000);
      expect(result).toBeLessThanOrEqual(1000 + 2000);
    }
  });

  it("respects custom jitter parameters", () => {
    for (let i = 0; i < 20; i++) {
      const result = addRandomJitter(500, 100, 300);
      expect(result).toBeGreaterThanOrEqual(500 + 100);
      expect(result).toBeLessThanOrEqual(500 + 300);
    }
  });

  it("returns base + minJitter when minJitter equals maxJitter", () => {
    // random * 0 = 0, so result = base + minJitter
    const result = addRandomJitter(1000, 200, 200);
    expect(result).toBe(1200);
  });
});

// ─── waitForListingSlot ─────────────────────────────────────────────

describe("waitForListingSlot", () => {
  it("exists and is a function", () => {
    expect(typeof waitForListingSlot).toBe("function");
  });

  it("returns a promise", () => {
    // Use a unique domain to avoid interference from other tests
    const result = waitForListingSlot("https://unique-listing-test-1234.zillow.com/page");
    expect(result).toBeInstanceOf(Promise);
    // Don't await — just verify it's a promise (avoids slow 3s+ delay)
  });
});

// ─── waitForSlot ────────────────────────────────────────────────────

describe("waitForSlot", () => {
  it("exists and is a function", () => {
    expect(typeof waitForSlot).toBe("function");
  });
});

// ─── setRateLimit ───────────────────────────────────────────────────

describe("setRateLimit", () => {
  it("exists and is a function", () => {
    expect(typeof setRateLimit).toBe("function");
  });
});

// ─── backoffDomain / resetDomainRate ────────────────────────────────

describe("backoffDomain", () => {
  it("exists and is a function", () => {
    expect(typeof backoffDomain).toBe("function");
  });
});

describe("resetDomainRate", () => {
  it("exists and is a function", () => {
    expect(typeof resetDomainRate).toBe("function");
  });
});
