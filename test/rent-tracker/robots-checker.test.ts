import { describe, it, expect, vi } from "vitest";

// Mock the cache module so robots-checker doesn't touch the filesystem
vi.mock("../../src/utils/cache.js", () => ({
  getCached: vi.fn(() => null),
  setCache: vi.fn(),
}));

// Mock global fetch so we don't make real network requests
const mockFetch = vi.fn();
vi.stubGlobal("fetch", mockFetch);

// Import after mocks are set up
const { isPathAllowed, getCrawlDelay } = await import("../../src/rent-tracker/robots-checker.js");

describe("robots-checker — module exports", () => {
  it("exports isPathAllowed as a function", () => {
    expect(typeof isPathAllowed).toBe("function");
  });

  it("exports getCrawlDelay as a function", () => {
    expect(typeof getCrawlDelay).toBe("function");
  });
});

describe("isPathAllowed — with mocked fetch", () => {
  it("allows a path when robots.txt has no matching disallow", async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      text: async () => "User-agent: *\nDisallow: /private/\n",
    });

    const allowed = await isPathAllowed("https://example.com/public/page");
    expect(allowed).toBe(true);
  });

  it("disallows a path that matches a Disallow rule", async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      text: async () => "User-agent: *\nDisallow: /private/\n",
    });

    const allowed = await isPathAllowed("https://example.com/private/secret");
    expect(allowed).toBe(false);
  });

  it("allows everything when robots.txt fetch fails (network error)", async () => {
    mockFetch.mockRejectedValueOnce(new Error("Network error"));

    const allowed = await isPathAllowed("https://broken.example.com/anything");
    expect(allowed).toBe(true);
  });

  it("allows everything when robots.txt returns 404", async () => {
    mockFetch.mockResolvedValueOnce({ ok: false, status: 404 });

    const allowed = await isPathAllowed("https://norobots.example.com/page");
    expect(allowed).toBe(true);
  });

  it("returns false for unparseable URL", async () => {
    const allowed = await isPathAllowed("not-a-valid-url");
    expect(allowed).toBe(false);
  });
});

describe("getCrawlDelay — with mocked fetch", () => {
  it("returns crawl-delay when specified", async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      text: async () => "User-agent: *\nCrawl-delay: 5\n",
    });

    const delay = await getCrawlDelay("crawldelay.example.com");
    expect(delay).toBe(5);
  });

  it("returns null when no crawl-delay is set", async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      text: async () => "User-agent: *\nDisallow: /admin/\n",
    });

    const delay = await getCrawlDelay("nodelay.example.com");
    expect(delay).toBeNull();
  });
});
