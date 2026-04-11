/**
 * Per-domain rate limiter using token bucket algorithm.
 * Government/county sites get much slower rates than commercial sites.
 */

interface DomainBucket {
  lastRequest: number;
  minInterval: number;
}

const buckets = new Map<string, DomainBucket>();

// Default intervals (milliseconds between requests)
const GOVERNMENT_INTERVAL = 8_000; // 8 seconds for .gov/.us/county sites
const COMMERCIAL_INTERVAL = 3_000; // 3 seconds for property websites
const CRAIGSLIST_INTERVAL = 5_000; // 5 seconds for craigslist

function getDefaultInterval(domain: string): number {
  const lower = domain.toLowerCase();
  if (
    lower.endsWith(".gov") ||
    lower.endsWith(".us") ||
    lower.includes("county") ||
    lower.includes("assessor") ||
    lower.includes("recorder") ||
    lower.includes("clerk")
  ) {
    return GOVERNMENT_INTERVAL;
  }
  if (lower.includes("craigslist")) {
    return CRAIGSLIST_INTERVAL;
  }
  return COMMERCIAL_INTERVAL;
}

function extractDomain(url: string): string {
  try {
    return new URL(url).hostname.toLowerCase();
  } catch {
    return url.toLowerCase();
  }
}

/**
 * Wait until we're allowed to make a request to this domain.
 * Returns the number of milliseconds waited.
 */
export async function waitForSlot(url: string): Promise<number> {
  const domain = extractDomain(url);

  if (!buckets.has(domain)) {
    buckets.set(domain, {
      lastRequest: 0,
      minInterval: getDefaultInterval(domain),
    });
  }

  const bucket = buckets.get(domain)!;
  const now = Date.now();
  const elapsed = now - bucket.lastRequest;
  const waitTime = Math.max(0, bucket.minInterval - elapsed);

  if (waitTime > 0) {
    await new Promise((resolve) => setTimeout(resolve, waitTime));
  }

  bucket.lastRequest = Date.now();
  return waitTime;
}

/**
 * Set a custom rate limit for a specific domain.
 */
export function setRateLimit(domain: string, intervalMs: number) {
  const existing = buckets.get(domain);
  if (existing) {
    existing.minInterval = intervalMs;
  } else {
    buckets.set(domain, { lastRequest: 0, minInterval: intervalMs });
  }
}
