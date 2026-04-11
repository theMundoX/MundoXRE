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
const GOVERNMENT_INTERVAL = 500;   // 500ms for .gov/.us/county sites (tested: they handle 5+ req/s)
const COMMERCIAL_INTERVAL = 800;   // 800ms for property websites
const CRAIGSLIST_INTERVAL = 3_000; // 3 seconds for craigslist (aggressive bot detection)
const LISTING_INTERVAL = 500;      // 500ms for listing sites (Redfin CSV is a public download endpoint)

const LISTING_DOMAINS = ["zillow.com", "redfin.com", "realtor.com", "trulia.com"];

function isListingDomain(domain: string): boolean {
  const lower = domain.toLowerCase();
  return LISTING_DOMAINS.some((d) => lower === d || lower.endsWith(`.${d}`));
}

function getDefaultInterval(domain: string): number {
  const lower = domain.toLowerCase();
  if (isListingDomain(lower)) {
    return LISTING_INTERVAL;
  }
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

/**
 * Adaptively slow down requests to a domain when errors occur.
 * Call this after a failed request to increase the interval.
 */
export function backoffDomain(url: string) {
  const domain = extractDomain(url);
  const bucket = buckets.get(domain);
  if (bucket) {
    // Double the interval, max 60 seconds
    bucket.minInterval = Math.min(bucket.minInterval * 2, 60_000);
  }
}

/**
 * Reset a domain's rate limit back to default after successful requests.
 */
export function resetDomainRate(url: string) {
  const domain = extractDomain(url);
  const bucket = buckets.get(domain);
  if (bucket) {
    bucket.minInterval = getDefaultInterval(domain);
  }
}

/**
 * Add random jitter to a base delay. Used by listing adapters to
 * look more human without being needlessly slow.
 */
export function addRandomJitter(baseMs: number, minJitter = 500, maxJitter = 2000): number {
  return baseMs + Math.floor(Math.random() * (maxJitter - minJitter)) + minJitter;
}

/**
 * Wait for a listing-specific slot with jitter.
 * Combines the per-domain rate limiter with random human-like delay.
 */
export async function waitForListingSlot(url: string): Promise<number> {
  const waited = await waitForSlot(url);
  const jitter = Math.floor(Math.random() * 1500) + 500; // 500-2000ms extra
  await new Promise((resolve) => setTimeout(resolve, jitter));
  return waited + jitter;
}
