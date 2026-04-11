/**
 * Rent Tracker — robots.txt compliance checker.
 * Fetches, parses, and caches robots.txt for any domain before scraping.
 * Demonstrates good-faith compliance — a legal safeguard.
 */

import { getCached, setCache } from "../utils/cache.js";

const ROBOTS_CACHE_TTL = 24 * 60 * 60 * 1000; // 24 hours

interface RobotsRule {
  path: string;
  allowed: boolean;
}

interface ParsedRobots {
  rules: RobotsRule[];
  crawlDelay?: number;
  sitemaps: string[];
}

/**
 * Parse a robots.txt string into structured rules.
 * Only extracts rules for the wildcard (*) user agent — we don't spoof a specific bot name.
 */
function parseRobotsTxt(content: string): ParsedRobots {
  const rules: RobotsRule[] = [];
  const sitemaps: string[] = [];
  let crawlDelay: number | undefined;
  let inWildcardBlock = false;
  let hasUserAgent = false;

  for (const rawLine of content.split("\n")) {
    const line = rawLine.trim();
    if (!line || line.startsWith("#")) continue;

    const [directive, ...rest] = line.split(":");
    const key = directive.trim().toLowerCase();
    const value = rest.join(":").trim();

    if (key === "user-agent") {
      hasUserAgent = true;
      inWildcardBlock = value === "*";
      continue;
    }

    if (key === "sitemap") {
      sitemaps.push(value);
      continue;
    }

    // If no user-agent block seen yet, or we're in the wildcard block
    if (!hasUserAgent || inWildcardBlock) {
      if (key === "disallow" && value) {
        rules.push({ path: value, allowed: false });
      } else if (key === "allow" && value) {
        rules.push({ path: value, allowed: true });
      } else if (key === "crawl-delay" && value) {
        const delay = parseFloat(value);
        if (!isNaN(delay) && delay > 0) {
          crawlDelay = delay;
        }
      }
    }
  }

  return { rules, crawlDelay, sitemaps };
}

/**
 * Check if a path matches a robots.txt rule pattern.
 * Supports * wildcards and $ end-of-string anchor.
 */
function pathMatchesRule(path: string, pattern: string): boolean {
  // Convert robots.txt pattern to regex
  let regex = pattern
    .replace(/[.+?^{}()|[\]\\]/g, "\\$&") // escape regex chars except *
    .replace(/\*/g, ".*"); // * → .*

  if (regex.endsWith("\\$")) {
    regex = regex.slice(0, -2) + "$"; // $ anchor at end
  }

  try {
    return new RegExp(`^${regex}`).test(path);
  } catch {
    // Invalid pattern — conservative: assume disallowed
    return pattern === "/" || path.startsWith(pattern);
  }
}

/**
 * Fetch and parse robots.txt for a domain. Results are cached for 24h.
 */
async function fetchRobots(domain: string): Promise<ParsedRobots> {
  const cacheKey = `robots:${domain}`;
  const cached = getCached(cacheKey, ROBOTS_CACHE_TTL);
  if (cached) {
    return JSON.parse(cached) as ParsedRobots;
  }

  const url = `https://${domain}/robots.txt`;
  try {
    const response = await fetch(url, {
      signal: AbortSignal.timeout(10_000),
      headers: {
        "User-Agent": "Mozilla/5.0 (compatible; MXREBot/1.0)",
      },
    });

    if (!response.ok) {
      // No robots.txt or error — assume everything is allowed
      const empty: ParsedRobots = { rules: [], sitemaps: [] };
      setCache(cacheKey, JSON.stringify(empty));
      return empty;
    }

    const text = await response.text();
    const parsed = parseRobotsTxt(text);
    setCache(cacheKey, JSON.stringify(parsed));
    return parsed;
  } catch {
    // Network error — assume everything is allowed (conservative in favor of scraping)
    const empty: ParsedRobots = { rules: [], sitemaps: [] };
    setCache(cacheKey, JSON.stringify(empty));
    return empty;
  }
}

/**
 * Check if a URL path is allowed by the domain's robots.txt.
 * Returns true if allowed, false if disallowed.
 *
 * When multiple rules match, the most specific (longest) rule wins.
 * If no rules match, access is allowed by default.
 */
export async function isPathAllowed(url: string): Promise<boolean> {
  try {
    const parsed = new URL(url);
    const domain = parsed.hostname;
    const path = parsed.pathname + parsed.search;

    const robots = await fetchRobots(domain);

    // Find all matching rules
    const matches = robots.rules.filter((rule) =>
      pathMatchesRule(path, rule.path),
    );

    if (matches.length === 0) return true; // No matching rules → allowed

    // Most specific (longest pattern) wins
    matches.sort((a, b) => b.path.length - a.path.length);
    return matches[0].allowed;
  } catch {
    // Can't parse URL — block to be safe
    return false;
  }
}

/**
 * Get the crawl-delay directive for a domain (in seconds).
 * Returns null if no crawl-delay is specified.
 */
export async function getCrawlDelay(domain: string): Promise<number | null> {
  const robots = await fetchRobots(domain);
  return robots.crawlDelay ?? null;
}
