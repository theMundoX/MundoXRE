/**
 * Domain allowlist — only approved domains get scraped.
 * If a domain isn't on this list, it's blocked. Period.
 */

const BLOCKED_DOMAINS = new Set([
  // CoStar Group (litigious)
  "apartments.com",
  "costar.com",
  "rent.com",
  "apartmentguide.com",
  "apartmentfinder.com",
  "apartamentos.com",
  "westside-rentals.com",

  // Zillow Group
  "zillow.com",
  "trulia.com",
  "hotpads.com",
  "streeteasy.com",

  // Other aggregators with anti-scraping ToS
  "realtor.com",
  "redfin.com",
  "rentpath.com",
  "zumper.com",
  "apartmentlist.com",
  "padmapper.com",
]);

const ALLOWED_PATTERNS = [
  // Property management platforms (individual property subdomains)
  /\.rentcafe\.com$/,
  /\.entrata\.com$/,
  /\.appfolio\.com$/,
  /\.buildium\.com$/,
  /\.myresman\.com$/,
  /\.onlineleasing\.realpage\.com$/,

  // Public listing sources
  /\.craigslist\.org$/,
  /\.facebook\.com$/,

  // Government / county sites
  /\.gov$/,
  /\.us$/,
  /county\./,
  /assessor\./,
  /recorder\./,
  /clerk\./,
];

function extractDomain(url: string): string {
  try {
    const hostname = new URL(url).hostname.toLowerCase();
    // Get the root domain (e.g., "apartments.com" from "www.apartments.com")
    const parts = hostname.split(".");
    if (parts.length >= 2) {
      return parts.slice(-2).join(".");
    }
    return hostname;
  } catch {
    return url.toLowerCase();
  }
}

function extractHostname(url: string): string {
  try {
    return new URL(url).hostname.toLowerCase();
  } catch {
    return url.toLowerCase();
  }
}

export function isDomainBlocked(url: string): boolean {
  const domain = extractDomain(url);
  return BLOCKED_DOMAINS.has(domain);
}

export function isDomainAllowed(url: string): boolean {
  if (isDomainBlocked(url)) return false;

  const hostname = extractHostname(url);

  // Check if it matches any allowed pattern
  for (const pattern of ALLOWED_PATTERNS) {
    if (pattern.test(hostname)) return true;
  }

  // Individual property websites (not on blocklist, not a known aggregator)
  // are allowed by default — these are marketing sites that WANT traffic
  return true;
}

export function validateUrlBeforeScrape(url: string): { allowed: boolean; reason?: string } {
  if (isDomainBlocked(url)) {
    const domain = extractDomain(url);
    return {
      allowed: false,
      reason: `Domain "${domain}" is on the blocklist. This is an aggregator site — do not scrape.`,
    };
  }

  return { allowed: true };
}
