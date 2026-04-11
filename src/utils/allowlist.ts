/**
 * Domain allowlist — default DENY. Only explicitly approved domains get scraped.
 * Blocked domains are NEVER scraped regardless of allowlist.
 */

const BLOCKED_DOMAINS = new Set([
  // CoStar Group
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

  // Other aggregators
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

  // County assessor data platforms
  /^(www\.)?oktaxrolls\.com$/,
  /^(www\.)?actdatascout\.com$/,

  // Public listing sources
  /\.craigslist\.org$/,
  /\.facebook\.com$/,

  // Government sites — must end with .gov, .us, or .org
  /\.(gov|us|org)$/,
];

function extractHostname(url: string): string {
  try {
    return new URL(url).hostname.toLowerCase();
  } catch {
    return "";
  }
}

/**
 * Check if a URL belongs to a blocked domain.
 * Uses hostname suffix matching to prevent bypass via subdomains or TLD tricks.
 */
export function isDomainBlocked(url: string): boolean {
  const hostname = extractHostname(url);
  if (!hostname) return true; // Block unparseable URLs

  for (const blocked of BLOCKED_DOMAINS) {
    if (hostname === blocked || hostname.endsWith(`.${blocked}`)) {
      return true;
    }
  }
  return false;
}

/**
 * Check if a URL is on the approved allowlist.
 * Default is DENY — unknown domains are blocked.
 */
export function isDomainAllowed(url: string): boolean {
  if (isDomainBlocked(url)) return false;

  const hostname = extractHostname(url);
  if (!hostname) return false;

  for (const pattern of ALLOWED_PATTERNS) {
    if (pattern.test(hostname)) return true;
  }

  // Default DENY — unknown domains must be explicitly added
  return false;
}

/**
 * Validate a URL before scraping. Returns reason if denied.
 */
export function validateUrlBeforeScrape(url: string): { allowed: boolean; reason?: string } {
  const hostname = extractHostname(url);

  if (!hostname) {
    return { allowed: false, reason: "Invalid URL." };
  }

  if (isDomainBlocked(url)) {
    return { allowed: false, reason: "Domain is on the blocklist." };
  }

  if (!isDomainAllowed(url)) {
    return { allowed: false, reason: "Domain is not on the allowlist." };
  }

  return { allowed: true };
}

/**
 * Validate a URL after a redirect. Must be called after every redirect.
 */
export function validateRedirect(originalUrl: string, redirectUrl: string): { allowed: boolean; reason?: string } {
  return validateUrlBeforeScrape(redirectUrl);
}

// ─── Listing Pipeline — Scoped Access ───────────────────────────────
// These domains are blocked for assessor/general scraping but allowed
// ONLY through the listing pipeline for on-market signal extraction.

const LISTING_ALLOWED_DOMAINS = new Set([
  "zillow.com",
  "redfin.com",
  "realtor.com",
]);

/**
 * Validate a URL for the listing pipeline only.
 * Allows Zillow, Redfin, and Realtor.com — domains normally blocked.
 * Also allows all domains on the standard allowlist.
 */
export function validateUrlForListings(url: string): { allowed: boolean; reason?: string } {
  const hostname = extractHostname(url);
  if (!hostname) {
    return { allowed: false, reason: "Invalid URL." };
  }

  // Check listing-specific allowed domains first
  for (const allowed of LISTING_ALLOWED_DOMAINS) {
    if (hostname === allowed || hostname.endsWith(`.${allowed}`)) {
      return { allowed: true };
    }
  }

  // Fall back to standard allowlist (for state license .gov sites, etc.)
  return validateUrlBeforeScrape(url);
}
