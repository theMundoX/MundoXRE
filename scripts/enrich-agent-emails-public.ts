#!/usr/bin/env tsx
import "dotenv/config";

const PG_URL = `${(process.env.SUPABASE_URL ?? "").replace(/\/$/, "")}/pg/query`;
const PG_KEY = process.env.SUPABASE_SERVICE_KEY ?? "";
const LIMIT = Math.max(1, parseInt(process.argv.find(a => a.startsWith("--limit="))?.split("=")[1] ?? "100", 10));
const DELAY_MS = Math.max(750, parseInt(process.argv.find(a => a.startsWith("--delay-ms="))?.split("=")[1] ?? "2500", 10));
const MAX_SEARCH_QUERIES = Math.max(1, parseInt(process.argv.find(a => a.startsWith("--max-search-queries="))?.split("=")[1] ?? "8", 10));
const MAX_SEARCH_LINKS = Math.max(1, parseInt(process.argv.find(a => a.startsWith("--max-search-links="))?.split("=")[1] ?? "12", 10));
const MAX_DIRECT_PROFILE_URLS = Math.max(0, parseInt(process.argv.find(a => a.startsWith("--max-direct-profile-urls="))?.split("=")[1] ?? "6", 10));
const MAX_PROFILE_LINKS_PER_PAGE = Math.max(0, parseInt(process.argv.find(a => a.startsWith("--max-profile-links-per-page="))?.split("=")[1] ?? "6", 10));
const FETCH_TIMEOUT_MS = Math.max(2_500, parseInt(process.argv.find(a => a.startsWith("--fetch-timeout-ms="))?.split("=")[1] ?? "8000", 10));
const ROW_TIMEOUT_MS = Math.max(5_000, parseInt(process.argv.find(a => a.startsWith("--row-timeout-ms="))?.split("=")[1] ?? "45000", 10));
const DRY_RUN = process.argv.includes("--dry-run");
const DISABLE_DUCKDUCKGO = process.argv.includes("--disable-duckduckgo");
const arg = (name: string) =>
  process.argv.find(a => a.startsWith(`--${name}=`))?.split("=").slice(1).join("=");
const STATE = arg("state")?.toUpperCase();
const CITY = arg("city")?.toUpperCase();
const BROKERAGE_PATTERN = arg("brokerage-pattern");

type ListingRow = {
  id: number;
  address?: string | null;
  city?: string | null;
  state_code?: string | null;
  listing_agent_name: string | null;
  listing_agent_first_name: string | null;
  listing_agent_last_name: string | null;
  listing_agent_phone: string | null;
  listing_brokerage: string | null;
  raw: Record<string, unknown> | null;
};

type Candidate = {
  email: string;
  url: string;
  confidence: "public_profile_verified";
};

const stats = {
  direct_profile_urls: 0,
  homepage_urls: 0,
  search_pages: 0,
  search_links: 0,
  duckduckgo_links: 0,
  bing_links: 0,
  search_pages_without_links: 0,
  profile_pages: 0,
  pages_with_email: 0,
  rejected_identity: 0,
  no_direct_brokerage_hint: 0,
  row_timeouts: 0,
  row_errors: 0,
};
const brokerageSamples = new Set<string>();

async function pg(query: string): Promise<Record<string, unknown>[]> {
  const response = await fetch(PG_URL, {
    method: "POST",
    headers: { apikey: PG_KEY, Authorization: `Bearer ${PG_KEY}`, "Content-Type": "application/json" },
    body: JSON.stringify({ query }),
    signal: AbortSignal.timeout(120_000),
  });
  if (!response.ok) throw new Error(`pg/query ${response.status}: ${await response.text()}`);
  return response.json();
}

function sql(value: unknown): string {
  if (value == null || value === "") return "null";
  return `'${String(value).replace(/'/g, "''")}'`;
}

function sleep(ms: number) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function withTimeout<T>(promise: Promise<T>, ms: number, label: string): Promise<T> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`${label} timed out after ${ms}ms`)), ms);
    promise.then(
      value => {
        clearTimeout(timer);
        resolve(value);
      },
      error => {
        clearTimeout(timer);
        reject(error);
      },
    );
  });
}

function cleanText(value: string): string {
  return value
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/\\u0040/gi, "@")
    .replace(/\\u002e/gi, ".")
    .replace(/\\u002d/gi, "-")
    .replace(/\\u002f/gi, "/")
    .replace(/&amp;/g, "&")
    .replace(/&#64;|&commat;/gi, "@")
    .replace(/&#46;|&period;/gi, ".")
    .replace(/&#x40;/gi, "@")
    .replace(/&#x2e;/gi, ".")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizePhone(value: string | null): string | null {
  if (!value) return null;
  const digits = value.replace(/\D/g, "");
  if (digits.length === 11 && digits.startsWith("1")) return digits.slice(1);
  return digits.length === 10 ? digits : null;
}

function splitName(row: ListingRow): { first: string; last: string; full: string } | null {
  const first = row.listing_agent_first_name?.trim();
  const last = row.listing_agent_last_name?.trim();
  if (first && last) return { first, last, full: `${first} ${last}` };

  const clean = row.listing_agent_name?.replace(/\s+/g, " ").trim();
  if (!clean) return null;
  const parts = clean.split(" ");
  if (parts.length < 2) return null;
  return { first: parts[0], last: parts.slice(1).join(" "), full: clean };
}

function brokerageTokens(value: string | null): string[] {
  if (!value) return [];
  return value
    .toLowerCase()
    .replace(/[^a-z0-9 ]+/g, " ")
    .split(/\s+/)
    .filter(token => token.length >= 4 && !["realty", "broker", "group", "home", "homes", "estate", "real", "llc", "inc"].includes(token));
}

function slug(value: string): string {
  return value.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "");
}

function rawString(row: ListingRow, keys: string[]): string | null {
  for (const key of keys) {
    const value = key.split(".").reduce<unknown>((current, part) => {
      if (current && typeof current === "object" && !Array.isArray(current)) return (current as Record<string, unknown>)[part];
      return undefined;
    }, row.raw);
    if (typeof value === "string" && value.trim()) return value.trim();
    if (typeof value === "number") return String(value);
  }
  return null;
}

function effectiveBrokerage(row: ListingRow): string | null {
  const rawBrokerage = rawString(row, ["redfinDetail.listingBrokerage", "listingBrokerage", "brokerageName", "brokerName"]);
  const current = row.listing_brokerage?.trim() ?? null;
  if (rawBrokerage && (!current || /^(ntreis|mls|mls grid|actris)$/i.test(current))) return rawBrokerage;
  return current ?? rawBrokerage;
}

function brokerageDomainHints(brokerage: string | null): string[] {
  const b = brokerage?.toLowerCase() ?? "";
  const hints: string[] = [];
  if (b.includes("f.c. tucker") || b.includes("fc tucker") || b.includes("tucker")) hints.push("talktotucker.com");
  if (b.includes("century 21") || b.includes("c21")) hints.push("century21.com");
  if (b.includes("kw ") || b.includes("keller williams")) hints.push("kw.com");
  if (b.includes("exp")) hints.push("exprealty.com");
  if (/\breal\b/.test(b) || b.includes("real brokerage")) hints.push("realbrokerage.com", "joinreal.com", "onereal.com");
  if (b.includes("compass")) hints.push("compass.com");
  if (b.includes("re/max") || b.includes("remax")) hints.push("remax.com");
  if (b.includes("coldwell")) hints.push("coldwellbanker.com");
  if (b.includes("berkshire") || b.includes("bhhs")) hints.push("bhhs.com");
  if (b.includes("carpenter")) hints.push("callcarpenter.com");
  if (b.includes("highgarden")) hints.push("highgarden.com");
  if (b.includes("trueblood")) hints.push("truebloodre.com");
  const words = b
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9 ]+/g, " ")
    .split(/\s+/)
    .filter(Boolean)
    .filter(word => !["llc", "inc", "corp", "corporation", "company", "co", "the"].includes(word));
  const domainWords = words.filter(word => !["real", "estate", "brokerage", "brokers"].includes(word));
  const compact = domainWords.join("");
  const noGeneric = domainWords.filter(word => !["realty", "group", "commercial", "properties"].includes(word)).join("");
  const withCommercial = domainWords.filter(word => !["realty", "group", "properties"].includes(word)).join("");
  for (const base of [compact, withCommercial, noGeneric]) {
    if (base.length >= 5) hints.push(`${base}.com`);
  }
  return [...new Set(hints)];
}

function brokerageSeedUrls(row: ListingRow): string[] {
  const b = effectiveBrokerage(row)?.toLowerCase() ?? "";
  const name = splitName(row);
  const urls: string[] = [];
  if (!name) return urls;

  if (b.includes("re/max") || b.includes("remax")) {
    urls.push(
      "https://town-center-columbus-oh.remax.com/agents.php",
      "https://connection-gahanna-oh.remax.com/agents.php",
      "https://www.homes4columbus.com/agents.php",
      "https://revealty-olentangy-valley-columbus-oh.remax.com/agents.php",
      "https://www.dublinhomesremax.com/agents.php",
      "https://www.remaxoneohio.com/agents.php",
      "https://www.achievers-columbus.com/agents.php",
      "https://www.ohiorealtypartners.com/agents.php",
      "https://www.remaxcentralohiohomes.com/agents.php",
    );
  }

  if (/\breal\b/.test(b) || b.includes("real brokerage")) {
    urls.push(
      "https://www.realbrokerage.com/agents",
      "https://www.joinreal.com/",
      "https://www.onereal.com/",
    );
  }

  return urls;
}

function directProfileUrls(row: ListingRow): string[] {
  const name = splitName(row);
  if (!name) return [];
  const first = slug(name.first);
  const last = slug(name.last);
  const full = slug(name.full);
  const urls: string[] = [];
  for (const domain of brokerageDomainHints(effectiveBrokerage(row))) {
    urls.push(
      `https://www.${domain}/agents/${full}`,
      `https://www.${domain}/agent/${full}`,
      `https://www.${domain}/real-estate-agent/${full}`,
      `https://www.${domain}/team/${full}`,
      `https://www.${domain}/our-team/${full}`,
      `https://www.${domain}/${full}`,
      `https://www.${domain}/agents/${first}-${last}`,
      `https://${domain}/agents/${full}`,
      `https://${domain}/agent/${full}`,
      `https://${domain}/real-estate-agent/${full}`,
      `https://${domain}/team/${full}`,
      `https://${domain}/our-team/${full}`,
      `https://${domain}/${full}`,
    );
  }
  return [...new Set(urls)];
}

function homepageUrls(row: ListingRow): string[] {
  return brokerageDomainHints(effectiveBrokerage(row))
    .flatMap(domain => [`https://www.${domain}/`, `https://${domain}/`])
    .filter((url, index, all) => all.indexOf(url) === index);
}

function extractEmails(html: string): string[] {
  const decoded = cleanText(html)
    .replace(/\s+\[at\]\s+|\s+\(at\)\s+|\s+ at \s+/gi, "@")
    .replace(/\s+\[dot\]\s+|\s+\(dot\)\s+|\s+ dot \s+/gi, ".");
  const matches = decoded.match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/gi) ?? [];
  return [...new Set(matches.map(email => email.toLowerCase()))]
    .filter(email => !email.endsWith(".png") && !email.endsWith(".jpg") && !email.includes("example.com"));
}

function isGenericOrHostedEmail(email: string): boolean {
  const [local, domain = ""] = email.toLowerCase().split("@");
  if (!local || !domain) return true;
  if (["info", "contact", "hello", "admin", "office", "support", "sales", "team", "homes", "realestate", "realtor", "filler"].includes(local)) return true;
  if (/^(no-?reply|donotreply|webmaster|privacy|marketing|leads?)$/.test(local)) return true;
  if (/(godaddy|example|sentry|wixpress|squarespace|wordpress|cloudflare)\./i.test(domain)) return true;
  return false;
}

function sameSiteUrl(baseUrl: string, href: string): string | null {
  try {
    const url = new URL(href, baseUrl);
    const base = new URL(baseUrl);
    if (url.hostname.replace(/^www\./, "") !== base.hostname.replace(/^www\./, "")) return null;
    url.hash = "";
    return url.toString();
  } catch {
    return null;
  }
}

function extractLikelyProfileLinks(baseUrl: string, html: string, row: ListingRow): string[] {
  const name = splitName(row);
  const last = name ? slug(name.last) : "";
  const full = name ? slug(name.full) : "";
  const decodedHtml = html.replace(/&amp;/g, "&").replace(/&quot;/g, '"');
  const links: string[] = [];

  for (const match of decodedHtml.matchAll(/<a[^>]+href="([^"]+)"[^>]*>([\s\S]*?)<\/a>/gi)) {
    const href = match[1];
    const text = cleanText(match[2]).toLowerCase();
    const url = sameSiteUrl(baseUrl, href);
    if (!url) continue;
    const haystack = `${url.toLowerCase()} ${text}`;
    if (
      /agent|advisor|broker|team|staff|people|about|contact|commercial|real-estate|profile/.test(haystack)
      || (last && haystack.includes(last))
      || (full && haystack.includes(full))
    ) {
      links.push(url);
    }
  }

  return [...new Set(links)].slice(0, 16);
}

function extractBrokerageRosterLinks(baseUrl: string, html: string, row: ListingRow): string[] {
  const name = splitName(row);
  if (!name) return [];
  const first = name.first.toLowerCase();
  const last = name.last.toLowerCase();
  const full = name.full.toLowerCase();
  const decodedHtml = html.replace(/&amp;/g, "&").replace(/&quot;/g, '"');
  const links: string[] = [];

  for (const match of decodedHtml.matchAll(/<a\b[^>]+href=["']([^"']+)["'][^>]*>([\s\S]*?)<\/a>/gi)) {
    const href = match[1];
    const label = cleanText(match[2]).toLowerCase();
    const url = sameSiteUrl(baseUrl, href);
    if (!url) continue;
    const haystack = `${url.toLowerCase()} ${label}`;
    if (haystack.includes(full) || (haystack.includes(first) && haystack.includes(last))) {
      links.push(url);
    }
    if ((label.includes("email") || href.toLowerCase().includes("email")) && (decodedHtml.toLowerCase().includes(full) || (decodedHtml.toLowerCase().includes(first) && decodedHtml.toLowerCase().includes(last)))) {
      links.push(url);
    }
  }

  for (const pattern of [
    new RegExp(`href=["']([^"']*(?:${first}|${last})[^"']*)["']`, "gi"),
    /href=["']([^"']*agents?\.php[^"']*)["']/gi,
  ]) {
    for (const match of decodedHtml.matchAll(pattern)) {
      const url = sameSiteUrl(baseUrl, match[1]);
      if (url) links.push(url);
    }
  }

  return [...new Set(links)].slice(0, 24);
}

function extractLinksFromDuckDuckGo(html: string): string[] {
  const links: string[] = [];
  const decodedHtml = html
    .replace(/&amp;/g, "&")
    .replace(/&#x2F;/gi, "/")
    .replace(/&quot;/g, '"');
  for (const match of decodedHtml.matchAll(/<a[^>]+href="([^"]+)"[^>]*>/gi)) {
    const href = match[1];
    if (!href) continue;
    if (href.includes("uddg=")) {
      const uddg = href.match(/[?&]uddg=([^&"]+)/)?.[1];
      if (uddg) links.push(decodeURIComponent(uddg));
      continue;
    }
    links.push(href);
  }
  for (const match of decodedHtml.matchAll(/uddg=([^&"]+)/gi)) links.push(decodeURIComponent(match[1]));
  return [...new Set(links)]
    .filter(url => /^https?:\/\//i.test(url))
    .filter(url => !/duckduckgo\.com|facebook\.com|instagram\.com|linkedin\.com/i.test(url))
    .slice(0, 8);
}

function decodeHtml(value: string): string {
  return value
    .replace(/&amp;/g, "&")
    .replace(/&quot;/g, '"')
    .replace(/&#39;|&apos;/g, "'")
    .replace(/&#x2F;/gi, "/")
    .replace(/&#x3A;/gi, ":")
    .replace(/&#x3D;/gi, "=");
}

function normalizeSearchResultUrl(value: string): string | null {
  const decoded = decodeHtml(value).trim();
  if (!decoded) return null;
  try {
    const url = new URL(decoded);
    if (/bing\.com$/i.test(url.hostname) || /\.bing\.com$/i.test(url.hostname)) {
      const target = url.searchParams.get("u") ?? url.searchParams.get("url");
      if (target) {
        if (/^a1[a-z0-9_-]+$/i.test(target)) {
          try {
            return normalizeSearchResultUrl(Buffer.from(target.slice(2), "base64url").toString("utf8"));
          } catch {
            return null;
          }
        }
        return normalizeSearchResultUrl(target);
      }
      return null;
    }
    url.hash = "";
    return /^https?:$/i.test(url.protocol) ? url.toString() : null;
  } catch {
    return null;
  }
}

function extractLinksFromBing(html: string): string[] {
  const decodedHtml = decodeHtml(html);
  const links: string[] = [];
  for (const match of decodedHtml.matchAll(/<li[^>]+class=["'][^"']*\bb_algo\b[^"']*["'][\s\S]*?<a[^>]+href=["']([^"']+)["']/gi)) {
    const url = normalizeSearchResultUrl(match[1]);
    if (url) links.push(url);
  }
  for (const match of decodedHtml.matchAll(/<h2[^>]*>[\s\S]*?<a[^>]+href=["']([^"']+)["']/gi)) {
    const url = normalizeSearchResultUrl(match[1]);
    if (url) links.push(url);
  }
  for (const match of decodedHtml.matchAll(/<a[^>]+href=["'](https?:\/\/[^"']+)["']/gi)) {
    const url = normalizeSearchResultUrl(match[1]);
    if (url) links.push(url);
  }
  return [...new Set(links)]
    .filter(url => !/bing\.com|microsoft\.com|facebook\.com|instagram\.com|linkedin\.com/i.test(url))
    .slice(0, 8);
}

async function fetchText(url: string): Promise<string | null> {
  try {
    const response = await fetch(url, {
      redirect: "follow",
      headers: {
        "user-agent": "MXREBot/0.1 public-contact-verification (+https://mxre.mundox.ai)",
        "accept": "text/html,application/xhtml+xml,text/plain",
        "accept-language": "en-US,en;q=0.9",
      },
      signal: AbortSignal.timeout(FETCH_TIMEOUT_MS),
    });
    if (!response.ok) return null;
    return response.text();
  } catch {
    return null;
  }
}

function verifyEmailPage(html: string, row: ListingRow, url: string): Candidate | null {
  const name = splitName(row);
  if (!name) return null;
  const text = cleanText(html).toLowerCase();
  const first = name.first.toLowerCase();
  const last = name.last.toLowerCase();
  const phone = normalizePhone(row.listing_agent_phone);
  const pageDigits = text.replace(/\D/g, "");
  const tokens = brokerageTokens(effectiveBrokerage(row));

  const hasName = text.includes(first) && text.includes(last);
  const hasPhone = Boolean(phone && pageDigits.includes(phone));
  const hasBrokerage = tokens.length > 0 && tokens.some(token => text.includes(token) || url.toLowerCase().includes(token));
  const emails = extractEmails(html);
  if (emails.length > 0) stats.pages_with_email++;
  if (!hasName || !hasPhone || !hasBrokerage) {
    if (emails.length > 0) stats.rejected_identity++;
    return null;
  }
  const personalEmails = emails.filter(email => !isGenericOrHostedEmail(email));
  const personal = personalEmails.find(email => {
    const local = email.split("@")[0].replace(/[^a-z]/g, "");
    return local.includes(first.replace(/[^a-z]/g, "")) || local.includes(last.replace(/[^a-z]/g, ""));
  }) ?? null;
  if (!personal) return null;

  return {
    email: personal,
    url,
    confidence: "public_profile_verified",
  };
}

function matchingAgentWhere(row: ListingRow): string {
  const clauses: string[] = [];
  const phone = normalizePhone(row.listing_agent_phone);
  const name = splitName(row);

  if (phone) {
    clauses.push(`regexp_replace(coalesce(listing_agent_phone,''), '\\D', '', 'g') in (${sql(phone)}, ${sql(`1${phone}`)})`);
  }

  if (name?.first && name.last && row.listing_brokerage) {
    clauses.push(`(
      lower(coalesce(listing_agent_first_name, split_part(listing_agent_name, ' ', 1))) = lower(${sql(name.first)})
      and lower(coalesce(listing_agent_last_name, regexp_replace(listing_agent_name, '^\\S+\\s+', ''))) = lower(${sql(name.last)})
      and lower(coalesce(listing_brokerage,'')) = lower(${sql(row.listing_brokerage)})
    )`);
  }

  if (name?.full && row.listing_brokerage) {
    clauses.push(`(
      lower(coalesce(listing_agent_name,'')) = lower(${sql(name.full)})
      and lower(coalesce(listing_brokerage,'')) = lower(${sql(row.listing_brokerage)})
    )`);
  }

  return clauses.length > 0 ? `(${clauses.join(" or ")})` : `id = ${row.id}`;
}

async function saveVerifiedEmail(row: ListingRow, candidate: Candidate): Promise<number> {
  const updated = await pg(`
    with updated as (
      update listing_signals
         set listing_agent_email = coalesce(listing_agent_email, ${sql(candidate.email)}),
             agent_contact_source = 'public_agent_profile',
             agent_contact_confidence = ${sql(candidate.confidence)},
             raw = coalesce(raw, '{}'::jsonb) || ${sql(JSON.stringify({
               publicAgentEmail: {
                 email: candidate.email,
                 sourceUrl: candidate.url,
                 confidence: candidate.confidence,
                 observedAt: new Date().toISOString(),
                 propagation: "matched_active_agent_rows",
               },
             }))}::jsonb,
             updated_at = now()
       where is_on_market = true
         and listing_agent_email is null
         and ${matchingAgentWhere(row)}
       returning id
    )
    select count(*)::int as updated from updated;
  `);
  return Number(updated?.[0]?.updated ?? 0);
}

async function findPublicEmail(row: ListingRow): Promise<Candidate | null> {
  for (const seedUrl of brokerageSeedUrls(row)) {
    await sleep(DELAY_MS);
    const seedHtml = await fetchText(seedUrl);
    if (!seedHtml) continue;
    stats.profile_pages++;
    const seedCandidate = verifyEmailPage(seedHtml, row, seedUrl);
    if (seedCandidate) return seedCandidate;
    for (const profileLink of extractBrokerageRosterLinks(seedUrl, seedHtml, row).slice(0, MAX_PROFILE_LINKS_PER_PAGE)) {
      await sleep(DELAY_MS);
      const profileHtml = await fetchText(profileLink);
      if (!profileHtml) continue;
      stats.profile_pages++;
      const candidate = verifyEmailPage(profileHtml, row, profileLink);
      if (candidate) return candidate;
    }
  }

  const profileUrls = directProfileUrls(row).slice(0, MAX_DIRECT_PROFILE_URLS);
  stats.direct_profile_urls += profileUrls.length;
  if (profileUrls.length === 0) {
    stats.no_direct_brokerage_hint++;
    if (effectiveBrokerage(row)) brokerageSamples.add(effectiveBrokerage(row)!);
  }
  for (const profileUrl of profileUrls) {
    await sleep(DELAY_MS);
    const html = await fetchText(profileUrl);
    if (!html) continue;
    stats.profile_pages++;
    const candidate = verifyEmailPage(html, row, profileUrl);
    if (candidate) return candidate;
  }

  const homeUrls = homepageUrls(row);
  stats.homepage_urls += homeUrls.length;
  for (const homeUrl of homeUrls) {
    await sleep(DELAY_MS);
    const homeHtml = await fetchText(homeUrl);
    if (!homeHtml) continue;
    stats.profile_pages++;
    const homeCandidate = verifyEmailPage(homeHtml, row, homeUrl);
    if (homeCandidate) return homeCandidate;

    const profileLinks = extractLikelyProfileLinks(homeUrl, homeHtml, row).slice(0, MAX_PROFILE_LINKS_PER_PAGE);
    for (const profileLink of profileLinks) {
      await sleep(DELAY_MS);
      const profileHtml = await fetchText(profileLink);
      if (!profileHtml) continue;
      stats.profile_pages++;
      const candidate = verifyEmailPage(profileHtml, row, profileLink);
      if (candidate) return candidate;
    }
  }

  const name = splitName(row);
  if (!name) return null;
  const phone = row.listing_agent_phone?.replace(/[^\d]/g, "");
  const phoneText = row.listing_agent_phone ?? "";
  const mlsNumber = rawString(row, ["mlsNumber", "mls_number", "mlsId", "mls_id"]);
  const address = row.address?.replace(/\s+/g, " ").trim();
  const portalQueries = ["realtor.com", "zillow.com", "homes.com", "redfin.com", "realty.com", "ezhomesearch.com", "coldwellbankerhomes.com"].flatMap(domain => [
    [`site:${domain}`, mlsNumber ? `"${mlsNumber}"` : "", `"${name.full}"`].filter(Boolean).join(" "),
    [`site:${domain}`, address ? `"${address}"` : "", `"${name.full}"`].filter(Boolean).join(" "),
    [`site:${domain}`, `"${name.full}"`, phone ? `"${phone}"` : "", "email"].filter(Boolean).join(" "),
    [`site:${domain}`, `"${name.full}"`, effectiveBrokerage(row) ? `"${effectiveBrokerage(row)}"` : "", "agent"].filter(Boolean).join(" "),
  ]);
  const queries = [
    ...portalQueries,
    [mlsNumber ? `"${mlsNumber}"` : "", `"${name.full}"`, effectiveBrokerage(row) ? `"${effectiveBrokerage(row)}"` : "", "email"].filter(Boolean).join(" "),
    [address ? `"${address}"` : "", `"${name.full}"`, "listing agent email"].filter(Boolean).join(" "),
    [`"${name.full}"`, phoneText ? `"${phoneText}"` : "", "email"].filter(Boolean).join(" "),
    [`"${name.full}"`, phone ? `"${phone}"` : "", "realtor email"].filter(Boolean).join(" "),
    [phoneText ? `"${phoneText}"` : "", "realtor email"].filter(Boolean).join(" "),
    [`"${name.full}"`, effectiveBrokerage(row) ? `"${effectiveBrokerage(row)}"` : "", "email realtor agent"].filter(Boolean).join(" "),
    [`"${name.full}"`, effectiveBrokerage(row) ? `"${effectiveBrokerage(row)}"` : "", "contact"].filter(Boolean).join(" "),
    [`"${name.full}"`, row.city ? `"${row.city}"` : "", row.state_code ?? "", "real estate agent email"].filter(Boolean).join(" "),
  ].filter((query, index, all) => query && all.indexOf(query) === index);

  const links: string[] = [];
  for (const query of queries.slice(0, MAX_SEARCH_QUERIES)) {
    if (!DISABLE_DUCKDUCKGO) {
      const duckUrl = `https://duckduckgo.com/html/?q=${encodeURIComponent(query)}`;
      const searchHtml = await fetchText(duckUrl);
      if (searchHtml) {
        stats.search_pages++;
        const found = extractLinksFromDuckDuckGo(searchHtml);
        stats.duckduckgo_links += found.length;
        if (found.length === 0) stats.search_pages_without_links++;
        links.push(...found);
      }
      await sleep(DELAY_MS);
    }
    const bingUrl = `https://www.bing.com/search?q=${encodeURIComponent(query)}`;
    const bingHtml = await fetchText(bingUrl);
    if (bingHtml) {
      stats.search_pages++;
      const found = extractLinksFromBing(bingHtml);
      stats.bing_links += found.length;
      if (found.length === 0) stats.search_pages_without_links++;
      links.push(...found);
    }
    if (links.length >= MAX_SEARCH_LINKS * 2) break;
  }
  const uniqueLinks = [...new Set(links)].slice(0, MAX_SEARCH_LINKS);
  stats.search_links += uniqueLinks.length;
  if (uniqueLinks.length === 0) return null;

  for (const link of uniqueLinks) {
    await sleep(DELAY_MS);
    const html = await fetchText(link);
    if (!html) continue;
    stats.profile_pages++;
    const candidate = verifyEmailPage(html, row, link);
    if (candidate) return candidate;
  }
  return null;
}

async function main() {
  console.log("MXRE - Public agent email enrichment");
  console.log(`Limit: ${LIMIT}; delay ${DELAY_MS}ms; dry run ${DRY_RUN}`);
  console.log(`Search caps: ${MAX_SEARCH_QUERIES} queries, ${MAX_SEARCH_LINKS} links`);
  console.log(`Timeouts: fetch ${FETCH_TIMEOUT_MS}ms, row ${ROW_TIMEOUT_MS}ms`);
  if (STATE || CITY) console.log(`Market filter: ${CITY ?? "all cities"}, ${STATE ?? "all states"}`);
  if (BROKERAGE_PATTERN) console.log(`Brokerage filter: ${BROKERAGE_PATTERN}`);

  const filters = [
    STATE ? `state_code = ${sql(STATE)}` : null,
    CITY ? `upper(coalesce(city,'')) = ${sql(CITY)}` : null,
    BROKERAGE_PATTERN ? `listing_brokerage ilike ${sql(BROKERAGE_PATTERN)}` : null,
  ].filter(Boolean).join("\n      and ");

  const rows = await pg(`
    select id, address, city, state_code, raw,
           listing_agent_name, listing_agent_first_name, listing_agent_last_name,
           listing_agent_phone, listing_brokerage
    from listing_signals
    where is_on_market = true
      and listing_agent_email is null
      and listing_agent_phone is not null
      and coalesce(listing_agent_first_name, listing_agent_name) is not null
      ${filters ? `and ${filters}` : ""}
    order by last_seen_at desc nulls last
    limit ${LIMIT};
  `) as ListingRow[];

  let found = 0;
  let updated = 0;
  let scanned = 0;

  for (const row of rows) {
    scanned++;
    const label = row.listing_agent_name || [row.listing_agent_first_name, row.listing_agent_last_name].filter(Boolean).join(" ") || `listing ${row.id}`;
    const started = Date.now();
    console.log(`  [${scanned}/${rows.length}] checking ${label} (${effectiveBrokerage(row) ?? "unknown brokerage"})`);
    await sleep(DELAY_MS);
    let candidate: Candidate | null = null;
    try {
      candidate = await withTimeout(findPublicEmail(row), ROW_TIMEOUT_MS, `row ${row.id}`);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      if (message.includes("timed out")) stats.row_timeouts++;
      else stats.row_errors++;
      console.log(`    skipped: ${message}`);
      continue;
    }
    if (!candidate) {
      console.log(`    no verified email (${Date.now() - started}ms)`);
      continue;
    }
    found++;
    console.log(`    email found: ${label} -> ${candidate.email} (${candidate.confidence}, ${Date.now() - started}ms)`);

    if (!DRY_RUN) {
      updated += await saveVerifiedEmail(row, candidate);
    }
  }

  console.log(JSON.stringify({
    scanned,
    found,
    updated,
    dry_run: DRY_RUN,
    ...stats,
    brokerage_samples_without_hints: [...brokerageSamples].slice(0, 20),
  }, null, 2));
}

main().catch(error => {
  console.error("Fatal:", error instanceof Error ? error.message : error);
  process.exit(1);
});
