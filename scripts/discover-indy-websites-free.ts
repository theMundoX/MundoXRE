#!/usr/bin/env tsx
/**
 * Free-first Indianapolis apartment website discovery.
 *
 * No paid APIs. Starts with public apartment platform city pages, extracts
 * property marketing URLs, fetches each property page, extracts the street
 * address from public page markup, and only stores a URL when it matches an
 * existing MXRE property.
 *
 * Usage:
 *   npx tsx scripts/discover-indy-websites-free.ts --dry-run
 *   npx tsx scripts/discover-indy-websites-free.ts --limit=100
 */

import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const args = process.argv.slice(2);
const hasFlag = (name: string) => args.includes(`--${name}`);
const getArg = (name: string) => args.find((arg) => arg.startsWith(`--${name}=`))?.split("=").slice(1).join("=");

const DRY_RUN = hasFlag("dry-run");
const LIMIT = Number.parseInt(getArg("limit") ?? "250", 10);
const MARION_COUNTY_ID = 797583;

const SOURCES = [
  {
    name: "rentcafe_indianapolis",
    url: "https://www.rentcafe.com/apartments-for-rent/us/in/indianapolis/",
    platform: "rentcafe",
    linkPattern: /href=["'](https?:\/\/[^"']*rentcafe\.com\/apartments[^"']+)["']/gi,
  },
  {
    name: "rentcafe_indianapolis_alt",
    url: "https://www.rentcafe.com/apartments-for-rent/us/in/marion-county/indianapolis/",
    platform: "rentcafe",
    linkPattern: /href=["'](https?:\/\/[^"']*rentcafe\.com\/apartments[^"']+)["']/gi,
  },
];

const PUBLIC_PORTFOLIO_SEEDS = [
  "https://www.deylen.com/downtown-indianapolis-apartments",
  "https://www.deylen.com/availability",
  "https://www.indyhinge.com/",
  "https://www.indyslate.com/",
  "https://www.indyforte.com/",
  "https://www.ardmoreindy.com/",
  "https://www.deylen.com/fletcher-place-lofts",
  "https://www.deylen.com/fletcher-place-terrace",
];

interface OverpassElement {
  type: string;
  id: number;
  lat?: number;
  lon?: number;
  center?: { lat: number; lon: number };
  tags?: Record<string, string>;
}

const SKIP_URL_PARTS = [
  "/login",
  "/register",
  "/rentalapplication",
  "/blog/",
  "/company/",
  "/about",
  "/contact",
  "/sitemap",
];

const RELATED_LINK_TEXT = /(apartment|apartments|availability|floor\s*plans?|floorplans|lofts|flats|townhomes|properties|portfolio|communities|rent)/i;
const INDIANAPOLIS_ZIP_RE = /\b462\d{2}\b/;
const RENT_DATA_PAGE_RE = /(apartments|availability|floor-plan|floorplans|floor-plans|rent)/i;
const LOW_VALUE_RELATED_PAGE_RE = /(privacy|accessibility|photo|gallery|amenit|pet-friendly|neighborhood|map-directions|residents|apply|contact)/i;

function detectPlatform(url: string): string {
  const host = new URL(url).hostname.toLowerCase();
  if (host.includes("rentcafe.com")) return "rentcafe";
  if (host.includes("entrata.com")) return "entrata";
  if (host.includes("appfolio.com")) return "appfolio";
  if (host.includes("realpage.com")) return "realpage";
  if (host.includes("myresman.com")) return "resman";
  return "direct";
}

function cleanUrl(raw: string): string | null {
  try {
    const decoded = raw.replace(/&amp;/g, "&");
    const url = new URL(decoded);
    url.hash = "";
    for (const part of SKIP_URL_PARTS) {
      if (url.pathname.toLowerCase().includes(part)) return null;
    }
    return url.toString();
  } catch {
    return null;
  }
}

function normalizeAddress(value: string): string {
  return value
    .toUpperCase()
    .replace(/&AMP;/g, "&")
    .replace(/[.,#]/g, " ")
    .replace(/\bSTREET\b/g, "ST")
    .replace(/\bAVENUE\b/g, "AVE")
    .replace(/\bBOULEVARD\b/g, "BLVD")
    .replace(/\bDRIVE\b/g, "DR")
    .replace(/\bLANE\b/g, "LN")
    .replace(/\bROAD\b/g, "RD")
    .replace(/\bCOURT\b/g, "CT")
    .replace(/\bPLACE\b/g, "PL")
    .replace(/\bCIRCLE\b/g, "CIR")
    .replace(/\bPARKWAY\b/g, "PKWY")
    .replace(/\bNORTH\b/g, "N")
    .replace(/\bSOUTH\b/g, "S")
    .replace(/\bEAST\b/g, "E")
    .replace(/\bWEST\b/g, "W")
    .replace(/\s+/g, " ")
    .trim();
}

function textFromHtml(value: string): string {
  return value
    .replace(/\\u0026/g, "&")
    .replace(/&amp;/g, "&")
    .replace(/&#39;/g, "'")
    .replace(/&quot;/g, '"')
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

async function fetchText(url: string): Promise<string | null> {
  try {
    const response = await fetch(url, {
      redirect: "follow",
      signal: AbortSignal.timeout(20_000),
      headers: {
        "user-agent":
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
        accept: "text/html,application/xhtml+xml",
      },
    });
    if (!response.ok) return null;
    return await response.text();
  } catch {
    return null;
  }
}

async function fetchOverpassElements(): Promise<OverpassElement[]> {
  // Marion County / Indianapolis bounding box. Overpass area relations can be
  // inconsistent by market, so bbox is the more dependable free daily path.
  const bbox = "39.60,-86.35,39.95,-85.90";
  const query = `
    [out:json][timeout:60];
    (
      nwr["website"]["name"~"Apartments|Apartment|Lofts|Flats|Townhomes|Townhome",i](${bbox});
      nwr["contact:website"]["name"~"Apartments|Apartment|Lofts|Flats|Townhomes|Townhome",i](${bbox});
      nwr["website"]["building"~"apartments|residential",i](${bbox});
      nwr["contact:website"]["building"~"apartments|residential",i](${bbox});
    );
    out center tags;
  `;

  try {
    const response = await fetch("https://overpass-api.de/api/interpreter", {
      method: "POST",
      signal: AbortSignal.timeout(90_000),
      headers: {
        "content-type": "application/x-www-form-urlencoded",
        "user-agent": "MXRE public-data discovery (contact: local development)",
      },
      body: new URLSearchParams({ data: query }),
    });
    if (!response.ok) {
      console.log(`  Overpass response ${response.status}`);
      return [];
    }
    const json = (await response.json()) as { elements?: OverpassElement[] };
    return json.elements ?? [];
  } catch (error) {
    console.log(`  Overpass unavailable: ${error instanceof Error ? error.message : "unknown error"}`);
    return [];
  }
}

function extractLinks(html: string, source: (typeof SOURCES)[number]): string[] {
  const urls = new Set<string>();
  for (const match of html.matchAll(source.linkPattern)) {
    const cleaned = cleanUrl(match[1]);
    if (cleaned) urls.add(cleaned);
  }
  return [...urls];
}

function extractAllLinks(html: string, baseUrl: string): string[] {
  const urls = new Set<string>();
  for (const match of html.matchAll(/<a\b[^>]*href=["']([^"']+)["'][^>]*>([\s\S]*?)<\/a>/gi)) {
    const href = match[1];
    const label = textFromHtml(match[2] ?? "");
    if (!RELATED_LINK_TEXT.test(`${label} ${href}`)) continue;
    try {
      const absolute = new URL(href.replace(/&amp;/g, "&"), baseUrl);
      if (absolute.protocol !== "http:" && absolute.protocol !== "https:") continue;
      if (/\/floor-plan\/[^/]+\/?$/i.test(absolute.pathname)) continue;
      if (!RENT_DATA_PAGE_RE.test(`${label} ${absolute.pathname}`)) continue;
      if (LOW_VALUE_RELATED_PAGE_RE.test(absolute.pathname)) continue;
      const cleaned = cleanUrl(absolute.toString());
      if (cleaned) urls.add(cleaned);
    } catch {
      // Ignore malformed links.
    }
  }
  return [...urls];
}

function extractIndianapolisAddressCandidates(html: string): Array<{ street: string; city: string | null; state: string | null; zip: string | null }> {
  const text = textFromHtml(html);
  const candidates = new Map<string, { street: string; city: string | null; state: string | null; zip: string | null }>();
  const patterns = [
    /\b(\d{1,6}\s+[A-Z0-9][A-Z0-9 .'-]{2,80}?\s+(?:ST|STREET|AVE|AVENUE|BLVD|BOULEVARD|DR|DRIVE|RD|ROAD|LN|LANE|CT|COURT|PL|PLACE|CIR|CIRCLE|PKWY|PARKWAY|WAY|TER|TERRACE))\s*(?:[,•·|-]\s*)?INDIANAPOLIS\s*,?\s*IN(?:DIANA)?\.?\s*(462\d{2})?/gi,
    /\b(\d{1,6}\s+(?:N|S|E|W|NORTH|SOUTH|EAST|WEST)\s+[A-Z0-9][A-Z0-9 .'-]{2,80}?)\s*(?:[,•-]\s*)?INDIANAPOLIS\s*,?\s*IN(?:DIANA)?\.?\s*(462\d{2})?/gi,
    /\b(\d{1,6}\s+[A-Z0-9][A-Z0-9 .'-]{2,80}?\s+(?:ST|STREET|AVE|AVENUE|BLVD|BOULEVARD|DR|DRIVE|RD|ROAD|LN|LANE|CT|COURT|PL|PLACE|CIR|CIRCLE|PKWY|PARKWAY|WAY|TER|TERRACE))\s*(462\d{2})\b/gi,
  ];

  for (const pattern of patterns) {
    for (const match of text.matchAll(pattern)) {
      const street = textFromHtml(match[1] ?? "");
      const zip = match[2]?.match(INDIANAPOLIS_ZIP_RE)?.[0] ?? null;
      if (!street || !/^\d+\s+/.test(street)) continue;
      candidates.set(normalizeAddress(street), { street, city: "Indianapolis", state: "IN", zip });
    }
  }

  return [...candidates.values()];
}

async function discoverSeedUrls(seedUrls: string[]) {
  let checked = 0;
  let matched = 0;
  let saved = 0;
  const matchedSeedUrls: Array<{ url: string; source: string }> = [];

  for (const url of seedUrls.slice(0, LIMIT)) {
    checked++;
    const html = await fetchText(url);
    if (!html) continue;

    const structuredAddress = extractAddress(html);
    const candidates = structuredAddress ? [structuredAddress] : extractIndianapolisAddressCandidates(html);
    const seenPropertyIds = new Set<number>();
    for (const address of candidates.slice(0, 12)) {
      const property = await matchProperty(address);
      if (!property || seenPropertyIds.has(property.id)) continue;
      seenPropertyIds.add(property.id);
      matched++;
      console.log(`  [seed] ${url} -> property ${property.id} (${property.address})`);
      await upsertWebsite(property.id, url, detectPlatform(url), "public_portfolio_seed");
      matchedSeedUrls.push({ url, source: "public_portfolio_seed" });
      saved++;
    }
  }

  return { checked, matched, saved, matchedSeedUrls };
}

function extractAddress(html: string): { street: string; city: string | null; state: string | null; zip: string | null } | null {
  const streetPatterns = [
    /"streetAddress"\s*:\s*"([^"]+)"/i,
    /itemprop=["']streetAddress["'][^>]*>([^<]+)</i,
    /property-address[^>]*>([^<]+)</i,
  ];
  const cityPatterns = [
    /"addressLocality"\s*:\s*"([^"]+)"/i,
    /itemprop=["']addressLocality["'][^>]*>([^<]+)</i,
  ];
  const statePatterns = [
    /"addressRegion"\s*:\s*"([^"]+)"/i,
    /itemprop=["']addressRegion["'][^>]*>([^<]+)</i,
  ];
  const zipPatterns = [
    /"postalCode"\s*:\s*"([^"]+)"/i,
    /itemprop=["']postalCode["'][^>]*>([^<]+)</i,
  ];

  const find = (patterns: RegExp[]) => {
    for (const pattern of patterns) {
      const match = html.match(pattern);
      if (match?.[1]) return textFromHtml(match[1]);
    }
    return null;
  };

  const street = find(streetPatterns);
  if (!street || !/^\d+\s+/.test(street)) return null;
  return {
    street,
    city: find(cityPatterns),
    state: find(statePatterns),
    zip: find(zipPatterns)?.match(/\d{5}/)?.[0] ?? null,
  };
}

async function matchProperty(address: { street: string; city: string | null; state: string | null; zip: string | null }) {
  const normalizedStreet = normalizeAddress(address.street);
  const houseNumber = normalizedStreet.match(/^(\d+)/)?.[1];
  if (!houseNumber) return null;

  let query = db
    .from("properties")
    .select("id,address,city,state_code,zip")
    .eq("county_id", MARION_COUNTY_ID)
    .like("address", `${houseNumber}%`)
    .limit(100);

  if (address.zip) query = query.eq("zip", address.zip);

  const { data, error } = await query;
  if (error) throw error;

  for (const row of data ?? []) {
    if (normalizeAddress(row.address ?? "") === normalizedStreet) return row;
  }

  const streetCore = normalizedStreet.replace(/^\d+\s+/, "").split(" ").slice(0, 2).join(" ");
  return (data ?? []).find((row) => normalizeAddress(row.address ?? "").includes(streetCore)) ?? null;
}

async function matchPropertyByPoint(lat: number, lon: number) {
  const latMin = lat - 0.003;
  const latMax = lat + 0.003;
  const lonMin = lon - 0.003;
  const lonMax = lon + 0.003;
  const { data, error } = await db
    .from("properties")
    .select("id,address,city,state_code,zip,lat,lng,latitude,longitude")
    .eq("county_id", MARION_COUNTY_ID)
    .or(`lat.gte.${latMin},latitude.gte.${latMin}`)
    .limit(100);
  if (error) throw error;

  let best: any = null;
  let bestDistance = Number.POSITIVE_INFINITY;
  for (const row of data ?? []) {
    const rowLat = Number(row.lat ?? row.latitude);
    const rowLon = Number(row.lng ?? row.longitude);
    if (!Number.isFinite(rowLat) || !Number.isFinite(rowLon)) continue;
    if (rowLat < latMin || rowLat > latMax || rowLon < lonMin || rowLon > lonMax) continue;
    const distance = Math.hypot(rowLat - lat, rowLon - lon);
    if (distance < bestDistance) {
      best = row;
      bestDistance = distance;
    }
  }
  return bestDistance <= 0.0015 ? best : null;
}

async function upsertWebsite(propertyId: number, url: string, platform: string, sourceName: string) {
  if (DRY_RUN) return;

  const { error: websiteError } = await db.from("property_websites").upsert(
    {
      property_id: propertyId,
      url,
      platform,
      discovery_method: sourceName,
      active: true,
    },
    { onConflict: "property_id,url" },
  );
  if (websiteError) throw websiteError;

  await db
    .from("properties")
    .update({ website: url, updated_at: new Date().toISOString() })
    .eq("id", propertyId)
    .or("website.is.null,website.eq.");
}

async function discoverFromRelatedPages(seedUrls: Array<{ url: string; source: string }>) {
  const related = new Map<string, string>();
  for (const seed of seedUrls) {
    const html = await fetchText(seed.url);
    if (!html) continue;
    for (const link of extractAllLinks(html, seed.url)) {
      related.set(link, seed.source);
    }
  }

  let checked = 0;
  let matched = 0;
  let saved = 0;
  const relatedUrls = [...related.entries()].slice(0, LIMIT);
  console.log(`  discovered ${relatedUrls.length} related candidate pages`);

  for (const [url, source] of relatedUrls) {
    checked++;
    const html = await fetchText(url);
    if (!html) continue;

    const structuredAddress = extractAddress(html);
    const candidates = structuredAddress ? [structuredAddress] : extractIndianapolisAddressCandidates(html);
    for (const address of candidates.slice(0, 12)) {
      const property = await matchProperty(address);
      if (!property) continue;
      matched++;
      console.log(`  [related] ${url} -> property ${property.id} (${property.address})`);
      await upsertWebsite(property.id, url, detectPlatform(url), `related_page:${source}`);
      saved++;
    }
  }

  return { checked, matched, saved };
}

async function main() {
  console.log("MXRE - Free Indianapolis website discovery");
  console.log("=".repeat(52));
  console.log(`Dry run: ${DRY_RUN}`);
  console.log(`Limit: ${LIMIT}`);

  const candidates: Array<{ url: string; source: string; platform: string }> = [];

  for (const source of SOURCES) {
    console.log(`\nSource: ${source.name}`);
    const html = await fetchText(source.url);
    if (!html) {
      console.log("  no response");
      continue;
    }
    const links = extractLinks(html, source);
    console.log(`  discovered ${links.length} candidate URLs`);
    for (const url of links) candidates.push({ url, source: source.name, platform: source.platform });
  }

  const unique = [...new Map(candidates.map((candidate) => [candidate.url, candidate])).values()].slice(0, LIMIT);
  let matched = 0;
  let noAddress = 0;
  let noMatch = 0;
  let saved = 0;
  const matchedSeedUrls: Array<{ url: string; source: string }> = [];

  console.log("\nSource: public_portfolio_seeds");
  const seedStats = await discoverSeedUrls(PUBLIC_PORTFOLIO_SEEDS);
  matched += seedStats.matched;
  saved += seedStats.saved;
  matchedSeedUrls.push(...seedStats.matchedSeedUrls);

  for (const [index, candidate] of unique.entries()) {
    console.log(`\n[${index + 1}/${unique.length}] ${candidate.url}`);
    const html = await fetchText(candidate.url);
    if (!html) {
      noAddress++;
      console.log("  page unavailable");
      continue;
    }

    const address = extractAddress(html);
    if (!address) {
      noAddress++;
      console.log("  no address found");
      continue;
    }

    const property = await matchProperty(address);
    if (!property) {
      noMatch++;
      console.log(`  no property match for ${address.street}, ${address.zip ?? ""}`);
      continue;
    }

    matched++;
    console.log(`  matched property ${property.id}: ${property.address}, ${property.zip}`);
    await upsertWebsite(property.id, candidate.url, candidate.platform, candidate.source);
    matchedSeedUrls.push({ url: candidate.url, source: candidate.source });
    saved++;
  }

  console.log("\nSource: openstreetmap_overpass");
  const osmElements = await fetchOverpassElements();
  console.log(`  discovered ${osmElements.length} OSM website candidates`);
  let osmMatched = 0;
  let osmSaved = 0;
  for (const element of osmElements.slice(0, LIMIT)) {
    const tags = element.tags ?? {};
    const url = tags.website ?? tags["contact:website"];
    if (!url) continue;
    const cleaned = cleanUrl(url.startsWith("http") ? url : `https://${url}`);
    if (!cleaned) continue;

    const street = [tags["addr:housenumber"], tags["addr:street"]].filter(Boolean).join(" ");
    let property = street ? await matchProperty({ street, city: tags["addr:city"] ?? "Indianapolis", state: tags["addr:state"] ?? "IN", zip: tags["addr:postcode"] ?? null }) : null;
    const lat = element.lat ?? element.center?.lat;
    const lon = element.lon ?? element.center?.lon;
    if (!property && Number.isFinite(lat) && Number.isFinite(lon)) {
      property = await matchPropertyByPoint(Number(lat), Number(lon));
    }
    if (!property) continue;

    osmMatched++;
    console.log(`  [osm] ${tags.name ?? cleaned} -> property ${property.id} (${property.address})`);
    await upsertWebsite(property.id, cleaned, detectPlatform(cleaned), "openstreetmap_overpass");
    matchedSeedUrls.push({ url: cleaned, source: "openstreetmap_overpass" });
    osmSaved++;
  }
  matched += osmMatched;
  saved += osmSaved;

  console.log("\nSource: related_public_pages");
  const relatedStats = await discoverFromRelatedPages(matchedSeedUrls);
  matched += relatedStats.matched;
  saved += relatedStats.saved;

  console.log("\nSummary");
  console.log("=".repeat(52));
  console.log(JSON.stringify({ platformCandidates: unique.length, portfolioSeedCandidates: seedStats.checked, osmCandidates: osmElements.length, relatedCandidates: relatedStats.checked, matched, noAddress, noMatch, saved, dryRun: DRY_RUN }, null, 2));
}

main().catch((error) => {
  console.error("Fatal:", error);
  process.exit(1);
});
