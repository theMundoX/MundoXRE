#!/usr/bin/env tsx
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const arg = (name: string, fallback: string) =>
  process.argv.find(a => a.startsWith(`--${name}=`))?.split("=").slice(1).join("=") ?? fallback;
const CITY = arg("city", "Columbus");
const STATE = arg("state", "OH").toUpperCase();
const COUNTY_ID = Number(arg("county_id", "1698985"));
const LIMIT = Number(arg("limit", "120"));
const DRY_RUN = process.argv.includes("--dry-run");

type Address = { street: string; city?: string | null; state?: string | null; zip?: string | null };

const SKIP_HOSTS = [
  "apartments.com", "zillow.com", "trulia.com", "realtor.com", "redfin.com", "rent.com",
  "apartmentguide.com", "apartmentfinder.com", "apartmentlist.com", "hotpads.com", "zumper.com",
  "facebook.com", "instagram.com", "linkedin.com", "yelp.com", "google.com", "bing.com",
];

function cleanText(value: string): string {
  return value
    .replace(/\\u0026/g, "&")
    .replace(/&amp;/g, "&")
    .replace(/&#39;/g, "'")
    .replace(/&quot;/g, '"')
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizeAddress(value: string): string {
  return value
    .toUpperCase()
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

function cleanUrl(raw: string): string | null {
  try {
    const url = new URL(raw.replace(/&amp;/g, "&"));
    url.hash = "";
    if (!/^https?:$/.test(url.protocol)) return null;
    const host = url.hostname.replace(/^www\./, "").toLowerCase();
    if (SKIP_HOSTS.some(skip => host === skip || host.endsWith(`.${skip}`))) return null;
    if (/login|register|privacy|accessibility|resident|apply|jobs|career/i.test(url.pathname)) return null;
    return url.toString();
  } catch {
    return null;
  }
}

function detectPlatform(url: string): string {
  const host = new URL(url).hostname.toLowerCase();
  if (host.includes("rentcafe.com")) return "rentcafe";
  if (host.includes("entrata.com")) return "entrata";
  if (host.includes("appfolio.com")) return "appfolio";
  if (host.includes("realpage.com")) return "realpage";
  if (host.includes("myresman.com")) return "resman";
  return "direct";
}

async function fetchText(url: string): Promise<string | null> {
  try {
    const response = await fetch(url, {
      redirect: "follow",
      signal: AbortSignal.timeout(25_000),
      headers: {
        "user-agent": "Mozilla/5.0 MXRE public apartment discovery",
        accept: "text/html,application/xhtml+xml,text/plain",
      },
    });
    if (!response.ok) return null;
    return response.text();
  } catch {
    return null;
  }
}

function extractSearchLinks(html: string): string[] {
  const links: string[] = [];
  const decoded = html.replace(/&amp;/g, "&").replace(/&quot;/g, '"');
  for (const match of decoded.matchAll(/uddg=([^&"]+)/gi)) links.push(decodeURIComponent(match[1]));
  for (const match of decoded.matchAll(/<h2[^>]*>\s*<a[^>]+href="([^"]+)"/gi)) links.push(match[1]);
  for (const match of decoded.matchAll(/<a[^>]+href="(https?:\/\/[^"]+)"/gi)) links.push(match[1]);
  return [...new Set(links)].flatMap(link => {
    const clean = cleanUrl(link);
    return clean ? [clean] : [];
  });
}

function extractAddress(html: string): Address | null {
  const find = (patterns: RegExp[]) => {
    for (const pattern of patterns) {
      const match = html.match(pattern);
      if (match?.[1]) return cleanText(match[1]);
    }
    return null;
  };
  const street = find([
    /"streetAddress"\s*:\s*"([^"]+)"/i,
    /itemprop=["']streetAddress["'][^>]*>([^<]+)</i,
    /property-address[^>]*>([^<]+)</i,
  ]);
  if (!street || !/^\d+\s+/.test(street)) return null;
  return {
    street,
    city: find([/"addressLocality"\s*:\s*"([^"]+)"/i, /itemprop=["']addressLocality["'][^>]*>([^<]+)</i]),
    state: find([/"addressRegion"\s*:\s*"([^"]+)"/i, /itemprop=["']addressRegion["'][^>]*>([^<]+)</i]),
    zip: find([/"postalCode"\s*:\s*"([^"]+)"/i, /itemprop=["']postalCode["'][^>]*>([^<]+)</i])?.match(/\d{5}/)?.[0] ?? null,
  };
}

function extractName(html: string, fallbackUrl: string): string | null {
  const raw =
    html.match(/"name"\s*:\s*"([^"]{3,120})"/i)?.[1] ??
    html.match(/<meta\s+property=["']og:title["']\s+content=["']([^"']{3,120})["']/i)?.[1] ??
    html.match(/<title[^>]*>([^<]{3,140})<\/title>/i)?.[1];
  const cleaned = cleanText(raw ?? "")
    .replace(/\s*\|\s*.*$/g, "")
    .replace(/\s+-\s+Apartments.*$/i, " Apartments")
    .replace(new RegExp(`\\s+in\\s+${CITY}.*$`, "i"), "")
    .trim();
  if (cleaned && !/^(apartments for rent|floor plans|availability|home)$/i.test(cleaned)) return cleaned;
  const host = new URL(fallbackUrl).hostname.replace(/^www\./, "").split(".")[0] ?? "";
  return host.replace(/[-_]+/g, " ").replace(/\b\w/g, c => c.toUpperCase()) || null;
}

async function matchProperty(address: Address) {
  const normalized = normalizeAddress(address.street);
  const house = normalized.match(/^(\d+)/)?.[1];
  if (!house) return null;
  let query = db.from("properties").select("id,address,city,state_code,zip").eq("county_id", COUNTY_ID).like("address", `${house}%`).limit(100);
  if (address.zip) query = query.eq("zip", address.zip);
  const { data, error } = await query;
  if (error) throw error;
  for (const row of data ?? []) {
    if (normalizeAddress(row.address ?? "") === normalized) return row;
  }
  const core = normalized.replace(/^\d+\s+/, "").split(" ").slice(0, 2).join(" ");
  return (data ?? []).find(row => normalizeAddress(row.address ?? "").includes(core)) ?? null;
}

async function save(propertyId: number, url: string, name: string | null, source: string) {
  if (DRY_RUN) return;
  const platform = detectPlatform(url);
  const { error: websiteError } = await db.from("property_websites").upsert(
    { property_id: propertyId, url, platform, discovery_method: source, active: true },
    { onConflict: "property_id,url" },
  );
  if (websiteError) throw websiteError;
  await db.from("properties").update({ website: url, is_apartment: true, updated_at: new Date().toISOString() }).eq("id", propertyId);
  if (name) {
    const { error: profileError } = await db.from("property_complex_profiles").upsert(
      {
        property_id: propertyId,
        complex_name: name,
        website: url,
        source,
        source_url: url,
        confidence: "medium",
        last_seen_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
        raw: { discovery_url: url },
      },
      { onConflict: "property_id" },
    );
    if (profileError) throw profileError;
  }
}

async function main() {
  console.log("MXRE - Public search apartment website discovery");
  console.log(JSON.stringify({ city: CITY, state: STATE, county_id: COUNTY_ID, limit: LIMIT, dry_run: DRY_RUN }, null, 2));

  const queries = [
    `${CITY} ${STATE} apartments official site floor plans`,
    `${CITY} ${STATE} apartment homes floorplans availability`,
    `${CITY} ${STATE} downtown apartments official website`,
    `${CITY} ${STATE} apartment communities official website`,
    `${CITY} ${STATE} lofts apartments floor plans`,
    `${CITY} ${STATE} townhomes apartments availability`,
  ];

  const candidates = new Set<string>();
  for (const query of queries) {
    for (const searchUrl of [
      `https://duckduckgo.com/html/?q=${encodeURIComponent(query)}`,
      `https://www.bing.com/search?q=${encodeURIComponent(query)}`,
    ]) {
      const html = await fetchText(searchUrl);
      if (!html) continue;
      for (const link of extractSearchLinks(html)) candidates.add(link);
    }
  }

  let checked = 0;
  let matched = 0;
  let saved = 0;
  let noAddress = 0;
  let noMatch = 0;

  for (const url of [...candidates].slice(0, LIMIT)) {
    checked++;
    const html = await fetchText(url);
    if (!html) continue;
    const address = extractAddress(html);
    if (!address) {
      noAddress++;
      continue;
    }
    const property = await matchProperty(address);
    if (!property) {
      noMatch++;
      continue;
    }
    matched++;
    await save(property.id, url, extractName(html, url), "public_search");
    saved++;
    console.log(`  matched ${property.id}: ${property.address} -> ${url}`);
  }

  console.log(JSON.stringify({ candidates: candidates.size, checked, matched, saved, noAddress, noMatch, dry_run: DRY_RUN }, null, 2));
}

main().catch(error => {
  console.error("Fatal:", error instanceof Error ? error.message : error);
  process.exit(1);
});
