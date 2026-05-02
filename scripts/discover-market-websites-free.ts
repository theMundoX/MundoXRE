#!/usr/bin/env tsx
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {
  auth: { persistSession: false },
});

const arg = (name: string, fallback: string) =>
  process.argv.find(a => a.startsWith(`--${name}=`))?.split("=").slice(1).join("=") ?? fallback;
const DRY_RUN = process.argv.includes("--dry-run");
const CITY = arg("city", "Columbus");
const CITY_UPPER = CITY.toUpperCase();
const STATE = arg("state", "OH").toUpperCase();
const COUNTY_ID = Number(arg("county_id", "1698985"));
const LIMIT = Number(arg("limit", "200"));
const BBOX = arg("bbox", "39.80,-83.20,40.18,-82.75");
const COUNTY_SLUG = arg("county-slug", "");

type Address = { street: string; city?: string | null; state?: string | null; zip?: string | null };

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
    const url = new URL(raw.replace(/&amp;/g, "&"));
    url.hash = "";
    if (!/^https?:$/.test(url.protocol)) return null;
    if (/login|register|privacy|accessibility|contact|resident|apply/i.test(url.pathname)) return null;
    return url.toString();
  } catch {
    return null;
  }
}

async function fetchText(url: string): Promise<string | null> {
  try {
    const response = await fetch(url, {
      redirect: "follow",
      signal: AbortSignal.timeout(25_000),
      headers: {
        "user-agent": "Mozilla/5.0 MXRE public apartment website discovery",
        accept: "text/html,application/xhtml+xml",
      },
    });
    if (!response.ok) return null;
    return response.text();
  } catch {
    return null;
  }
}

function extractAddress(html: string): Address | null {
  const find = (patterns: RegExp[]) => {
    for (const pattern of patterns) {
      const match = html.match(pattern);
      if (match?.[1]) return textFromHtml(match[1]);
    }
    return null;
  };
  const street = find([/"streetAddress"\s*:\s*"([^"]+)"/i, /itemprop=["']streetAddress["'][^>]*>([^<]+)</i]);
  if (!street || !/^\d+\s+/.test(street)) return null;
  return {
    street,
    city: find([/"addressLocality"\s*:\s*"([^"]+)"/i, /itemprop=["']addressLocality["'][^>]*>([^<]+)</i]),
    state: find([/"addressRegion"\s*:\s*"([^"]+)"/i, /itemprop=["']addressRegion["'][^>]*>([^<]+)</i]),
    zip: find([/"postalCode"\s*:\s*"([^"]+)"/i, /itemprop=["']postalCode["'][^>]*>([^<]+)</i])?.match(/\d{5}/)?.[0] ?? null,
  };
}

function extractComplexName(html: string, fallbackUrl: string): string | null {
  const raw =
    html.match(/"name"\s*:\s*"([^"]{3,120})"/i)?.[1] ??
    html.match(/<meta\s+property=["']og:title["']\s+content=["']([^"']{3,120})["']/i)?.[1] ??
    html.match(/<title[^>]*>([^<]{3,140})<\/title>/i)?.[1];
  const cleaned = textFromHtml(raw ?? "")
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
  let query = db
    .from("properties")
    .select("id,address,city,state_code,zip")
    .eq("county_id", COUNTY_ID)
    .like("address", `${house}%`)
    .limit(100);
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

async function rentCafeCandidates(): Promise<string[]> {
  const stateSlug = STATE.toLowerCase();
  const citySlug = CITY.toLowerCase().replace(/\s+/g, "-");
  const pages = [
    `https://www.rentcafe.com/apartments-for-rent/${citySlug}-${stateSlug}/`,
    `https://www.rentcafe.com/apartments-for-rent/us/${stateSlug}/${citySlug}/`,
    COUNTY_SLUG ? `https://www.rentcafe.com/apartments-for-rent/us/${stateSlug}/${COUNTY_SLUG}/${citySlug}/` : null,
    STATE === "OH" ? `https://www.rentcafe.com/apartments-for-rent/us/${stateSlug}/franklin-county/${citySlug}/` : null,
  ].filter(Boolean) as string[];
  const urls = new Set<string>();
  for (const page of pages) {
    const html = await fetchText(page);
    if (!html) continue;
    for (const match of html.matchAll(/href=["'](https?:\/\/[^"']*rentcafe\.com\/apartments[^"']+)["']/gi)) {
      const url = cleanUrl(match[1]);
      if (url) urls.add(url);
    }
  }
  return [...urls];
}

async function overpassCandidates(): Promise<Array<{ url: string; name: string | null; address: Address | null }>> {
  const query = `
    [out:json][timeout:60];
    (
      nwr["website"]["name"~"Apartments|Apartment|Lofts|Flats|Townhomes|Townhome",i](${BBOX});
      nwr["contact:website"]["name"~"Apartments|Apartment|Lofts|Flats|Townhomes|Townhome",i](${BBOX});
    );
    out center tags;
  `;
  const response = await fetch("https://overpass-api.de/api/interpreter", {
    method: "POST",
    signal: AbortSignal.timeout(90_000),
    headers: { "content-type": "application/x-www-form-urlencoded", "user-agent": "MXRE public-data discovery" },
    body: new URLSearchParams({ data: query }),
  });
  if (!response.ok) return [];
  const json = await response.json() as { elements?: Array<{ tags?: Record<string, string> }> };
  return (json.elements ?? []).flatMap(element => {
    const tags = element.tags ?? {};
    const raw = tags.website ?? tags["contact:website"];
    if (!raw) return [];
    const url = cleanUrl(raw.startsWith("http") ? raw : `https://${raw}`);
    if (!url) return [];
    const street = [tags["addr:housenumber"], tags["addr:street"]].filter(Boolean).join(" ");
    return [{ url, name: tags.name ?? null, address: street ? { street, city: tags["addr:city"], state: tags["addr:state"], zip: tags["addr:postcode"] } : null }];
  });
}

async function main() {
  console.log("MXRE - Free market apartment website discovery");
  console.log(JSON.stringify({ city: CITY, state: STATE, county_id: COUNTY_ID, bbox: BBOX, limit: LIMIT, dry_run: DRY_RUN }, null, 2));

  const candidateMap = new Map<string, { url: string; source: string; name: string | null; address: Address | null }>();
  for (const url of await rentCafeCandidates()) candidateMap.set(url, { url, source: "rentcafe_city_page", name: null, address: null });
  for (const item of await overpassCandidates()) candidateMap.set(item.url, { ...item, source: "openstreetmap_overpass" });

  let checked = 0;
  let matched = 0;
  let saved = 0;
  let noAddress = 0;
  let noMatch = 0;

  for (const candidate of [...candidateMap.values()].slice(0, LIMIT)) {
    checked++;
    const html = await fetchText(candidate.url);
    const address = candidate.address ?? (html ? extractAddress(html) : null);
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
    await save(property.id, candidate.url, candidate.name ?? (html ? extractComplexName(html, candidate.url) : null), candidate.source);
    saved++;
    console.log(`  matched ${property.id}: ${property.address} -> ${candidate.url}`);
  }

  console.log(JSON.stringify({ candidates: candidateMap.size, checked, matched, saved, noAddress, noMatch, dry_run: DRY_RUN }, null, 2));
}

main().catch(error => {
  console.error("Fatal:", error instanceof Error ? error.message : error);
  process.exit(1);
});
