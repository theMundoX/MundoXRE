import "dotenv/config";
import { getWriteDb } from "../src/db/client.js";
import { hydrateWindowsUserEnv } from "./lib/env.ts";

hydrateWindowsUserEnv();

const db = getWriteDb();
const observedAt = new Date().toISOString();

type EvidenceRow = {
  market: string;
  title: string;
  address: string;
  city: string;
  state_code: string;
  zip: string | null;
  source_url: string;
  list_price?: number | null;
  units?: number | null;
  status: "active" | "off_market";
  confidence: "medium" | "high";
  raw: Record<string, unknown>;
};

const rows: EvidenceRow[] = [
  {
    market: "indianapolis",
    title: "4500 S Keystone Ave - self-storage development land",
    address: "4500 S Keystone Ave",
    city: "Indianapolis",
    state_code: "IN",
    zip: "46227",
    source_url: "https://www.loopnet.com/Listing/4500-S-Keystone-Ave-Indianapolis-IN/40212210/",
    list_price: 600000,
    status: "active",
    confidence: "medium",
    raw: {
      evidence_type: "self_storage_development_land",
      listing_id: "40212210",
      parcel_number: "49-10-31-121-002.000-574",
      lot_acres: 8.22,
      source_summary: "LoopNet detail page says the land property is currently available and marketing text says ground is available for self-storage or multifamily use.",
      verification: "public_detail_page",
      accepted_reason: "Active public detail page plus explicit self-storage use language; classified as development land, not an operating self-storage facility.",
    },
  },
  {
    market: "indianapolis",
    title: "5815 E 42nd St - approved indoor self-storage / mini-warehouse conversion",
    address: "5815 E 42nd St",
    city: "Indianapolis",
    state_code: "IN",
    zip: "46226",
    source_url: "https://www.loopnet.com/Listing/5815-E-42nd-St-Indianapolis-IN/26041666/",
    list_price: 1200000,
    status: "active",
    confidence: "medium",
    raw: {
      evidence_type: "self_storage_conversion_opportunity",
      listing_id: "26041666",
      parcel_number: "49-07-15-110-001.000-401",
      building_sqft: 80000,
      lot_acres: 11,
      broker: "MRE Connect Brokerage",
      source_summary: "LoopNet detail page says the property is currently available and zoning/summary identify approved indoor self storage and/or mini warehousing or flex commercial.",
      verification: "public_detail_page",
      accepted_reason: "Active public detail page plus explicit approved self-storage / mini-warehouse language; classified as conversion opportunity, not an operating self-storage facility.",
    },
  },
  {
    market: "dallas",
    title: "2339 Inwood Rd - self-storage covered land play",
    address: "2339 Inwood Rd",
    city: "Dallas",
    state_code: "TX",
    zip: "75235",
    source_url: "https://www.loopnet.com/Listing/2339-Inwood-Rd-Dallas-TX/39303130/",
    status: "active",
    confidence: "high",
    raw: {
      evidence_type: "operating_self_storage_or_redevelopment",
      listing_id: "39303130",
      building_sqft: 31460,
      year_built_renovated: "1964/1998",
      property_subtype: "Self-Storage",
      source_summary: "LoopNet detail page identifies property subtype Self-Storage and says Weitzman is marketing the property at 2339 Inwood Road.",
      verification: "public_detail_page",
      accepted_reason: "Active public detail page with Property Subtype = Self-Storage.",
    },
  },
  {
    market: "dallas",
    title: "Self Storage Portfolio - Mesquite / Balch Springs",
    address: "3818 N Town East Blvd; 12504 Quail Dr",
    city: "Mesquite",
    state_code: "TX",
    zip: "75150",
    source_url: "https://www.loopnet.com/Listing/Self-Storage-Portfolio/25786169/",
    list_price: 3600000,
    units: 273,
    status: "active",
    confidence: "high",
    raw: {
      evidence_type: "self_storage_portfolio",
      listing_id: "25786169",
      activated_on: "2022-05-25",
      building_sqft: 34420,
      lot_acres: 2.51,
      portfolio_properties: 2,
      portfolio_name: "Self Storage Portfolio",
      source_summary: "LoopNet detail page says the portfolio has two storage facilities/businesses: Town East Self Storage and Balch Springs Mini Warehouses, 273 units total, status active.",
      verification: "public_detail_page",
      accepted_reason: "Active public detail page with self-storage operating portfolio details.",
    },
  },
  {
    market: "dayton",
    title: "1764 Guenther Rd - in-progress self-storage development",
    address: "1764 Guenther Rd",
    city: "Dayton",
    state_code: "OH",
    zip: "45417",
    source_url: "https://www.loopnet.com/Listing/1764-Guenther-Rd-Dayton-OH/39354732/",
    list_price: 2500000,
    status: "active",
    confidence: "high",
    raw: {
      evidence_type: "self_storage_development",
      listing_id: "39354732",
      building_sqft: 36000,
      planned_buildings: 26,
      source_summary: "LoopNet detail page says the property is a partially completed self-storage development consisting of a planned 26-building facility.",
      verification: "public_detail_page",
      accepted_reason: "Active public detail page with explicit in-progress self-storage development language.",
    },
  },
  {
    market: "akron",
    title: "1025 S Broadway St - self-storage / flex building",
    address: "1025 S Broadway St",
    city: "Akron",
    state_code: "OH",
    zip: "44311",
    source_url: "https://www.loopnet.com/Listing/1025-S-Broadway-St-Akron-OH/39645924/",
    units: 303,
    status: "active",
    confidence: "high",
    raw: {
      evidence_type: "self_storage_flex_building",
      listing_id: "39645924",
      building_sqft: 63675,
      year_built_renovated: "1919/2017",
      property_subtype: "Self-Storage",
      source_summary: "LoopNet detail page says the building produces income from self-storage units and has 303 self-storage units currently.",
      verification: "public_detail_page",
      accepted_reason: "Active public detail page with Property Subtype = Self-Storage and current unit count.",
    },
  },
  {
    market: "fort-wayne",
    title: "6515 Stellhorn Rd - Next Level Storage",
    address: "6515 Stellhorn Rd",
    city: "Fort Wayne",
    state_code: "IN",
    zip: "46815",
    source_url: "https://www.loopnet.com/Listing/6515-Stellhorn-Rd-Fort-Wayne-IN/39172064/",
    units: 49,
    status: "active",
    confidence: "high",
    raw: {
      evidence_type: "storage_condo_units",
      listing_id: "39172064",
      building_sqft: 45400,
      lot_acres: 2.58,
      property_subtype: "Self-Storage",
      source_summary: "LoopNet detail page identifies Next Level Storage with 49 RV/Toy Shed/warehouse storage units available for lease or purchase and Property Subtype = Self-Storage.",
      verification: "public_detail_page",
      accepted_reason: "Active public detail page with explicit storage units and Property Subtype = Self-Storage.",
    },
  },
  {
    market: "peoria",
    title: "3101-3211 W Harmon Hwy - approved self-storage conversion",
    address: "3101-3211 W Harmon Hwy",
    city: "Peoria",
    state_code: "IL",
    zip: "61604",
    source_url: "https://www.loopnet.com/Listing/3101-3211-W-Harmon-Hwy-Peoria-IL/38502320/",
    status: "active",
    confidence: "high",
    raw: {
      evidence_type: "self_storage_conversion_opportunity",
      listing_id: "38502320",
      activated_on: "2025-11-19",
      approved_units: 700,
      building_sqft: 84346,
      lot_acres: 12.86,
      source_summary: "LoopNet detail page says the property has special-use permit approval for approximately 700 climate-controlled and drive-up self-storage units and is currently available.",
      verification: "public_detail_page",
      accepted_reason: "Active public detail page with approved self-storage conversion and Date on Market.",
    },
  },
  {
    market: "san-antonio",
    title: "4930 Callaghan Rd - Secure Spaces Self Storage",
    address: "4930 Callaghan Rd",
    city: "San Antonio",
    state_code: "TX",
    zip: "78228",
    source_url: "https://www.loopnet.com/Listing/4930-Callaghan-Rd-San-Antonio-TX/40133195/",
    list_price: 910000,
    units: 93,
    status: "active",
    confidence: "high",
    raw: {
      evidence_type: "operating_self_storage",
      listing_id: "40133195",
      building_sqft: 22500,
      nrsf: 22450,
      noi: 80231,
      cap_rate: "8.82%",
      source_summary: "LoopNet detail page says Secure Spaces Self Storage is a 93-unit, 22,450 NRSF self-storage investment opportunity.",
      verification: "public_detail_page",
      accepted_reason: "Active public detail page with explicit operating self-storage details.",
    },
  },
  {
    market: "san-antonio",
    title: "Storage 4U Self Storage 2 Portfolio",
    address: "San Antonio portfolio",
    city: "San Antonio",
    state_code: "TX",
    zip: null,
    source_url: "https://www.loopnet.com/Listing/San-Antonio-TX/39167455/",
    units: 808,
    status: "active",
    confidence: "high",
    raw: {
      evidence_type: "self_storage_portfolio",
      listing_id: "39167455",
      portfolio_name: "Storage 4U Self Storage 2 Portfolio",
      portfolio_properties: 2,
      nrsf: 95540,
      parking_units: 344,
      parking_nrsf: 115974,
      broker: "CBRE Self Storage Advisory Group",
      source_summary: "LoopNet detail page says CBRE is exclusive listing advisor for a pair of San Antonio self-storage properties with 808 storage units and 344 uncovered parking spaces.",
      verification: "public_detail_page",
      accepted_reason: "Active public detail page with explicit self-storage portfolio details.",
    },
  },
  {
    market: "san-antonio",
    title: "3608 S Gevers Street Portfolio - Doc's Space Center Self Storage",
    address: "3608 S Gevers St",
    city: "San Antonio",
    state_code: "TX",
    zip: "78210",
    source_url: "https://www.loopnet.com/Listing/3608-S-Gevers-Street-Portfolio/37693214/",
    list_price: 4579000,
    status: "active",
    confidence: "medium",
    raw: {
      evidence_type: "mixed_portfolio_with_self_storage",
      listing_id: "37693214",
      building_sqft: 19800,
      portfolio_properties: 2,
      source_summary: "LoopNet detail page says the portfolio includes Doc's Spacecenter Self Storage and a veterinary practice/hospital property.",
      verification: "public_detail_page",
      accepted_reason: "Active public detail page with explicit self-storage property in mixed portfolio.",
    },
  },
  {
    market: "columbus",
    title: "750 E 11th Ave - former Gateway Self Storage listing",
    address: "750 E 11th Ave",
    city: "Columbus",
    state_code: "OH",
    zip: "43211",
    source_url: "https://www.loopnet.com/Listing/750-E-11th-Ave-Columbus-OH/8710467/",
    status: "off_market",
    confidence: "high",
    raw: {
      evidence_type: "former_self_storage_listing",
      listing_id: "8710467",
      source_summary: "LoopNet detail page identifies Gateway Self Storage but says the property is no longer advertised on LoopNet.",
      verification: "public_detail_page",
      rejected_as_active_reason: "No longer advertised on LoopNet.",
    },
  },
  {
    market: "detroit",
    title: "14225 Schaefer Hwy - former self-storage / industrial listing",
    address: "14225 Schaefer Hwy",
    city: "Detroit",
    state_code: "MI",
    zip: "48227",
    source_url: "https://www.loopnet.com/Listing/14225-Schaefer-Hwy-Detroit-MI/38285959/",
    status: "off_market",
    confidence: "high",
    raw: {
      evidence_type: "former_self_storage_listing",
      listing_id: "38285959",
      activated_on: "2025-10-31",
      building_sqft: 43688,
      lot_acres: 1.78,
      property_subtype: "Self-Storage",
      source_summary: "LoopNet detail page identifies Property Subtype = Self-Storage but says it is no longer advertised.",
      verification: "public_detail_page",
      rejected_as_active_reason: "No longer advertised on LoopNet.",
    },
  },
  {
    market: "memphis",
    title: "1699 Airways Blvd - former Airways Storage listing",
    address: "1699 Airways Blvd",
    city: "Memphis",
    state_code: "TN",
    zip: "38114",
    source_url: "https://www.loopnet.com/Listing/1699-Airways-Blvd-Memphis-TN/36739338/",
    units: 1185,
    status: "off_market",
    confidence: "high",
    raw: {
      evidence_type: "former_operating_self_storage_listing",
      listing_id: "36739338",
      activated_on: "2025-07-15",
      building_sqft: 146580,
      lot_acres: 6,
      property_subtype: "Self-Storage",
      source_summary: "LoopNet detail page identifies Airways Storage with 1,185 climate-controlled units and 80 parking spaces, but says it is no longer advertised.",
      verification: "public_detail_page",
      rejected_as_active_reason: "No longer advertised on LoopNet.",
    },
  },
  {
    market: "san-antonio",
    title: "1301 E Commerce St - former Downtown SA Self Storage listing",
    address: "1301 E Commerce St",
    city: "San Antonio",
    state_code: "TX",
    zip: "78205",
    source_url: "https://www.loopnet.com/Listing/1301-E-Commerce-St-San-Antonio-TX/12355665/",
    status: "off_market",
    confidence: "high",
    raw: {
      evidence_type: "former_self_storage_listing",
      listing_id: "12355665",
      building_sqft: 80400,
      property_subtype: "Self-Storage",
      source_summary: "LoopNet detail page identifies Downtown SA Self Storage but says it is no longer advertised.",
      verification: "public_detail_page",
      rejected_as_active_reason: "No longer advertised on LoopNet.",
    },
  },
  {
    market: "south-bend",
    title: "52447 Portage Rd - former self-storage listing",
    address: "52447 Portage Rd",
    city: "South Bend",
    state_code: "IN",
    zip: "46628",
    source_url: "https://www.loopnet.com/Listing/52447-Portage-Rd-South-Bend-IN/30624303/",
    status: "off_market",
    confidence: "high",
    raw: {
      evidence_type: "former_self_storage_listing",
      listing_id: "30624303",
      activated_on: "2024-01-10",
      building_sqft: 125000,
      lot_acres: 7.83,
      property_subtype: "Self-Storage",
      parcel_numbers: [
        "71-03-22-153-008.000-009",
        "71-03-22-153-010.000-009",
        "71-03-22-153-009.000-009",
      ],
      source_summary: "LoopNet detail page identifies Property Subtype = Self-Storage and says the property is no longer advertised.",
      verification: "public_detail_page",
      rejected_as_active_reason: "No longer advertised on LoopNet.",
    },
  },
];

await db
  .from("external_market_listings")
  .delete()
  .eq("asset_class", "self_storage")
  .eq("source", "free_web_loopnet_detail");

let upserted = 0;
for (const row of rows) {
  const { error } = await db.from("external_market_listings").insert({
    market: row.market,
    asset_class: "self_storage",
    source: "free_web_loopnet_detail",
    source_url: row.source_url,
    title: row.title,
    address: row.address,
    city: row.city,
    state_code: row.state_code,
    zip: row.zip,
    units: row.units ?? null,
    list_price: row.list_price ?? null,
    price_per_unit: null,
    cap_rate: null,
    noi: typeof row.raw.noi === "number" ? row.raw.noi : null,
    status: row.status,
    confidence: row.confidence,
    observed_at: observedAt,
    first_seen_at: observedAt,
    last_seen_at: observedAt,
    raw: row.raw,
  });
  if (error) throw error;
  upserted++;
}

const activeByMarket = rows
  .filter((row) => row.status === "active")
  .reduce<Record<string, number>>((acc, row) => {
    acc[row.market] = (acc[row.market] ?? 0) + 1;
    return acc;
  }, {});

const offMarketByMarket = rows
  .filter((row) => row.status === "off_market")
  .reduce<Record<string, number>>((acc, row) => {
    acc[row.market] = (acc[row.market] ?? 0) + 1;
    return acc;
  }, {});

console.log(JSON.stringify({
  upserted,
  active_rows: rows.filter((row) => row.status === "active").length,
  off_market_rows: rows.filter((row) => row.status === "off_market").length,
  activeByMarket,
  offMarketByMarket,
  source: "free_web_loopnet_detail",
}, null, 2));
