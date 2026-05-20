import "dotenv/config";
import { getWriteDb } from "../src/db/client.js";
import { hydrateWindowsUserEnv } from "./lib/env.ts";

hydrateWindowsUserEnv();

const db = getWriteDb();
const observedAt = new Date().toISOString();

const rows = [
  {
    title: "4500 S Keystone Ave - self-storage development land",
    address: "4500 S Keystone Ave",
    city: "Indianapolis",
    state_code: "IN",
    zip: "46227",
    source: "free_web_loopnet_detail",
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
    title: "5815 E 42nd St - approved indoor self-storage / mini-warehouse conversion",
    address: "5815 E 42nd St",
    city: "Indianapolis",
    state_code: "IN",
    zip: "46226",
    source: "free_web_loopnet_detail",
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
    title: "12147 65th St - former self-storage development listing",
    address: "12147 65th St",
    city: "Indianapolis",
    state_code: "IN",
    zip: "46236",
    source: "free_web_loopnet_detail",
    source_url: "https://www.loopnet.com/Listing/12147-65th-st-Indianapolis-IN/34921764/",
    status: "off_market",
    confidence: "high",
    raw: {
      evidence_type: "former_self_storage_development_listing",
      listing_id: "34921764",
      parcel_number: "49-01-34-124-019.000-407",
      building_sqft: 72025,
      source_summary: "LoopNet detail page identifies self-storage development/property subtype but says it is no longer advertised on LoopNet.",
      verification: "public_detail_page",
      rejected_as_active_reason: "No longer advertised on LoopNet.",
    },
  },
  {
    title: "6800 Pendleton Pike - former self-storage listing",
    address: "6800 Pendleton Pike",
    city: "Indianapolis",
    state_code: "IN",
    zip: "46226",
    source: "free_web_loopnet_detail",
    source_url: "https://www.loopnet.com/Listing/6800-Pendleton-Pike-Indianapolis-IN/3642681/",
    status: "off_market",
    confidence: "high",
    raw: {
      evidence_type: "former_self_storage_listing",
      listing_id: "3642681",
      parcel_numbers: ["49-07-14-121-012.000-401", "49-07-14-121-008.000-401"],
      building_sqft: 107985,
      source_summary: "LoopNet detail page identifies property subtype Self-Storage but says it is no longer advertised on LoopNet.",
      verification: "public_detail_page",
      rejected_as_active_reason: "No longer advertised on LoopNet.",
    },
  },
];

await db
  .from("external_market_listings")
  .delete()
  .eq("market", "indianapolis")
  .eq("asset_class", "self_storage")
  .eq("source", "free_web_loopnet_detail");

let upserted = 0;
for (const row of rows) {
  const { error } = await db.from("external_market_listings").insert({
    market: "indianapolis",
    asset_class: "self_storage",
    units: null,
    price_per_unit: null,
    cap_rate: null,
    noi: null,
    observed_at: observedAt,
    last_seen_at: observedAt,
    ...row,
  });
  if (error) throw error;
  upserted++;
}

console.log(JSON.stringify({
  upserted,
  active_rows: rows.filter((row) => row.status === "active").length,
  off_market_rows: rows.filter((row) => row.status === "off_market").length,
  source: "free_web_loopnet_detail",
}, null, 2));
