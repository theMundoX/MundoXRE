import "dotenv/config";
import { getWriteDb } from "../src/db/client.js";
import { hydrateWindowsUserEnv } from "./lib/env.ts";

hydrateWindowsUserEnv();

const db = getWriteDb();

const observedAt = new Date().toISOString();

const rows = [
  {
    market: "indianapolis",
    asset_class: "multifamily",
    source: "crexi_search_snapshot",
    source_url: "https://www.crexi.com/properties/IN/Indianapolis/Multifamily/",
    title: "16-Unit Multi-Family Property",
    address: "5128 E Washington St; 5140 E Washington St",
    city: "Indianapolis",
    state_code: "IN",
    zip: "46219",
    units: 16,
    status: "active",
    confidence: "low",
    observed_at: observedAt,
    last_seen_at: observedAt,
    raw: {
      note: "Public search-result snapshot; needs direct source verification before underwriting.",
      snippet: "Exceptional 16-Unit Group Home Property in Historic Irvington",
    },
  },
  {
    market: "indianapolis",
    asset_class: "multifamily",
    source: "crexi_search_snapshot",
    source_url: "https://www.crexi.com/properties/IN/Indianapolis/Multifamily/",
    title: "Irvington Studio",
    address: "5819 E Washington St",
    city: "Indianapolis",
    state_code: "IN",
    zip: "46219",
    units: 14,
    list_price: 1075000,
    price_per_unit: 76786,
    status: "active",
    confidence: "low",
    observed_at: observedAt,
    last_seen_at: observedAt,
    raw: {
      note: "Public search-result snapshot; needs direct source verification before underwriting.",
      snippet: "Multifamily - 14 Units - $76,786/unit",
    },
  },
  {
    market: "indianapolis",
    asset_class: "multifamily",
    source: "crexi_search_snapshot",
    source_url: "https://www.crexi.com/properties/IN/Indianapolis/Multifamily/",
    title: "10 Unit SRO Multifamily",
    address: "3710 E Washington St",
    city: "Indianapolis",
    state_code: "IN",
    zip: "46201",
    units: 10,
    list_price: 865000,
    status: "active",
    confidence: "low",
    observed_at: observedAt,
    last_seen_at: observedAt,
    raw: {
      note: "Public search-result snapshot; needs direct source verification before underwriting.",
      snippet: "Multifamily - 10 Units",
    },
  },
  {
    market: "indianapolis",
    asset_class: "multifamily",
    source: "crexi_search_snapshot",
    source_url: "https://www.crexi.com/properties/IN/Indianapolis/Multifamily/",
    title: "Indianapolis Portfolio",
    address: "3707 N Meridian St",
    city: "Indianapolis",
    state_code: "IN",
    zip: "46208",
    units: 139,
    status: "active",
    confidence: "low",
    observed_at: observedAt,
    last_seen_at: observedAt,
    raw: {
      note: "Public search-result snapshot; needs direct source verification before underwriting.",
      snippet: "Multifamily - 139 Units",
    },
  },
  {
    market: "indianapolis",
    asset_class: "multifamily",
    source: "crexi_search_snapshot",
    source_url: "https://www.crexi.com/properties/IN/Indianapolis/Multifamily/",
    title: "1800 and 1812 N Meridian",
    address: "1800 N Meridian St; 1812 N Meridian St",
    city: "Indianapolis",
    state_code: "IN",
    zip: "46202",
    list_price: 6900000,
    status: "active",
    confidence: "low",
    observed_at: observedAt,
    last_seen_at: observedAt,
    raw: {
      note: "Public search-result snapshot; needs direct source verification before underwriting.",
      snippet: "Multifamily, Office, Hospitality, Mixed Use",
    },
  },
  {
    market: "indianapolis",
    asset_class: "self_storage",
    source: "crexi_search_snapshot",
    source_url: "https://www.crexi.com/properties/IN/Indianapolis/Self-Storage/",
    title: "2.93 AC Redevelopment Opportunity | Indianapolis, IN",
    address: "3415 English Ave",
    city: "Indianapolis",
    state_code: "IN",
    zip: "46201",
    list_price: 2000000,
    status: "active",
    confidence: "low",
    observed_at: observedAt,
    last_seen_at: observedAt,
    raw: {
      note: "Public Crexi self-storage search-result snapshot; verify direct source before underwriting.",
      snippet: "Industrial, Mixed Use, Retail, Self Storage - 3415 English Ave Indianapolis, IN 46201",
    },
  },
  {
    market: "indianapolis",
    asset_class: "self_storage",
    source: "crexi_search_snapshot",
    source_url: "https://www.crexi.com/properties/IN/Indianapolis/Self-Storage/",
    title: "auto Shop",
    address: "5990 E Raymond St",
    city: "Indianapolis",
    state_code: "IN",
    zip: "46203",
    list_price: 279000,
    status: "active",
    confidence: "low",
    observed_at: observedAt,
    last_seen_at: observedAt,
    raw: {
      note: "Public Crexi self-storage search-result snapshot; verify direct source before underwriting.",
      snippet: "Industrial, Office, Retail, Self Storage - Industrial - 20,423 SF",
    },
  },
  {
    market: "indianapolis",
    asset_class: "self_storage",
    source: "crexi_search_snapshot",
    source_url: "https://www.crexi.com/properties/IN/Indianapolis/Self-Storage/",
    title: "8301 Craig St",
    address: "8301 Craig St",
    city: "Indianapolis",
    state_code: "IN",
    zip: "46250",
    list_price: 550000,
    status: "active",
    confidence: "low",
    observed_at: observedAt,
    last_seen_at: observedAt,
    raw: {
      note: "Public Crexi self-storage search-result snapshot; verify direct source before underwriting.",
      snippet: "Hospitality, Land, Multifamily, Office, Self Storage - Land Opportunity in northeast Indianapolis",
    },
  },
];

let upserted = 0;
for (const row of rows) {
  const { error } = await db
    .from("external_market_listings")
    .delete()
    .eq("market", row.market)
    .eq("asset_class", row.asset_class)
    .eq("source", row.source)
    .eq("title", row.title);
  if (error) throw error;

  const { error: insertError } = await db.from("external_market_listings").insert(row);
  if (insertError) throw insertError;
  upserted++;
}

console.log(JSON.stringify({ upserted, source: "crexi_search_snapshot" }, null, 2));
