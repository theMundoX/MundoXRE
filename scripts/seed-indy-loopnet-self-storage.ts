import "dotenv/config";
import { getWriteDb } from "../src/db/client.js";
import { hydrateWindowsUserEnv } from "./lib/env.ts";

hydrateWindowsUserEnv();

const db = getWriteDb();
const observedAt = new Date().toISOString();
const sourceUrl = "https://www.loopnet.com/Listing/Storage-of-America-10-Portfolio/39960844/";

const portfolioRaw = {
  listing_id: "39960844",
  portfolio_name: "Storage of America 10 Portfolio",
  portfolio_status: "Active",
  portfolio_properties: 10,
  portfolio_units: 6586,
  portfolio_nrsf: 823891,
  portfolio_occupancy_pct: 78.9,
  portfolio_total_land_acres: 58.15,
  broker: "CBRE Self Storage Advisory Group",
  contacts: ["Nicholas Walker", "Adam Alexander"],
  debt: {
    status: "reported",
    note: "$61.5MM CMBS loan at 6.007% fixed, interest-only through April 2031",
  },
  noi: {
    status: "not_reported",
    note: "LoopNet public listing snippet did not report NOI.",
  },
  cap_rate: {
    status: "not_reported",
    note: "LoopNet public listing snippet shows request cap rate / no public cap rate value.",
  },
  listing_description:
    "CBRE's Self Storage Advisory Group is retained as exclusive listing advisor for the Storage of America 10 Portfolio, a diversified collection of ten self-storage properties across Indiana, Michigan, and Ohio. The portfolio comprises 6,586 units totaling approximately 823,891 net rentable square feet, plus additional uncovered parking area, with a mix of drive-up and interior storage configurations.",
  investment_highlights: [
    "Assumable fixed-rate debt with long runway: $61.5MM CMBS loan at 6.007% fixed, interest-only through April 2031.",
    "Portfolio is currently 78.9% occupied and remote-managed.",
    "10 self-storage properties, 823,891 NRSF plus approximately 13,700 NRSF of parking.",
    "Markets average 121,000 population within a five-mile radius.",
  ],
  source_note: "Open-web LoopNet listing snapshot; portfolio is active and individual Indianapolis assets are part of the offering.",
};

const rows = [
  {
    title: "Storage of America 10 Portfolio - Self Storage of America",
    address: "7339 E Washington St",
    city: "Indianapolis",
    state_code: "IN",
    zip: "46219",
    confidence: "high",
    raw: {
      ...portfolioRaw,
      individual_building_sqft: 144295,
      individual_year_built: 2006,
      matched_property_id: 50866088,
      matched_parcel_id: "491001118031000700",
      individual_property_name: "Self Storage of America",
    },
  },
  {
    title: "Storage of America 10 Portfolio - Storage of America",
    address: "7910 W Washington St",
    city: "Indianapolis",
    state_code: "IN",
    zip: "46231",
    confidence: "high",
    raw: {
      ...portfolioRaw,
      individual_building_sqft: 51775,
      individual_year_built: 2019,
      matched_property_id: 51190720,
      matched_parcel_id: "491215105011000900",
      individual_property_name: "Storage of America",
    },
  },
  {
    title: "Storage of America 10 Portfolio - Storage of America",
    address: "4225 W 62nd St",
    city: "Indianapolis",
    state_code: "IN",
    zip: "46268",
    confidence: "high",
    raw: {
      ...portfolioRaw,
      individual_building_sqft: 50850,
      individual_year_built: 2006,
      matched_property_id: 54725929,
      matched_parcel_id: "490605101005000600",
      individual_property_name: "Storage of America",
    },
  },
  {
    title: "Storage of America 10 Portfolio - 8805 Pendleton Pike",
    address: "8805 Pendleton Pike",
    city: "Indianapolis",
    state_code: "IN",
    zip: "46226",
    confidence: "medium",
    raw: {
      ...portfolioRaw,
      individual_building_sqft: 34300,
      individual_year_built: 2017,
      matched_property_id: null,
      matched_parcel_id: null,
      individual_property_name: "8805 Pendleton Pike",
      match_note: "Active external evidence is clear; MXRE parcel match still needs cleanup.",
    },
  },
];

await db
  .from("external_market_listings")
  .delete()
  .eq("market", "indianapolis")
  .eq("asset_class", "self_storage")
  .eq("source", "loopnet_search_snapshot")
  .like("title", "Storage of America 10 Portfolio%");

let upserted = 0;
for (const row of rows) {
  const { error } = await db.from("external_market_listings").insert({
    market: "indianapolis",
    asset_class: "self_storage",
    source: "loopnet_search_snapshot",
    source_url: sourceUrl,
    title: row.title,
    address: row.address,
    city: row.city,
    state_code: row.state_code,
    zip: row.zip,
    units: null,
    list_price: null,
    cap_rate: null,
    noi: null,
    status: "active",
    confidence: row.confidence,
    observed_at: observedAt,
    last_seen_at: observedAt,
    raw: row.raw,
  });
  if (error) throw error;
  upserted++;
}

console.log(JSON.stringify({ upserted, source: "loopnet_search_snapshot" }, null, 2));
