#!/usr/bin/env node
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { z } from "zod";
import {
  getProperties,
  getPropertyById,
  getLatestRents,
  getRentHistory,
  getLeaseEvents,
  getLatestFees,
  getAmenities,
  getLatestReputation,
  getMortgageRecords,
  getFloorplans,
  getCounties,
  getPropertyCount,
  getListingSignals,
  getOnMarketProperties,
  getAgentByName,
  type PropertyFilters,
  type ListingFilters,
} from "./db/queries.js";

function safeError(err: unknown): string {
  if (err instanceof Error) {
    console.error("[mxre]", err.message);
  }
  return "Request failed. Check server logs.";
}

async function main() {
  const server = new McpServer({
    name: "mxre",
    version: "0.2.0",
  });

  // ─── search_properties ────────────────────────────────────────────

  server.tool(
    "search_properties",
    "Search properties by location, type, units, owner, or management company.",
    {
      county: z.string().optional().describe("County name"),
      city: z.string().optional().describe("City name (partial match)"),
      zip: z.string().optional().describe("ZIP code"),
      state: z.string().optional().describe("State 2-letter code"),
      property_type: z.string().optional().describe("e.g. 'multifamily', 'single_family', 'commercial'"),
      min_units: z.number().optional().describe("Minimum unit count"),
      owner: z.string().optional().describe("Owner name (partial match)"),
      mgmt_company: z.string().optional().describe("Management company (partial match)"),
      apartments_only: z.boolean().optional().describe("Only apartment properties"),
      sfr_only: z.boolean().optional().describe("Only single-family rentals"),
      limit: z.number().min(1).max(100).optional().default(20).describe("Max results (1-100)"),
      offset: z.number().min(0).optional().default(0).describe("Pagination offset"),
    },
    async (params) => {
      try {
        const filters: PropertyFilters = {
          county_name: params.county,
          city: params.city,
          zip: params.zip,
          state_code: params.state,
          property_type: params.property_type,
          min_units: params.min_units,
          owner: params.owner,
          mgmt_company: params.mgmt_company,
          is_apartment: params.apartments_only,
          is_sfr: params.sfr_only,
          limit: params.limit,
          offset: params.offset,
        };

        const properties = await getProperties(filters);

        return {
          content: [
            {
              type: "text" as const,
              text: JSON.stringify({ returned: properties.length, properties }, null, 2),
            },
          ],
        };
      } catch (err) {
        return {
          content: [{ type: "text" as const, text: JSON.stringify({ error: safeError(err) }) }],
          isError: true,
        };
      }
    },
  );

  // ─── get_property_details ─────────────────────────────────────────

  server.tool(
    "get_property_details",
    "Full property profile: floorplans, current rents, fees, amenities, reviews, and mortgage records.",
    {
      property_id: z.number().describe("Property ID from search_properties"),
    },
    async (params) => {
      try {
        const [property, floorplans, rents, fees, amenities, reputation, mortgages, listings] =
          await Promise.all([
            getPropertyById(params.property_id),
            getFloorplans(params.property_id),
            getLatestRents(params.property_id),
            getLatestFees(params.property_id),
            getAmenities(params.property_id),
            getLatestReputation(params.property_id),
            getMortgageRecords(params.property_id),
            getListingSignals(params.property_id),
          ]);

        return {
          content: [
            {
              type: "text" as const,
              text: JSON.stringify(
                { property, floorplans, rents, fees, amenities, reputation, mortgages, listings },
                null,
                2,
              ),
            },
          ],
        };
      } catch (err) {
        return {
          content: [{ type: "text" as const, text: JSON.stringify({ error: safeError(err) }) }],
          isError: true,
        };
      }
    },
  );

  // ─── get_rent_history ─────────────────────────────────────────────

  server.tool(
    "get_rent_history",
    "Historical rent trends for a property over time. Includes asking rent, effective rent, occupancy, and PSF.",
    {
      property_id: z.number().describe("Property ID"),
      months: z.number().min(1).max(36).optional().default(12).describe("Months of history (1-36)"),
    },
    async (params) => {
      try {
        const history = await getRentHistory(params.property_id, params.months);

        return {
          content: [
            {
              type: "text" as const,
              text: JSON.stringify({ property_id: params.property_id, months: params.months, snapshots: history }, null, 2),
            },
          ],
        };
      } catch (err) {
        return {
          content: [{ type: "text" as const, text: JSON.stringify({ error: safeError(err) }) }],
          isError: true,
        };
      }
    },
  );

  // ─── get_lease_activity ───────────────────────────────────────────

  server.tool(
    "get_lease_activity",
    "Recent lease transactions for a property: new leases, renewals, and notices to vacate.",
    {
      property_id: z.number().describe("Property ID"),
    },
    async (params) => {
      try {
        const events = await getLeaseEvents(params.property_id);

        return {
          content: [
            {
              type: "text" as const,
              text: JSON.stringify({ property_id: params.property_id, lease_events: events }, null, 2),
            },
          ],
        };
      } catch (err) {
        return {
          content: [{ type: "text" as const, text: JSON.stringify({ error: safeError(err) }) }],
          isError: true,
        };
      }
    },
  );

  // ─── list_counties ────────────────────────────────────────────────

  server.tool(
    "list_counties",
    "List all tracked counties with property counts.",
    {},
    async () => {
      try {
        const counties = await getCounties();
        const countyStats = await Promise.all(
          counties.map(async (c: { id: number; county_name: string; state_code: string }) => {
            const count = await getPropertyCount({ county_id: c.id });
            return { ...c, property_count: count };
          }),
        );

        return {
          content: [{ type: "text" as const, text: JSON.stringify(countyStats, null, 2) }],
        };
      } catch (err) {
        return {
          content: [{ type: "text" as const, text: JSON.stringify({ error: safeError(err) }) }],
          isError: true,
        };
      }
    },
  );

  // ─── search_on_market ────────────────────────────────────────────

  server.tool(
    "search_on_market",
    "Search for on-market properties with listing price, agent info, and brokerage.",
    {
      city: z.string().optional().describe("City name (partial match)"),
      state: z.string().optional().describe("State 2-letter code"),
      zip: z.string().optional().describe("ZIP code"),
      min_price: z.number().optional().describe("Minimum listing price"),
      max_price: z.number().optional().describe("Maximum listing price"),
      source: z.string().optional().describe("Listing source: zillow, redfin, or realtor"),
      limit: z.number().min(1).max(100).optional().default(20).describe("Max results (1-100)"),
      offset: z.number().min(0).optional().default(0).describe("Pagination offset"),
    },
    async (params) => {
      try {
        const filters: ListingFilters = {
          city: params.city,
          state_code: params.state,
          zip: params.zip,
          min_price: params.min_price,
          max_price: params.max_price,
          listing_source: params.source,
          is_on_market: true,
          limit: params.limit,
          offset: params.offset,
        };

        const listings = await getOnMarketProperties(filters);

        return {
          content: [
            { type: "text" as const, text: JSON.stringify({ returned: listings.length, listings }, null, 2) },
          ],
        };
      } catch (err) {
        return {
          content: [{ type: "text" as const, text: JSON.stringify({ error: safeError(err) }) }],
          isError: true,
        };
      }
    },
  );

  // ─── get_on_market_status ──────────────────────────────────────────

  server.tool(
    "get_on_market_status",
    "Check if a specific property is currently listed for sale, with listing price and agent details.",
    {
      property_id: z.number().describe("Property ID from search_properties"),
    },
    async (params) => {
      try {
        const signals = await getListingSignals(params.property_id);

        const active = signals.filter((s: { is_on_market: boolean; delisted_at?: string }) =>
          s.is_on_market && !s.delisted_at,
        );

        return {
          content: [
            {
              type: "text" as const,
              text: JSON.stringify({
                property_id: params.property_id,
                is_on_market: active.length > 0,
                listing_count: active.length,
                signals: active,
              }, null, 2),
            },
          ],
        };
      } catch (err) {
        return {
          content: [{ type: "text" as const, text: JSON.stringify({ error: safeError(err) }) }],
          isError: true,
        };
      }
    },
  );

  // ─── get_agent_contact ─────────────────────────────────────────────

  server.tool(
    "get_agent_contact",
    "Look up a real estate agent's contact info (phone, email) from state license databases.",
    {
      agent_name: z.string().describe("Agent name to look up"),
      state: z.string().describe("State 2-letter code"),
    },
    async (params) => {
      try {
        const agents = await getAgentByName(params.agent_name, params.state);

        return {
          content: [
            {
              type: "text" as const,
              text: JSON.stringify({
                agent_name: params.agent_name,
                state: params.state,
                results: agents,
              }, null, 2),
            },
          ],
        };
      } catch (err) {
        return {
          content: [{ type: "text" as const, text: JSON.stringify({ error: safeError(err) }) }],
          isError: true,
        };
      }
    },
  );

  const transport = new StdioServerTransport();
  await server.connect(transport);
}

main().catch(() => {
  process.exit(1);
});
