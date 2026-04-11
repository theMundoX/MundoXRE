#!/usr/bin/env node
/**
 * MXRE — MCP server for rental listing data queries.
 *
 * Exposes tools to search properties, rents, and mortgage records
 * stored in the MXRE Supabase database.
 *
 * Usage:
 *   claude mcp add mxre -- node /path/to/mxre/dist/index.js
 *
 * Environment:
 *   SUPABASE_URL         — Supabase project URL (required)
 *   SUPABASE_SERVICE_KEY  — Supabase service role key (required)
 */

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { z } from "zod";
import {
  getProperties,
  getPropertyById,
  getLatestRents,
  getMortgageRecords,
  getCounties,
  getPropertyCount,
  type PropertyFilters,
} from "./db/queries.js";

async function main() {
  const server = new McpServer({
    name: "mxre",
    version: "0.1.0",
  });

  // ─── search_properties ────────────────────────────────────────────

  server.tool(
    "search_properties",
    "Search properties in the MXRE database by location, type, units, or owner. Returns address, owner, units, assessed value, and more.",
    {
      county: z.string().optional().describe("County name (e.g., 'Oklahoma', 'Tulsa', 'Comanche')"),
      city: z.string().optional().describe("City name (partial match)"),
      zip: z.string().optional().describe("ZIP code (exact match)"),
      state: z.string().optional().describe("State 2-letter code (e.g., 'OK')"),
      property_type: z
        .string()
        .optional()
        .describe("Property type: 'multifamily', 'single_family', 'commercial', 'land', 'mixed_use'"),
      min_units: z.number().optional().describe("Minimum unit count (e.g., 5 for multifamily)"),
      owner: z.string().optional().describe("Owner name (partial match)"),
      limit: z.number().optional().default(20).describe("Max results (default 20)"),
      offset: z.number().optional().default(0).describe("Offset for pagination"),
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
          limit: params.limit,
          offset: params.offset,
        };

        const properties = await getProperties(filters);
        const total = await getPropertyCount(
          filters.county_id ? { county_id: filters.county_id } : undefined,
        );

        return {
          content: [
            {
              type: "text" as const,
              text: JSON.stringify(
                {
                  total_matching: total,
                  returned: properties.length,
                  offset: params.offset ?? 0,
                  properties,
                },
                null,
                2,
              ),
            },
          ],
        };
      } catch (err) {
        return {
          content: [
            {
              type: "text" as const,
              text: JSON.stringify({
                error: err instanceof Error ? err.message : String(err),
              }),
            },
          ],
          isError: true,
        };
      }
    },
  );

  // ─── get_property_details ─────────────────────────────────────────

  server.tool(
    "get_property_details",
    "Get full details for a property including latest rent observations and mortgage records.",
    {
      property_id: z.number().describe("Property ID from search_properties results"),
    },
    async (params) => {
      try {
        const property = await getPropertyById(params.property_id);
        const rents = await getLatestRents(params.property_id);
        const mortgages = await getMortgageRecords(params.property_id);

        return {
          content: [
            {
              type: "text" as const,
              text: JSON.stringify({ property, rents, mortgages }, null, 2),
            },
          ],
        };
      } catch (err) {
        return {
          content: [
            {
              type: "text" as const,
              text: JSON.stringify({
                error: err instanceof Error ? err.message : String(err),
              }),
            },
          ],
          isError: true,
        };
      }
    },
  );

  // ─── list_counties ────────────────────────────────────────────────

  server.tool(
    "list_counties",
    "List all counties currently tracked in the MXRE database with property counts.",
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
          content: [
            {
              type: "text" as const,
              text: JSON.stringify(countyStats, null, 2),
            },
          ],
        };
      } catch (err) {
        return {
          content: [
            {
              type: "text" as const,
              text: JSON.stringify({
                error: err instanceof Error ? err.message : String(err),
              }),
            },
          ],
          isError: true,
        };
      }
    },
  );

  // ─── Connect ──────────────────────────────────────────────────────

  const transport = new StdioServerTransport();
  await server.connect(transport);
  console.error("[mxre] Server running on stdio");
}

main().catch((err) => {
  console.error("[mxre] Fatal error:", err);
  process.exit(1);
});
