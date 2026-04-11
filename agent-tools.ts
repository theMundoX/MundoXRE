/**
 * Autonomous Agent Tools
 *
 * Gives the MXRE agent the ability to:
 * - Read/modify adapter configs
 * - Query Supabase for adapter status
 * - Test adapter endpoints
 * - Commit fixes to git
 */

import { readFileSync, writeFileSync } from "fs";
import { execSync } from "child_process";
import { getDb } from "./src/db/client.js";

export class AgentTools {
  /**
   * Get current config for a county
   */
  static getCountyConfig(state: string, county: string): any {
    const files = [
      `./data/counties/${state.toLowerCase()}.json`,
      `./data/counties/arcgis-registry.json`,
      `./data/counties/socrata-registry.json`,
    ];

    for (const file of files) {
      try {
        const data = JSON.parse(readFileSync(file, "utf-8"));
        const config = Array.isArray(data)
          ? data.find((c: any) => c.state === state && c.name === county)
          : data;
        if (config) return config;
      } catch {
        // Continue
      }
    }

    return null;
  }

  /**
   * Update county config with new adapter endpoint
   */
  static updateCountyConfig(
    state: string,
    county: string,
    newUrl: string,
    fieldMap?: Record<string, string>
  ): boolean {
    const file = `./data/counties/${state.toLowerCase()}.json`;

    try {
      const data = JSON.parse(readFileSync(file, "utf-8"));
      const index = Array.isArray(data)
        ? data.findIndex((c: any) => c.state === state && c.name === county)
        : -1;

      if (index !== -1) {
        data[index].base_url = newUrl;
        if (fieldMap) {
          data[index].field_map = { ...data[index].field_map, ...fieldMap };
        }
        writeFileSync(file, JSON.stringify(data, null, 2));
        return true;
      }
    } catch (err) {
      console.error(`Failed to update config: ${err}`);
    }

    return false;
  }

  /**
   * Test if an adapter endpoint is reachable
   */
  static async testAdapterEndpoint(url: string): Promise<boolean> {
    try {
      const response = await fetch(url, { method: "HEAD", timeout: 5000 });
      return response.ok || response.status === 400; // 400 might be expected
    } catch (err) {
      return false;
    }
  }

  /**
   * Get property count for a county from Supabase
   */
  static async getCountyPropertyCount(
    state: string,
    county: string
  ): Promise<number> {
    const db = getDb();
    const { data } = await db
      .from("counties")
      .select("id")
      .eq("state_code", state)
      .eq("county_name", county)
      .limit(1);

    if (!data || data.length === 0) return 0;

    const countyId = data[0].id;
    const { data: props } = await db
      .from("properties")
      .select("count", { count: "exact" })
      .eq("county_id", countyId);

    return props?.length || 0;
  }

  /**
   * Commit a fix to git
   */
  static commitFix(message: string): boolean {
    try {
      execSync(`git add -A`, { cwd: process.cwd() });
      execSync(`git commit -m "${message}"`, { cwd: process.cwd() });
      return true;
    } catch (err) {
      console.error(`Git commit failed: ${err}`);
      return false;
    }
  }

  /**
   * Find correct ArcGIS endpoint from county website
   * (In production, could scrape county GIS sites)
   */
  static async findCorrectArcGISEndpoint(state: string, county: string): Promise<string | null> {
    // Common ArcGIS endpoint patterns by state
    const patterns: Record<string, string[]> = {
      AZ: [
        `https://gis.az${county.toLowerCase()}.gov/arcgis/rest/services/Parcels/MapServer/0`,
        `https://gis.${county.toLowerCase()}-az.gov/arcgis/rest/services/Parcels/MapServer/0`,
      ],
      NV: [
        `https://gis.${county.toLowerCase()}-nv.gov/arcgis/rest/services/Parcels/MapServer/0`,
        `https://maps.${county.toLowerCase()}county.us/arcgis/rest/services/Parcels/MapServer/0`,
      ],
      NY: [
        `https://gis.${county.toLowerCase()}-ny.gov/arcgis/rest/services/Parcels/MapServer/0`,
        `https://data.ny.gov/gis/arcgis/rest/services/Parcels/MapServer/0`,
      ],
    };

    const urls = patterns[state] || [];

    for (const url of urls) {
      if (await this.testAdapterEndpoint(url)) {
        return url;
      }
    }

    return null;
  }

  /**
   * Get list of failed adapters from current ingest run
   */
  static getFailedAdapters(): Array<{
    county: string;
    state: string;
    platform: string;
    error: string;
  }> {
    // This would be populated from ingest pipeline logs
    // For now, return example
    return [
      {
        county: "Maricopa",
        state: "AZ",
        platform: "arcgis",
        error: "Invalid URL",
      },
      {
        county: "Clark",
        state: "NV",
        platform: "arcgis",
        error: "Token Required",
      },
    ];
  }
}
