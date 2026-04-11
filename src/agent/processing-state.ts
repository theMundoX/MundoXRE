/**
 * Processing State Tracker
 * Tracks what's been processed so agent knows where to continue
 * Fully autonomous - no Claude Code dependency
 */

import { createClient } from "@supabase/supabase-js";

interface ProcessingRecord {
  county_id: number;
  county_name: string;
  state_code: string;
  properties_processed: number;
  last_processed_at: string;
  completion_percent: number;
  status: "pending" | "in_progress" | "complete";
}

export class ProcessingStateManager {
  private db: ReturnType<typeof createClient>;

  constructor() {
    const url = process.env.SUPABASE_URL!;
    const key = process.env.SUPABASE_SERVICE_KEY!;

    if (!url || !key) {
      throw new Error("SUPABASE_URL and SUPABASE_SERVICE_KEY required");
    }

    this.db = createClient(url, key, {
      auth: { persistSession: false },
    });
  }

  /**
   * Initialize processing state table if not exists
   */
  async initialize() {
    // Table will be created if needed - for now just test connection
    const { error } = await this.db.from("counties").select("id").limit(1);
    if (error) {
      throw new Error(`Cannot connect to Supabase: ${error.message}`);
    }
  }

  /**
   * Get next county to process (least recently processed)
   */
  async getNextCounty(): Promise<ProcessingRecord | null> {
    try {
      // Get all counties and their processing status
      const { data: counties, error } = await this.db
        .from("counties")
        .select("id, county_name, state_code");

      if (error || !counties) {
        console.error("Failed to fetch counties:", error);
        return null;
      }

      // For now, return the first unprocessed county
      // In production, track completion in a separate table
      if (counties.length > 0) {
        const first = counties[0] as any;
        return {
          county_id: first.id,
          county_name: first.county_name,
          state_code: first.state_code,
          properties_processed: 0,
          last_processed_at: new Date().toISOString(),
          completion_percent: 0,
          status: "pending",
        };
      }

      return null;
    } catch (err) {
      console.error("Error getting next county:", err);
      return null;
    }
  }

  /**
   * Record that a batch was processed
   */
  async recordProgress(
    countyId: number,
    propertiesProcessed: number,
    completionPercent: number
  ) {
    try {
      // In production, write to a processing_log table
      // For now, just log to console
      console.log(
        `Progress: County ${countyId} - ${propertiesProcessed} properties, ${completionPercent}% complete`
      );
    } catch (err) {
      console.error("Error recording progress:", err);
    }
  }

  /**
   * Get overall progress across all counties
   */
  async getOverallProgress(): Promise<{
    totalCounties: number;
    processedCounties: number;
    completionPercent: number;
  }> {
    try {
      const { data: counties } = await this.db
        .from("counties")
        .select("id")
        .eq("active", true);

      const { data: properties } = await this.db
        .from("properties")
        .select("id", { count: "exact", head: true });

      return {
        totalCounties: counties?.length || 0,
        processedCounties: 0, // Would track in processing_log table
        completionPercent: Math.min(
          100,
          ((properties?.length || 0) / 1000000) * 100
        ), // Rough estimate
      };
    } catch (err) {
      console.error("Error getting overall progress:", err);
      return {
        totalCounties: 0,
        processedCounties: 0,
        completionPercent: 0,
      };
    }
  }
}
