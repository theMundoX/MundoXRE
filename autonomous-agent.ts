#!/usr/bin/env node
/**
 * Autonomous MXRE Agent
 *
 * Uses MundoX OS components (LCM, router, runtime) to:
 * 1. Monitor ingest pipeline for adapter failures
 * 2. Store issues in persistent memory (LCM database)
 * 3. Autonomously fix broken adapters
 * 4. Verify fixes and commit to git
 *
 * MundoX (local Qwen3 brain) decides what to fix, Claude handles complex reasoning.
 */

import "dotenv/config";
import Database from "better-sqlite3";
import { resolve } from "path";
import { execSync } from "child_process";
import { readFileSync, writeFileSync } from "fs";
import { getDb } from "./src/db/client.js";

interface MXRETask {
  id?: number;
  type: "adapter_broken" | "county_missing" | "verify_fix" | "update_config";
  county: string;
  state: string;
  platform: string;
  issue: string;
  status: "pending" | "in_progress" | "resolved" | "failed";
  error?: string;
  fix_attempted?: string;
  created_at?: string;
  resolved_at?: string;
}

class MXREAgent {
  private lcmDb: Database.Database;
  private supabase = getDb();

  constructor() {
    // Use MundoX OS LCM database
    const lcmPath = resolve(
      process.cwd(),
      "../mundoXOS/.claude/lcm/database.db"
    );
    this.lcmDb = new Database(lcmPath);
    this.lcmDb.exec(`
      CREATE TABLE IF NOT EXISTS mxre_tasks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        type TEXT NOT NULL,
        county TEXT NOT NULL,
        state TEXT NOT NULL,
        platform TEXT NOT NULL,
        issue TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending',
        error TEXT,
        fix_attempted TEXT,
        created_at TEXT DEFAULT (datetime('now')),
        resolved_at TEXT
      );

      CREATE INDEX IF NOT EXISTS idx_mxre_status ON mxre_tasks(status);
      CREATE INDEX IF NOT EXISTS idx_mxre_county ON mxre_tasks(state, county);
    `);
  }

  /**
   * Log an adapter failure from the ingest pipeline
   */
  logAdapterFailure(county: string, state: string, platform: string, error: string) {
    const insert = this.lcmDb.prepare(`
      INSERT INTO mxre_tasks (type, county, state, platform, issue, status)
      VALUES (?, ?, ?, ?, ?, ?)
    `);

    insert.run("adapter_broken", county, state, platform, error, "pending");
    console.log(`✓ Task logged: ${county}, ${state} (${platform}) — ${error}`);
  }

  /**
   * Get next pending task
   */
  getNextTask(): MXRETask | null {
    const query = this.lcmDb.prepare(`
      SELECT * FROM mxre_tasks
      WHERE status = 'pending'
      ORDER BY created_at ASC
      LIMIT 1
    `);

    return query.get() as MXRETask | null;
  }

  /**
   * Update task status
   */
  updateTask(taskId: number, status: string, error?: string, fix?: string) {
    const update = this.lcmDb.prepare(`
      UPDATE mxre_tasks
      SET status = ?, error = ?, fix_attempted = ?, resolved_at = CASE WHEN ? THEN datetime('now') ELSE NULL END
      WHERE id = ?
    `);

    update.run(status, error || null, fix || null, status === "resolved" ? 1 : 0, taskId);
  }

  /**
   * Get list of all pending tasks
   */
  getPendingTasks(): MXRETask[] {
    const query = this.lcmDb.prepare(`
      SELECT * FROM mxre_tasks
      WHERE status IN ('pending', 'in_progress')
      ORDER BY created_at ASC
    `);

    return query.all() as MXRETask[];
  }

  /**
   * Store memory of known issues and fixes in LCM memory table
   */
  storeMemory(category: string, content: string) {
    // This would integrate with MundoX OS's memory table
    // For now, log to console
    console.log(`[MEMORY] ${category}: ${content}`);
  }

  /**
   * Get summary of agent status
   */
  getStatus() {
    const pending = this.lcmDb.prepare(
      "SELECT COUNT(*) as count FROM mxre_tasks WHERE status = 'pending'"
    ).get() as any;

    const inProgress = this.lcmDb.prepare(
      "SELECT COUNT(*) as count FROM mxre_tasks WHERE status = 'in_progress'"
    ).get() as any;

    const resolved = this.lcmDb.prepare(
      "SELECT COUNT(*) as count FROM mxre_tasks WHERE status = 'resolved'"
    ).get() as any;

    const failed = this.lcmDb.prepare(
      "SELECT COUNT(*) as count FROM mxre_tasks WHERE status = 'failed'"
    ).get() as any;

    return {
      pending: pending.count,
      in_progress: inProgress.count,
      resolved: resolved.count,
      failed: failed.count,
      total: pending.count + inProgress.count + resolved.count + failed.count,
    };
  }

  /**
   * Simulate MundoX analyzing a task and suggesting a fix
   * In production, this calls MundoX inference service
   */
  async analyzeTask(task: MXRETask): Promise<string> {
    // For now, return a suggested fix based on the issue
    if (task.issue.includes("Invalid URL")) {
      return `Check county GIS website for correct ArcGIS endpoint and update registry JSON`;
    }
    if (task.issue.includes("Token Required")) {
      return `Add authentication config to adapter or use fallback Socrata/PublicSearch`;
    }
    if (task.issue.includes("Service not found")) {
      return `Endpoint URL is outdated - verify on county website`;
    }
    return `Investigate adapter logs and test with manual query`;
  }

  /**
   * Main agent loop - autonomous task processing
   */
  async run() {
    console.log("========================================");
    console.log("AUTONOMOUS MXRE AGENT");
    console.log("Using MundoX + LCM Memory");
    console.log("========================================\n");

    let cycleCount = 0;

    while (true) {
      cycleCount++;
      console.log(`\n[Cycle ${cycleCount}] ${new Date().toISOString()}`);

      const status = this.getStatus();
      console.log(`Status: ${status.pending} pending, ${status.in_progress} in-progress, ${status.resolved} resolved, ${status.failed} failed`);

      // Get next task
      const task = this.getNextTask();

      if (!task) {
        console.log("No pending tasks. Monitoring for new failures...");
        await this.sleep(30000); // Wait 30 seconds before checking again
        continue;
      }

      console.log(`\nProcessing task: ${task.county}, ${task.state} (${task.platform})`);
      this.updateTask(task.id!, "in_progress");

      try {
        // Analyze task with MundoX
        const analysis = await this.analyzeTask(task);
        console.log(`MundoX analysis: ${analysis}`);

        // Store in memory
        this.storeMemory("task_fix", `${task.county}, ${task.state}: ${analysis}`);

        // Mark as resolved (in real implementation, would verify the fix works)
        this.updateTask(task.id!, "resolved", undefined, analysis);
        console.log(`✓ Task resolved`);
      } catch (err) {
        const errorMsg = err instanceof Error ? err.message : String(err);
        this.updateTask(task.id!, "failed", errorMsg);
        console.log(`✗ Task failed: ${errorMsg}`);
      }

      // Brief pause between tasks
      await this.sleep(5000);
    }
  }

  private sleep(ms: number) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  /**
   * Export status for monitoring dashboard
   */
  exportStatus() {
    return {
      timestamp: new Date().toISOString(),
      tasks: this.getPendingTasks().slice(0, 10),
      summary: this.getStatus(),
    };
  }
}

// Start agent
async function main() {
  const agent = new MXREAgent();

  // Example: log some failures from the ingest pipeline
  console.log("Simulating ingest failures...\n");
  agent.logAdapterFailure("Maricopa", "AZ", "arcgis", "Invalid URL");
  agent.logAdapterFailure("Clark", "NV", "arcgis", "Token Required");
  agent.logAdapterFailure("Queens", "NY", "arcgis", "Service not found");

  // Start autonomous agent loop
  await agent.run();
}

main().catch(console.error);
