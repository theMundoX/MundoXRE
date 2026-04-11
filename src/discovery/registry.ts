/**
 * County Registry — loads county configs and resolves platform adapters.
 * Adding a new county = adding a JSON entry. Adding a new platform = adding an adapter class.
 */

import { readFileSync, readdirSync, existsSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { z } from "zod";
import type { CountyConfig } from "./adapters/base.js";
import type { AssessorAdapter } from "./adapters/base.js";

// ─── Adapter Imports ─────────────────────────────────────────────────
import { SocrataAdapter } from "./adapters/socrata.js";
import { CookCountyAdapter } from "./adapters/cook-county.js";
import { ActDataScoutAdapter } from "./adapters/actdatascout.js";
import { ArcGISAdapter } from "./adapters/arcgis.js";
import { DCADAdapter } from "./adapters/dcad.js";
import { HCADAdapter } from "./adapters/hcad.js";
import { DentonAdapter } from "./adapters/denton.js";
import { TADAdapter } from "./adapters/tad.js";
import { OKTaxRollsAdapter } from "./adapters/oktaxrolls.js";
import { FloridaNALAdapter } from "./adapters/florida-nal.js";
import { CAStatewideAdapter } from "./adapters/ca-statewide.js";
import { MNStatewideAdapter } from "./adapters/mn-statewide.js";
import { NYStatewideAdapter } from "./adapters/ny-statewide.js";
import { PAStatewideAdapter } from "./adapters/pa-statewide.js";
import { NCStatewideAdapter } from "./adapters/nc-statewide.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const DATA_DIR = join(__dirname, "../../data/counties");

// Runtime schema validation for county configs
const CountyConfigSchema = z.object({
  state_fips: z.string().regex(/^\d{2}$/),
  county_fips: z.string().regex(/^\d{3}$/),
  name: z.string().min(1).max(100),
  state: z.string().length(2),
  platform: z.string().min(1).max(50),
  base_url: z.string().url(),
  alt_platform: z.string().max(50).optional(),
  alt_url: z.string().url().optional(),
  search_params: z.record(z.string()).optional(),
  field_map: z.record(z.string()).optional(),
});

// ─── Adapter Registry ───────────────────────────────────────────────

const adapters = new Map<string, AssessorAdapter>();

export function registerAdapter(adapter: AssessorAdapter) {
  adapters.set(adapter.platform, adapter);
}

// Auto-register all known adapters at module load
registerAdapter(new SocrataAdapter());
registerAdapter(new CookCountyAdapter());
registerAdapter(new ActDataScoutAdapter());
registerAdapter(new ArcGISAdapter());
registerAdapter(new DCADAdapter());
registerAdapter(new HCADAdapter());
registerAdapter(new DentonAdapter());
registerAdapter(new TADAdapter());
registerAdapter(new OKTaxRollsAdapter());
registerAdapter(new FloridaNALAdapter());
registerAdapter(new CAStatewideAdapter());
registerAdapter(new MNStatewideAdapter());
registerAdapter(new NYStatewideAdapter());
registerAdapter(new PAStatewideAdapter());
registerAdapter(new NCStatewideAdapter());

export function getAdapter(platform: string): AssessorAdapter | null {
  return adapters.get(platform) ?? null;
}

export function getAdapterForCounty(config: CountyConfig): AssessorAdapter | null {
  // Try primary platform first
  let adapter = adapters.get(config.platform);
  if (adapter?.canHandle(config)) return adapter;

  // Try alternate platform
  if (config.alt_platform) {
    adapter = adapters.get(config.alt_platform);
    if (adapter?.canHandle(config)) return adapter;
  }

  return null;
}

export function listAdapters(): string[] {
  return Array.from(adapters.keys());
}

// ─── County Config Loading ──────────────────────────────────────────

let countyCache: CountyConfig[] | null = null;

function loadAllConfigs(): CountyConfig[] {
  if (countyCache) return countyCache;

  const configs: CountyConfig[] = [];

  try {
    if (!existsSync(DATA_DIR)) return configs;
    const files = readdirSync(DATA_DIR).filter((f) => f.endsWith(".json"));
    for (const file of files) {
      const raw = readFileSync(join(DATA_DIR, file), "utf-8");
      const parsed = JSON.parse(raw) as unknown[];
      if (!Array.isArray(parsed)) continue;
      for (const entry of parsed) {
        const result = CountyConfigSchema.safeParse(entry);
        if (result.success) {
          configs.push(result.data as CountyConfig);
        }
      }
    }
  } catch {
    // Data dir may not exist or have invalid files
  }

  countyCache = configs;
  return configs;
}

export function getCountyConfigs(filters?: {
  state?: string;
  county?: string;
  platform?: string;
}): CountyConfig[] {
  let configs = loadAllConfigs();

  if (filters?.state) {
    const s = filters.state.toUpperCase();
    configs = configs.filter((c) => c.state.toUpperCase() === s);
  }
  if (filters?.county) {
    const n = filters.county.toLowerCase();
    configs = configs.filter((c) => c.name.toLowerCase().includes(n));
  }
  if (filters?.platform) {
    const p = filters.platform.toLowerCase();
    configs = configs.filter(
      (c) =>
        c.platform.toLowerCase() === p ||
        c.alt_platform?.toLowerCase() === p,
    );
  }

  return configs;
}

export function getCountyConfig(state: string, county: string): CountyConfig | null {
  const configs = getCountyConfigs({ state, county });
  return configs.length > 0 ? configs[0] : null;
}

/**
 * Clear the config cache (useful after adding new county files).
 */
export function clearConfigCache() {
  countyCache = null;
}
