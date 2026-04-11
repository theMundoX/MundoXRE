/**
 * Local filesystem cache for raw HTTP responses.
 * Prevents re-fetching pages we already have.
 * Stored in .cache/ directory, organized by domain.
 */

import { createHash } from "node:crypto";
import { existsSync, mkdirSync, readFileSync, writeFileSync, statSync } from "node:fs";
import { join } from "node:path";

const CACHE_DIR = join(process.cwd(), ".cache");
const DEFAULT_TTL_MS = 7 * 24 * 60 * 60 * 1000; // 7 days

function ensureCacheDir(subdir: string) {
  const dir = join(CACHE_DIR, subdir);
  if (!existsSync(dir)) {
    mkdirSync(dir, { recursive: true });
  }
  return dir;
}

function cacheKey(url: string): string {
  return createHash("sha256").update(url).digest("hex").slice(0, 32);
}

function domainDir(url: string): string {
  try {
    return new URL(url).hostname.replace(/[^a-z0-9.-]/gi, "_");
  } catch {
    return "unknown";
  }
}

/**
 * Get a cached response for a URL. Returns null if not cached or expired.
 */
export function getCached(url: string, ttlMs = DEFAULT_TTL_MS): string | null {
  const dir = join(CACHE_DIR, domainDir(url));
  const file = join(dir, cacheKey(url));

  if (!existsSync(file)) return null;

  const stat = statSync(file);
  const age = Date.now() - stat.mtimeMs;
  if (age > ttlMs) return null;

  return readFileSync(file, "utf-8");
}

/**
 * Store a response in the cache.
 */
export function setCache(url: string, content: string) {
  const dir = ensureCacheDir(domainDir(url));
  const file = join(dir, cacheKey(url));
  writeFileSync(file, content, "utf-8");
}

/**
 * Check if a URL is cached (and not expired).
 */
export function isCached(url: string, ttlMs = DEFAULT_TTL_MS): boolean {
  return getCached(url, ttlMs) !== null;
}
