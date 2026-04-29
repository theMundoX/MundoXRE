/**
 * Rent Tracker — Normalizer
 * Normalizes raw on-market listing data into database-ready ListingSignal records.
 * Mirrors the assessor normalizer pattern but for listing data.
 */

import type { OnMarketRecord } from "./adapters/base.js";
import type { ListingSignal } from "../db/queries.js";

// ─── Address Normalization ──────────────────────────────────────────

function normalizeAddress(address: string): string {
  return address
    .replace(/\s+/g, " ")
    .trim()
    .replace(/^(\d+)\s+/, "$1 ") // normalize spacing after house number
    .toUpperCase();
}

function extractZip(zip: string | undefined): string {
  if (!zip) return "";
  const match = zip.match(/\d{5}/);
  return match ? match[0] : zip.trim();
}

function normalizeCity(city: string): string {
  return city.replace(/\s+/g, " ").trim().toUpperCase();
}

function splitAgentName(name: string | undefined): { first?: string; last?: string } {
  const clean = name?.replace(/\s+/g, " ").trim();
  if (!clean) return {};
  const parts = clean.split(" ").filter(Boolean);
  if (parts.length === 1) return { first: parts[0] };
  return { first: parts[0], last: parts.slice(1).join(" ") };
}

// ─── Validation ─────────────────────────────────────────────────────

function isValidPrice(price: number | undefined): boolean {
  if (price === undefined || price === null) return false;
  return price > 0 && price < 500_000_000; // 500M cap — reject obvious errors
}

function isValidAddress(address: string): boolean {
  if (!address || address.length < 5) return false;
  // Must contain at least one digit (house number)
  return /\d/.test(address);
}

// ─── Deduplication Key ──────────────────────────────────────────────

/**
 * Generate a deduplication key for a listing record.
 * Used to match records across multiple scrape runs.
 */
export function listingDedupeKey(record: OnMarketRecord): string {
  const addr = normalizeAddress(record.address);
  const city = normalizeCity(record.city);
  const state = record.state.toUpperCase().trim().slice(0, 2);
  return `${addr}|${city}|${state}|${record.listing_source}`;
}

// ─── Main Normalizer ────────────────────────────────────────────────

export function normalizeListing(raw: OnMarketRecord): ListingSignal | null {
  if (!isValidAddress(raw.address)) return null;
  if (!raw.city || !raw.state) return null;

  const address = normalizeAddress(raw.address);
  const city = normalizeCity(raw.city);
  const stateCode = raw.state.toUpperCase().trim().slice(0, 2);
  const zip = extractZip(raw.zip);
  const agentNameParts = splitAgentName(raw.listing_agent_name);

  return {
    address,
    city,
    state_code: stateCode,
    zip: zip || undefined,
    is_on_market: raw.is_on_market,
    mls_list_price: isValidPrice(raw.mls_list_price) ? raw.mls_list_price : undefined,
    listing_agent_name: raw.listing_agent_name?.trim() || undefined,
    listing_agent_first_name: raw.listing_agent_first_name?.trim() || agentNameParts.first,
    listing_agent_last_name: raw.listing_agent_last_name?.trim() || agentNameParts.last,
    listing_agent_email: raw.listing_agent_email?.trim() || undefined,
    listing_agent_phone: raw.listing_agent_phone?.trim() || undefined,
    agent_contact_source: raw.listing_agent_email || raw.listing_agent_phone ? raw.listing_source : undefined,
    agent_contact_confidence: raw.listing_agent_email || raw.listing_agent_phone ? "source_listing" : undefined,
    listing_brokerage: raw.listing_brokerage?.trim() || undefined,
    listing_source: raw.listing_source,
    listing_url: raw.listing_url || undefined,
    days_on_market: raw.days_on_market,
    confidence: "single", // upgraded to "high" when cross-referenced
    first_seen_at: raw.observed_at,
    last_seen_at: raw.observed_at,
    raw: raw.raw,
  };
}

/**
 * Normalize a batch and filter out invalid records.
 */
export function normalizeListings(records: OnMarketRecord[]): ListingSignal[] {
  return records
    .map(normalizeListing)
    .filter((r): r is ListingSignal => r !== null);
}

/**
 * Cross-reference listings from multiple sources.
 * If the same address appears from 2+ sources with matching price (±5%),
 * mark confidence as "high".
 */
export function crossReferenceListings(signals: ListingSignal[]): ListingSignal[] {
  // Group by normalized address + city + state
  const byAddress = new Map<string, ListingSignal[]>();

  for (const signal of signals) {
    const key = `${signal.address}|${signal.city}|${signal.state_code}`;
    const group = byAddress.get(key) ?? [];
    group.push(signal);
    byAddress.set(key, group);
  }

  // For addresses with multiple sources, check price agreement
  for (const [, group] of byAddress) {
    if (group.length < 2) continue;

    const sources = new Set(group.map((s) => s.listing_source));
    if (sources.size < 2) continue; // same source duplicates don't count

    // Check if prices agree within 5%
    const prices = group
      .map((s) => s.mls_list_price)
      .filter((p): p is number => p !== undefined && p > 0);

    let priceAgreement = prices.length < 2;
    if (prices.length >= 2) {
      const minPrice = Math.min(...prices);
      const maxPrice = Math.max(...prices);
      priceAgreement = maxPrice <= minPrice * 1.05;
    }

    if (priceAgreement) {
      for (const signal of group) {
        signal.confidence = "high";
      }
    }
  }

  return signals;
}
