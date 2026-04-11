/**
 * Rent Tracker — adapter registry. Manages available listing adapters.
 */

import type { ListingAdapter, ListingSearchArea } from "./adapters/base.js";
import { ZillowListingAdapter } from "./adapters/zillow.js";
import { RedfinListingAdapter } from "./adapters/redfin.js";
import { RealtorListingAdapter } from "./adapters/realtor.js";

// ─── Adapter Registry ───────────────────────────────────────────────

const adapters: ListingAdapter[] = [
  new ZillowListingAdapter(),
  new RedfinListingAdapter(),
  new RealtorListingAdapter(),
];

export function getListingAdapters(): ListingAdapter[] {
  return adapters;
}

export function getAdaptersForArea(area: ListingSearchArea): ListingAdapter[] {
  return adapters.filter((a) => a.canHandle(area));
}

export function getAdapterBySource(source: string): ListingAdapter | undefined {
  return adapters.find((a) => a.source === source);
}
