import { getDb, getWriteDb } from "./client.js";

// ─── Types ───────────────────────────────────────────────────────────

export interface County {
  id?: number;
  state_fips: string;
  county_fips: string;
  state_code: string;
  county_name: string;
  msa?: string;
  assessor_url?: string;
  recorder_url?: string;
  active?: boolean;
}

export interface Property {
  id?: number;
  county_id: number;
  parcel_id?: string;
  address: string;
  address2?: string;
  city: string;
  state_code: string;
  zip: string;
  lat?: number;
  lng?: number;
  msa?: string;
  property_type?: string;
  total_units?: number;
  stories?: number;
  year_built?: number;
  total_sqft?: number;
  is_apartment?: boolean;
  is_sfr?: boolean;
  is_condo?: boolean;
  is_btr?: boolean;
  is_senior?: boolean;
  is_student?: boolean;
  is_affordable?: boolean;
  owner_name?: string;
  mgmt_company?: string;
  website?: string;
  assessed_value?: number;
  market_value?: number;
  taxable_value?: number;
  land_value?: number;
  property_tax?: number;
  last_sale_price?: number;
  last_sale_date?: string;
  construction_class?: string;
  improvement_quality?: string;
  total_buildings?: number;
  land_sqft?: number;
  lot_acres?: number;
  assessor_url?: string;
  source?: string;
  // Owner mailing address — drives absentee owner detection.
  mailing_address?: string;
  mailing_city?: string;
  mailing_state?: string;
  mailing_zip?: string;
  absentee_owner?: boolean;
  corporate_owned?: boolean;
  // Asset class signals (Indiana DLGF + similar) — used for filtering and Buy Box scoring.
  property_class?: string;       // RESIDENTIAL / COMMERCIAL / etc
  property_use?: string;         // sub-class description, e.g. "RES ONE FAMILY UNPLAT"
  legal_description?: string;
  subdivision?: string;
  neighborhood_code?: string;
  // Asssesor breakdown
  appraised_land?: number;
  appraised_building?: number;
  // Unit details (residential)
  bedrooms?: number;
  bathrooms?: number;
  // Tax / lien
  annual_tax?: number;
  lien_status?: string;
}

export interface Floorplan {
  id?: number;
  property_id: number;
  name?: string;
  beds: number;
  baths: number;
  half_baths?: number;
  sqft?: number;
  estimated_count?: number;
}

export interface RentSnapshot {
  id?: number;
  property_id: number;
  floorplan_id?: number;
  website_id?: number;
  observed_at: string;
  beds?: number;
  baths?: number;
  sqft?: number;
  asking_rent?: number;
  effective_rent?: number;
  concession_value?: number;
  asking_psf?: number;
  effective_psf?: number;
  deposit?: number;
  concession_text?: string;
  available_count?: number;
  days_on_market?: number;
  leased_pct?: number;
  exposure_pct?: number;
  renewal_pct?: number;
  raw?: Record<string, unknown>;
}

export interface LeaseEvent {
  id?: number;
  property_id: number;
  floorplan_id?: number;
  event_type: string;
  event_date: string;
  lease_start?: string;
  lease_end?: string;
  term_months?: number;
  signed_rent?: number;
  signed_psf?: number;
  signed_concession?: number;
  signed_effective?: number;
  beds?: number;
  baths?: number;
  sqft?: number;
}

export interface FeeSchedule {
  id?: number;
  property_id: number;
  observed_at: string;
  app_fee?: number;
  admin_fee?: number;
  amenity_fee?: number;
  storage_fee?: number;
  pet_deposit?: number;
  pet_monthly?: number;
  pet_onetime?: number;
  parking_covered?: number;
  parking_garage?: number;
  parking_surface?: number;
  raw?: Record<string, unknown>;
}

export interface Amenity {
  id?: number;
  property_id: number;
  scope: "building" | "unit";
  amenity: string;
  present: boolean;
  observed_at: string;
}

export interface Reputation {
  id?: number;
  property_id: number;
  observed_at: string;
  platform?: string;
  avg_rating?: number;
  review_count?: number;
  pos_amenities?: number;
  pos_cleanliness?: number;
  pos_location?: number;
  pos_staff?: number;
  pos_value?: number;
  neg_amenities?: number;
  neg_cleanliness?: number;
  neg_location?: number;
  neg_staff?: number;
  neg_value?: number;
}

export interface MortgageRecord {
  id?: number;
  property_id: number;
  document_type: string;
  recording_date?: string;
  loan_amount?: number;
  original_amount?: number;
  interest_rate?: number;
  term_months?: number;
  estimated_monthly_payment?: number;
  estimated_current_balance?: number;
  balance_as_of?: string;
  maturity_date?: string;
  loan_type?: string;
  deed_type?: string;
  lender_name?: string;
  borrower_name?: string;
  document_number?: string;
  book_page?: string;
  source_url?: string;
}

// ─── Listing Signals ─────────────────────────────────────────────────

export interface ListingSignal {
  id?: number;
  property_id?: number;
  address: string;
  city: string;
  state_code: string;
  zip?: string;
  is_on_market: boolean;
  mls_list_price?: number;
  listing_agent_name?: string;
  listing_brokerage?: string;
  listing_source: string; // "zillow" | "redfin" | "realtor"
  listing_url?: string;
  days_on_market?: number;
  confidence: string; // "high" | "single"
  first_seen_at: string;
  last_seen_at: string;
  delisted_at?: string;
  raw?: Record<string, unknown>;
}

export interface AgentLicense {
  id?: number;
  agent_name: string;
  license_number: string;
  license_state: string;
  license_status: string; // "active" | "inactive" | "expired"
  brokerage_name?: string;
  phone?: string;
  email?: string;
  license_type?: string;
  source_url: string;
  observed_at: string;
}

export interface ListingFilters {
  state_code?: string;
  city?: string;
  zip?: string;
  is_on_market?: boolean;
  listing_source?: string;
  min_price?: number;
  max_price?: number;
  limit?: number;
  offset?: number;
}

export interface PropertyFilters {
  county_id?: number;
  county_name?: string;
  city?: string;
  zip?: string;
  state_code?: string;
  property_type?: string;
  min_units?: number;
  owner?: string;
  mgmt_company?: string;
  is_apartment?: boolean;
  is_sfr?: boolean;
  limit?: number;
  offset?: number;
}

// ─── Helpers ─────────────────────────────────────────────────────────

const MAX_QUERY_LIMIT = 100;

function escapeLike(input: string): string {
  return input.replace(/[%_\\]/g, "\\$&");
}

function safeLimit(limit: number | undefined): number {
  return Math.min(Math.max(limit ?? 20, 1), MAX_QUERY_LIMIT);
}

// ─── Counties ────────────────────────────────────────────────────────

export async function insertCounty(county: County) {
  const db = getWriteDb();
  const { data, error } = await db
    .from("counties")
    .upsert(county, { onConflict: "state_fips,county_fips" })
    .select()
    .single();
  if (error) throw new Error("Failed to insert county record.");
  return data;
}

export async function getCounties() {
  const db = getDb();
  const { data, error } = await db
    .from("counties")
    .select("*")
    .eq("active", true)
    .order("state_code")
    .order("county_name");
  if (error) throw new Error("Failed to retrieve counties.");
  return data ?? [];
}

// ─── Properties ──────────────────────────────────────────────────────

export async function upsertProperty(property: Property) {
  const db = getWriteDb();
  const { data, error } = await db
    .from("properties")
    .upsert(
      { ...property, updated_at: new Date().toISOString() },
      { onConflict: "county_id,parcel_id" },
    )
    .select()
    .single();
  if (error) throw new Error("Failed to upsert property record.");
  return data;
}

export async function upsertProperties(properties: Property[]) {
  const db = getWriteDb();
  const rows = properties.map((p) => ({
    ...p,
    updated_at: new Date().toISOString(),
  }));
  const { data, error } = await db
    .from("properties")
    .upsert(rows, { onConflict: "county_id,parcel_id" })
    .select();
  if (error) throw new Error("Failed to bulk upsert property records.");
  return data ?? [];
}

export async function getProperties(filters: PropertyFilters) {
  const db = getDb();
  let query = db.from("properties").select("*, counties(county_name, state_code)");

  if (filters.county_id) query = query.eq("county_id", filters.county_id);
  if (filters.city) query = query.ilike("city", `%${escapeLike(filters.city)}%`);
  if (filters.zip) query = query.eq("zip", filters.zip);
  if (filters.state_code) query = query.eq("state_code", filters.state_code);
  if (filters.property_type) query = query.eq("property_type", filters.property_type);
  if (filters.min_units) query = query.gte("total_units", filters.min_units);
  if (filters.owner) query = query.ilike("owner_name", `%${escapeLike(filters.owner)}%`);
  if (filters.mgmt_company) query = query.ilike("mgmt_company", `%${escapeLike(filters.mgmt_company)}%`);
  if (filters.is_apartment !== undefined) query = query.eq("is_apartment", filters.is_apartment);
  if (filters.is_sfr !== undefined) query = query.eq("is_sfr", filters.is_sfr);

  const limit = safeLimit(filters.limit);
  const offset = filters.offset ?? 0;

  const { data, error } = await query
    .order("total_units", { ascending: false, nullsFirst: false })
    .range(offset, offset + limit - 1);

  if (error) throw new Error("Failed to query properties.");
  return data ?? [];
}

export async function getPropertyById(id: number) {
  const db = getDb();
  const { data, error } = await db
    .from("properties")
    .select("*, counties(county_name, state_code)")
    .eq("id", id)
    .single();
  if (error) throw new Error("Property not found.");
  return data;
}

export async function getPropertyCount(filters?: { county_id?: number }) {
  const db = getDb();
  let query = db.from("properties").select("id", { count: "exact", head: true });
  if (filters?.county_id) query = query.eq("county_id", filters.county_id);
  const { count, error } = await query;
  if (error) throw new Error("Failed to count properties.");
  return count ?? 0;
}

// ─── Floorplans ─────────────────────────────────────────────────────

export async function upsertFloorplan(fp: Floorplan) {
  const db = getWriteDb();
  const { data, error } = await db
    .from("floorplans")
    .upsert(
      { ...fp, updated_at: new Date().toISOString() },
      { onConflict: "property_id,name" },
    )
    .select()
    .single();
  if (error) throw new Error("Failed to upsert floorplan.");
  return data;
}

export async function getFloorplans(propertyId: number) {
  const db = getDb();
  const { data, error } = await db
    .from("floorplans")
    .select("*")
    .eq("property_id", propertyId)
    .order("beds")
    .order("sqft");
  if (error) throw new Error("Failed to retrieve floorplans.");
  return data ?? [];
}

// ─── Rent Snapshots ─────────────────────────────────────────────────

export async function insertRentSnapshot(snap: RentSnapshot) {
  const db = getWriteDb();
  const { data, error } = await db
    .from("rent_snapshots")
    .insert(snap)
    .select()
    .single();
  if (error) throw new Error("Failed to insert rent snapshot.");
  return data;
}

export async function insertRentSnapshots(snaps: RentSnapshot[]) {
  const db = getWriteDb();
  const { data, error } = await db
    .from("rent_snapshots")
    .insert(snaps)
    .select();
  if (error) throw new Error("Failed to bulk insert rent snapshots.");
  return data ?? [];
}

export async function getLatestRents(propertyId: number) {
  const db = getDb();
  const { data, error } = await db
    .from("rent_snapshots")
    .select("*")
    .eq("property_id", propertyId)
    .order("observed_at", { ascending: false })
    .limit(50);
  if (error) throw new Error("Failed to retrieve rent data.");
  return data ?? [];
}

export async function getRentHistory(propertyId: number, months = 12) {
  const db = getDb();
  const since = new Date();
  since.setMonth(since.getMonth() - months);

  const { data, error } = await db
    .from("rent_snapshots")
    .select("observed_at, beds, baths, sqft, asking_rent, effective_rent, asking_psf, available_count, leased_pct")
    .eq("property_id", propertyId)
    .gte("observed_at", since.toISOString().split("T")[0])
    .order("observed_at");
  if (error) throw new Error("Failed to retrieve rent history.");
  return data ?? [];
}

// ─── Lease Events ───────────────────────────────────────────────────

export async function insertLeaseEvent(event: LeaseEvent) {
  const db = getWriteDb();
  const { data, error } = await db
    .from("lease_events")
    .insert(event)
    .select()
    .single();
  if (error) throw new Error("Failed to insert lease event.");
  return data;
}

export async function getLeaseEvents(propertyId: number) {
  const db = getDb();
  const { data, error } = await db
    .from("lease_events")
    .select("*")
    .eq("property_id", propertyId)
    .order("event_date", { ascending: false })
    .limit(50);
  if (error) throw new Error("Failed to retrieve lease events.");
  return data ?? [];
}

// ─── Fee Schedules ──────────────────────────────────────────────────

export async function insertFeeSchedule(fees: FeeSchedule) {
  const db = getWriteDb();
  const { data, error } = await db
    .from("fee_schedules")
    .insert(fees)
    .select()
    .single();
  if (error) throw new Error("Failed to insert fee schedule.");
  return data;
}

export async function getLatestFees(propertyId: number) {
  const db = getDb();
  const { data, error } = await db
    .from("fee_schedules")
    .select("*")
    .eq("property_id", propertyId)
    .order("observed_at", { ascending: false })
    .limit(1)
    .single();
  if (error && error.code !== "PGRST116") throw new Error("Failed to retrieve fees.");
  return data;
}

// ─── Amenities ──────────────────────────────────────────────────────

export async function upsertAmenities(amenities: Amenity[]) {
  const db = getWriteDb();
  const { data, error } = await db
    .from("amenities")
    .upsert(amenities, { onConflict: "property_id,scope,amenity" })
    .select();
  if (error) throw new Error("Failed to upsert amenities.");
  return data ?? [];
}

export async function getAmenities(propertyId: number) {
  const db = getDb();
  const { data, error } = await db
    .from("amenities")
    .select("*")
    .eq("property_id", propertyId)
    .eq("present", true)
    .order("scope")
    .order("amenity");
  if (error) throw new Error("Failed to retrieve amenities.");
  return data ?? [];
}

// ─── Reputation ─────────────────────────────────────────────────────

export async function insertReputation(rep: Reputation) {
  const db = getWriteDb();
  const { data, error } = await db
    .from("reputation")
    .insert(rep)
    .select()
    .single();
  if (error) throw new Error("Failed to insert reputation data.");
  return data;
}

export async function getLatestReputation(propertyId: number) {
  const db = getDb();
  const { data, error } = await db
    .from("reputation")
    .select("*")
    .eq("property_id", propertyId)
    .order("observed_at", { ascending: false })
    .limit(5);
  if (error) throw new Error("Failed to retrieve reputation data.");
  return data ?? [];
}

// ─── Mortgage Records ───────────────────────────────────────────────

export async function insertMortgageRecord(record: MortgageRecord) {
  const db = getWriteDb();
  const { data, error } = await db
    .from("mortgage_records")
    .insert(record)
    .select()
    .single();
  if (error) throw new Error("Failed to insert mortgage record.");
  return data;
}

export async function getMortgageRecords(propertyId: number) {
  const db = getDb();
  const { data, error } = await db
    .from("mortgage_records")
    .select("*")
    .eq("property_id", propertyId)
    .order("recording_date", { ascending: false });
  if (error) throw new Error("Failed to retrieve mortgage records.");
  return data ?? [];
}

// ─── Listing Signals ────────────────────────────────────────────────

export async function upsertListingSignal(signal: ListingSignal) {
  const db = getWriteDb();
  const { data, error } = await db
    .from("listing_signals")
    .upsert(
      { ...signal, updated_at: new Date().toISOString() },
      { onConflict: "address,city,state_code,listing_source" },
    )
    .select()
    .single();
  if (error) throw new Error(`Failed to upsert listing signal: ${error.message}`);
  return data;
}

export async function upsertListingSignals(signals: ListingSignal[]) {
  const db = getWriteDb();
  const rows = signals.map((s) => ({
    ...s,
    updated_at: new Date().toISOString(),
  }));
  const { data, error } = await db
    .from("listing_signals")
    .upsert(rows, { onConflict: "address,city,state_code,listing_source" })
    .select();
  if (error) throw new Error(`Failed to bulk upsert listing signals: ${error.message}`);
  return data ?? [];
}

export async function getListingSignals(propertyId: number) {
  const db = getDb();
  const { data, error } = await db
    .from("listing_signals")
    .select("*")
    .eq("property_id", propertyId)
    .order("last_seen_at", { ascending: false });
  if (error) throw new Error("Failed to retrieve listing signals.");
  return data ?? [];
}

export async function getOnMarketProperties(filters: ListingFilters) {
  const db = getDb();
  let query = db.from("listing_signals").select("*");

  if (filters.state_code) query = query.eq("state_code", filters.state_code);
  if (filters.city) query = query.ilike("city", `%${escapeLike(filters.city)}%`);
  if (filters.zip) query = query.eq("zip", filters.zip);
  if (filters.is_on_market !== undefined) query = query.eq("is_on_market", filters.is_on_market);
  if (filters.listing_source) query = query.eq("listing_source", filters.listing_source);
  if (filters.min_price) query = query.gte("mls_list_price", filters.min_price);
  if (filters.max_price) query = query.lte("mls_list_price", filters.max_price);

  const limit = safeLimit(filters.limit);
  const offset = filters.offset ?? 0;

  const { data, error } = await query
    .order("last_seen_at", { ascending: false })
    .range(offset, offset + limit - 1);

  if (error) throw new Error("Failed to query on-market properties.");
  return data ?? [];
}

export async function markDelisted(signalIds: number[]) {
  if (signalIds.length === 0) return;
  const db = getWriteDb();
  const { error } = await db
    .from("listing_signals")
    .update({
      is_on_market: false,
      delisted_at: new Date().toISOString(),
      updated_at: new Date().toISOString(),
    })
    .in("id", signalIds);
  if (error) throw new Error("Failed to mark listings as delisted.");
}

export async function getActiveListingsByArea(stateCode: string, city: string) {
  const db = getDb();
  const { data, error } = await db
    .from("listing_signals")
    .select("id, address, city, state_code, listing_source")
    .eq("state_code", stateCode)
    .ilike("city", `%${escapeLike(city)}%`)
    .eq("is_on_market", true)
    .is("delisted_at", null);
  if (error) throw new Error("Failed to retrieve active listings.");
  return data ?? [];
}

// ─── Agent Licenses ─────────────────────────────────────────────────

export async function upsertAgentLicense(license: AgentLicense) {
  const db = getWriteDb();
  const { data, error } = await db
    .from("agent_licenses")
    .upsert(
      { ...license, updated_at: new Date().toISOString() },
      { onConflict: "license_number,license_state" },
    )
    .select()
    .single();
  if (error) throw new Error(`Failed to upsert agent license: ${error.message}`);
  return data;
}

export async function getAgentByName(name: string, state: string) {
  const db = getDb();
  const { data, error } = await db
    .from("agent_licenses")
    .select("*")
    .ilike("agent_name", `%${escapeLike(name)}%`)
    .eq("license_state", state.toUpperCase())
    .eq("license_status", "active")
    .limit(5);
  if (error) throw new Error("Failed to look up agent license.");
  return data ?? [];
}
