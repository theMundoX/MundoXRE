import { getDb } from "./client.js";

// ─── Types ───────────────────────────────────────────────────────────

export interface County {
  id?: number;
  state_fips: string;
  county_fips: string;
  state_code: string;
  county_name: string;
  assessor_url?: string;
  recorder_url?: string;
  active?: boolean;
}

export interface Property {
  id?: number;
  county_id: number;
  parcel_id?: string;
  address_line1: string;
  address_line2?: string;
  city: string;
  state_code: string;
  zip: string;
  lat?: number;
  lng?: number;
  property_type?: string;
  unit_count?: number;
  year_built?: number;
  total_sqft?: number;
  owner_name?: string;
  assessed_value?: number;
  last_sale_price?: number;
  last_sale_date?: string;
  assessor_url?: string;
  source?: string;
}

export interface RentObservation {
  id?: number;
  property_id: number;
  website_id?: number;
  observed_at: string;
  unit_type: string;
  unit_name?: string;
  sqft?: number;
  rent_min?: number;
  rent_max?: number;
  rent_avg?: number;
  available_units?: number;
  deposit?: number;
  specials?: string;
  raw_json?: Record<string, unknown>;
}

export interface MortgageRecord {
  id?: number;
  property_id: number;
  document_type: string;
  recording_date?: string;
  loan_amount?: number;
  lender_name?: string;
  borrower_name?: string;
  document_number?: string;
  book_page?: string;
  source_url?: string;
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
  limit?: number;
  offset?: number;
}

// ─── Counties ────────────────────────────────────────────────────────

export async function insertCounty(county: County) {
  const db = getDb();
  const { data, error } = await db
    .from("counties")
    .upsert(county, { onConflict: "state_fips,county_fips" })
    .select()
    .single();
  if (error) throw new Error(`Failed to insert county: ${error.message}`);
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
  if (error) throw new Error(`Failed to get counties: ${error.message}`);
  return data ?? [];
}

// ─── Properties ──────────────────────────────────────────────────────

export async function upsertProperty(property: Property) {
  const db = getDb();
  const { data, error } = await db
    .from("properties")
    .upsert(
      { ...property, updated_at: new Date().toISOString() },
      { onConflict: "county_id,parcel_id" },
    )
    .select()
    .single();
  if (error) throw new Error(`Failed to upsert property: ${error.message}`);
  return data;
}

export async function upsertProperties(properties: Property[]) {
  const db = getDb();
  const rows = properties.map((p) => ({
    ...p,
    updated_at: new Date().toISOString(),
  }));
  const { data, error } = await db
    .from("properties")
    .upsert(rows, { onConflict: "county_id,parcel_id" })
    .select();
  if (error) throw new Error(`Failed to bulk upsert properties: ${error.message}`);
  return data ?? [];
}

export async function getProperties(filters: PropertyFilters) {
  const db = getDb();
  let query = db.from("properties").select("*, counties(county_name, state_code)");

  if (filters.county_id) query = query.eq("county_id", filters.county_id);
  if (filters.city) query = query.ilike("city", `%${filters.city}%`);
  if (filters.zip) query = query.eq("zip", filters.zip);
  if (filters.state_code) query = query.eq("state_code", filters.state_code);
  if (filters.property_type) query = query.eq("property_type", filters.property_type);
  if (filters.min_units) query = query.gte("unit_count", filters.min_units);
  if (filters.owner) query = query.ilike("owner_name", `%${filters.owner}%`);

  const limit = filters.limit ?? 20;
  const offset = filters.offset ?? 0;

  const { data, error } = await query
    .order("unit_count", { ascending: false, nullsFirst: false })
    .range(offset, offset + limit - 1);

  if (error) throw new Error(`Failed to get properties: ${error.message}`);
  return data ?? [];
}

export async function getPropertyById(id: number) {
  const db = getDb();
  const { data, error } = await db
    .from("properties")
    .select("*, counties(county_name, state_code)")
    .eq("id", id)
    .single();
  if (error) throw new Error(`Failed to get property: ${error.message}`);
  return data;
}

export async function getPropertyCount(filters?: { county_id?: number }) {
  const db = getDb();
  let query = db.from("properties").select("id", { count: "exact", head: true });
  if (filters?.county_id) query = query.eq("county_id", filters.county_id);
  const { count, error } = await query;
  if (error) throw new Error(`Failed to count properties: ${error.message}`);
  return count ?? 0;
}

// ─── Rent Observations ──────────────────────────────────────────────

export async function insertRentObservation(obs: RentObservation) {
  const db = getDb();
  const { data, error } = await db
    .from("rent_observations")
    .insert(obs)
    .select()
    .single();
  if (error) throw new Error(`Failed to insert rent observation: ${error.message}`);
  return data;
}

export async function getLatestRents(propertyId: number) {
  const db = getDb();
  const { data, error } = await db
    .from("rent_observations")
    .select("*")
    .eq("property_id", propertyId)
    .order("observed_at", { ascending: false })
    .limit(20);
  if (error) throw new Error(`Failed to get rents: ${error.message}`);
  return data ?? [];
}

// ─── Mortgage Records ───────────────────────────────────────────────

export async function insertMortgageRecord(record: MortgageRecord) {
  const db = getDb();
  const { data, error } = await db
    .from("mortgage_records")
    .insert(record)
    .select()
    .single();
  if (error) throw new Error(`Failed to insert mortgage record: ${error.message}`);
  return data;
}

export async function getMortgageRecords(propertyId: number) {
  const db = getDb();
  const { data, error } = await db
    .from("mortgage_records")
    .select("*")
    .eq("property_id", propertyId)
    .order("recording_date", { ascending: false });
  if (error) throw new Error(`Failed to get mortgage records: ${error.message}`);
  return data ?? [];
}
