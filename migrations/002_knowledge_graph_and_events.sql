-- MXRE-side moat layer:
--   • Knowledge graph (entity_relationships) — who owns what LLCs, who's on title together, lender clusters
--   • LISTEN/NOTIFY triggers → event-driven agent dispatch (no cron polling)
--   • Agent citation index (fast lookup of supporting evidence)

-- ── Knowledge graph ────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS entities (
  id text PRIMARY KEY,                   -- 'llc:sunrise-holdings-llc-az' | 'person:john-doe-az' | 'lender:wells-fargo'
  entity_type text NOT NULL,             -- 'llc' | 'person' | 'lender' | 'servicer' | 'trust'
  name text NOT NULL,
  aliases text[],
  metadata jsonb DEFAULT '{}',
  first_seen timestamptz DEFAULT now(),
  last_seen timestamptz DEFAULT now(),
  occurrences int DEFAULT 1
);
CREATE INDEX IF NOT EXISTS idx_entities_type ON entities (entity_type);
CREATE INDEX IF NOT EXISTS idx_entities_name_trgm ON entities USING gin (name gin_trgm_ops);

CREATE TABLE IF NOT EXISTS entity_relationships (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  from_entity text REFERENCES entities(id) ON DELETE CASCADE,
  to_entity text REFERENCES entities(id) ON DELETE CASCADE,
  relationship_type text NOT NULL,       -- 'owns' | 'borrower_at' | 'co_owner_with' | 'same_address' | 'registered_agent'
  property_id text,                       -- if relationship is anchored to a specific parcel
  county_fips text,
  first_observed_at timestamptz,
  last_observed_at timestamptz,
  strength numeric(3,2) DEFAULT 1.0,      -- how confident we are
  evidence jsonb,                         -- {source_table, source_ids[]}
  created_at timestamptz DEFAULT now(),
  UNIQUE (from_entity, to_entity, relationship_type)
);
CREATE INDEX IF NOT EXISTS idx_rel_from ON entity_relationships (from_entity);
CREATE INDEX IF NOT EXISTS idx_rel_to ON entity_relationships (to_entity);
CREATE INDEX IF NOT EXISTS idx_rel_type ON entity_relationships (relationship_type);
CREATE INDEX IF NOT EXISTS idx_rel_property ON entity_relationships (property_id) WHERE property_id IS NOT NULL;

-- Lender distress clusters — materialized view refreshed nightly
CREATE TABLE IF NOT EXISTS lender_distress_signals (
  lender_entity text REFERENCES entities(id),
  county_fips text,
  window_start date,
  window_end date,
  foreclosure_count int DEFAULT 0,
  default_count int DEFAULT 0,
  assignment_count int DEFAULT 0,
  total_originations int,
  distress_rate numeric(5,4),
  z_score numeric(5,2),                   -- vs historical baseline — >2.0 flags a cluster
  computed_at timestamptz DEFAULT now(),
  PRIMARY KEY (lender_entity, county_fips, window_start)
);

-- ── LISTEN/NOTIFY triggers for event-driven dispatch ───────────────────────
-- Every new recorder filing fires a NOTIFY 'mxre_events' that the event-bus
-- edge function picks up and routes to subscribed agents.
CREATE OR REPLACE FUNCTION emit_mxre_event() RETURNS trigger AS $$
DECLARE
  event_type text;
  payload jsonb;
BEGIN
  IF TG_TABLE_NAME = 'mortgage_records' THEN
    event_type := CASE
      WHEN NEW.document_type IN ('mortgage','deed_of_trust') THEN 'recorder.new_mortgage'
      WHEN NEW.document_type IN ('lien','tax_lien') THEN 'recorder.new_lien'
      WHEN NEW.document_type IN ('satisfaction','release','discharge') THEN 'recorder.release'
      ELSE 'recorder.new_filing'
    END;
    payload := jsonb_build_object(
      'id', NEW.id,
      'document_type', NEW.document_type,
      'document_number', NEW.document_number,
      'county_fips', NEW.county_fips,
      'recording_date', NEW.recording_date,
      'original_amount', NEW.original_amount,
      'lender_name', NEW.lender_name,
      'borrower_name', NEW.borrower_name
    );
  ELSIF TG_TABLE_NAME = 'listing_signals' THEN
    event_type := CASE
      WHEN NEW.signal_type = 'price_drop' THEN 'listing.price_drop'
      WHEN NEW.signal_type = 'new_listing' THEN 'listing.new'
      WHEN NEW.signal_type = 'dom_alert' THEN 'listing.dom_90_plus'
      ELSE 'listing.signal'
    END;
    payload := to_jsonb(NEW);
  ELSIF TG_TABLE_NAME = 'rent_snapshots' THEN
    event_type := 'rent.new_snapshot';
    payload := jsonb_build_object(
      'property_id', NEW.property_id,
      'rent_estimate', NEW.rent_estimate,
      'snapshot_date', NEW.snapshot_date
    );
  ELSE
    RETURN NEW;
  END IF;

  PERFORM pg_notify('mxre_events', jsonb_build_object(
    'event_type', event_type,
    'source', 'mxre:' || TG_TABLE_NAME,
    'source_id', NEW.id::text,
    'payload', payload,
    'emitted_at', now()
  )::text);

  RETURN NEW;
END; $$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS tr_mortgage_records_event ON mortgage_records;
CREATE TRIGGER tr_mortgage_records_event
  AFTER INSERT ON mortgage_records
  FOR EACH ROW EXECUTE FUNCTION emit_mxre_event();

DROP TRIGGER IF EXISTS tr_listing_signals_event ON listing_signals;
CREATE TRIGGER tr_listing_signals_event
  AFTER INSERT ON listing_signals
  FOR EACH ROW EXECUTE FUNCTION emit_mxre_event();

DROP TRIGGER IF EXISTS tr_rent_snapshots_event ON rent_snapshots;
CREATE TRIGGER tr_rent_snapshots_event
  AFTER INSERT ON rent_snapshots
  FOR EACH ROW EXECUTE FUNCTION emit_mxre_event();

-- ── Enable pg_trgm for fuzzy entity matching ───────────────────────────────
CREATE EXTENSION IF NOT EXISTS pg_trgm;
