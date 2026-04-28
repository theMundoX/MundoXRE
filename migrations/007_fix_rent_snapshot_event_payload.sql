-- Keep rent snapshot event payload aligned with the current rent_snapshots schema.
-- Older event trigger code referenced rent_estimate/snapshot_date, which are not
-- columns on the current table and caused inserts to fail.

CREATE OR REPLACE FUNCTION public.emit_mxre_event()
RETURNS trigger
LANGUAGE plpgsql
AS $$
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
      WHEN NEW.is_on_market = false THEN 'listing.delisted'
      WHEN COALESCE(NEW.days_on_market, 0) >= 90 THEN 'listing.dom_90_plus'
      ELSE 'listing.new'
    END;
    payload := to_jsonb(NEW);
  ELSIF TG_TABLE_NAME = 'rent_snapshots' THEN
    event_type := 'rent.new_snapshot';
    payload := jsonb_build_object(
      'id', NEW.id,
      'property_id', NEW.property_id,
      'floorplan_id', NEW.floorplan_id,
      'website_id', NEW.website_id,
      'observed_at', NEW.observed_at,
      'asking_rent', NEW.asking_rent,
      'effective_rent', NEW.effective_rent,
      'beds', NEW.beds,
      'baths', NEW.baths,
      'sqft', NEW.sqft,
      'available_count', NEW.available_count
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
END;
$$;
