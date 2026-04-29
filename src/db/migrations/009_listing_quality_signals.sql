ALTER TABLE listing_signals ADD COLUMN IF NOT EXISTS listing_agent_first_name TEXT;
ALTER TABLE listing_signals ADD COLUMN IF NOT EXISTS listing_agent_last_name TEXT;
ALTER TABLE listing_signals ADD COLUMN IF NOT EXISTS listing_agent_email TEXT;
ALTER TABLE listing_signals ADD COLUMN IF NOT EXISTS listing_agent_phone TEXT;
ALTER TABLE listing_signals ADD COLUMN IF NOT EXISTS agent_contact_source TEXT;
ALTER TABLE listing_signals ADD COLUMN IF NOT EXISTS agent_contact_confidence TEXT;

ALTER TABLE listing_signals ADD COLUMN IF NOT EXISTS creative_finance_score INTEGER;
ALTER TABLE listing_signals ADD COLUMN IF NOT EXISTS creative_finance_status TEXT;
ALTER TABLE listing_signals ADD COLUMN IF NOT EXISTS creative_finance_terms TEXT[];
ALTER TABLE listing_signals ADD COLUMN IF NOT EXISTS creative_finance_negative_terms TEXT[];
ALTER TABLE listing_signals ADD COLUMN IF NOT EXISTS creative_finance_rate_text TEXT;
ALTER TABLE listing_signals ADD COLUMN IF NOT EXISTS creative_finance_source TEXT;
ALTER TABLE listing_signals ADD COLUMN IF NOT EXISTS creative_finance_observed_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_listing_signals_agent_contact_complete
  ON listing_signals (state_code, is_on_market)
  WHERE listing_agent_email IS NOT NULL AND listing_agent_phone IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_listing_signals_creative_finance_status
  ON listing_signals (creative_finance_status)
  WHERE creative_finance_status IS NOT NULL;
