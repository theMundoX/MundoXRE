-- Add mortgage calculation fields
-- Run in Supabase SQL Editor

-- New fields for mortgage analysis
ALTER TABLE mortgage_records ADD COLUMN IF NOT EXISTS original_amount INTEGER;
ALTER TABLE mortgage_records ADD COLUMN IF NOT EXISTS interest_rate NUMERIC(5,3);
ALTER TABLE mortgage_records ADD COLUMN IF NOT EXISTS term_months INTEGER DEFAULT 360;
ALTER TABLE mortgage_records ADD COLUMN IF NOT EXISTS estimated_monthly_payment INTEGER;
ALTER TABLE mortgage_records ADD COLUMN IF NOT EXISTS estimated_current_balance INTEGER;
ALTER TABLE mortgage_records ADD COLUMN IF NOT EXISTS balance_as_of DATE;
ALTER TABLE mortgage_records ADD COLUMN IF NOT EXISTS maturity_date DATE;
ALTER TABLE mortgage_records ADD COLUMN IF NOT EXISTS loan_type TEXT; -- 'purchase', 'refinance', 'heloc', 'construction'
ALTER TABLE mortgage_records ADD COLUMN IF NOT EXISTS deed_type TEXT; -- 'warranty', 'quitclaim', 'trust', 'special_warranty'

-- Copy existing loan_amount to original_amount for any existing records
UPDATE mortgage_records SET original_amount = loan_amount WHERE original_amount IS NULL AND loan_amount IS NOT NULL;

-- Index for mortgage analysis queries
CREATE INDEX IF NOT EXISTS idx_mort_amount ON mortgage_records(original_amount) WHERE original_amount IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_mort_lender ON mortgage_records(lender_name) WHERE lender_name IS NOT NULL;
