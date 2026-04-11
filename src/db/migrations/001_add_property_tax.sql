-- Add property_tax column and fix assessed_value data
-- Run in Supabase SQL Editor

-- Add the new column
ALTER TABLE properties ADD COLUMN IF NOT EXISTS property_tax INTEGER;

-- Move the incorrectly stored tax amounts from assessed_value to property_tax
UPDATE properties SET property_tax = assessed_value, assessed_value = NULL
WHERE source = 'assessor' AND assessed_value IS NOT NULL;

-- Create index for tax queries
CREATE INDEX IF NOT EXISTS idx_prop_tax ON properties(property_tax) WHERE property_tax IS NOT NULL;
