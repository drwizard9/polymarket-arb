-- Add outcomes_json column to store all N outcomes without data loss.
-- The legacy yes_bid_price/no_bid_price columns are kept for backward compatibility.
ALTER TABLE arbitrage_opportunities
    ADD COLUMN IF NOT EXISTS outcomes_json JSONB NOT NULL DEFAULT '[]'::jsonb;

-- Index for querying by outcome name (e.g. find all trades involving a specific candidate)
CREATE INDEX IF NOT EXISTS idx_arb_opportunities_outcomes_gin
    ON arbitrage_opportunities USING gin(outcomes_json);
