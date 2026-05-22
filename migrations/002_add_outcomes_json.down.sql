-- Rollback: remove outcomes_json column and its index
DROP INDEX IF EXISTS idx_arb_opportunities_outcomes_gin;

ALTER TABLE arbitrage_opportunities
    DROP COLUMN IF EXISTS outcomes_json;
