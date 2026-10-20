ALTER TABLE user_metrics_daily ADD COLUMN IF NOT EXISTS pr_interruption_load UInt32 DEFAULT 0;
ALTER TABLE user_metrics_daily ADD COLUMN IF NOT EXISTS context_spread_count UInt32 DEFAULT 0;
ALTER TABLE user_metrics_daily ADD COLUMN IF NOT EXISTS review_request_load UInt32 DEFAULT 0;
