-- Cognitive-load derived metric columns for user_metrics_daily.
-- Privacy-first: derived from existing PR/review/git data only; no IDE/keystroke/session telemetry.
--
-- pr_interruption_load  : distinct PRs this user reviewed in the day window
--                         (each unique PR = one context-switch interruption as a reviewer).
-- context_spread_count  : distinct repos the user had any activity in (commits, PRs authored,
--                         PRs reviewed) during the day.  Measures breadth-of-attention fragmentation.
-- review_request_load   : distinct authored PRs that received at least one review event (any state)
--                         in the day window.  Measures incoming feedback pressure on the author.

ALTER TABLE user_metrics_daily ADD COLUMN IF NOT EXISTS pr_interruption_load UInt32 DEFAULT 0;
ALTER TABLE user_metrics_daily ADD COLUMN IF NOT EXISTS context_spread_count UInt32 DEFAULT 0;
ALTER TABLE user_metrics_daily ADD COLUMN IF NOT EXISTS review_request_load UInt32 DEFAULT 0;
