-- capacity_forecasts keeps p50 / p85 / p95 of each Monte Carlo run and nothing
-- else; the run itself (10 000 completion-day or item samples) was discarded.
-- A chart of the forecast needs the distribution, so it is persisted too, as a
-- histogram: the distinct sample values and how many runs produced each one.
-- It is exact (nothing is binned), so any percentile can be recomputed from it,
-- and it is small (completion days are capped at 365 distinct values).
--
-- Four parallel Array columns, not Array(Tuple(...)): they scan and append as
-- plain slices in every client, and Values[i] pairs with Counts[i].
--   completion_days_*  fixed-scope mode  ("when will N items be done?")
--   completion_items_* fixed-date mode   ("how many items by date X?")
--
-- "No distribution" is the EMPTY array, which is also what every row written
-- before this migration reads (DEFAULT []): readers report no distribution for
-- it and never read it as a distribution of zeros. A mode that did not run
-- (no target date, or no days available) is empty for the same reason.
ALTER TABLE capacity_forecasts ADD COLUMN IF NOT EXISTS completion_days_values Array(UInt16) DEFAULT [] COMMENT 'Distinct completion-day values of the fixed-scope Monte Carlo run, ascending. Parallel to completion_days_counts. Empty = no distribution (rows before migration 101, or the mode did not run).';

ALTER TABLE capacity_forecasts ADD COLUMN IF NOT EXISTS completion_days_counts Array(UInt32) DEFAULT [] COMMENT 'How many simulation runs produced each completion_days_values entry. Sums to simulation_count when present.';

ALTER TABLE capacity_forecasts ADD COLUMN IF NOT EXISTS completion_items_values Array(UInt32) DEFAULT [] COMMENT 'Distinct items-completed values of the fixed-date Monte Carlo run, ascending. Parallel to completion_items_counts. Empty = no distribution (rows before migration 101, or the mode did not run).';

ALTER TABLE capacity_forecasts ADD COLUMN IF NOT EXISTS completion_items_counts Array(UInt32) DEFAULT [] COMMENT 'How many simulation runs produced each completion_items_values entry. Sums to simulation_count when present.';
