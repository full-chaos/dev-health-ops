-- work_item_team_attributions has three independent producers: the daily
-- work_item_attribution family, the remaining-family staleness backstop, and
-- providersync's sync-time deriver. Their rows are byte-identical in every
-- stored column, so a row that survives a merge carries nothing that names
-- the path or the run that wrote it, and a divergence between two producers
-- cannot be traced back to either of them.
--
-- writer names the path (daily, backstop, sync). run_id names that path's
-- individual run, and for the backstop it is the same identifier its
-- work_item_attribution_backstop_runs marker carries, so a row joins back to
-- the run record that published it.
--
-- BOTH COLUMNS STAY OUT OF THE SORTING KEY, deliberately. This table is a
-- ReplacingMergeTree(computed_at) ordered by
-- (org_id, repo_id, work_item_id, ifNull(team_id, ''), source). Adding a
-- producer column to that key would stop two producers' rows for one key
-- from ever collapsing, and every reader's
-- (work_item_id, max(computed_at)) fence would start returning one row per
-- producer instead of one row per key.
--
-- ADD COLUMN is a metadata change: no part is rewritten and no mutation is
-- queued, whatever the table's row count. Rows written before this migration
-- read the DEFAULT empty string, which is the truthful answer for them --
-- their producer was never recorded.
ALTER TABLE work_item_team_attributions ADD COLUMN IF NOT EXISTS writer LowCardinality(String) DEFAULT '';

ALTER TABLE work_item_team_attributions ADD COLUMN IF NOT EXISTS run_id String DEFAULT '';
