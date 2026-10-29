-- CHAOS-8863: the rows of an item that is stored under two repositories that name two work scopes count once
-- per view. row_view says which view a row of the three aggregate work-item daily tables belongs to:
--
--   ''      a LEGACY row: every row written before this migration, and every row of a writer that does not name
--           the column. It counts in every view (the rule before this change).
--   'org'   a row computed over the items counted once in the organization.
--   'scope' a row computed over the items counted once in each work scope.
--
-- This migration adds the column and the sorting-key part. No writer and no reader names row_view yet, so
-- every row is a legacy row and every sum is what it was. The writer starts to store 'org' and 'scope' rows only
-- after every reader takes the view of its question (CHAOS-8863 build order).
--
-- One statement for each table, no DEFAULT: ClickHouse refuses a column with a default expression in the
-- sorting key (code 36), and refuses MODIFY ORDER BY for a column that was not added in the same ALTER (see
-- 107). The implicit default of String is the empty string, which is the legacy marker. The sorting-key change
-- is metadata only: existing parts are not rewritten. A second run of a statement is a no-op (ADD COLUMN IF
-- NOT EXISTS, and MODIFY ORDER BY to the key the table already has).
ALTER TABLE work_item_metrics_daily
    ADD COLUMN IF NOT EXISTS row_view LowCardinality(String),
    MODIFY ORDER BY (org_id, provider, day, work_scope_id, team_id, row_view);

ALTER TABLE work_item_state_durations_daily
    ADD COLUMN IF NOT EXISTS row_view LowCardinality(String),
    MODIFY ORDER BY (org_id, provider, work_scope_id, team_id, status, day, row_view);

ALTER TABLE work_item_user_metrics_daily
    ADD COLUMN IF NOT EXISTS row_view LowCardinality(String),
    MODIFY ORDER BY (org_id, provider, work_scope_id, user_identity, day, row_view);
