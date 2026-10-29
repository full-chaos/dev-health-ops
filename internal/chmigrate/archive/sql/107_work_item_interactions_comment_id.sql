-- CHAOS-8790: two comments of one work item in the same millisecond have the
-- same sorting key (org_id, work_item_id, occurred_at, interaction_type), so
-- ReplacingMergeTree kept one of them. interaction_id is the provider's own
-- comment id, appended LAST to the sorting key. Every provider (jira, github,
-- gitlab, linear) writes it.
--
-- One statement, no DEFAULT: ClickHouse refuses a column with a default
-- expression in the sorting key (code 36), and refuses MODIFY ORDER BY for a
-- column that was not added in the same ALTER. String's implicit default is
-- the empty string. The sorting key change is metadata only; existing parts
-- are not rewritten and the primary key stays the old four-column prefix. A
-- second run of the statement is a no-op (ADD COLUMN IF NOT EXISTS).
--
-- interaction_id = '' marks a LEGACY row, written before the key carried the
-- id. A new row is never written with ''. The legacy row of a comment and the
-- keyed row of the same comment have different keys and are never merged, so
-- a reader must read the view below, never the table, to count comments.
ALTER TABLE work_item_interactions
    ADD COLUMN IF NOT EXISTS interaction_id String,
    MODIFY ORDER BY (org_id, work_item_id, occurred_at, interaction_type, interaction_id);

-- Reader contract for work_item_interactions. FINAL collapses re-synced keyed
-- rows. A legacy row (interaction_id = '') is hidden once ANY keyed row exists
-- in its slot (org_id, work_item_id, occurred_at, interaction_type): the keyed
-- rows of a re-fetched slot replace it. A legacy row whose slot has no keyed
-- row yet stays visible, flagged by is_legacy_id. A slot that held several
-- comments and was only partly re-fetched shows only the re-fetched ones: the
-- rest is missing, never invented. No NOT EXISTS: this runs with default
-- settings.
CREATE VIEW IF NOT EXISTS work_item_interactions_current AS
SELECT
    work_item_id,
    provider,
    interaction_type,
    occurred_at,
    actor,
    body_length,
    last_synced,
    org_id,
    interaction_id,
    interaction_id = '' AS is_legacy_id
FROM work_item_interactions FINAL
WHERE interaction_id != ''
   OR (org_id, work_item_id, occurred_at, interaction_type) NOT IN (
        SELECT org_id, work_item_id, occurred_at, interaction_type
        FROM work_item_interactions FINAL
        WHERE interaction_id != '');
