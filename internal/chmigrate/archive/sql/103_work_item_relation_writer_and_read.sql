-- The blocked rule (CHAOS-8493) ends a stored relation when an item that WRITES
-- it has a later sync that did not write it again. Two facts it needs are not
-- stored, so two named cases end a relation too early (CHAOS-8578):
--
-- 1. WHO writes a relation. A native link is on both items and the sync of
--    either writes it. A relation read from text is written only by the item
--    that holds the text. The writer is derived from the stored raw value, but
--    gitlab's raw value "blocks" is both its native link type and a description
--    keyword, so both items are taken as writers.
--
--    relation_writer stores it: 'source', 'target' or 'both', the items of the
--    row that write it. The normalizer of each sync writes it (gitlab today).
--    NULL means not stored (a row written before this column, a provider whose
--    writer is derived exactly from the row). work_item_dependencies is
--    ReplacingMergeTree(last_synced) and every re-sync replaces the row, with
--    the writer known then.
--
-- 2. WHEN an item's relations were last read. An item's last_synced is the
--    time of its latest work_items row, and the github Projects v2 board pass
--    writes that row again (project_id 'ghprojv2:...') without reading the
--    issue text. Its text relations then look not written again.
--
--    work_item_relations_read stores, per item, the latest last_synced of a
--    work_items row written by a pass that reads the item's relations: every
--    row except a github row whose project_id starts with 'ghprojv2:' (the
--    board pass mints exactly that form, and no other writer does). A
--    materialized view fills it on every insert into work_items. No writer
--    changes. A reader takes max(relations_read_at) grouped by the key.
--
--    An item stored before this migration gets the latest such last_synced
--    still stored for it. When its latest stored row is a board row the
--    earlier text row may be gone, and the item has no row here until its next
--    text sync. A reader then falls back to work_items.last_synced, which is
--    the rule as it was.
--
-- Reader contract: the writers of a relation = relation_writer when it is not
-- NULL, else the derivation from the row. The time of a writer = its
-- max(relations_read_at) when it has a row here, else its work_items
-- last_synced.
ALTER TABLE work_item_dependencies ADD COLUMN IF NOT EXISTS relation_writer Nullable(String) COMMENT 'The items of the row that write it when they are synced: source, target or both. NULL = not stored, derive it from the row.';

CREATE TABLE IF NOT EXISTS work_item_relations_read (
    org_id String,
    provider String,
    work_item_id String,
    relations_read_at DateTime64(3) COMMENT 'last_synced of a work_items row written by a pass that reads the item relations. Read it as max(relations_read_at) grouped by org_id and work_item_id.'
) ENGINE = ReplacingMergeTree(relations_read_at)
ORDER BY (org_id, work_item_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS work_item_relations_read_mv
TO work_item_relations_read
AS SELECT
    org_id,
    provider,
    work_item_id,
    max(last_synced) AS relations_read_at
FROM work_items
WHERE NOT (provider = 'github' AND startsWith(project_id, 'ghprojv2:'))
GROUP BY org_id, provider, work_item_id;

-- The items that are already stored. The view above only sees inserts made
-- after it exists. Run again after a failed migration, this inserts the same
-- rows again, and they merge to the same maximum.
INSERT INTO work_item_relations_read
SELECT
    org_id,
    provider,
    work_item_id,
    max(last_synced) AS relations_read_at
FROM work_items
WHERE NOT (provider = 'github' AND startsWith(project_id, 'ghprojv2:'))
GROUP BY org_id, provider, work_item_id;
