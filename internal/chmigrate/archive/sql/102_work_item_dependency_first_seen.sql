-- work_item_dependencies says THAT a relation between two work items exists,
-- as of its last_synced. It does not say SINCE WHEN. A reader that needs an
-- interval ("this item has been blocked since ...") had nothing to start it
-- from except the creation time of the two items, which is a guess: a link
-- added months after both items were created would count for all those months.
--
-- This migration stores the two times a relation start can honestly come from.
--
-- 1. relation_started_at: the PROVIDER's own time of the link, when the synced
--    payload carries one. The normalizer of each sync writes it. NULL means the
--    provider payload gave no time (a relation read from text, a provider that
--    does not report it, a row written before this column). It is an ordinary
--    column of the row: work_item_dependencies is ReplacingMergeTree(last_synced)
--    and every re-sync replaces the row, with the time the payload carries then.
--
-- 2. first_seen_at: the first time OUR sync wrote the relation. It cannot be a
--    column of work_item_dependencies, for the same reason: the row is replaced
--    by each re-sync, so nothing in it can stay "first". It is kept beside the
--    table instead, as min(last_synced) per relation key, by a materialized view
--    that fires on every insert into work_item_dependencies. No writer changes:
--    the provider sync and customer push are both covered.
--
--    Stored once, never moved by a later sync: last_synced is the sync time, and
--    the minimum of sync times does not move when a later one arrives. A reader
--    takes min(first_seen_at) over the key (AggregatingMergeTree keeps the
--    minimum per part until parts merge).
--
--    A relation that existed before this migration gets the smallest
--    last_synced still stored for it. That is a time a sync really saw the
--    relation, and it is LATER than the first time one did (earlier versions of
--    the row are gone). So first_seen_at can be too late, never too early: an
--    interval that starts from it understates and never overstates.
--
-- Reader contract: relation start = relation_started_at when it is not NULL,
-- else first_seen_at. Never an item's created_at.
ALTER TABLE work_item_dependencies ADD COLUMN IF NOT EXISTS relation_started_at Nullable(DateTime64(3)) COMMENT 'Provider time of the link, when the synced payload carries one. NULL = not reported. See work_item_dependency_first_seen for the fallback.';

CREATE TABLE IF NOT EXISTS work_item_dependency_first_seen (
    org_id String,
    source_work_item_id String,
    target_work_item_id String,
    relationship_type String,
    first_seen_at SimpleAggregateFunction(min, DateTime64(3)) COMMENT 'min(work_item_dependencies.last_synced) of the relation: the first time a sync wrote it. Read it as min(first_seen_at) grouped by the key.'
) ENGINE = AggregatingMergeTree
ORDER BY (org_id, source_work_item_id, target_work_item_id, relationship_type);

CREATE MATERIALIZED VIEW IF NOT EXISTS work_item_dependency_first_seen_mv
TO work_item_dependency_first_seen
AS SELECT
    org_id,
    source_work_item_id,
    target_work_item_id,
    relationship_type,
    min(last_synced) AS first_seen_at
FROM work_item_dependencies
GROUP BY org_id, source_work_item_id, target_work_item_id, relationship_type;

-- The relations that are already stored. The view above only sees inserts made
-- after it exists. Run again after a failed migration, this adds nothing new:
-- the minimum of the same values is the same value.
INSERT INTO work_item_dependency_first_seen
SELECT
    org_id,
    source_work_item_id,
    target_work_item_id,
    relationship_type,
    min(last_synced) AS first_seen_at
FROM work_item_dependencies
GROUP BY org_id, source_work_item_id, target_work_item_id, relationship_type;
