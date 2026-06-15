-- Migration 046: node-level membership table for Work Graph theme/subcategory
-- filtering (CHAOS-2429/2430).
--
-- work_unit_investments records one row per connected component (a work unit).
-- it has no node->unit index, so the resolver cannot join edges to theme data
-- without scanning the full investments table. This table is the O(nodes)
-- reverse index.
--
-- GRAIN: one row per (node, category). A node is a member of a category when
-- that category's weight in the parent work unit's distribution is >=
-- MEMBERSHIP_WEIGHT_THRESHOLD (0.2). This is MULTI-membership, not dominant-
-- only: a 45%-feature / 40%-maintenance unit emits BOTH a feature_delivery row
-- and a maintenance row, so it is findable under either theme (dominant-only
-- would systematically hide the 40% category). category_kind distinguishes
-- 'theme' rows from 'subcategory' rows. The argmax category within each kind is
-- ALWAYS emitted (even if below threshold) with is_dominant=1, so every node is
-- findable under at least its dominant category (argmax ties broken lexically).
--
-- STALE ROWS: ReplacingMergeTree dedups by the full sort key, which includes
-- (category_kind, category). If a category drops below threshold on a later run
-- its row is simply not re-emitted, so the old row is NOT overwritten and
-- lingers. Readers MUST therefore scope to each node's latest run, NOT the
-- latest run per work_unit_id. work_unit_id is a hash of the connected
-- component, so when edge churn moves a node into a new component the OLD
-- work_unit_id is never re-emitted and a per-work_unit_id guard would keep that
-- dead unit alive forever (the node keeps matching obsolete categories). A node
-- belongs to exactly one component per run and stamps one computed_at, so
-- keeping only rows whose computed_at equals max(computed_at) per
-- (org_id, node_type, node_id) makes the node's most recent run supersede its
-- prior component rows. This fixes split/merge and below-threshold drop-off.
-- An orphaned node (removed from the graph entirely) is never re-emitted and
-- retains its last categories. The materialization job can run repo/team-scoped
-- (NOT a guaranteed global org sweep) so no global-run backstop or tombstone is
-- used. No edge references an orphaned node anyway once its edges age out.
--
-- ROLLOUT: this table is EMPTY until the next investment materialization
-- populates it, so existing tenants would see empty theme-filtered graphs right
-- after migrating. Deploy sequence MUST be: apply this migration, then RE-RUN
-- the investment materialization (which now writes work_unit_membership), then
-- serve theme-filtered queries. Do NOT add a fallback that masks real empties.
-- The resolver logs a one-line diagnostic when a theme filter returns nothing
-- while work_unit_investments is non-empty (observability only).
CREATE TABLE IF NOT EXISTS work_unit_membership (
    org_id String,
    node_type String,
    node_id String,
    work_unit_id String,
    category_kind String,
    category String,
    weight Float64,
    is_dominant UInt8,
    categorization_status String,
    computed_at DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (org_id, node_type, node_id, category_kind, category);
