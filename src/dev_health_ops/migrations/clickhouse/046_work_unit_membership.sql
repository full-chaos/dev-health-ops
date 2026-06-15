-- Migration 046: node-level membership table for Work Graph theme/subcategory
-- filtering (CHAOS-2429).
--
-- work_unit_investments records one row per connected component (work unit);
-- it has no node→unit index, so the resolver cannot join edges to theme data
-- without scanning the full investments table. This table provides the
-- O(nodes) reverse index: one row per (org_id, node_type, node_id) with the
-- dominant theme and subcategory of the work unit that contains that node.
--
-- Dominant theme/subcategory are computed as argmax over the distribution
-- vectors in work_unit_investments, with lexical (smallest-key) tie-breaking
-- for determinism. categorization_status is propagated from the parent unit so
-- callers can filter out low-confidence rows.
--
-- ReplacingMergeTree on computed_at ensures idempotent re-materializations:
-- a re-run overwrites stale rows with a newer computed_at. Read with FINAL or
-- argMax(col, computed_at) for ReplacingMergeTree dedup semantics.
CREATE TABLE IF NOT EXISTS work_unit_membership (
    org_id String,
    node_type String,
    node_id String,
    work_unit_id String,
    dominant_theme String,
    dominant_subcategory String,
    categorization_status String,
    computed_at DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (org_id, node_type, node_id);
