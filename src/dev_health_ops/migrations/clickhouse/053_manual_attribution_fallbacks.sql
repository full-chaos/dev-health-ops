-- CHAOS-2600 CS1: ClickHouse-only team attribution — storage foundations.
-- Additive only, no behavior change: existing source/confidence integer codes are preserved and
-- new values are appended so the resolver (CS2/CS3) can later emit issue_project / manual_fallback
-- and manual / none confidence without an insert failure. See team-attribution.md s0 (Schema
-- prerequisite) and CHAOS-2600 ordering rule s4.1 (migrate enums BEFORE emitting new values).

-- Widen work_item_team_attributions.source. New codes 7/8 are appended — ranks (precedence) live in
-- compute_work_items._SOURCE_ORDER, NOT in these integer codes.
ALTER TABLE work_item_team_attributions
    MODIFY COLUMN source Enum8('native_team' = 1, 'linked_issue' = 2, 'project_ownership' = 3, 'repo_ownership' = 4, 'assignee_membership' = 5, 'unassigned' = 6, 'issue_project' = 7, 'manual_fallback' = 8);

-- Widen confidence (only this table has a confidence column — the edge tables do not).
ALTER TABLE work_item_team_attributions
    MODIFY COLUMN confidence Enum8('high' = 1, 'medium' = 2, 'low' = 3, 'manual' = 4, 'none' = 5);

-- Explicit manual fallback attribution records. These are FALLBACK config, never overrides:
-- the resolver only consults them after native/imported/linked attribution fails (CS3).
-- scope_type allowed values: repo | project | member | issue_key_prefix.
-- ReplacingMergeTree(updated_at) — ORDER BY is the LOGICAL REPLACEMENT IDENTITY: one active
-- fallback per (org_id, provider, scope_type, scope_id). team_id is deliberately NOT in the sort
-- key — if it were, reassigning a scope to a different team would leave the old team row alive as a
-- second active fallback (RMT only replaces rows sharing the full ORDER BY tuple). Reassigning a
-- scope's team now REPLACES the row — readers take the latest by updated_at (FINAL/argMax, CS3).
-- org_id leads the key (matches the 051 tables) — no per-org PARTITION BY (avoids partition explosion).
CREATE TABLE IF NOT EXISTS manual_attribution_fallbacks (
    org_id String,
    provider LowCardinality(String),
    scope_type LowCardinality(String),
    scope_id String,
    team_id String,
    team_name String,
    reason String,
    priority Int32 DEFAULT 100,
    valid_from DateTime64(3, 'UTC') DEFAULT now64(3),
    valid_to Nullable(DateTime64(3, 'UTC')),
    created_by Nullable(String),
    created_at DateTime64(3, 'UTC') DEFAULT now64(3),
    updated_at DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (org_id, provider, scope_type, scope_id);
