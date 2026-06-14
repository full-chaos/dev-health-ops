-- Migration 044: add repo_id to the ai_attribution base-table dedup key (CHAOS-2379).
--
-- Migration 035 created ai_attribution as
--   ENGINE = ReplacingMergeTree(computed_at)
--   ORDER BY (org_id, provider, subject_type, subject_id, source)
-- The ORDER BY tuple is the ReplacingMergeTree dedup key. repo_id was NOT in it.
--
-- subject_id is the bare, repo-local provider PR/MR number
-- (toString(git_pull_requests.number) == GitLab MR iid == GitHub PR number),
-- which is only unique WITHIN a repository. This branch is the first live
-- writer of GitLab MR attributions with subject_id = str(mr.iid). Two GitLab
-- repos in ONE org that each have MR !1 labeled ai-assisted from the SAME
-- source therefore produce IDENTICAL dedup keys
-- (org_id, provider, subject_type, subject_id, source). Under
-- ReplacingMergeTree FINAL (ai_attribution_resolved reads FROM ai_attribution
-- FINAL) one repo's row is PERMANENTLY collapsed at the base table BEFORE
-- migration 043's repo-scoped view ever runs. 043 fixes the read-time view
-- partition but cannot resurrect a base row that the engine already merged
-- away. Net: silent cross-repo AI-attribution loss.
--
-- Fix: rebuild ai_attribution with repo_id in the ORDER BY, placed BEFORE
-- subject_id, so each repository's PR/MR attribution dedups independently:
--   ORDER BY (org_id, provider, subject_type, repo_id, subject_id, source)
-- The dedup version column (computed_at) is preserved unchanged.
--
-- Backward compatibility: GitLab ai_attribution is empty in prod (this branch
-- is its first writer) and GitHub uses a PREFIXED subject_id
-- (ghpr:{repo}#{number}), which never collides across repos, so this rebuild
-- loses no real data. repo_id stays Nullable(UUID) and repo-less
-- (work-item-level) rows keep repo_id NULL and form their own dedup group.
--
-- repo_id is the FIRST Nullable column to appear in this table's ORDER BY
-- (migration 035's sorting key was all non-Nullable), so the rebuilt table
-- MUST set allow_nullable_key = 1 or ClickHouse rejects the CREATE with
-- "Sorting key cannot contain nullable columns" (same pattern as migration
-- 007's investment_velocity_daily). The setting is harmless and preserves the
-- NULL-repo dedup group as its own sort-key bucket.
--
-- subject_id deliberately stays the bare iid (round-2 pinned this so the
-- governance loader join a.subject_id = toString(pr.number) keeps working).
-- The base-table ORDER BY is the correct lever, not the subject_id shape.
--
-- Rebuild uses the standard ClickHouse RMT-key-change pattern:
--   0. DROP any leftover _new table from a prior partial run, so the CREATE
--      below never silently appends INTO stale data (CREATE IF NOT EXISTS
--      would otherwise no-op and the INSERT would target the old rows).
--   1. CREATE the _new table with the corrected ORDER BY.
--   2. INSERT all rows (explicit column list, exact 035 order).
--   3. EXCHANGE TABLES (atomic on the Atomic db engine) so readers never see
--      a missing table, then DROP the old table now parked under _new.
-- The migration runner (storage/clickhouse.py) splits this file on the
-- statement separator and skips comment-only chunks. Every statement below is
-- a single clean separator-terminated statement, and no separator character
-- appears inside any comment line (a stray one would corrupt the split).

DROP TABLE IF EXISTS ai_attribution_new;

CREATE TABLE IF NOT EXISTS ai_attribution_new
(
    record_id      UUID,
    org_id         UUID,
    provider       LowCardinality(String),
    subject_type   LowCardinality(String),
    subject_id     String,
    repo_id        Nullable(UUID),
    kind           LowCardinality(String),
    source         LowCardinality(String),
    confidence     Float32,
    actor          Nullable(String),
    evidence       String,
    observed_at    DateTime64(3, 'UTC'),
    ingested_at    DateTime64(3, 'UTC'),
    superseded_by  Nullable(UUID),
    computed_at    DateTime64(3, 'UTC') DEFAULT now64()
)
ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY toYYYYMM(observed_at)
ORDER BY (org_id, provider, subject_type, repo_id, subject_id, source)
SETTINGS index_granularity = 8192, allow_nullable_key = 1;

INSERT INTO ai_attribution_new
(
    record_id,
    org_id,
    provider,
    subject_type,
    subject_id,
    repo_id,
    kind,
    source,
    confidence,
    actor,
    evidence,
    observed_at,
    ingested_at,
    superseded_by,
    computed_at
)
SELECT
    record_id,
    org_id,
    provider,
    subject_type,
    subject_id,
    repo_id,
    kind,
    source,
    confidence,
    actor,
    evidence,
    observed_at,
    ingested_at,
    superseded_by,
    computed_at
FROM ai_attribution;

EXCHANGE TABLES ai_attribution AND ai_attribution_new;

DROP TABLE IF EXISTS ai_attribution_new;
