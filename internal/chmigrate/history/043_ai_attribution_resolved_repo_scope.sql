-- Migration 043: repo-scope the ai_attribution_resolved view (CHAOS-2379).
--
-- The view in migration 035 resolved one row per (org_id, subject_type,
-- subject_id). That key is NOT globally unique: `subject_id` is the bare
-- provider PR/MR number (`toString(git_pull_requests.number)`), and PR/MR
-- numbers are only unique WITHIN a repository. GitLab MR `iid` and GitHub PR
-- `number` both restart at 1 per repo, so two repos in the same org that each
-- have PR/MR #1 produced two base rows but the view's
-- `PARTITION BY org_id, subject_type, subject_id` collapsed them to ONE,
-- keeping a single winning `repo_id`. Every downstream read path
-- (audit/ai_governance/loaders, metrics/loaders/ai_impact,
-- metrics/opportunities/ai_detector) joins `attr.repo_id = pr.repo_id`, so the
-- dropped repo's MR/PR silently disappeared from AI governance coverage and
-- impact — the second repo's attribution row never matched any PR.
--
-- Fix: add `repo_id` to the resolution partition so each repository's PR/MR
-- attribution resolves independently. Records are still cross-source
-- precedence-resolved, but now per (org_id, subject_type, repo_id,
-- subject_id). `repo_id` is Nullable, so repo-less work-item-level
-- attributions (repo_id IS NULL) form their own partition group and are NOT
-- collapsed against repo-pinned rows that happen to share a subject_id.
--
-- This is a metadata-only view redefinition. The base `ai_attribution` table
-- is unchanged and no data is rewritten.

CREATE OR REPLACE VIEW ai_attribution_resolved AS
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
FROM (
    SELECT
        *,
        multiIf(
            source = 'manual',          1,
            source = 'pr_label',        2,
            source = 'bot_author',      3,
            source = 'commit_trailer',  4,
            source = 'ci_annotation',   5,
            source = 'branch_name',     6,
            source = 'pr_body',         7,
            8
        ) AS _source_priority
    FROM ai_attribution FINAL
    WHERE superseded_by IS NULL
)
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY org_id, subject_type, repo_id, subject_id
    ORDER BY _source_priority ASC, confidence DESC
) = 1;
