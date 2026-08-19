-- Migration 076: add truthful Linear issue-label AI attribution provenance.
--
-- Linear work items can carry the same fixed explicit AI label registry as
-- pull requests, but persisting that signal as `pr_label` would corrupt its
-- provenance. `issue_label` is therefore a distinct source. It has the same
-- explicit-declaration strength as a PR label and sits directly below manual
-- attribution. Existing source order is otherwise unchanged.

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
            source = 'issue_label',     2,
            source = 'pr_label',        3,
            source = 'bot_author',      4,
            source = 'commit_trailer',  5,
            source = 'ci_annotation',   6,
            source = 'branch_name',     7,
            source = 'pr_body',         8,
            9
        ) AS _source_priority
    FROM ai_attribution FINAL
    WHERE superseded_by IS NULL
)
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY org_id, subject_type, repo_id, subject_id
    ORDER BY _source_priority ASC, confidence DESC
) = 1;
