-- Queryable branch digests keep ACR evidence handles opaque and branch lookups bounded
ALTER TABLE repos
    ADD COLUMN IF NOT EXISTS ref_sha256 String
    MATERIALIZED lower(hex(SHA256(ifNull(ref, ''))));

ALTER TABLE repos
    ADD INDEX IF NOT EXISTS idx_repos_ref_sha256 ref_sha256
    TYPE bloom_filter(0.01) GRANULARITY 1;

ALTER TABLE repos
    MATERIALIZE INDEX IF EXISTS idx_repos_ref_sha256
    SETTINGS mutations_sync = 2;

ALTER TABLE git_pull_requests
    ADD COLUMN IF NOT EXISTS head_branch_sha256 String
    MATERIALIZED lower(hex(SHA256(ifNull(head_branch, ''))));

ALTER TABLE git_pull_requests
    ADD INDEX IF NOT EXISTS idx_git_pull_requests_head_branch_sha256 head_branch_sha256
    TYPE bloom_filter(0.01) GRANULARITY 1;

ALTER TABLE git_pull_requests
    MATERIALIZE INDEX IF EXISTS idx_git_pull_requests_head_branch_sha256
    SETTINGS mutations_sync = 2;

ALTER TABLE git_pull_requests
    ADD COLUMN IF NOT EXISTS base_branch_sha256 String
    MATERIALIZED lower(hex(SHA256(ifNull(base_branch, ''))));

ALTER TABLE git_pull_requests
    ADD INDEX IF NOT EXISTS idx_git_pull_requests_base_branch_sha256 base_branch_sha256
    TYPE bloom_filter(0.01) GRANULARITY 1;

ALTER TABLE git_pull_requests
    MATERIALIZE INDEX IF EXISTS idx_git_pull_requests_base_branch_sha256
    SETTINGS mutations_sync = 2;

ALTER TABLE ci_pipeline_runs
    ADD COLUMN IF NOT EXISTS branch_sha256 String
    MATERIALIZED lower(hex(SHA256(ifNull(branch, ''))));

ALTER TABLE ci_pipeline_runs
    ADD INDEX IF NOT EXISTS idx_ci_pipeline_runs_branch_sha256 branch_sha256
    TYPE bloom_filter(0.01) GRANULARITY 1;

ALTER TABLE ci_pipeline_runs
    MATERIALIZE INDEX IF EXISTS idx_ci_pipeline_runs_branch_sha256
    SETTINGS mutations_sync = 2;

ALTER TABLE file_complexity_snapshots
    ADD COLUMN IF NOT EXISTS ref_sha256 String
    MATERIALIZED lower(hex(SHA256(ref)));

ALTER TABLE file_complexity_snapshots
    ADD INDEX IF NOT EXISTS idx_file_complexity_snapshots_ref_sha256 ref_sha256
    TYPE bloom_filter(0.01) GRANULARITY 1;

ALTER TABLE file_complexity_snapshots
    MATERIALIZE INDEX IF EXISTS idx_file_complexity_snapshots_ref_sha256
    SETTINGS mutations_sync = 2;

ALTER TABLE file_complexity_snapshots
    ADD PROJECTION IF NOT EXISTS prj_acr_file_complexity_digest_runs
    (
        SELECT org_id, repo_id, ref_sha256, ref, as_of_day, computed_at
        GROUP BY org_id, repo_id, ref_sha256, ref, as_of_day, computed_at
    );

ALTER TABLE file_complexity_snapshots
    MATERIALIZE PROJECTION IF EXISTS prj_acr_file_complexity_digest_runs
    SETTINGS mutations_sync = 2;
