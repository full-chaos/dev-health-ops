-- Bound ACR branch-miss scans without relaxing its read safety limit
ALTER TABLE file_complexity_snapshots
    ADD INDEX IF NOT EXISTS idx_file_complexity_ref ref
    TYPE bloom_filter(0.01) GRANULARITY 1;

-- Existing parts need the index before ACR can rely on it
ALTER TABLE file_complexity_snapshots
    MATERIALIZE INDEX IF EXISTS idx_file_complexity_ref
    SETTINGS mutations_sync = 2;

-- Keep one compact row per hotspot computation run for bounded latest-run lookup
ALTER TABLE file_hotspot_daily
    ADD PROJECTION IF NOT EXISTS prj_acr_file_hotspot_runs
    (
        SELECT org_id, repo_id, day, computed_at
        GROUP BY org_id, repo_id, day, computed_at
    );

ALTER TABLE file_hotspot_daily
    MATERIALIZE PROJECTION IF EXISTS prj_acr_file_hotspot_runs
    SETTINGS mutations_sync = 2;

-- Keep one compact row per complexity computation run and ref
ALTER TABLE file_complexity_snapshots
    ADD PROJECTION IF NOT EXISTS prj_acr_file_complexity_runs
    (
        SELECT org_id, repo_id, ref, as_of_day, computed_at
        GROUP BY org_id, repo_id, ref, as_of_day, computed_at
    );

ALTER TABLE file_complexity_snapshots
    MATERIALIZE PROJECTION IF EXISTS prj_acr_file_complexity_runs
    SETTINGS mutations_sync = 2;
