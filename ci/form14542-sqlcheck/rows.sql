-- 4 synthetic spans of one trace in the window [__START_S__, __START_S__ + 86400) s (FORM-14542-1; run.sh fixes START).
-- The table TTL (15 d) drops older rows at insert.
-- job.execute (job id 42) -> download -> request (target redirect); job.execute -> request (target api).
-- The download also holds dev_health.work.cancelled, an off-list key, so OUTSIDE_ALLOW_LIST has a row.
INSERT INTO TABLE_NAME
    (ts_bucket_start, timestamp, trace_id, span_id, parent_span_id, name, duration_nano,
     attributes_string, attributes_number, attributes_bool, resources_string)
VALUES
    (__BUCKET_S__, toDateTime64(__START_S__ + 600, 9, 'UTC'), '00000000000000000000000000000001', '0000000000000001', '',
     'dev_health.job.execute', 30000000000,
     map(), map('dev_health.job.id', 42), map(), map('service.name', 'dev-health-go-sync-provider')),
    (__BUCKET_S__, toDateTime64(__START_S__ + 601, 9, 'UTC'), '00000000000000000000000000000001', '0000000000000002', '0000000000000001',
     'dev_health.github_tests.artifact_download', 5000000000,
     map('outcome', 'read'),
     map('dev_health.artifact.bytes', 1000, 'dev_health.artifact.listed_bytes', 1000, 'dev_health.provider.requests', 2),
     map('dev_health.work.cancelled', true), map('service.name', 'dev-health-go-sync-provider')),
    (__BUCKET_S__, toDateTime64(__START_S__ + 602, 9, 'UTC'), '00000000000000000000000000000001', '0000000000000003', '0000000000000002',
     'dev_health.provider.request', 1000000000,
     map('provider', 'github', 'http.request.method', 'GET', 'http.route', '/repos/{owner}/{repo}/actions/artifacts/{artifact}/zip',
         'family', 'unclassified', 'dev_health.provider.target', 'redirect', 'outcome', 'response'),
     map('dev_health.provider.attempts', 1, 'dev_health.provider.gate_wait_ms', 0, 'dev_health.provider.budget_ms', 1,
         'dev_health.provider.lease_ms', 2, 'http.response.status_code', 200),
     map(), map('service.name', 'dev-health-go-sync-provider')),
    (__BUCKET_S__, toDateTime64(__START_S__ + 620, 9, 'UTC'), '00000000000000000000000000000001', '0000000000000004', '0000000000000001',
     'dev_health.provider.request', 2000000000,
     map('provider', 'github', 'http.request.method', 'GET', 'http.route', '/repos/{owner}/{repo}/actions/runs',
         'family', 'unclassified', 'dev_health.provider.target', 'api', 'outcome', 'response'),
     map('dev_health.provider.attempts', 1, 'dev_health.provider.gate_wait_ms', 3, 'dev_health.provider.budget_ms', 1,
         'dev_health.provider.lease_ms', 2, 'http.response.status_code', 200),
     map(), map('service.name', 'dev-health-go-sync-provider'));
