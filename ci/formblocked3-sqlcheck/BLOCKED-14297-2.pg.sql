-- BLOCKED-14297-2.pg = BLOCKED-14297-1.pg (90a45eefd501) + the stmt2 WANT (acp FORM 11-21 16:1xZ). Nothing else changed.
-- BLOCKED-14297-1.pg (fcdev, CHAOS-14297 card backup; acp ask 11-21 16:0xZ): every TERMINAL unit of ONE backfill run that carries an
-- Incomplete entry (any component / cause), with WHICH bound fired. Form = PROGRESS-14297-1.pg (ae9877ec5f94): PostgreSQL (domain DB),
-- REPEATABLE READ READ ONLY, counts / times / 8-char prefixes / decoded cursor numbers only (no name, no full id, never next_url), one org
-- prefix, no SET. u.result is read as u.result::jsonb, as RESYNC-UNITS-14297-4 stmt2 does on prod. Placeholders {{ORG_PREFIX}} (8 chars), {{RUN_PREFIX}} (8 chars of the sync_run id). Every FROM filters
-- starts_with(org_id::text, '{{ORG_PREFIX}}').
-- Literals (ops dafd414e = workers W): budget_blocked (card) = status 'success' AND result.incomplete[*] has cause 'page_budget_exhausted'
-- AND component 'run_inventory' (RESYNC-UNITS-14297-4 stmt2 = TestOpsBudgetBlockedUnitsSQL testops_window_narrowing.go:39). TWO bounds
-- write that SAME entry (markGitHubTestsInventoryTruncated github_tests_chunked_route.go:604): the run-listing PAGE budget
-- (recordGitHubTestsInventoryTruncation :658, githubTestsMaxRuns = 100 pages x 100 runs, route.go:28 / native_rest.go:16-17; WARN
-- "provider inventory page budget exhausted") and the walk RECORD bound (recordGitHubTestsWalkBound :620, 4,100,000 records :586; WARN
-- "provider walk record bound reached"). The checkpoint counters below tell them apart where the checkpoint row still exists:
-- c_run_pages = 100 -> page budget; c_walk_records >= 4,100,000 -> record bound (both can be true). Rows not re-read: the CH pair
-- DENSITY-14297-1.ch per UTC day.
BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;

-- stmt00 ORG-1. WANT 1.
SELECT count(DISTINCT org_id) AS orgs FROM public.sync_runs WHERE starts_with(org_id::text, '{{ORG_PREFIX}}');

-- stmt01 server version. WANT pgv >= 160000 (pg_input_is_valid).
SELECT current_setting('server_version_num')::int AS pgv;

-- stmt0 the run. WANT 1.
SELECT count(*) AS runs FROM public.sync_runs
WHERE starts_with(org_id::text, '{{ORG_PREFIX}}') AND starts_with(id::text, '{{RUN_PREFIX}}') AND triggered_by = 'backfill';

-- stmt1 the run's units by status and budget_blocked (the card's count; WANT budget_blocked = the RESYNC-UNITS-14297-4 value).
SELECT u.status, count(*) AS units,
       count(*) FILTER (WHERE u.status = 'success' AND COALESCE(jsonb_path_exists(u.result::jsonb,
         '$.incomplete[*] ? (@.cause == "page_budget_exhausted" && @.component == "run_inventory")'), false)) AS budget_blocked
FROM public.sync_run_units AS u
JOIN public.sync_runs AS r ON r.id = u.sync_run_id AND r.org_id = u.org_id AND starts_with(r.org_id::text, '{{ORG_PREFIX}}')
WHERE starts_with(u.org_id::text, '{{ORG_PREFIX}}') AND starts_with(r.id::text, '{{RUN_PREFIX}}') AND r.triggered_by = 'backfill'
GROUP BY u.status ORDER BY u.status;

-- stmt2 one row per terminal unit of the run with ANY Incomplete entry: unit / source prefixes, repo_key, window, artifact range start,
-- the entries as component:cause:count, the result's top-level key names (INFO: where the watermark lives), and the checkpoint counters
-- (LEFT JOIN: NULL when the row was removed at the end of the unit).
-- WANT >= 1 row when stmt1 budget_blocked >= 1, holding EVERY unit stmt1 counts as budget_blocked (aaaaaaaa: bbbbbbbb); stmt1 is its
-- control. A budget_blocked unit with no row here = STOP. For an S-run prefix (CARD-14297-18 (c)): WANT 0 rows.
SELECT left(u.id::text, 8) AS unit_8, left(u.source_id::text, 8) AS source_8,
       left(md5(lower(s.full_name)), 8) AS repo_key, u.status,
       to_char(u.since_at AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS') AS since_utc,
       to_char(u.before_at AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.US') AS before_utc,
       to_char(((u.since_at - greatest(interval '24 hours', u.before_at - u.since_at)) AT TIME ZONE 'UTC')::date, 'YYYY-MM-DD') || ' 00:00:00'
                                                                                                    AS artifact_from_utc,
       (SELECT string_agg((e ->> 'component') || ':' || (e ->> 'cause') || ':' || coalesce(e ->> 'count', ''), ',' ORDER BY e ->> 'component')
          FROM jsonb_array_elements(CASE WHEN jsonb_typeof(u.result::jsonb -> 'incomplete') = 'array' THEN u.result::jsonb -> 'incomplete'
                                         ELSE '[]'::jsonb END) AS e)                               AS incomplete,
       (SELECT string_agg(k, ',' ORDER BY k) FROM jsonb_object_keys(CASE WHEN jsonb_typeof(u.result::jsonb) = 'object' THEN u.result::jsonb
          ELSE '{}'::jsonb END) AS k)            AS result_keys,
       c.next_ordinal AS ck_next_ordinal, c.committed_rows AS ck_committed_rows,
       CASE WHEN pg_input_is_valid(c.next_cursor, 'jsonb') THEN (c.next_cursor::jsonb ->> 'run_pages')::bigint END      AS c_run_pages,
       CASE WHEN pg_input_is_valid(c.next_cursor, 'jsonb') THEN (c.next_cursor::jsonb ->> 'artifact_pages')::bigint END AS c_artifact_pages,
       CASE WHEN pg_input_is_valid(c.next_cursor, 'jsonb') THEN (c.next_cursor::jsonb ->> 'pipelines')::bigint END      AS c_pipelines,
       CASE WHEN pg_input_is_valid(c.next_cursor, 'jsonb') THEN
         coalesce((c.next_cursor::jsonb ->> 'pipelines')::bigint, 0) + coalesce((c.next_cursor::jsonb ->> 'jobs')::bigint, 0)
         + coalesce((c.next_cursor::jsonb ->> 'acceptance')::bigint, 0) + coalesce((c.next_cursor::jsonb ->> 'suites')::bigint, 0)
         + coalesce((c.next_cursor::jsonb ->> 'cases')::bigint, 0) + coalesce((c.next_cursor::jsonb ->> 'coverage')::bigint, 0) END
                                                                                                    AS c_walk_records,
       CASE WHEN pg_input_is_valid(c.next_cursor, 'jsonb') THEN c.next_cursor::jsonb ->> 'rh' END                        AS c_rh
FROM public.sync_run_units AS u
JOIN public.sync_runs AS r ON r.id = u.sync_run_id AND r.org_id = u.org_id AND starts_with(r.org_id::text, '{{ORG_PREFIX}}')
LEFT JOIN public.integration_sources AS s ON s.id = u.source_id AND s.org_id = u.org_id AND starts_with(s.org_id::text, '{{ORG_PREFIX}}')
LEFT JOIN public.sync_run_unit_chunk_checkpoints AS c
  ON c.sync_run_unit_id = u.id AND c.org_id = u.org_id AND starts_with(c.org_id::text, '{{ORG_PREFIX}}')
WHERE starts_with(u.org_id::text, '{{ORG_PREFIX}}') AND starts_with(r.id::text, '{{RUN_PREFIX}}') AND r.triggered_by = 'backfill'
  AND u.status IN ('success', 'failed')
  AND COALESCE(jsonb_path_exists(u.result::jsonb, '$.incomplete[*]'), false)
ORDER BY u.id;

ROLLBACK;
