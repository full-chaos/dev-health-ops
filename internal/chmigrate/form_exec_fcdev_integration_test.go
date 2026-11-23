//go:build integration

package chmigrate_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// DO NOT MERGE. This file executes the text of two prod read FORMs (FORM-7829-3.sql, FORM-14259-SIZE-3.sql), byte for byte
// (the sha256 below is the sha256 of each .sql file), on the head schema with synthetic fixtures. It checks each WANT, that
// each statement's STOP-COST-GATE fires before the start with a cap set to 0 (query log), that each size GATE fires on a
// copy of the text with only its bound lowered, and that each CONTROL fails under a planted mutation.
const (
	formExec7829SHA256  = "b919b830e66abb3234ef1007f05dfcaf6a1dde963eb77ec1db3c408a98e31267"
	formExec14259SHA256 = "ec41eafa0a7ba09ed16c2f5d7bb6732a49aa92dc0057341b5bf919165daabbf6"
	formExec7829SQL     = `-- FORM-7829-3 (lane lformch for fcdev, 2026-11-23). CHAOS-7829: one Linear event_id stored at two occurred_at values.
-- Shape: pm-guard/FORM-TEMPLATE-1.md (e220dd03e6fc) (a) ClickHouse. Replaces FORM-7829-2 (frozen).
-- ClickHouse, SELECT only, one statement per call. Org prefix c6a38355 on every FROM with org_id.
-- Output = counts, 8-char sha256 prefixes of event_id, days (UTC), spans in ms.
-- Run each statement once, in order. Any SQL error (STOP-COST-GATE and GATE errors included) = STOP, report the text.
-- STOP when S1 orgs != 1.
-- Table: project_membership_transitions, ReplacingMergeTree(last_synced),
--   ORDER BY (org_id, subject_kind, repo_id, subject_id, occurred_at, event_id), no PARTITION BY, org_id String.
-- Gates, all scalar subqueries (run at analysis, before any table read), in this order:
--   cost_gate (S1 to S5) = active parts of the table over 20000000 rows or 4 GiB on disk: 'STOP-COST-GATE'.
--   events (S2 to S5) = org distinct event_id (uniq) over 2000000 (the GROUP BY event_id memory). Gated by cost_gate.
--   multi (S4, S5) = over 10000 multi-occurred events or over 1000000 of their rows (the dedup memory). Gated by events.
-- cost_gate = 0 is in the WHERE of every query block that reads the table.
-- Dedup: S4 and S5 group the rows of the multi-occurred events by the full sort key, max(last_synced) = the version
--   FINAL keeps. Two occurred_at values are two sort keys and stay two keys.

-- S0 cost inputs (system.parts only). WANT 1 row, table_rows > 0 (a wrong table name gives no row).
SELECT table, sum(rows) AS table_rows, sum(bytes_on_disk) AS disk_bytes, count() AS active_parts
FROM system.parts
WHERE active AND database = currentDatabase()
  AND table IN ('project_membership_transitions')
GROUP BY table
ORDER BY table;

-- S1 ORG-1 and size report. WANT orgs = 1, raw_linear_rows > 0. INFO events_approx (S2 to S5 gate at 2000000).
WITH (SELECT throwIf(
    (SELECT sum(rows) FROM system.parts WHERE active AND database = currentDatabase()
      AND table IN ('project_membership_transitions')) > 20000000
    OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = currentDatabase()
      AND table IN ('project_membership_transitions')) > 4294967296,
    'STOP-COST-GATE')) AS cost_gate
SELECT uniqExact(org_id) AS orgs,
       count() AS raw_rows,
       countIf(provider = 'linear') AS raw_linear_rows,
       uniq(event_id) AS events_approx
FROM project_membership_transitions
WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0;

-- S2 per provider (raw rows, no FINAL: an occurred_at value is in the sort key, so versions never add or remove one).
-- WANT linear events > 0. WANT linear events_multi_occ >= 3 (ticket: 3 seen in CHAOS-7659, no path retires a key).
-- INFO multi_occ_native_id = of those, event_id 'linear:<history entry id>'. INFO events_multi_subject = one event_id
--   under 2+ subjects. raw_rows_of_multi_occ counts unmerged versions too.
WITH (SELECT throwIf(
    (SELECT sum(rows) FROM system.parts WHERE active AND database = currentDatabase()
      AND table IN ('project_membership_transitions')) > 20000000
    OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = currentDatabase()
      AND table IN ('project_membership_transitions')) > 4294967296,
    'STOP-COST-GATE')) AS cost_gate
SELECT m.provider AS provider,
       count() AS events,
       countIf(m.min_occ < m.max_occ) AS events_multi_occ,
       countIf(m.min_occ < m.max_occ AND m.native = 1) AS multi_occ_native_id,
       countIf(m.min_sh != m.max_sh) AS events_multi_subject,
       sumIf(m.n, m.min_occ < m.max_occ) AS raw_rows_of_multi_occ
FROM (
    SELECT provider,
           event_id,
           startsWith(event_id, 'linear:') AS native,
           count() AS n,
           min(occurred_at) AS min_occ,
           max(occurred_at) AS max_occ,
           min(cityHash64(subject_kind, repo_id, subject_id)) AS min_sh,
           max(cityHash64(subject_kind, repo_id, subject_id)) AS max_sh
    FROM project_membership_transitions
    WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0 AND (SELECT throwIf(uniq(event_id) > 2000000, 'FORM-7829-3 GATE events: org event_ids over 2000000') FROM project_membership_transitions WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0) = 0
    GROUP BY provider, event_id
) AS m
GROUP BY m.provider
ORDER BY provider;

-- S3 CONTROL: S2 events_multi_occ by another rule (2 distinct occurred_at kept by groupUniqArray(2), not min < max).
-- WANT per provider events_multi_occ_alt = S2 events_multi_occ. WANT linear events_multi_occ_alt > 0.
WITH (SELECT throwIf(
    (SELECT sum(rows) FROM system.parts WHERE active AND database = currentDatabase()
      AND table IN ('project_membership_transitions')) > 20000000
    OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = currentDatabase()
      AND table IN ('project_membership_transitions')) > 4294967296,
    'STOP-COST-GATE')) AS cost_gate
SELECT m.provider AS provider,
       countIf(m.occ2 = 2) AS events_multi_occ_alt
FROM (
    SELECT provider,
           event_id,
           length(groupUniqArray(2)(occurred_at)) AS occ2
    FROM project_membership_transitions
    WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0 AND (SELECT throwIf(uniq(event_id) > 2000000, 'FORM-7829-3 GATE events: org event_ids over 2000000') FROM project_membership_transitions WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0) = 0
    GROUP BY provider, event_id
) AS m
GROUP BY m.provider
ORDER BY provider;

-- S4 one row per multi-occurred event, LIMIT 100. WANT row count = S2 sum of events_multi_occ (if <= 100), > 0.
-- key_rows = distinct sort keys. occ_span_ms = max - min occurred_at. Go cut per provider (writer literals: 'github'
--   github_project_membership_snapshot_diff.go:314, writer aca100589, 2026-08-24 20:33:09 UTC, 'linear'
--   linear_work_items_route.go:1869, writer a21768303, 2026-08-25 01:33:44 UTC, jira the same commit).
--   WANT keys_ls_before_cut = 0.
WITH (SELECT throwIf(
    (SELECT sum(rows) FROM system.parts WHERE active AND database = currentDatabase()
      AND table IN ('project_membership_transitions')) > 20000000
    OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = currentDatabase()
      AND table IN ('project_membership_transitions')) > 4294967296,
    'STOP-COST-GATE')) AS cost_gate
SELECT substring(lower(hex(SHA256(e.event_id))), 1, 8) AS event_h8,
       e.provider AS provider,
       e.native AS native_id,
       e.keys AS key_rows,
       e.occ AS occurred_values,
       e.subj AS subjects,
       toString(toDate(e.min_occ, 'UTC')) AS min_occurred_day,
       toString(toDate(e.max_occ, 'UTC')) AS max_occurred_day,
       toUnixTimestamp64Milli(e.max_occ) - toUnixTimestamp64Milli(e.min_occ) AS occ_span_ms,
       toString(toDate(e.min_ls, 'UTC')) AS min_last_synced_day,
       toString(toDate(e.max_ls, 'UTC')) AS max_last_synced_day,
       toString(toDate(e.min_ing, 'UTC')) AS min_ingested_day,
       toString(toDate(e.max_ing, 'UTC')) AS max_ingested_day,
       e.before_cut AS keys_ls_before_cut,
       e.keys - e.before_cut AS keys_ls_after_cut
FROM (
    SELECT k.provider AS provider,
           k.event_id AS event_id,
           startsWith(k.event_id, 'linear:') AS native,
           count() AS keys,
           uniqExact(k.occurred_at) AS occ,
           uniqExact(k.subject_kind, k.repo_id, k.subject_id) AS subj,
           min(k.occurred_at) AS min_occ,
           max(k.occurred_at) AS max_occ,
           min(k.ls) AS min_ls,
           max(k.ls) AS max_ls,
           min(k.ing) AS min_ing,
           max(k.ing) AS max_ing,
           countIf(k.ls < if(k.provider = 'github', toDateTime64('2026-08-24 20:33:09', 3, 'UTC'),
                             toDateTime64('2026-08-25 01:33:44', 3, 'UTC'))) AS before_cut
    FROM (
    SELECT provider, event_id, subject_kind, repo_id, subject_id, occurred_at,
           max(last_synced) AS ls,
           argMax(ingested_at, last_synced) AS ing
    FROM project_membership_transitions
    WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0 AND (SELECT throwIf(count() > 10000 OR sum(g.n) > 1000000, 'FORM-7829-3 GATE multi: over 10000 multi-occurred events or 1000000 of their rows') FROM (SELECT count() AS n FROM project_membership_transitions WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0 AND (SELECT throwIf(uniq(event_id) > 2000000, 'FORM-7829-3 GATE events: org event_ids over 2000000') FROM project_membership_transitions WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0) = 0 GROUP BY provider, event_id HAVING min(occurred_at) < max(occurred_at)) AS g) = 0
      AND (provider, event_id) IN (
        SELECT provider, event_id
        FROM project_membership_transitions
        WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0 AND (SELECT throwIf(count() > 10000 OR sum(g.n) > 1000000, 'FORM-7829-3 GATE multi: over 10000 multi-occurred events or 1000000 of their rows') FROM (SELECT count() AS n FROM project_membership_transitions WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0 AND (SELECT throwIf(uniq(event_id) > 2000000, 'FORM-7829-3 GATE events: org event_ids over 2000000') FROM project_membership_transitions WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0) = 0 GROUP BY provider, event_id HAVING min(occurred_at) < max(occurred_at)) AS g) = 0
        GROUP BY provider, event_id
        HAVING min(occurred_at) < max(occurred_at)
      )
    GROUP BY org_id, provider, event_id, subject_kind, repo_id, subject_id, occurred_at
    ) AS k
    GROUP BY k.provider, k.event_id
) AS e
ORDER BY event_h8
LIMIT 100;

-- S5 one row per (event, occurred_at) of the S4 events, LIMIT 300. WANT row count = sum of S4 occurred_values (if <= 300).
-- ms_after_first = ms after the event's first occurred_at. Reading: one occurred_at whose max_last_synced_day stopped
--   long ago while the other is recent = the provider changed createdAt upstream and the old key is never retired (old
--   row). Both recent = the provider returns both now (live).
WITH (SELECT throwIf(
    (SELECT sum(rows) FROM system.parts WHERE active AND database = currentDatabase()
      AND table IN ('project_membership_transitions')) > 20000000
    OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = currentDatabase()
      AND table IN ('project_membership_transitions')) > 4294967296,
    'STOP-COST-GATE')) AS cost_gate
SELECT substring(lower(hex(SHA256(k.event_id))), 1, 8) AS event_h8,
       toString(toDate(k.occurred_at, 'UTC')) AS occurred_day,
       toUnixTimestamp64Milli(k.occurred_at)
           - toUnixTimestamp64Milli(min(k.occurred_at) OVER (PARTITION BY k.provider, k.event_id)) AS ms_after_first,
       count() AS key_rows,
       uniqExact(k.subject_kind, k.repo_id, k.subject_id) AS subjects,
       toString(toDate(min(k.ls), 'UTC')) AS min_last_synced_day,
       toString(toDate(max(k.ls), 'UTC')) AS max_last_synced_day,
       toString(toDate(max(k.ing), 'UTC')) AS max_ingested_day
FROM (
    SELECT provider, event_id, subject_kind, repo_id, subject_id, occurred_at,
           max(last_synced) AS ls,
           argMax(ingested_at, last_synced) AS ing
    FROM project_membership_transitions
    WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0 AND (SELECT throwIf(count() > 10000 OR sum(g.n) > 1000000, 'FORM-7829-3 GATE multi: over 10000 multi-occurred events or 1000000 of their rows') FROM (SELECT count() AS n FROM project_membership_transitions WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0 AND (SELECT throwIf(uniq(event_id) > 2000000, 'FORM-7829-3 GATE events: org event_ids over 2000000') FROM project_membership_transitions WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0) = 0 GROUP BY provider, event_id HAVING min(occurred_at) < max(occurred_at)) AS g) = 0
      AND (provider, event_id) IN (
        SELECT provider, event_id
        FROM project_membership_transitions
        WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0 AND (SELECT throwIf(count() > 10000 OR sum(g.n) > 1000000, 'FORM-7829-3 GATE multi: over 10000 multi-occurred events or 1000000 of their rows') FROM (SELECT count() AS n FROM project_membership_transitions WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0 AND (SELECT throwIf(uniq(event_id) > 2000000, 'FORM-7829-3 GATE events: org event_ids over 2000000') FROM project_membership_transitions WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0) = 0 GROUP BY provider, event_id HAVING min(occurred_at) < max(occurred_at)) AS g) = 0
        GROUP BY provider, event_id
        HAVING min(occurred_at) < max(occurred_at)
      )
    GROUP BY org_id, provider, event_id, subject_kind, repo_id, subject_id, occurred_at
) AS k
GROUP BY k.provider, k.event_id, k.occurred_at
ORDER BY event_h8, ms_after_first
LIMIT 300;
`
	formExec14259SQL = `-- FORM-14259-SIZE-3 (lane lformch for fcdev, 2026-11-23). CHAOS-14259 sizing: stored GitHub runs that hold a report slot
--   the current planner (CHAOS-14045, 37b7d692a) would no longer write.
-- Shape: pm-guard/FORM-TEMPLATE-1.md (e220dd03e6fc) (a) ClickHouse. Replaces FORM-14259-SIZE-2 (frozen).
-- ClickHouse, SELECT only, one statement per call. Org prefix c6a38355 on every FROM with org_id. Output = counts, days.
-- Run each statement once, in order. Any SQL error (STOP-COST-GATE and GATE errors included) = STOP, report the text.
-- STOP when S1 suite_orgs != 1.
-- Terms (code at ops 340c3e05e3ca, internal/providersync/github_tests_run_report_rows.go):
--   slot attempt = (test_suite_results.report_slot, run_attempt). The measured slot S is the newest attempt of its slot
--   (readers count only that one). It is compared with every stored slot of the SAME run_attempt (the planner of that
--   attempt saw them together, the suite content holds run_attempt). A suite instance = the suite row without suite_id,
--   report_slot, legacy_suite_ids, last_synced, plus the multiset of its case rows without suite_id, case_id,
--   last_synced (githubTestsReportContent). head = the suite row part only.
-- UPPER (S2) = runs with a slot S whose every suite head has an equal head in the other slots of S's attempt, as many
--   times. Each slot the planner of that attempt drops (copy or lossless aggregate) passes it: a true upper bound
--   within one run_attempt.
-- ESTIMATE (S3), no guarantee in either direction: S has cases and no coverage row, the slots of S's attempt that share
--   an instance with S (P) all lie inside S and sum to S, and either |P| >= 2 with no case identity in two parts
--   (aggregate) or |P| = 1 with a case duration > 0 (copy). It can count a slot the planner keeps: a third slot with
--   the copy key (e_slots_third_copy), case rows with no suite row (e_runs_orphan_cases), partition work over the
--   bound, or a drop undone by admission (e_runs_capped). It can miss a slot the planner drops through a partition
--   among more slots than P.
-- Gates, all scalar subqueries (run at analysis, before any table read), in this order:
--   cost_gate (S1 to S4) = active parts of the tables that statement reads over its row or byte cap: 'STOP-COST-GATE'.
--   suites = org suite rows (raw) over 3000000. slots = a run with over 500 slot attempts. heads = one suite head over
--   16 times in one run attempt (bounds every self-join to 16 x the suite rows). cases (S3 only) = case rows of the
--   S2 runs over 10000000. Each is gated by the one before (cost_gate first).
-- cost_gate = 0 is in the WHERE of every query block that reads a table.
-- Text literals: repos.provider 'github' = github_repository_route.go:206. 'aggregate_mismatch' =
--   github_tests_reports.go:426, 'per_run_cap' = :420.
-- The CTE chain between CHAIN BEGIN and CHAIN END is byte-equal in S2, S3, S4 (the CI test checks it).

-- S0 cost inputs (system.parts only). WANT 5 rows (one per table), table_rows > 0 in each.
SELECT table, sum(rows) AS table_rows, sum(bytes_on_disk) AS disk_bytes, count() AS active_parts
FROM system.parts
WHERE active AND database = currentDatabase()
  AND table IN ('coverage_snapshots', 'repos', 'test_case_results', 'test_suite_results', 'testops_run_report_completeness')
GROUP BY table
ORDER BY table;

-- S1 ORG-1 and size report. WANT suite_orgs = 1, repo_orgs = 1, github_repos > 0, slot_suite_rows_raw > 0.
--   INFO completeness_orgs (0 = every run reads no_row) and raw sizes.
WITH (SELECT throwIf(
    (SELECT sum(rows) FROM system.parts WHERE active AND database = currentDatabase()
      AND table IN ('coverage_snapshots', 'repos', 'test_case_results', 'test_suite_results', 'testops_run_report_completeness')) > 300000000
    OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = currentDatabase()
      AND table IN ('coverage_snapshots', 'repos', 'test_case_results', 'test_suite_results', 'testops_run_report_completeness')) > 68719476736,
    'STOP-COST-GATE')) AS cost_gate
SELECT
    (SELECT uniqExact(org_id) FROM test_suite_results WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0) AS suite_orgs,
    (SELECT uniqExact(org_id) FROM repos WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0) AS repo_orgs,
    (SELECT uniqExact(org_id) FROM testops_run_report_completeness WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0) AS completeness_orgs,
    (SELECT count() FROM test_suite_results WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0) AS suite_rows_raw,
    (SELECT countIf(report_slot != '') FROM test_suite_results WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0) AS slot_suite_rows_raw,
    (SELECT count() FROM test_case_results WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0) AS case_rows_raw,
    (SELECT count() FROM coverage_snapshots WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0) AS coverage_rows_raw,
    (SELECT count() FROM repos FINAL WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0 AND provider = 'github') AS github_repos;

-- S2 UPPER bound. WANT newest_slots > 0 (real rows). WANT u_runs >= S3 e_runs. INFO splits: newest completeness row of
--   the run names aggregate_mismatch (the old planner kept a mismatch there), is complete, or is missing.
--   u_slots_stale = the slot's newest last_synced is older than its run's. u_slots_before_14045_commit = slot
--   last_synced before 2026-11-20 02:45:39 UTC.
WITH (SELECT throwIf(
    (SELECT sum(rows) FROM system.parts WHERE active AND database = currentDatabase()
      AND table IN ('repos', 'test_suite_results', 'testops_run_report_completeness')) > 30000000
    OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = currentDatabase()
      AND table IN ('repos', 'test_suite_results', 'testops_run_report_completeness')) > 8589934592,
    'STOP-COST-GATE')) AS cost_gate,
-- CHAIN BEGIN (byte-equal in S2, S3, S4)
gh AS (
    SELECT id FROM repos FINAL WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0 AND provider = 'github'
),
sa AS (
    SELECT repo_id, run_id, suite_id, report_slot AS slot, run_attempt AS att, last_synced AS ls,
           cityHash64(concat(suite_name, '|', ifNull(framework, '~'), '|', ifNull(environment, '~'), '|',
               toString(total_count), '|', toString(passed_count), '|', toString(failed_count), '|',
               toString(skipped_count), '|', toString(error_count), '|', toString(quarantined_count), '|',
               toString(retried_count), '|', ifNull(toString(duration_seconds), '~'), '|',
               ifNull(toString(started_at), '~'), '|', ifNull(toString(finished_at), '~'), '|',
               ifNull(team_id, '~'), '|', ifNull(service_id, '~'), '|', toString(run_attempt))) AS hk,
           cityHash64(concat(suite_name, '|', ifNull(toString(started_at), 'none'), '|',
               ifNull(toString(duration_seconds), 'none'))) AS sk
    FROM test_suite_results FINAL
    WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0 AND report_slot != '' AND repo_id IN (SELECT id FROM gh)
      AND (SELECT throwIf(max(z.c) > 16, 'FORM-14259-SIZE-3 GATE heads: a suite head over 16 times in one run attempt') FROM (SELECT count() AS c FROM test_suite_results WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0 AND report_slot != '' AND (SELECT throwIf(max(x.slots) > 500, 'FORM-14259-SIZE-3 GATE slots: a run with over 500 slot attempts') FROM (SELECT count() AS slots FROM (SELECT repo_id, run_id FROM test_suite_results WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0 AND report_slot != '' AND (SELECT throwIf(count() > 3000000, 'FORM-14259-SIZE-3 GATE suites: org suite rows over 3000000') FROM test_suite_results WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0) = 0 GROUP BY repo_id, run_id, report_slot, run_attempt) AS y GROUP BY y.repo_id, y.run_id) AS x) = 0 GROUP BY repo_id, run_id, run_attempt,
           cityHash64(concat(suite_name, '|', ifNull(framework, '~'), '|', ifNull(environment, '~'), '|',
               toString(total_count), '|', toString(passed_count), '|', toString(failed_count), '|',
               toString(skipped_count), '|', toString(error_count), '|', toString(quarantined_count), '|',
               toString(retried_count), '|', ifNull(toString(duration_seconds), '~'), '|',
               ifNull(toString(started_at), '~'), '|', ifNull(toString(finished_at), '~'), '|',
               ifNull(team_id, '~'), '|', ifNull(service_id, '~'), '|', toString(run_attempt)))) AS z) = 0
),
nw AS (
    SELECT repo_id, run_id, slot, max(att) AS att FROM sa GROUP BY repo_id, run_id, slot
),
sn AS (
    SELECT * FROM sa WHERE (repo_id, run_id, slot, att) IN (SELECT repo_id, run_id, slot, att FROM nw)
),
hk_slot AS (
    SELECT repo_id, run_id, slot, att, hk, count() AS n FROM sa GROUP BY repo_id, run_id, slot, att, hk
),
hk_run AS (
    SELECT repo_id, run_id, att, hk, count() AS n FROM sa GROUP BY repo_id, run_id, att, hk
),
uslots AS (
    SELECT k.repo_id AS repo_id, k.run_id AS run_id, k.slot AS slot, k.att AS att
    FROM hk_slot AS k
    INNER JOIN hk_run AS r ON r.repo_id = k.repo_id AND r.run_id = k.run_id AND r.att = k.att AND r.hk = k.hk
    WHERE (k.repo_id, k.run_id, k.slot, k.att) IN (SELECT repo_id, run_id, slot, att FROM nw)
    GROUP BY k.repo_id, k.run_id, k.slot, k.att
    HAVING min(r.n >= 2 * k.n) = 1
),
-- CHAIN END
slot_ls AS (
    SELECT repo_id, run_id, slot, max(ls) AS ls FROM sn GROUP BY repo_id, run_id, slot
),
run_ls AS (
    SELECT repo_id, run_id, max(ls) AS ls FROM sn GROUP BY repo_id, run_id
),
comp AS (
    SELECT repo_id, run_id, 1 AS has_row,
           argMax(complete, run_attempt) AS complete,
           argMax(has(reasons, 'aggregate_mismatch'), run_attempt) AS marked,
           argMax(has(reasons, 'per_run_cap'), run_attempt) AS capped
    FROM testops_run_report_completeness FINAL
    WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0
    GROUP BY repo_id, run_id
)
SELECT (SELECT count() FROM nw) AS newest_slots,
       uniqExact(U.repo_id, U.run_id) AS u_runs,
       count() AS u_slots,
       uniqExactIf((U.repo_id, U.run_id), ifNull(c.marked, 0) = 1) AS u_runs_marked_mismatch,
       uniqExactIf((U.repo_id, U.run_id), ifNull(c.has_row, 0) = 1 AND ifNull(c.complete, 0) = 1) AS u_runs_complete,
       uniqExactIf((U.repo_id, U.run_id), ifNull(c.has_row, 0) = 0) AS u_runs_no_row,
       countIf(sl.ls < rl.ls) AS u_slots_stale,
       countIf(sl.ls < toDateTime64('2026-11-20 02:45:39', 3, 'UTC')) AS u_slots_before_14045_commit,
       toString(toDate(min(sl.ls), 'UTC')) AS u_first_day,
       toString(toDate(max(sl.ls), 'UTC')) AS u_last_day
FROM uslots AS U
INNER JOIN slot_ls AS sl ON sl.repo_id = U.repo_id AND sl.run_id = U.run_id AND sl.slot = U.slot
INNER JOIN run_ls AS rl ON rl.repo_id = U.repo_id AND rl.run_id = U.run_id
LEFT JOIN comp AS c ON c.repo_id = U.repo_id AND c.run_id = U.run_id;

-- S3 ESTIMATE (the S2 runs only), no guarantee. WANT newest_slots > 0 (real rows). INFO e_runs and splits. Caveat
--   counts (WANT 0, else the estimate is weaker): e_slots_third_copy, e_runs_orphan_cases, e_runs_capped.
WITH (SELECT throwIf(
    (SELECT sum(rows) FROM system.parts WHERE active AND database = currentDatabase()
      AND table IN ('coverage_snapshots', 'repos', 'test_case_results', 'test_suite_results', 'testops_run_report_completeness')) > 300000000
    OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = currentDatabase()
      AND table IN ('coverage_snapshots', 'repos', 'test_case_results', 'test_suite_results', 'testops_run_report_completeness')) > 68719476736,
    'STOP-COST-GATE')) AS cost_gate,
-- CHAIN BEGIN (byte-equal in S2, S3, S4)
gh AS (
    SELECT id FROM repos FINAL WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0 AND provider = 'github'
),
sa AS (
    SELECT repo_id, run_id, suite_id, report_slot AS slot, run_attempt AS att, last_synced AS ls,
           cityHash64(concat(suite_name, '|', ifNull(framework, '~'), '|', ifNull(environment, '~'), '|',
               toString(total_count), '|', toString(passed_count), '|', toString(failed_count), '|',
               toString(skipped_count), '|', toString(error_count), '|', toString(quarantined_count), '|',
               toString(retried_count), '|', ifNull(toString(duration_seconds), '~'), '|',
               ifNull(toString(started_at), '~'), '|', ifNull(toString(finished_at), '~'), '|',
               ifNull(team_id, '~'), '|', ifNull(service_id, '~'), '|', toString(run_attempt))) AS hk,
           cityHash64(concat(suite_name, '|', ifNull(toString(started_at), 'none'), '|',
               ifNull(toString(duration_seconds), 'none'))) AS sk
    FROM test_suite_results FINAL
    WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0 AND report_slot != '' AND repo_id IN (SELECT id FROM gh)
      AND (SELECT throwIf(max(z.c) > 16, 'FORM-14259-SIZE-3 GATE heads: a suite head over 16 times in one run attempt') FROM (SELECT count() AS c FROM test_suite_results WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0 AND report_slot != '' AND (SELECT throwIf(max(x.slots) > 500, 'FORM-14259-SIZE-3 GATE slots: a run with over 500 slot attempts') FROM (SELECT count() AS slots FROM (SELECT repo_id, run_id FROM test_suite_results WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0 AND report_slot != '' AND (SELECT throwIf(count() > 3000000, 'FORM-14259-SIZE-3 GATE suites: org suite rows over 3000000') FROM test_suite_results WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0) = 0 GROUP BY repo_id, run_id, report_slot, run_attempt) AS y GROUP BY y.repo_id, y.run_id) AS x) = 0 GROUP BY repo_id, run_id, run_attempt,
           cityHash64(concat(suite_name, '|', ifNull(framework, '~'), '|', ifNull(environment, '~'), '|',
               toString(total_count), '|', toString(passed_count), '|', toString(failed_count), '|',
               toString(skipped_count), '|', toString(error_count), '|', toString(quarantined_count), '|',
               toString(retried_count), '|', ifNull(toString(duration_seconds), '~'), '|',
               ifNull(toString(started_at), '~'), '|', ifNull(toString(finished_at), '~'), '|',
               ifNull(team_id, '~'), '|', ifNull(service_id, '~'), '|', toString(run_attempt)))) AS z) = 0
),
nw AS (
    SELECT repo_id, run_id, slot, max(att) AS att FROM sa GROUP BY repo_id, run_id, slot
),
sn AS (
    SELECT * FROM sa WHERE (repo_id, run_id, slot, att) IN (SELECT repo_id, run_id, slot, att FROM nw)
),
hk_slot AS (
    SELECT repo_id, run_id, slot, att, hk, count() AS n FROM sa GROUP BY repo_id, run_id, slot, att, hk
),
hk_run AS (
    SELECT repo_id, run_id, att, hk, count() AS n FROM sa GROUP BY repo_id, run_id, att, hk
),
uslots AS (
    SELECT k.repo_id AS repo_id, k.run_id AS run_id, k.slot AS slot, k.att AS att
    FROM hk_slot AS k
    INNER JOIN hk_run AS r ON r.repo_id = k.repo_id AND r.run_id = k.run_id AND r.att = k.att AND r.hk = k.hk
    WHERE (k.repo_id, k.run_id, k.slot, k.att) IN (SELECT repo_id, run_id, slot, att FROM nw)
    GROUP BY k.repo_id, k.run_id, k.slot, k.att
    HAVING min(r.n >= 2 * k.n) = 1
),
-- CHAIN END
slot_ls AS (
    SELECT repo_id, run_id, slot, max(ls) AS ls FROM sn GROUP BY repo_id, run_id, slot
),
run_ls AS (
    SELECT repo_id, run_id, max(ls) AS ls FROM sn GROUP BY repo_id, run_id
),
comp AS (
    SELECT repo_id, run_id, 1 AS has_row,
           argMax(complete, run_attempt) AS complete,
           argMax(has(reasons, 'aggregate_mismatch'), run_attempt) AS marked,
           argMax(has(reasons, 'per_run_cap'), run_attempt) AS capped
    FROM testops_run_report_completeness FINAL
    WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0
    GROUP BY repo_id, run_id
),
cand AS (
    SELECT DISTINCT repo_id, run_id FROM uslots
),
cs AS (
    SELECT repo_id, run_id, suite_id,
           count() AS n,
           sum(cityHash64(concat(case_name, '|', ifNull(class_name, '~'), '|', toString(status), '|',
               ifNull(toString(duration_seconds), '~'), '|', toString(retry_attempt), '|', ifNull(failure_message, '~'), '|',
               ifNull(failure_type, '~'), '|', ifNull(stack_trace, '~'), '|', toString(is_quarantined)))) AS h,
           sum(cityHash64(concat(ifNull(class_name, ''), '|', case_name, '|', toString(status), '|',
               ifNull(toString(duration_seconds), 'none')))) AS ch,
           max(ifNull(duration_seconds, 0) > 0) AS timed,
           groupUniqArray(cityHash64(concat(ifNull(class_name, ''), '|', case_name))) AS idks
    FROM test_case_results FINAL
    WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0 AND (repo_id, run_id) IN (SELECT repo_id, run_id FROM cand)
      AND (SELECT throwIf(count() > 10000000, 'FORM-14259-SIZE-3 GATE cases: case rows of the candidate runs over 10000000') FROM test_case_results WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0 AND (repo_id, run_id) IN (SELECT repo_id, run_id FROM cand)) = 0
    GROUP BY repo_id, run_id, suite_id
),
orph AS (
    SELECT DISTINCT repo_id, run_id FROM cs
    WHERE (repo_id, run_id, suite_id) NOT IN (SELECT repo_id, run_id, suite_id FROM sa)
),
inst AS (
    SELECT s.repo_id AS repo_id, s.run_id AS run_id, s.slot AS slot, s.att AS att,
           cityHash64(concat(toString(s.hk), '|', toString(ifNull(c.n, 0)), '|', toString(ifNull(c.h, 0)))) AS ik,
           s.sk AS sk, ifNull(c.ch, 0) AS ch,
           ifNull(c.n, 0) AS n, ifNull(c.timed, 0) AS timed, c.idks AS idks
    FROM sa AS s
    LEFT JOIN cs AS c ON c.repo_id = s.repo_id AND c.run_id = s.run_id AND c.suite_id = s.suite_id
    WHERE (s.repo_id, s.run_id) IN (SELECT repo_id, run_id FROM cand)
),
slot_tot AS (
    SELECT repo_id, run_id, slot, att, count() AS inst_n, sum(n) AS cases, max(timed) AS timed,
           length(groupUniqArrayArray(idks)) AS idents,
           cityHash64(concat(toString(sum(sk)), '|', toString(sum(ch)), '|', toString(count()), '|', toString(sum(n)))) AS ck
    FROM inst GROUP BY repo_id, run_id, slot, att
),
slot_inst AS (
    SELECT repo_id, run_id, slot, att, ik, count() AS k FROM inst GROUP BY repo_id, run_id, slot, att, ik
),
run_inst AS (
    SELECT repo_id, run_id, att, ik, count() AS k FROM inst GROUP BY repo_id, run_id, att, ik
),
cond_a AS (
    SELECT si.repo_id AS repo_id, si.run_id AS run_id, si.slot AS slot, si.att AS att
    FROM slot_inst AS si
    INNER JOIN run_inst AS ri ON ri.repo_id = si.repo_id AND ri.run_id = si.run_id AND ri.att = si.att AND ri.ik = si.ik
    WHERE (si.repo_id, si.run_id, si.slot, si.att) IN (SELECT repo_id, run_id, slot, att FROM nw)
    GROUP BY si.repo_id, si.run_id, si.slot, si.att
    HAVING min(ri.k = 2 * si.k) = 1
),
pairs AS (
    SELECT a.repo_id AS repo_id, a.run_id AS run_id, a.att AS att, a.slot AS s_slot, b.slot AS t_slot,
           sum(least(a.k, b.k)) AS matched
    FROM slot_inst AS a
    INNER JOIN slot_inst AS b ON b.repo_id = a.repo_id AND b.run_id = a.run_id AND b.att = a.att AND b.ik = a.ik
    WHERE b.slot != a.slot
    GROUP BY a.repo_id, a.run_id, a.att, a.slot, b.slot
),
pstat AS (
    SELECT p.repo_id AS repo_id, p.run_id AS run_id, p.att AS att, p.s_slot AS slot,
           count() AS parts, min(p.matched = t.inst_n) AS all_within,
           sum(t.idents) AS part_idents, min(t.cases) AS min_part_cases
    FROM pairs AS p
    INNER JOIN slot_tot AS t ON t.repo_id = p.repo_id AND t.run_id = p.run_id AND t.att = p.att AND t.slot = p.t_slot
    GROUP BY p.repo_id, p.run_id, p.att, p.s_slot
),
cov AS (
    SELECT DISTINCT repo_id, run_id, report_slot AS slot, run_attempt AS att, 1 AS has_cov
    FROM coverage_snapshots FINAL
    WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0 AND report_slot != '' AND (repo_id, run_id) IN (SELECT repo_id, run_id FROM cand)
),
eslots AS (
    SELECT q.repo_id AS repo_id, q.run_id AS run_id, q.slot AS slot, q.att AS att, ps.parts AS parts
    FROM cond_a AS q
    INNER JOIN pstat AS ps ON ps.repo_id = q.repo_id AND ps.run_id = q.run_id AND ps.att = q.att AND ps.slot = q.slot
    INNER JOIN slot_tot AS st ON st.repo_id = q.repo_id AND st.run_id = q.run_id AND st.att = q.att AND st.slot = q.slot
    LEFT JOIN cov AS v ON v.repo_id = q.repo_id AND v.run_id = q.run_id AND v.slot = q.slot AND v.att = q.att
    WHERE ps.all_within = 1 AND ps.min_part_cases > 0 AND st.cases > 0 AND ifNull(v.has_cov, 0) = 0
      AND ((ps.parts >= 2 AND ps.part_idents = st.idents) OR (ps.parts = 1 AND st.timed = 1))
),
third AS (
    SELECT a.repo_id AS repo_id, a.run_id AS run_id, a.slot AS slot, count() AS q
    FROM slot_tot AS a
    INNER JOIN slot_tot AS b ON b.repo_id = a.repo_id AND b.run_id = a.run_id AND b.att = a.att AND b.ck = a.ck
    WHERE b.slot != a.slot
      AND (a.repo_id, a.run_id, a.att, a.slot, b.slot) NOT IN (SELECT repo_id, run_id, att, s_slot, t_slot FROM pairs)
    GROUP BY a.repo_id, a.run_id, a.slot
)
SELECT (SELECT count() FROM nw) AS newest_slots,
       uniqExact(E.repo_id, E.run_id) AS e_runs,
       uniqExactIf((E.repo_id, E.run_id), E.parts >= 2) AS e_runs_aggregate,
       countIf(E.parts >= 2) AS e_slots_aggregate,
       uniqExactIf((E.repo_id, E.run_id), E.parts = 1) AS e_runs_copy,
       uniqExactIf((E.repo_id, E.run_id), ifNull(c.marked, 0) = 1) AS e_runs_marked_mismatch,
       uniqExactIf((E.repo_id, E.run_id), ifNull(c.has_row, 0) = 1 AND ifNull(c.complete, 0) = 1) AS e_runs_complete,
       uniqExactIf((E.repo_id, E.run_id), ifNull(c.has_row, 0) = 0) AS e_runs_no_row,
       countIf(sl.ls < rl.ls) AS e_slots_stale,
       countIf(sl.ls < toDateTime64('2026-11-20 02:45:39', 3, 'UTC')) AS e_slots_before_14045_commit,
       countIf((E.repo_id, E.run_id, E.slot) IN (SELECT repo_id, run_id, slot FROM third)) AS e_slots_third_copy,
       uniqExactIf((E.repo_id, E.run_id), (E.repo_id, E.run_id) IN (SELECT repo_id, run_id FROM orph)) AS e_runs_orphan_cases,
       uniqExactIf((E.repo_id, E.run_id), ifNull(c.capped, 0) = 1) AS e_runs_capped
FROM eslots AS E
INNER JOIN slot_ls AS sl ON sl.repo_id = E.repo_id AND sl.run_id = E.run_id AND sl.slot = E.slot
INNER JOIN run_ls AS rl ON rl.repo_id = E.repo_id AND rl.run_id = E.run_id
LEFT JOIN comp AS c ON c.repo_id = E.repo_id AND c.run_id = E.run_id;

-- S4 CONTROL: S2 u_slots recomputed by window functions over its own read (no shared CTE). WANT u_slots_cte =
--   u_slots_window, newest_slots > 0 (real rows).
WITH (SELECT throwIf(
    (SELECT sum(rows) FROM system.parts WHERE active AND database = currentDatabase()
      AND table IN ('repos', 'test_suite_results')) > 30000000
    OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = currentDatabase()
      AND table IN ('repos', 'test_suite_results')) > 8589934592,
    'STOP-COST-GATE')) AS cost_gate,
-- CHAIN BEGIN (byte-equal in S2, S3, S4)
gh AS (
    SELECT id FROM repos FINAL WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0 AND provider = 'github'
),
sa AS (
    SELECT repo_id, run_id, suite_id, report_slot AS slot, run_attempt AS att, last_synced AS ls,
           cityHash64(concat(suite_name, '|', ifNull(framework, '~'), '|', ifNull(environment, '~'), '|',
               toString(total_count), '|', toString(passed_count), '|', toString(failed_count), '|',
               toString(skipped_count), '|', toString(error_count), '|', toString(quarantined_count), '|',
               toString(retried_count), '|', ifNull(toString(duration_seconds), '~'), '|',
               ifNull(toString(started_at), '~'), '|', ifNull(toString(finished_at), '~'), '|',
               ifNull(team_id, '~'), '|', ifNull(service_id, '~'), '|', toString(run_attempt))) AS hk,
           cityHash64(concat(suite_name, '|', ifNull(toString(started_at), 'none'), '|',
               ifNull(toString(duration_seconds), 'none'))) AS sk
    FROM test_suite_results FINAL
    WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0 AND report_slot != '' AND repo_id IN (SELECT id FROM gh)
      AND (SELECT throwIf(max(z.c) > 16, 'FORM-14259-SIZE-3 GATE heads: a suite head over 16 times in one run attempt') FROM (SELECT count() AS c FROM test_suite_results WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0 AND report_slot != '' AND (SELECT throwIf(max(x.slots) > 500, 'FORM-14259-SIZE-3 GATE slots: a run with over 500 slot attempts') FROM (SELECT count() AS slots FROM (SELECT repo_id, run_id FROM test_suite_results WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0 AND report_slot != '' AND (SELECT throwIf(count() > 3000000, 'FORM-14259-SIZE-3 GATE suites: org suite rows over 3000000') FROM test_suite_results WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0) = 0 GROUP BY repo_id, run_id, report_slot, run_attempt) AS y GROUP BY y.repo_id, y.run_id) AS x) = 0 GROUP BY repo_id, run_id, run_attempt,
           cityHash64(concat(suite_name, '|', ifNull(framework, '~'), '|', ifNull(environment, '~'), '|',
               toString(total_count), '|', toString(passed_count), '|', toString(failed_count), '|',
               toString(skipped_count), '|', toString(error_count), '|', toString(quarantined_count), '|',
               toString(retried_count), '|', ifNull(toString(duration_seconds), '~'), '|',
               ifNull(toString(started_at), '~'), '|', ifNull(toString(finished_at), '~'), '|',
               ifNull(team_id, '~'), '|', ifNull(service_id, '~'), '|', toString(run_attempt)))) AS z) = 0
),
nw AS (
    SELECT repo_id, run_id, slot, max(att) AS att FROM sa GROUP BY repo_id, run_id, slot
),
sn AS (
    SELECT * FROM sa WHERE (repo_id, run_id, slot, att) IN (SELECT repo_id, run_id, slot, att FROM nw)
),
hk_slot AS (
    SELECT repo_id, run_id, slot, att, hk, count() AS n FROM sa GROUP BY repo_id, run_id, slot, att, hk
),
hk_run AS (
    SELECT repo_id, run_id, att, hk, count() AS n FROM sa GROUP BY repo_id, run_id, att, hk
),
uslots AS (
    SELECT k.repo_id AS repo_id, k.run_id AS run_id, k.slot AS slot, k.att AS att
    FROM hk_slot AS k
    INNER JOIN hk_run AS r ON r.repo_id = k.repo_id AND r.run_id = k.run_id AND r.att = k.att AND r.hk = k.hk
    WHERE (k.repo_id, k.run_id, k.slot, k.att) IN (SELECT repo_id, run_id, slot, att FROM nw)
    GROUP BY k.repo_id, k.run_id, k.slot, k.att
    HAVING min(r.n >= 2 * k.n) = 1
),
-- CHAIN END
alt AS (
    SELECT w.repo_id AS repo_id, w.run_id AS run_id, w.slot AS slot, w.att AS att,
           min(w.run_n >= 2 * w.slot_n) AS twinned, max(w.newest) AS is_newest
    FROM (
        SELECT repo_id, run_id, report_slot AS slot, run_attempt AS att,
               run_attempt = max(run_attempt) OVER (PARTITION BY repo_id, run_id, report_slot) AS newest,
               count() OVER (PARTITION BY repo_id, run_id, run_attempt,
                   cityHash64(concat(suite_name, '|', ifNull(framework, '~'), '|', ifNull(environment, '~'), '|',
               toString(total_count), '|', toString(passed_count), '|', toString(failed_count), '|',
               toString(skipped_count), '|', toString(error_count), '|', toString(quarantined_count), '|',
               toString(retried_count), '|', ifNull(toString(duration_seconds), '~'), '|',
               ifNull(toString(started_at), '~'), '|', ifNull(toString(finished_at), '~'), '|',
               ifNull(team_id, '~'), '|', ifNull(service_id, '~'), '|', toString(run_attempt)))) AS run_n,
               count() OVER (PARTITION BY repo_id, run_id, report_slot, run_attempt,
                   cityHash64(concat(suite_name, '|', ifNull(framework, '~'), '|', ifNull(environment, '~'), '|',
               toString(total_count), '|', toString(passed_count), '|', toString(failed_count), '|',
               toString(skipped_count), '|', toString(error_count), '|', toString(quarantined_count), '|',
               toString(retried_count), '|', ifNull(toString(duration_seconds), '~'), '|',
               ifNull(toString(started_at), '~'), '|', ifNull(toString(finished_at), '~'), '|',
               ifNull(team_id, '~'), '|', ifNull(service_id, '~'), '|', toString(run_attempt)))) AS slot_n
        FROM test_suite_results FINAL
        WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0 AND report_slot != ''
          AND repo_id IN (SELECT id FROM repos FINAL WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0 AND provider = 'github')
          AND (SELECT throwIf(max(z.c) > 16, 'FORM-14259-SIZE-3 GATE heads: a suite head over 16 times in one run attempt') FROM (SELECT count() AS c FROM test_suite_results WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0 AND report_slot != '' AND (SELECT throwIf(max(x.slots) > 500, 'FORM-14259-SIZE-3 GATE slots: a run with over 500 slot attempts') FROM (SELECT count() AS slots FROM (SELECT repo_id, run_id FROM test_suite_results WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0 AND report_slot != '' AND (SELECT throwIf(count() > 3000000, 'FORM-14259-SIZE-3 GATE suites: org suite rows over 3000000') FROM test_suite_results WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0) = 0 GROUP BY repo_id, run_id, report_slot, run_attempt) AS y GROUP BY y.repo_id, y.run_id) AS x) = 0 GROUP BY repo_id, run_id, run_attempt,
           cityHash64(concat(suite_name, '|', ifNull(framework, '~'), '|', ifNull(environment, '~'), '|',
               toString(total_count), '|', toString(passed_count), '|', toString(failed_count), '|',
               toString(skipped_count), '|', toString(error_count), '|', toString(quarantined_count), '|',
               toString(retried_count), '|', ifNull(toString(duration_seconds), '~'), '|',
               ifNull(toString(started_at), '~'), '|', ifNull(toString(finished_at), '~'), '|',
               ifNull(team_id, '~'), '|', ifNull(service_id, '~'), '|', toString(run_attempt)))) AS z) = 0
    ) AS w
    GROUP BY w.repo_id, w.run_id, w.slot, w.att
)
SELECT (SELECT count() FROM uslots) AS u_slots_cte,
       (SELECT countIf(twinned = 1 AND is_newest = 1) FROM alt) AS u_slots_window,
       (SELECT count() FROM nw) AS newest_slots;
`
)

type formExecStatement struct {
	id, text string
}

type formExecResult struct {
	columns []string
	rows    [][]string
}

func formExecOpen(t *testing.T) (context.Context, driver.Conn) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = instance.Close(closeCtx)
	})
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return ctx, conn
}

func formExecExec(ctx context.Context, t *testing.T, conn driver.Conn, statement string) {
	t.Helper()
	if err := conn.Exec(ctx, statement); err != nil {
		t.Fatalf("%s: %v", statement, err)
	}
}

// formExecStatements checks the sha256 of the FORM text and splits it (formExecSplit).
func formExecStatements(t *testing.T, name, text, wantSHA string, want int) []formExecStatement {
	t.Helper()
	sum := sha256.Sum256([]byte(text))
	if got := hex.EncodeToString(sum[:]); got != wantSHA {
		t.Fatalf("%s: sha256 of the embedded text = %s, want %s (the .sql file)", name, got, wantSHA)
	}
	return formExecSplit(t, name, text, want)
}

// formExecLowered is a copy of the FORM text with one gate's bound literal(s) replaced by a small value: old must occur
// exactly n times, the replacement nowhere, so the copy differs from the FORM only in those literals.
func formExecLowered(t *testing.T, text, old, replacement string, n int) string {
	t.Helper()
	if got := strings.Count(text, old); got != n {
		t.Fatalf("gate literal %q occurs %d times, want %d", old, got, n)
	}
	if strings.Contains(text, replacement) {
		t.Fatalf("lowered literal %q already occurs in the FORM text", replacement)
	}
	lowered := strings.ReplaceAll(text, old, replacement)
	if strings.Count(lowered, replacement) != n || strings.ReplaceAll(lowered, replacement, old) != text {
		t.Fatalf("lowered copy differs from the FORM text in more than the %d literal(s) %q", n, old)
	}
	return lowered
}

// formExecSplit splits the FORM text as pm runs it: the pieces between ";\n", each named by its "-- S<n> ..." line.
// A piece with no SQL line (only comments) is not a statement.
func formExecSplit(t *testing.T, name, text string, want int) []formExecStatement {
	t.Helper()
	var statements []formExecStatement
	for _, piece := range strings.Split(text, ";\n") {
		statement := strings.TrimSpace(piece)
		id, hasSQL := "", false
		for _, line := range strings.Split(statement, "\n") {
			if strings.HasPrefix(line, "--") {
				if id == "" && len(line) > 4 && strings.HasPrefix(line, "-- S") && line[4] >= '0' && line[4] <= '9' {
					id = strings.Fields(line)[1]
				}
				continue
			}
			if strings.TrimSpace(line) != "" {
				hasSQL = true
			}
		}
		if !hasSQL {
			continue
		}
		if id == "" {
			t.Fatalf("%s: a statement has no \"-- S<n>\" line:\n%s", name, statement)
		}
		statements = append(statements, formExecStatement{id: id, text: statement})
	}
	if len(statements) != want {
		t.Fatalf("%s: %d statements, want %d", name, len(statements), want)
	}
	return statements
}

func formExecQuery(ctx context.Context, conn driver.Conn, text string) (formExecResult, error) {
	rows, err := conn.Query(ctx, text)
	if err != nil {
		return formExecResult{}, err
	}
	defer func() { _ = rows.Close() }()
	result := formExecResult{columns: rows.Columns()}
	types := rows.ColumnTypes()
	for rows.Next() {
		dest := make([]any, len(types))
		for index, columnType := range types {
			dest[index] = reflect.New(columnType.ScanType()).Interface()
		}
		if err := rows.Scan(dest...); err != nil {
			return formExecResult{}, err
		}
		row := make([]string, len(dest))
		for index := range dest {
			value := reflect.ValueOf(dest[index]).Elem()
			for value.Kind() == reflect.Pointer && !value.IsNil() {
				value = value.Elem()
			}
			if value.Kind() == reflect.Pointer {
				row[index] = "NULL"
				continue
			}
			row[index] = fmt.Sprint(value.Interface())
		}
		result.rows = append(result.rows, row)
	}
	return result, rows.Err()
}

func formExecLog(t *testing.T, name, id string, result formExecResult) {
	t.Helper()
	t.Logf("%s %s columns %v", name, id, result.columns)
	for index, row := range result.rows {
		t.Logf("%s %s row %d %v", name, id, index, row)
	}
}

func formExecRunAll(ctx context.Context, t *testing.T, conn driver.Conn, name string, statements []formExecStatement) map[string]formExecResult {
	t.Helper()
	results := map[string]formExecResult{}
	for _, statement := range statements {
		result, err := formExecQuery(ctx, conn, statement.text)
		if err != nil {
			t.Fatalf("%s %s: %v", name, statement.id, err)
		}
		formExecLog(t, name, statement.id, result)
		results[statement.id] = result
	}
	return results
}

// formExecGateCell runs every statement on an over-bound fixture: the statements in fire must fail with the gate text,
// every other statement must run.
func formExecGateCell(ctx context.Context, t *testing.T, conn driver.Conn, name string, statements []formExecStatement, gate string, fire ...string) {
	t.Helper()
	firing := map[string]bool{}
	for _, id := range fire {
		firing[id] = true
	}
	for _, statement := range statements {
		result, err := formExecQuery(ctx, conn, statement.text)
		switch {
		case firing[statement.id] && err == nil:
			t.Errorf("%s %s: ran on the over-bound fixture, want the error %q", name, statement.id, gate)
		case firing[statement.id] && !strings.Contains(err.Error(), gate):
			t.Errorf("%s %s: error %v, want the error %q", name, statement.id, err, gate)
		case firing[statement.id]:
			t.Logf("%s %s: gate fired: %v", name, statement.id, err)
		case err != nil:
			t.Errorf("%s %s: %v, want no gate here", name, statement.id, err)
		default:
			formExecLog(t, name, statement.id, result)
		}
	}
}

func formExecFind(t *testing.T, statements []formExecStatement, id string) string {
	t.Helper()
	for _, statement := range statements {
		if statement.id == id {
			return statement.text
		}
	}
	t.Fatalf("no statement %s", id)
	return ""
}

func formExecMutate(t *testing.T, text, old, replacement string) string {
	t.Helper()
	if !strings.Contains(text, old) {
		t.Fatalf("mutation target %q not in the statement", old)
	}
	return strings.Replace(text, old, replacement, 1)
}

func formExecCell(t *testing.T, results map[string]formExecResult, id string, row int, column string) string {
	t.Helper()
	result, ok := results[id]
	if !ok {
		t.Fatalf("%s: not run", id)
	}
	if row >= len(result.rows) {
		t.Fatalf("%s: %d row(s), want a row %d", id, len(result.rows), row)
	}
	for index, name := range result.columns {
		if name == column {
			return result.rows[row][index]
		}
	}
	t.Fatalf("%s: no column %s in %v", id, column, result.columns)
	return ""
}

func formExecWant(t *testing.T, results map[string]formExecResult, id string, row int, column, want string) {
	t.Helper()
	if got := formExecCell(t, results, id, row, column); got != want {
		t.Errorf("%s row %d %s = %s, want %s", id, row, column, got, want)
	}
}

func formExecWantRange(t *testing.T, results map[string]formExecResult, id string, row int, column string, low, high int64) {
	t.Helper()
	got, err := strconv.ParseInt(formExecCell(t, results, id, row, column), 10, 64)
	if err != nil || got < low || got > high {
		t.Errorf("%s row %d %s = %d (%v), want %d..%d", id, row, column, got, err, low, high)
	}
}

func formExecWantRows(t *testing.T, results map[string]formExecResult, id string, want int) {
	t.Helper()
	if got := len(results[id].rows); got != want {
		t.Errorf("%s: %d row(s), want %d", id, got, want)
	}
}

var formExecCapPattern = regexp.MustCompile(`table IN \(([^)]*)\)\) > ([0-9]+)`)

// formExecCostGates checks the FORM-TEMPLATE-1 (a) cost gate of every statement but S0, on the fixture's tables: one gate
// per statement, its table list = the tables the statement reads (of known), and each cap set to 0 (one at a time, the
// rest byte for byte) stops the statement with code 395 STOP-COST-GATE, typed ExceptionBeforeStart in the query log,
// with read_rows <= 2 x P + 1 (P = the active parts the gate lists).
func formExecCostGates(ctx context.Context, t *testing.T, conn driver.Conn, name string, statements []formExecStatement, known []string) {
	t.Helper()
	version, err := formExecQuery(ctx, conn, "SELECT version()")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%s: ClickHouse version %v", name, version.rows)
	type fire struct {
		id, comment string
		parts       uint64
	}
	var fires []fire
	for _, statement := range statements {
		// The checks and the cap-0 copies read the SQL without its comment lines: a comment can neither satisfy nor
		// break them, and the copy differs from the statement only in comments and the one cap.
		text := formExecSQLOnly(statement.text)
		if statement.id == "S0" {
			if strings.Contains(text, "STOP-COST-GATE") {
				t.Errorf("%s S0: reads system.parts only, want no gate", name)
			}
			continue
		}
		if strings.Count(text, "'STOP-COST-GATE'") != 1 || strings.Count(text, "WITH (SELECT throwIf(") != 1 {
			t.Errorf("%s %s: want exactly one WITH (SELECT throwIf(... 'STOP-COST-GATE')) AS cost_gate", name, statement.id)
			continue
		}
		gateEnd := strings.Index(text, "'STOP-COST-GATE'")
		caps := formExecCapPattern.FindAllStringSubmatchIndex(text[:gateEnd], -1)
		if len(caps) != 2 || text[caps[0][2]:caps[0][3]] != text[caps[1][2]:caps[1][3]] {
			t.Errorf("%s %s: want a rows cap and a bytes cap over one table list", name, statement.id)
			continue
		}
		list := text[caps[0][2]:caps[0][3]]
		for _, table := range known {
			listed := strings.Contains(list, "'"+table+"'")
			read := strings.Contains(text, "FROM "+table+" ") || strings.Contains(text, "FROM "+table+"\n")
			if listed != read {
				t.Errorf("%s %s: table %s listed in the gate %v, read %v", name, statement.id, table, listed, read)
			}
		}
		var parts uint64
		if err := conn.QueryRow(ctx, "SELECT count() FROM system.parts WHERE active AND database = currentDatabase() AND table IN ("+list+")").Scan(&parts); err != nil {
			t.Fatalf("%s %s: parts: %v", name, statement.id, err)
		}
		for index, capIndex := range caps {
			lowered := text[:capIndex[4]] + "0" + text[capIndex[5]:]
			comment := fmt.Sprintf("fcdev-form-%s-%s-cap%d-%d", strings.ReplaceAll(name, " ", "-"), statement.id, index, time.Now().UnixNano())
			qctx := clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{"log_comment": comment}))
			_, err := formExecQuery(qctx, conn, lowered)
			if err == nil || !strings.Contains(err.Error(), "STOP-COST-GATE") || !strings.Contains(err.Error(), "395") {
				t.Errorf("%s %s cap %d set to 0: err = %v, want code 395 STOP-COST-GATE", name, statement.id, index, err)
				continue
			}
			t.Logf("%s %s cap %d set to 0: %s", name, statement.id, index, strings.SplitN(err.Error(), "\n", 2)[0])
			fires = append(fires, fire{statement.id, comment, parts})
		}
	}
	if len(fires) != 2*(len(statements)-1) {
		t.Errorf("%s: %d cost gate fires, want %d", name, len(fires), 2*(len(statements)-1))
	}
	formExecExec(ctx, t, conn, "SYSTEM FLUSH LOGS")
	for _, f := range fires {
		rows, err := conn.Query(ctx, "SELECT toString(type), read_rows FROM system.query_log WHERE log_comment = ? AND type != 'QueryStart'", f.comment)
		if err != nil {
			t.Fatalf("%s %s: query log: %v", name, f.id, err)
		}
		var types []string
		var read uint64
		for rows.Next() {
			var typ string
			var r uint64
			if err := rows.Scan(&typ, &r); err != nil {
				t.Fatalf("%s %s: query log scan: %v", name, f.id, err)
			}
			types = append(types, typ)
			read += r
		}
		_ = rows.Close()
		t.Logf("%s %s fire: query_log types %v read_rows %d parts %d", name, f.id, types, read, f.parts)
		if len(types) != 1 || types[0] != "ExceptionBeforeStart" || read > 2*f.parts+1 {
			t.Errorf("%s %s fire: query_log types %v read_rows %d, want [ExceptionBeforeStart] and <= 2 x %d + 1", name, f.id, types, read, f.parts)
		}
	}
}

// formExecSQLOnly drops every line whose first non-blank characters are "--" (the FORM's comment lines).
func formExecSQLOnly(text string) string {
	var lines []string
	for _, line := range strings.Split(text, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "--") {
			continue
		}
		lines = append(lines, line)
	}
	return strings.Join(lines, "\n")
}

// formExecPartsInfo checks S0: one row per table, table_rows > 0.
func formExecPartsInfo(t *testing.T, results map[string]formExecResult, tables []string) {
	t.Helper()
	formExecWantRows(t, results, "S0", len(tables))
	for row, table := range tables {
		formExecWant(t, results, "S0", row, "table", table)
		formExecWantRange(t, results, "S0", row, "table_rows", 1, 1<<40)
	}
}

func formExecH8(eventID string) string {
	sum := sha256.Sum256([]byte(eventID))
	return hex.EncodeToString(sum[:])[:8]
}

const (
	formExec7829Org   = "c6a38355-7829-4000-8000-000000000001"
	formExec7829Other = "d6a38355-7829-4000-8000-000000000002"
	formExec7829Cols  = "org_id, repo_id, subject_kind, subject_id, provider, from_project_id, to_project_id, from_project_key, to_project_key, actor, occurred_at, last_synced, event_id"
)

// TestFormExecFcdevCHAOS7829: the fixture (org c6a38355-... is read, org d6a38355-... holds the same event id and must not
// count): event a has two occurred_at (250 ms apart), one subject, and an older version of one key; event b one occurred_at
// and two subjects; event c two occurred_at, two subjects, one row synced before the linear cut; event d a jira hash id;
// event e two occurred_at 1.5 s apart; event f (github) two occurred_at, one row synced before the github cut and one
// synced after the github cut and before the linear cut (counts only under the github cut).
func TestFormExecFcdevCHAOS7829(t *testing.T) {
	statements := formExecStatements(t, "FORM-7829-3", formExec7829SQL, formExec7829SHA256, 6)

	t.Run("fixture", func(t *testing.T) {
		ctx, conn := formExecOpen(t)
		insert := func(orgID, provider, subject, occurred, lastSynced, eventID string) {
			formExecExec(ctx, t, conn, fmt.Sprintf(`INSERT INTO project_membership_transitions (`+formExec7829Cols+`) VALUES ('%s', '00000000-0000-0000-0000-000000000000', 'work_item', '%s', '%s', '', 'P1', '', '', '', '%s', '%s', '%s')`,
				orgID, subject, provider, occurred, lastSynced, eventID))
		}
		org := formExec7829Org
		insert(org, "linear", "linear:FX-1", "2026-09-01 10:00:00.000", "2026-09-01 12:00:00.000", "linear:hist-a")
		insert(org, "linear", "linear:FX-1", "2026-09-01 10:00:00.000", "2026-09-03 12:00:00.000", "linear:hist-a")
		insert(org, "linear", "linear:FX-1", "2026-09-01 10:00:00.250", "2026-09-02 12:00:00.000", "linear:hist-a")
		insert(org, "linear", "linear:FX-2", "2026-09-05 08:00:00.000", "2026-09-06 00:00:00.000", "linear:hist-b")
		insert(org, "linear", "linear:FX-3", "2026-09-05 08:00:00.000", "2026-09-06 00:00:00.000", "linear:hist-b")
		insert(org, "linear", "linear:FX-4", "2026-09-07 08:00:00.000", "2026-08-20 00:00:00.000", "linear:hist-c")
		insert(org, "linear", "linear:FX-5", "2026-09-07 08:00:00.000", "2026-09-08 00:00:00.000", "linear:hist-c")
		insert(org, "linear", "linear:FX-5", "2026-09-07 09:00:00.000", "2026-09-08 00:00:00.000", "linear:hist-c")
		insert(org, "jira", "jira:FX-6", "2026-09-09 00:00:00.000", "2026-09-10 00:00:00.000", "abc123contenthash")
		insert(org, "linear", "linear:FX-7", "2026-09-11 08:00:00.000", "2026-09-12 00:00:00.000", "linear:hist-e")
		insert(org, "linear", "linear:FX-7", "2026-09-11 08:00:01.500", "2026-09-12 00:00:00.000", "linear:hist-e")
		insert(org, "github", "gh:FX-8", "2026-08-24 22:00:00.000", "2026-08-25 00:00:00.000", "ghprojv2:item-f")
		insert(org, "github", "gh:FX-8", "2026-08-24 23:00:00.000", "2026-08-24 00:00:00.000", "ghprojv2:item-f")
		for _, occurred := range []string{"2026-09-01 10:00:00.000", "2026-09-01 11:00:00.000", "2026-09-01 12:00:00.000"} {
			insert(formExec7829Other, "linear", "linear:FX-1", occurred, "2026-09-02 00:00:00.000", "linear:hist-a")
		}

		results := formExecRunAll(ctx, t, conn, "FORM-7829-3", statements)
		formExecPartsInfo(t, results, []string{"project_membership_transitions"})
		formExecWant(t, results, "S1", 0, "orgs", "1")
		formExecWantRange(t, results, "S1", 0, "raw_rows", 12, 13)
		formExecWantRange(t, results, "S1", 0, "raw_linear_rows", 9, 10)
		formExecWant(t, results, "S1", 0, "events_approx", "6")

		formExecWantRows(t, results, "S2", 3)
		formExecWant(t, results, "S2", 0, "provider", "github")
		formExecWant(t, results, "S2", 0, "events", "1")
		formExecWant(t, results, "S2", 0, "events_multi_occ", "1")
		formExecWant(t, results, "S2", 0, "multi_occ_native_id", "0")
		formExecWant(t, results, "S2", 1, "provider", "jira")
		formExecWant(t, results, "S2", 1, "events", "1")
		formExecWant(t, results, "S2", 1, "events_multi_occ", "0")
		formExecWant(t, results, "S2", 2, "provider", "linear")
		formExecWant(t, results, "S2", 2, "events", "4")
		formExecWant(t, results, "S2", 2, "events_multi_occ", "3")
		formExecWant(t, results, "S2", 2, "multi_occ_native_id", "3")
		formExecWant(t, results, "S2", 2, "events_multi_subject", "2")
		formExecWantRange(t, results, "S2", 2, "raw_rows_of_multi_occ", 7, 8)

		formExecWantRows(t, results, "S3", 3)
		for row, want := range []string{"1", "0", "3"} {
			formExecWant(t, results, "S3", row, "events_multi_occ_alt", want)
			formExecWant(t, results, "S3", row, "events_multi_occ_alt", formExecCell(t, results, "S2", row, "events_multi_occ"))
		}

		formExecWantRows(t, results, "S4", 4)
		h8 := map[string]string{
			formExecH8("linear:hist-a"): "a", formExecH8("linear:hist-c"): "c",
			formExecH8("linear:hist-e"): "e", formExecH8("ghprojv2:item-f"): "f",
		}
		for row := 0; row < len(results["S4"].rows); row++ {
			want := map[string]string{}
			switch h8[formExecCell(t, results, "S4", row, "event_h8")] {
			case "a":
				want = map[string]string{"provider": "linear", "native_id": "1", "key_rows": "2", "occurred_values": "2", "subjects": "1",
					"occ_span_ms": "250", "min_occurred_day": "2026-09-01", "min_last_synced_day": "2026-09-02",
					"max_last_synced_day": "2026-09-03", "keys_ls_before_cut": "0", "keys_ls_after_cut": "2"}
			case "c":
				want = map[string]string{"key_rows": "3", "occurred_values": "2", "subjects": "2", "occ_span_ms": "3600000",
					"min_last_synced_day": "2026-08-20", "keys_ls_before_cut": "1", "keys_ls_after_cut": "2"}
			case "e":
				want = map[string]string{"key_rows": "2", "occ_span_ms": "1500", "keys_ls_before_cut": "0"}
			case "f":
				want = map[string]string{"provider": "github", "native_id": "0", "key_rows": "2", "occ_span_ms": "3600000",
					"keys_ls_before_cut": "1", "keys_ls_after_cut": "1"}
			default:
				t.Errorf("S4 row %d: event_h8 %s is not event a, c, e or f", row, formExecCell(t, results, "S4", row, "event_h8"))
			}
			for column, value := range want {
				formExecWant(t, results, "S4", row, column, value)
			}
		}

		formExecWantRows(t, results, "S5", 8)
		keys := make([]string, 0, 8)
		for row := 0; row < len(results["S5"].rows); row++ {
			event := h8[formExecCell(t, results, "S5", row, "event_h8")]
			second := formExecCell(t, results, "S5", row, "occurred_day")
			after := formExecCell(t, results, "S5", row, "ms_after_first")
			keys = append(keys, event+" "+second+" +"+after)
			if event == "a" && after == "0" {
				formExecWant(t, results, "S5", row, "key_rows", "1")
				formExecWant(t, results, "S5", row, "max_last_synced_day", "2026-09-03")
			}
			if event == "c" && after == "0" {
				formExecWant(t, results, "S5", row, "key_rows", "2")
				formExecWant(t, results, "S5", row, "subjects", "2")
			}
		}
		sort.Strings(keys)
		wantKeys := "a 2026-09-01 +0,a 2026-09-01 +250,c 2026-09-07 +0,c 2026-09-07 +3600000," +
			"e 2026-09-11 +0,e 2026-09-11 +1500,f 2026-08-24 +0,f 2026-08-24 +3600000"
		if got := strings.Join(keys, ","); got != wantKeys {
			t.Errorf("S5 keys = %s, want %s", got, wantKeys)
		}

		// CONTROL mutation: S3 counting every event as multi-occurred must break S3 = S2.
		mutated := formExecMutate(t, formExecFind(t, statements, "S3"), "countIf(m.occ2 = 2)", "countIf(m.occ2 >= 1)")
		result, err := formExecQuery(ctx, conn, mutated)
		if err != nil {
			t.Fatalf("mutated S3: %v", err)
		}
		formExecLog(t, "FORM-7829-3 mutated", "S3", result)
		if len(result.rows) != 3 || result.rows[2][1] == formExecCell(t, results, "S2", 2, "events_multi_occ") {
			t.Errorf("mutated S3 rows %v still equal S2 linear events_multi_occ: the control cannot fail", result.rows)
		}

		formExecCostGates(ctx, t, conn, "FORM-7829-3", statements, []string{"project_membership_transitions"})
	})

	// Gate cells: a copy of the FORM text with only that gate's bound literal(s) lowered, on a fixture just over it.
	membership := func(ctx context.Context, t *testing.T, conn driver.Conn, orgID, subject, occurred, eventID string, n int) {
		t.Helper()
		formExecExec(ctx, t, conn, fmt.Sprintf(`INSERT INTO project_membership_transitions (`+formExec7829Cols+`) SELECT '%s', toUUID('00000000-0000-0000-0000-000000000000'), 'work_item', %s, 'linear', '', 'P1', '', '', '', %s, toDateTime64('2026-09-02 00:00:00', 3), %s FROM numbers(%d)`,
			orgID, subject, occurred, eventID, n))
	}

	t.Run("gate_events", func(t *testing.T) {
		ctx, conn := formExecOpen(t)
		lowered := formExecSplit(t, "FORM-7829-3 events>5", formExecLowered(t, formExec7829SQL, "uniq(event_id) > 2000000", "uniq(event_id) > 5", 6), 6)
		membership(ctx, t, conn, formExec7829Org, "concat('linear:FX-', toString(number))", "toDateTime64('2026-09-01 00:00:00', 3)", "concat('linear:ev-', toString(number))", 6)
		formExecGateCell(ctx, t, conn, "FORM-7829-3 events>5", lowered, "FORM-7829-3 GATE events", "S2", "S3", "S4", "S5")
	})

	t.Run("gate_multi", func(t *testing.T) {
		ctx, conn := formExecOpen(t)
		lowered := formExecSplit(t, "FORM-7829-3 multi>1/5", formExecLowered(t, formExec7829SQL, "count() > 10000 OR sum(g.n) > 1000000", "count() > 1 OR sum(g.n) > 5", 4), 6)
		membership(ctx, t, conn, formExec7829Org, "'linear:FX-1'", "addMilliseconds(toDateTime64('2026-09-01 00:00:00', 3), number)", "concat('linear:hist-', toString(number % 2))", 6)
		formExecGateCell(ctx, t, conn, "FORM-7829-3 multi>1/5", lowered, "FORM-7829-3 GATE multi", "S4", "S5")
	})
}

type formExecCase struct {
	name, status, duration, message string
}

type formExecSuite struct {
	name, started string
	failed        int
	cases         []formExecCase
}

const (
	formExec14259Org    = "c6a38355-1425-4000-8000-000000000001"
	formExec14259Other  = "e6a38355-1425-4000-8000-000000000002"
	formExec14259GitHub = "00000000-0000-4000-8000-00000000a001"
	formExec14259GitLab = "00000000-0000-4000-8000-00000000a002"
	formExec14259Alien  = "00000000-0000-4000-8000-00000000a003"
	formExec14259Suites = "org_id, repo_id, run_id, suite_id, suite_name, framework, environment, total_count, passed_count, failed_count, skipped_count, error_count, quarantined_count, retried_count, duration_seconds, started_at, finished_at, team_id, service_id, last_synced, report_slot, run_attempt"
	formExec14259Cases  = "org_id, repo_id, run_id, suite_id, case_id, case_name, class_name, status, duration_seconds, retry_attempt, failure_message, failure_type, stack_trace, is_quarantined, last_synced"
)

func formExec14259Repos(ctx context.Context, t *testing.T, conn driver.Conn) {
	t.Helper()
	formExecExec(ctx, t, conn, `INSERT INTO repos (id, repo, org_id, provider, created_at, last_synced) VALUES ('`+formExec14259GitHub+`', 'fixture/github', '`+formExec14259Org+`', 'github', '2026-01-01 00:00:00', '2026-01-01 00:00:00'), ('`+formExec14259GitLab+`', 'fixture/gitlab', '`+formExec14259Org+`', 'gitlab', '2026-01-01 00:00:00', '2026-01-01 00:00:00'), ('`+formExec14259Alien+`', 'fixture/other', '`+formExec14259Other+`', 'github', '2026-01-01 00:00:00', '2026-01-01 00:00:00')`)
}

// TestFormExecFcdevCHAOS14259 fixture (repo uuids and run ids are synthetic). Runs of the github repo of org c6a38355-...:
//
//	r1 (attempt 2): matrix legs. Leg B: shards b1, b2 and an exact total bt (bt last synced older). Leg A: shards a1, a2
//	   and a total at whose pkg1 suite disagrees. Newest completeness row names aggregate_mismatch.
//	r2: two equal timed copies c1, c2 and one case row with no suite row, no completeness row.
//	r3: an exact total r3t with a coverage row (never dropped), complete after FINAL.
//	r6: slot t at attempts 1 and 2, slot u at attempt 1 equal to t@1 (u is the newest of its slot, t@1 is not).
//	r7: a total of two legs that share a case identity (not a partition).
//	r8: two equal copies without case durations (not timed).
//	r9: two equal timed copies e1, e2 and a third slot q with their copy key but another failure message; per_run_cap.
//
// r4 (a gitlab repo of the same org) and r5 (another org) hold an exact total and must not count.
func TestFormExecFcdevCHAOS14259(t *testing.T) {
	statements := formExecStatements(t, "FORM-14259-SIZE-3", formExec14259SQL, formExec14259SHA256, 5)
	chain := func(id string) string {
		text := formExecFind(t, statements, id)
		begin, end := strings.Index(text, "-- CHAIN BEGIN"), strings.Index(text, "-- CHAIN END")
		if begin < 0 || end < begin {
			t.Fatalf("%s: no CHAIN BEGIN / CHAIN END", id)
		}
		return text[begin:end]
	}
	if chain("S3") != chain("S2") || chain("S4") != chain("S2") {
		t.Fatalf("the CTE chain of S2, S3 and S4 is not byte-equal")
	}

	t.Run("fixture", func(t *testing.T) {
		ctx, conn := formExecOpen(t)
		org, other := formExec14259Org, formExec14259Other
		github, gitlab, alien := formExec14259GitHub, formExec14259GitLab, formExec14259Alien
		const newLS, oldLS, copyLS = "2026-11-21 00:00:00.000", "2026-11-10 00:00:00.000", "2026-11-01 00:00:00.000"
		formExec14259Repos(ctx, t, conn)

		var suiteValues, caseValues []string
		orgSuites, newestSlots := 0, map[string]int{}
		slot := func(orgID, repo, run, name string, attempt int, lastSynced string, suites ...formExecSuite) {
			for index, suite := range suites {
				suiteID := fmt.Sprintf("%s@%d/%d-%s", name, attempt, index, suite.name)
				total := len(suite.cases)
				suiteValues = append(suiteValues, fmt.Sprintf("('%s', '%s', '%s', '%s', '%s', 'go', NULL, %d, %d, %d, 0, 0, 0, 0, 1.5, '%s', '%s', NULL, NULL, '%s', '%s', %d)",
					orgID, repo, run, suiteID, suite.name, total, total-suite.failed, suite.failed, suite.started, suite.started, lastSynced, name, attempt))
				if orgID == org {
					orgSuites++
				}
				for _, testCase := range suite.cases {
					caseValues = append(caseValues, fmt.Sprintf("('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', %s, 0, %s, NULL, NULL, 0, '%s')",
						orgID, repo, run, suiteID, testCase.name, testCase.name, suite.name, testCase.status, testCase.duration, testCase.message, lastSynced))
				}
			}
			if orgID == org && repo == github {
				newestSlots[run+"/"+name]++
			}
		}
		c := func(name, status, duration string) []formExecCase {
			return []formExecCase{{name, status, duration, "NULL"}}
		}
		const legB, legA = "2026-11-01 10:00:00.000", "2026-11-01 11:00:00.000"
		pkg1B := formExecSuite{"pkg1", legB, 0, c("x1", "passed", "0.5")}
		pkg2B := formExecSuite{"pkg2", legB, 0, c("x2", "passed", "0.5")}
		pkg1A := formExecSuite{"pkg1", legA, 0, c("x1", "passed", "0.7")}
		pkg1AFailed := formExecSuite{"pkg1", legA, 1, c("x1", "failed", "0.7")}
		pkg2A := formExecSuite{"pkg2", legA, 0, c("x2", "passed", "0.7")}
		slot(org, github, "r1", "b1", 2, newLS, pkg1B)
		slot(org, github, "r1", "b2", 2, newLS, pkg2B)
		slot(org, github, "r1", "bt", 2, oldLS, pkg1B, pkg2B)
		slot(org, github, "r1", "a1", 2, newLS, pkg1A)
		slot(org, github, "r1", "a2", 2, newLS, pkg2A)
		slot(org, github, "r1", "at", 2, newLS, pkg1AFailed, pkg2A)

		pkg3 := formExecSuite{"pkg3", "2026-10-31 10:00:00.000", 0, c("y1", "passed", "0.2")}
		slot(org, github, "r2", "c1", 1, copyLS, pkg3)
		slot(org, github, "r2", "c2", 1, copyLS, pkg3)
		caseValues = append(caseValues, "('"+org+"', '"+github+"', 'r2', 'orphan', 'y9', 'y9', 'pkg3', 'passed', 0.2, 0, NULL, NULL, NULL, 0, '"+copyLS+"')")

		pkg4 := formExecSuite{"pkg4", "2026-11-02 10:00:00.000", 0, c("z1", "passed", "0.1")}
		pkg5 := formExecSuite{"pkg5", "2026-11-02 10:00:00.000", 0, c("z2", "passed", "0.1")}
		slot(org, github, "r3", "r3a", 1, newLS, pkg4)
		slot(org, github, "r3", "r3b", 1, newLS, pkg5)
		slot(org, github, "r3", "r3t", 1, newLS, pkg4, pkg5)
		slot(org, gitlab, "r4", "g1", 1, newLS, pkg4)
		slot(org, gitlab, "r4", "g2", 1, newLS, pkg5)
		slot(org, gitlab, "r4", "gt", 1, newLS, pkg4, pkg5)
		slot(other, alien, "r5", "x1", 1, newLS, pkg4)
		slot(other, alien, "r5", "x2", 1, newLS, pkg5)
		slot(other, alien, "r5", "xt", 1, newLS, pkg4, pkg5)

		pkg6 := formExecSuite{"pkg6", "2026-11-03 10:00:00.000", 0, c("w1", "passed", "0.3")}
		pkg6Failed := formExecSuite{"pkg6", "2026-11-03 10:00:00.000", 1, c("w1", "failed", "0.3")}
		slot(org, github, "r6", "t", 1, newLS, pkg6)
		slot(org, github, "r6", "u", 1, newLS, pkg6)
		slot(org, github, "r6", "t", 2, newLS, pkg6Failed)

		slot(org, github, "r7", "p1", 1, newLS, formExecSuite{"pkg7", "2026-11-04 10:00:00.000", 0, c("v1", "passed", "0.4")})
		slot(org, github, "r7", "p2", 1, newLS, formExecSuite{"pkg7", "2026-11-04 11:00:00.000", 0, c("v1", "passed", "0.4")})
		slot(org, github, "r7", "tot", 1, newLS,
			formExecSuite{"pkg7", "2026-11-04 10:00:00.000", 0, c("v1", "passed", "0.4")},
			formExecSuite{"pkg7", "2026-11-04 11:00:00.000", 0, c("v1", "passed", "0.4")})

		pkg8 := formExecSuite{"pkg8", "2026-11-05 10:00:00.000", 0, c("u1", "passed", "NULL")}
		slot(org, github, "r8", "d1", 1, newLS, pkg8)
		slot(org, github, "r8", "d2", 1, newLS, pkg8)

		pkg9 := formExecSuite{"pkg9", "2026-11-06 10:00:00.000", 0, c("q1", "passed", "0.6")}
		slot(org, github, "r9", "e1", 1, newLS, pkg9)
		slot(org, github, "r9", "e2", 1, newLS, pkg9)
		slot(org, github, "r9", "q", 1, newLS, formExecSuite{"pkg9", "2026-11-06 10:00:00.000", 0, []formExecCase{{"q1", "passed", "0.6", "'flaky'"}}})

		formExecExec(ctx, t, conn, `INSERT INTO test_suite_results (`+formExec14259Suites+`) VALUES `+strings.Join(suiteValues, ", "))
		formExecExec(ctx, t, conn, `INSERT INTO test_case_results (`+formExec14259Cases+`) VALUES `+strings.Join(caseValues, ", "))
		// An older version of b1's suite row: a second part, FINAL keeps the newer row.
		formExecExec(ctx, t, conn, `INSERT INTO test_suite_results (`+formExec14259Suites+`) VALUES ('`+org+`', '`+github+`', 'r1', 'b1@2/0-pkg1', 'pkg1', 'go', NULL, 9, 9, 0, 0, 0, 0, 0, 9.5, '`+legB+`', '`+legB+`', NULL, NULL, '2026-11-09 00:00:00.000', 'b1', 2)`)
		formExecExec(ctx, t, conn, `INSERT INTO coverage_snapshots (org_id, repo_id, run_id, snapshot_id, report_format, lines_total, lines_covered, last_synced, report_slot, run_attempt) VALUES ('`+org+`', '`+github+`', 'r3', 'cov-r3t', 'lcov', 10, 7, '`+newLS+`', 'r3t', 1)`)
		completeness := `INSERT INTO testops_run_report_completeness (org_id, repo_id, run_id, run_attempt, provider, complete, reasons, reports_expected, reports_admitted, last_synced, verdict_version) VALUES `
		formExecExec(ctx, t, conn, completeness+`('`+org+`', '`+github+`', 'r1', 1, 'github', 1, [], 6, 6, '`+newLS+`', 4398046512104), ('`+org+`', '`+github+`', 'r1', 2, 'github', 0, ['aggregate_mismatch'], 6, 6, '`+newLS+`', 2000), ('`+org+`', '`+github+`', 'r9', 1, 'github', 0, ['per_run_cap'], 3, 2, '`+newLS+`', 1000), ('`+other+`', '`+alien+`', 'r5', 1, 'github', 0, ['aggregate_mismatch'], 3, 3, '`+newLS+`', 1000)`)
		formExecExec(ctx, t, conn, completeness+`('`+org+`', '`+github+`', 'r3', 1, 'github', 0, ['aggregate_mismatch'], 3, 3, '`+newLS+`', 1000)`)
		formExecExec(ctx, t, conn, completeness+`('`+org+`', '`+github+`', 'r3', 1, 'github', 1, [], 3, 3, '`+newLS+`', 4398046513104)`)

		results := formExecRunAll(ctx, t, conn, "FORM-14259-SIZE-3", statements)
		formExecPartsInfo(t, results, []string{"coverage_snapshots", "repos", "test_case_results", "test_suite_results", "testops_run_report_completeness"})
		formExecWant(t, results, "S1", 0, "suite_orgs", "1")
		formExecWant(t, results, "S1", 0, "repo_orgs", "1")
		formExecWant(t, results, "S1", 0, "completeness_orgs", "1")
		formExecWant(t, results, "S1", 0, "github_repos", "1")
		formExecWantRange(t, results, "S1", 0, "suite_rows_raw", int64(orgSuites), int64(orgSuites)+1)
		formExecWantRange(t, results, "S1", 0, "slot_suite_rows_raw", int64(orgSuites), int64(orgSuites)+1)
		formExecWant(t, results, "S1", 0, "coverage_rows_raw", "1")

		for column, want := range map[string]string{
			"u_runs": "7", "u_slots": "18", "u_runs_marked_mismatch": "1", "u_runs_complete": "1", "u_runs_no_row": "4",
			"u_slots_stale": "1", "u_slots_before_14045_commit": "3",
			"u_first_day": "2026-11-01", "u_last_day": "2026-11-21", "newest_slots": "21",
		} {
			formExecWant(t, results, "S2", 0, column, want)
		}
		for column, want := range map[string]string{
			"newest_slots": "21", "e_runs": "4", "e_runs_aggregate": "1", "e_slots_aggregate": "1", "e_runs_copy": "3",
			"e_runs_marked_mismatch": "1", "e_runs_complete": "0", "e_runs_no_row": "2", "e_slots_stale": "1",
			"e_slots_before_14045_commit": "3", "e_slots_third_copy": "2", "e_runs_orphan_cases": "1", "e_runs_capped": "1",
		} {
			formExecWant(t, results, "S3", 0, column, want)
		}
		formExecWant(t, results, "S4", 0, "u_slots_cte", "18")
		formExecWant(t, results, "S4", 0, "u_slots_window", "18")
		formExecWant(t, results, "S4", 0, "newest_slots", strconv.Itoa(len(newestSlots)))
		if len(newestSlots) != 21 {
			t.Errorf("fixture newest github slots = %d, want 21", len(newestSlots))
		}

		// CONTROL mutations of the CTE chain copy in S4: each must break u_slots_cte = u_slots_window.
		for _, mutation := range []struct{ name, old, replacement, want string }{
			{"every slot twinned", "HAVING min(r.n >= 2 * k.n) = 1", "HAVING min(r.n >= k.n) = 1", "21"},
			{"head without outcome counts", "toString(passed_count), '|', toString(failed_count), '|',", "", "20"},
		} {
			mutated := formExecMutate(t, formExecFind(t, statements, "S4"), mutation.old, mutation.replacement)
			result, err := formExecQuery(ctx, conn, mutated)
			if err != nil {
				t.Fatalf("mutated S4 (%s): %v", mutation.name, err)
			}
			formExecLog(t, "FORM-14259-SIZE-3 mutated "+mutation.name, "S4", result)
			if len(result.rows) != 1 || result.rows[0][0] != mutation.want || result.rows[0][0] == result.rows[0][1] {
				t.Errorf("mutated S4 (%s) = %v, want u_slots_cte %s != u_slots_window", mutation.name, result.rows, mutation.want)
			}
		}

		formExecCostGates(ctx, t, conn, "FORM-14259-SIZE-3", statements, []string{"coverage_snapshots", "repos", "test_case_results", "test_suite_results", "testops_run_report_completeness"})
	})

	// Gate cells: a copy of the FORM text with only that gate's bound literal(s) lowered, on a fixture just over it.
	bulk := func(ctx context.Context, t *testing.T, conn driver.Conn, run, suiteID, suiteName, slot string, n int) {
		t.Helper()
		formExecExec(ctx, t, conn, fmt.Sprintf(`INSERT INTO test_suite_results (org_id, repo_id, run_id, suite_id, suite_name, total_count, passed_count, failed_count, skipped_count, last_synced, report_slot, run_attempt) SELECT '%s', toUUID('%s'), %s, %s, %s, 1, 1, 0, 0, toDateTime64('2026-11-21 00:00:00', 3, 'UTC'), %s, 1 FROM numbers(%d)`,
			formExec14259Org, formExec14259GitHub, run, suiteID, suiteName, slot, n))
	}
	lowered := func(t *testing.T, old, replacement string, n int) []formExecStatement {
		t.Helper()
		return formExecSplit(t, "FORM-14259-SIZE-3 "+replacement, formExecLowered(t, formExec14259SQL, old, replacement, n), 5)
	}

	t.Run("gate_suites", func(t *testing.T) {
		ctx, conn := formExecOpen(t)
		statements := lowered(t, "throwIf(count() > 3000000,", "throwIf(count() > 20,", 4)
		formExec14259Repos(ctx, t, conn)
		bulk(ctx, t, conn, "concat('big-', toString(intDiv(number, 3)))", "toString(number)", "concat('pkg', toString(number))", "concat('s', toString(number % 3))", 21)
		formExecGateCell(ctx, t, conn, "FORM-14259-SIZE-3 suites>20", statements, "FORM-14259-SIZE-3 GATE suites", "S2", "S3", "S4")
	})

	t.Run("gate_slots", func(t *testing.T) {
		ctx, conn := formExecOpen(t)
		statements := lowered(t, "max(x.slots) > 500", "max(x.slots) > 3", 4)
		formExec14259Repos(ctx, t, conn)
		bulk(ctx, t, conn, "'many'", "toString(number)", "concat('pkg', toString(number))", "concat('s', toString(number))", 4)
		formExecGateCell(ctx, t, conn, "FORM-14259-SIZE-3 slots>3", statements, "FORM-14259-SIZE-3 GATE slots", "S2", "S3", "S4")
	})

	t.Run("gate_heads", func(t *testing.T) {
		ctx, conn := formExecOpen(t)
		statements := lowered(t, "max(z.c) > 16", "max(z.c) > 2", 4)
		formExec14259Repos(ctx, t, conn)
		bulk(ctx, t, conn, "'heads'", "toString(number)", "'pkg'", "concat('s', toString(number))", 3)
		formExecGateCell(ctx, t, conn, "FORM-14259-SIZE-3 heads>2", statements, "FORM-14259-SIZE-3 GATE heads", "S2", "S3", "S4")
	})

	t.Run("gate_cases", func(t *testing.T) {
		ctx, conn := formExecOpen(t)
		statements := lowered(t, "count() > 10000000", "count() > 5", 1)
		formExec14259Repos(ctx, t, conn)
		bulk(ctx, t, conn, "'cases'", "concat('c', toString(number + 1), '@1/0-pkg')", "'pkg'", "concat('c', toString(number + 1))", 2)
		formExecExec(ctx, t, conn, `INSERT INTO test_case_results (org_id, repo_id, run_id, suite_id, case_id, case_name, status, last_synced) SELECT '`+formExec14259Org+`', toUUID('`+formExec14259GitHub+`'), 'cases', 'c1@1/0-pkg', toString(number), 'x', 'passed', toDateTime64('2026-11-21 00:00:00', 3, 'UTC') FROM numbers(6)`)
		formExecGateCell(ctx, t, conn, "FORM-14259-SIZE-3 cases>5", statements, "FORM-14259-SIZE-3 GATE cases", "S3")
	})
}
