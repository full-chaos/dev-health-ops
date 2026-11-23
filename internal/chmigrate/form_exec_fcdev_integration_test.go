//go:build integration

package chmigrate_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// DO NOT MERGE. This file executes the text of two prod read FORMs (FORM-7829-1.sql, FORM-14259-SIZE-1.sql), byte for byte
// (the sha256 below is the sha256 of each .sql file), on the head schema with a synthetic fixture, and checks each WANT.
const (
	formExec7829SHA256  = "98593aba336ed86509ae3835754b939e472d6266225cfaa27cf7199261e31ce9"
	formExec14259SHA256 = "f8d9ba4be1e7bddc54040d20505ff6aa1c91141c0c2c6e0912538e8f808e8604"
	formExec7829SQL     = `-- FORM-7829-1 (lane lformch for fcdev, 2026-11-22). CHAOS-7829: one Linear event_id stored at two occurred_at values.
-- ClickHouse, SELECT only. Org prefix c6a38355 only. Output = counts, 8-char sha256 prefixes of event_id, UTC timestamps.
-- Run each statement once, in order. A SQL error = STOP. S0 orgs != 1 = STOP.
-- Table: project_membership_transitions, ReplacingMergeTree(last_synced),
--   ORDER BY (org_id, subject_kind, repo_id, subject_id, occurred_at, event_id), no PARTITION BY, org_id String.
--   FINAL collapses rows of one sort key (same subject, occurred_at, event_id) to the newest last_synced.
--   Two occurred_at values of one event_id are two sort keys: FINAL keeps both. So FINAL, not argMax.
-- Every statement reads the org's rows of this one table through the sort-key prefix org_id LIKE 'c6a38355%'.
--   Rows read <= S0 raw_rows. S1 to S4 group by event_id: memory ~ distinct event_ids of the org.

-- S0 ORG-1 and size report. WANT orgs = 1. INFO raw_rows (unmerged rows, the read bound of S1 to S4).
SELECT uniqExact(org_id) AS orgs,
       count() AS raw_rows,
       countIf(provider = 'linear') AS raw_linear_rows
FROM project_membership_transitions
WHERE org_id LIKE 'c6a38355%';

-- S1 per provider: event_ids with more than one occurred_at (FINAL rows).
-- WANT linear events_multi_occ >= 0 (the measured quantity). 0 = the CHAOS-7659 shape is gone.
-- INFO multi_occ_native_id = of those, event_id 'linear:<history entry id>' (a content-hash id holds occurred_at, so it
--   cannot repeat at two occurred_at). INFO events_multi_subject = one event_id under 2+ subjects.
SELECT m.provider AS provider,
       count() AS events,
       countIf(m.occ > 1) AS events_multi_occ,
       countIf(m.occ > 1 AND m.native = 1) AS multi_occ_native_id,
       countIf(m.subj > 1) AS events_multi_subject,
       sumIf(m.n, m.occ > 1) AS rows_of_multi_occ
FROM (
    SELECT provider,
           event_id,
           startsWith(event_id, 'linear:') AS native,
           count() AS n,
           uniqExact(occurred_at) AS occ,
           uniqExact(subject_kind, repo_id, subject_id) AS subj
    FROM project_membership_transitions FINAL
    WHERE org_id LIKE 'c6a38355%'
    GROUP BY provider, event_id
) AS m
GROUP BY m.provider
ORDER BY provider;

-- S2 CONTROL: S1 events_multi_occ recomputed on the raw rows (no FINAL).
-- WANT per provider events_multi_occ_raw = S1 events_multi_occ (FINAL never removes an occurred_at value).
-- WANT raw_rows_of_multi_occ >= S1 rows_of_multi_occ (unmerged versions add rows only).
SELECT m.provider AS provider,
       countIf(m.occ > 1) AS events_multi_occ_raw,
       sumIf(m.n, m.occ > 1) AS raw_rows_of_multi_occ
FROM (
    SELECT provider,
           event_id,
           count() AS n,
           uniqExact(occurred_at) AS occ
    FROM project_membership_transitions
    WHERE org_id LIKE 'c6a38355%'
    GROUP BY provider, event_id
) AS m
GROUP BY m.provider
ORDER BY provider;

-- S3 one row per event_id with more than one occurred_at, LIMIT 100. WANT row count = S1 sum of events_multi_occ (if <= 100).
-- occ_span_ms = max - min occurred_at. Go cut = a21768303 (2026-08-25 01:33:44 UTC), the first writer of this table for
--   linear (Go). WANT rows_ls_before_cut = 0. rows_ls_before_cut > 0 = a row stamped before any writer existed.
SELECT substring(lower(hex(SHA256(m.event_id))), 1, 8) AS event_h8,
       m.provider AS provider,
       m.native AS native_id,
       m.n AS row_count,
       m.occ AS occurred_values,
       m.subj AS subjects,
       toString(m.min_occ, 'UTC') AS min_occurred_utc,
       toString(m.max_occ, 'UTC') AS max_occurred_utc,
       toUnixTimestamp64Milli(m.max_occ) - toUnixTimestamp64Milli(m.min_occ) AS occ_span_ms,
       toString(m.min_ls, 'UTC') AS min_last_synced_utc,
       toString(m.max_ls, 'UTC') AS max_last_synced_utc,
       toString(m.min_ing, 'UTC') AS min_ingested_utc,
       toString(m.max_ing, 'UTC') AS max_ingested_utc,
       m.before_cut AS rows_ls_before_cut,
       m.n - m.before_cut AS rows_ls_after_cut
FROM (
    SELECT provider,
           event_id,
           startsWith(event_id, 'linear:') AS native,
           count() AS n,
           uniqExact(occurred_at) AS occ,
           uniqExact(subject_kind, repo_id, subject_id) AS subj,
           min(occurred_at) AS min_occ,
           max(occurred_at) AS max_occ,
           min(last_synced) AS min_ls,
           max(last_synced) AS max_ls,
           min(ingested_at) AS min_ing,
           max(ingested_at) AS max_ing,
           countIf(last_synced < toDateTime64('2026-08-25 01:33:44', 3, 'UTC')) AS before_cut
    FROM project_membership_transitions FINAL
    WHERE org_id LIKE 'c6a38355%'
    GROUP BY provider, event_id
    HAVING occ > 1
) AS m
ORDER BY event_h8
LIMIT 100;

-- S4 one row per (event_id, occurred_at) of the S3 events, LIMIT 300. WANT row count = sum of S3 occurred_values (if <= 300).
-- Reading: max_last_synced_utc of one occurred_at far behind the other = the provider stopped returning it (createdAt
--   changed upstream, the old key is never retired = old row). Both recent = the provider returns both now (live).
SELECT substring(lower(hex(SHA256(t.event_id))), 1, 8) AS event_h8,
       toString(t.occurred_at, 'UTC') AS occurred_utc,
       count() AS row_count,
       uniqExact(t.subject_kind, t.repo_id, t.subject_id) AS subjects,
       toString(min(t.last_synced), 'UTC') AS min_last_synced_utc,
       toString(max(t.last_synced), 'UTC') AS max_last_synced_utc,
       toString(max(t.ingested_at), 'UTC') AS max_ingested_utc
FROM project_membership_transitions AS t FINAL
WHERE t.org_id LIKE 'c6a38355%'
  AND (t.provider, t.event_id) IN (
    SELECT provider, event_id
    FROM project_membership_transitions FINAL
    WHERE org_id LIKE 'c6a38355%'
    GROUP BY provider, event_id
    HAVING uniqExact(occurred_at) > 1
  )
GROUP BY t.event_id, t.occurred_at
ORDER BY event_h8, occurred_utc
LIMIT 300;
`
	formExec14259SQL = `-- FORM-14259-SIZE-1 (lane lformch for fcdev, 2026-11-22). CHAOS-14259 sizing: stored GitHub runs that hold a report slot
--   the current planner (CHAOS-14045, 37b7d692a) would no longer write. ClickHouse, SELECT only, counts and UTC times only.
-- Org prefix c6a38355 only. Run each statement once, in order. A SQL error = STOP. S0 suite_orgs != 1 = STOP.
-- Terms (code at ops 340c3e05e3ca, internal/providersync/github_tests_run_report_rows.go):
--   slot = test_suite_results.report_slot (artifact name) at the newest run_attempt of that slot in the run (readers count
--   only that attempt). A slot's suite instance = the suite row without suite_id, report_slot, legacy_suite_ids,
--   last_synced, plus the multiset of its case rows without suite_id, case_id, last_synced (githubTestsReportContent).
--   head = the suite row part of an instance only.
-- UPPER (S1) = runs with a slot whose every suite head has an equal head in the other slots of the run, as many times.
--   Each slot the planner drops (copy or lossless aggregate) passes this test. Not proven: cases, parts, coverage.
-- LOWER (S2) = runs with a slot S that the current planner drops for sure: S has cases and no coverage row, and the other
--   slots that share an instance with S (P) all lie inside S, and P together equal S exactly. Then either |P| >= 2 and
--   P members share no case identity (class_name, case_name) = a lossless aggregate, or |P| = 1 and S has a case
--   duration > 0 = a copy. No slot outside S and P shares an instance, so S's touch group holds no mismatch.
-- Tables (all ReplacingMergeTree, no PARTITION BY, read FINAL, filtered on the sort-key prefix org_id LIKE 'c6a38355%'):
--   test_suite_results ORDER BY (org_id, repo_id, run_id, suite_id), test_case_results ORDER BY (org_id, repo_id, run_id,
--   suite_id, case_id), coverage_snapshots ORDER BY (org_id, repo_id, run_id, snapshot_id), testops_run_report_completeness
--   ORDER BY (org_id, repo_id, run_id, run_attempt), repos ORDER BY (org_id, id).
-- Cost: S1 reads the org's suite rows about 6 times (CTEs are not materialized) plus repos and completeness once.
--   S2 = S1 plus the case and coverage rows of the S1 runs only (IN on the sort-key prefix (repo_id, run_id)).
--   S0 sizes each table. 14045 commit time 2026-11-20 02:45:39 UTC. The prod roll time of 37b7d692a is not in code.

-- S0 ORG-1 and size report. WANT suite_orgs = 1, repo_orgs = 1. INFO completeness_orgs (0 = every run reads no_row).
-- INFO the raw row counts (read bound of S1 to S3).
SELECT
    (SELECT uniqExact(org_id) FROM test_suite_results WHERE org_id LIKE 'c6a38355%') AS suite_orgs,
    (SELECT uniqExact(org_id) FROM repos WHERE org_id LIKE 'c6a38355%') AS repo_orgs,
    (SELECT uniqExact(org_id) FROM testops_run_report_completeness WHERE org_id LIKE 'c6a38355%') AS completeness_orgs,
    (SELECT count() FROM test_suite_results WHERE org_id LIKE 'c6a38355%') AS suite_rows_raw,
    (SELECT countIf(report_slot != '') FROM test_suite_results WHERE org_id LIKE 'c6a38355%') AS slot_suite_rows_raw,
    (SELECT count() FROM test_case_results WHERE org_id LIKE 'c6a38355%') AS case_rows_raw,
    (SELECT count() FROM coverage_snapshots WHERE org_id LIKE 'c6a38355%') AS coverage_rows_raw,
    (SELECT count() FROM repos FINAL WHERE org_id LIKE 'c6a38355%' AND provider = 'github') AS github_repos;

-- S1 UPPER bound. WANT u_runs >= S2 l_runs. INFO splits: newest completeness row of the run names aggregate_mismatch
--   (the old planner kept a mismatch there), is complete, or is missing. u_slots_stale = the slot's newest last_synced
--   is older than the run's (a later sync did not rewrite it). u_slots_before_14045_commit = slot last_synced before
--   the 14045 commit time.
WITH
gh AS (
    SELECT id FROM repos FINAL WHERE org_id LIKE 'c6a38355%' AND provider = 'github'
),
sa AS (
    SELECT repo_id, run_id, suite_id, report_slot AS slot, run_attempt AS att, last_synced AS ls,
           cityHash64(concat(suite_name, '|', ifNull(framework, '~'), '|', ifNull(environment, '~'), '|',
               toString(total_count), '|', toString(passed_count), '|', toString(failed_count), '|',
               toString(skipped_count), '|', toString(error_count), '|', toString(quarantined_count), '|',
               toString(retried_count), '|', ifNull(toString(duration_seconds), '~'), '|',
               ifNull(toString(started_at), '~'), '|', ifNull(toString(finished_at), '~'), '|',
               ifNull(team_id, '~'), '|', ifNull(service_id, '~'), '|', toString(run_attempt))) AS hk
    FROM test_suite_results FINAL
    WHERE org_id LIKE 'c6a38355%' AND report_slot != '' AND repo_id IN (SELECT id FROM gh)
),
sn AS (
    SELECT * FROM sa
    WHERE (repo_id, run_id, slot, att) IN (SELECT repo_id, run_id, slot, max(att) FROM sa GROUP BY repo_id, run_id, slot)
),
hk_slot AS (
    SELECT repo_id, run_id, slot, att, hk, count() AS n FROM sn GROUP BY repo_id, run_id, slot, att, hk
),
hk_run AS (
    SELECT repo_id, run_id, att, hk, count() AS n FROM sn GROUP BY repo_id, run_id, att, hk
),
uslots AS (
    SELECT k.repo_id AS repo_id, k.run_id AS run_id, k.slot AS slot
    FROM hk_slot AS k
    INNER JOIN hk_run AS r ON r.repo_id = k.repo_id AND r.run_id = k.run_id AND r.att = k.att AND r.hk = k.hk
    GROUP BY k.repo_id, k.run_id, k.slot
    HAVING min(r.n >= 2 * k.n) = 1
),
slot_ls AS (
    SELECT repo_id, run_id, slot, max(ls) AS ls FROM sn GROUP BY repo_id, run_id, slot
),
run_ls AS (
    SELECT repo_id, run_id, max(ls) AS ls FROM sn GROUP BY repo_id, run_id
),
comp AS (
    SELECT repo_id, run_id, 1 AS has_row,
           argMax(complete, run_attempt) AS complete,
           argMax(has(reasons, 'aggregate_mismatch'), run_attempt) AS marked
    FROM testops_run_report_completeness FINAL
    WHERE org_id LIKE 'c6a38355%'
    GROUP BY repo_id, run_id
)
SELECT uniqExact(U.repo_id, U.run_id) AS u_runs,
       count() AS u_slots,
       uniqExactIf((U.repo_id, U.run_id), ifNull(c.marked, 0) = 1) AS u_runs_marked_mismatch,
       uniqExactIf((U.repo_id, U.run_id), ifNull(c.has_row, 0) = 1 AND ifNull(c.complete, 0) = 1) AS u_runs_complete,
       uniqExactIf((U.repo_id, U.run_id), ifNull(c.has_row, 0) = 0) AS u_runs_no_row,
       countIf(sl.ls < rl.ls) AS u_slots_stale,
       countIf(sl.ls < toDateTime64('2026-11-20 02:45:39', 3, 'UTC')) AS u_slots_before_14045_commit,
       toString(min(sl.ls)) AS u_min_ls_utc,
       toString(max(sl.ls)) AS u_max_ls_utc
FROM uslots AS U
INNER JOIN slot_ls AS sl ON sl.repo_id = U.repo_id AND sl.run_id = U.run_id AND sl.slot = U.slot
INNER JOIN run_ls AS rl ON rl.repo_id = U.repo_id AND rl.run_id = U.run_id
LEFT JOIN comp AS c ON c.repo_id = U.repo_id AND c.run_id = U.run_id;

-- S2 LOWER bound (the S1 runs only). WANT l_runs <= S1 u_runs. CONTROL WANT l_slots_not_in_u = 0 (a LOWER slot that
--   fails the UPPER test = an error in this SQL). INFO l_runs_aggregate / l_runs_copy, and the S1 splits.
WITH
gh AS (
    SELECT id FROM repos FINAL WHERE org_id LIKE 'c6a38355%' AND provider = 'github'
),
sa AS (
    SELECT repo_id, run_id, suite_id, report_slot AS slot, run_attempt AS att, last_synced AS ls,
           cityHash64(concat(suite_name, '|', ifNull(framework, '~'), '|', ifNull(environment, '~'), '|',
               toString(total_count), '|', toString(passed_count), '|', toString(failed_count), '|',
               toString(skipped_count), '|', toString(error_count), '|', toString(quarantined_count), '|',
               toString(retried_count), '|', ifNull(toString(duration_seconds), '~'), '|',
               ifNull(toString(started_at), '~'), '|', ifNull(toString(finished_at), '~'), '|',
               ifNull(team_id, '~'), '|', ifNull(service_id, '~'), '|', toString(run_attempt))) AS hk
    FROM test_suite_results FINAL
    WHERE org_id LIKE 'c6a38355%' AND report_slot != '' AND repo_id IN (SELECT id FROM gh)
),
sn AS (
    SELECT * FROM sa
    WHERE (repo_id, run_id, slot, att) IN (SELECT repo_id, run_id, slot, max(att) FROM sa GROUP BY repo_id, run_id, slot)
),
hk_slot AS (
    SELECT repo_id, run_id, slot, att, hk, count() AS n FROM sn GROUP BY repo_id, run_id, slot, att, hk
),
hk_run AS (
    SELECT repo_id, run_id, att, hk, count() AS n FROM sn GROUP BY repo_id, run_id, att, hk
),
uslots AS (
    SELECT k.repo_id AS repo_id, k.run_id AS run_id, k.slot AS slot
    FROM hk_slot AS k
    INNER JOIN hk_run AS r ON r.repo_id = k.repo_id AND r.run_id = k.run_id AND r.att = k.att AND r.hk = k.hk
    GROUP BY k.repo_id, k.run_id, k.slot
    HAVING min(r.n >= 2 * k.n) = 1
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
           max(ifNull(duration_seconds, 0) > 0) AS timed,
           groupUniqArray(cityHash64(concat(ifNull(class_name, ''), '|', case_name))) AS idks
    FROM test_case_results FINAL
    WHERE org_id LIKE 'c6a38355%' AND (repo_id, run_id) IN (SELECT repo_id, run_id FROM cand)
    GROUP BY repo_id, run_id, suite_id
),
inst AS (
    SELECT s.repo_id AS repo_id, s.run_id AS run_id, s.slot AS slot, s.att AS att,
           cityHash64(concat(toString(s.hk), '|', toString(ifNull(c.n, 0)), '|', toString(ifNull(c.h, 0)))) AS ik,
           ifNull(c.n, 0) AS n, ifNull(c.timed, 0) AS timed, c.idks AS idks
    FROM sn AS s
    LEFT JOIN cs AS c ON c.repo_id = s.repo_id AND c.run_id = s.run_id AND c.suite_id = s.suite_id
    WHERE (s.repo_id, s.run_id) IN (SELECT repo_id, run_id FROM cand)
),
slot_tot AS (
    SELECT repo_id, run_id, slot, att, count() AS inst_n, sum(n) AS cases, max(timed) AS timed,
           length(groupUniqArrayArray(idks)) AS idents
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
    GROUP BY si.repo_id, si.run_id, si.slot, si.att
    HAVING min(ri.k = 2 * si.k) = 1
),
pairs AS (
    SELECT a.repo_id AS repo_id, a.run_id AS run_id, a.slot AS s_slot, b.slot AS t_slot, sum(least(a.k, b.k)) AS matched
    FROM slot_inst AS a
    INNER JOIN slot_inst AS b ON b.repo_id = a.repo_id AND b.run_id = a.run_id AND b.att = a.att AND b.ik = a.ik
    WHERE b.slot != a.slot
    GROUP BY a.repo_id, a.run_id, a.slot, b.slot
),
pstat AS (
    SELECT p.repo_id AS repo_id, p.run_id AS run_id, p.s_slot AS slot,
           count() AS parts, min(p.matched = t.inst_n) AS all_within,
           sum(t.idents) AS part_idents, min(t.cases) AS min_part_cases
    FROM pairs AS p
    INNER JOIN slot_tot AS t ON t.repo_id = p.repo_id AND t.run_id = p.run_id AND t.slot = p.t_slot
    GROUP BY p.repo_id, p.run_id, p.s_slot
),
cov AS (
    SELECT DISTINCT repo_id, run_id, report_slot AS slot, run_attempt AS att, 1 AS has_cov
    FROM coverage_snapshots FINAL
    WHERE org_id LIKE 'c6a38355%' AND report_slot != '' AND (repo_id, run_id) IN (SELECT repo_id, run_id FROM cand)
),
lslots AS (
    SELECT q.repo_id AS repo_id, q.run_id AS run_id, q.slot AS slot, ps.parts AS parts
    FROM cond_a AS q
    INNER JOIN pstat AS ps ON ps.repo_id = q.repo_id AND ps.run_id = q.run_id AND ps.slot = q.slot
    INNER JOIN slot_tot AS st ON st.repo_id = q.repo_id AND st.run_id = q.run_id AND st.slot = q.slot
    LEFT JOIN cov AS v ON v.repo_id = q.repo_id AND v.run_id = q.run_id AND v.slot = q.slot AND v.att = q.att
    WHERE ps.all_within = 1 AND ps.min_part_cases > 0 AND st.cases > 0 AND ifNull(v.has_cov, 0) = 0
      AND ((ps.parts >= 2 AND ps.part_idents = st.idents) OR (ps.parts = 1 AND st.timed = 1))
),
slot_ls AS (
    SELECT repo_id, run_id, slot, max(ls) AS ls FROM sn GROUP BY repo_id, run_id, slot
),
run_ls AS (
    SELECT repo_id, run_id, max(ls) AS ls FROM sn GROUP BY repo_id, run_id
),
comp AS (
    SELECT repo_id, run_id, 1 AS has_row,
           argMax(complete, run_attempt) AS complete,
           argMax(has(reasons, 'aggregate_mismatch'), run_attempt) AS marked
    FROM testops_run_report_completeness FINAL
    WHERE org_id LIKE 'c6a38355%'
    GROUP BY repo_id, run_id
)
SELECT uniqExact(L.repo_id, L.run_id) AS l_runs,
       count() AS l_slots,
       uniqExactIf((L.repo_id, L.run_id), L.parts >= 2) AS l_runs_aggregate,
       uniqExactIf((L.repo_id, L.run_id), L.parts = 1) AS l_runs_copy,
       uniqExactIf((L.repo_id, L.run_id), ifNull(c.marked, 0) = 1) AS l_runs_marked_mismatch,
       uniqExactIf((L.repo_id, L.run_id), ifNull(c.has_row, 0) = 1 AND ifNull(c.complete, 0) = 1) AS l_runs_complete,
       uniqExactIf((L.repo_id, L.run_id), ifNull(c.has_row, 0) = 0) AS l_runs_no_row,
       countIf(sl.ls < rl.ls) AS l_slots_stale,
       countIf(sl.ls < toDateTime64('2026-11-20 02:45:39', 3, 'UTC')) AS l_slots_before_14045_commit,
       countIf((L.repo_id, L.run_id, L.slot) NOT IN (SELECT repo_id, run_id, slot FROM uslots)) AS l_slots_not_in_u
FROM lslots AS L
INNER JOIN slot_ls AS sl ON sl.repo_id = L.repo_id AND sl.run_id = L.run_id AND sl.slot = L.slot
INNER JOIN run_ls AS rl ON rl.repo_id = L.repo_id AND rl.run_id = L.run_id
LEFT JOIN comp AS c ON c.repo_id = L.repo_id AND c.run_id = L.run_id;

-- S3 CONTROL: FINAL row count = distinct sort keys of the raw rows. WANT suite_rows_final = suite_keys_raw.
SELECT
    (SELECT count() FROM test_suite_results FINAL WHERE org_id LIKE 'c6a38355%') AS suite_rows_final,
    (SELECT uniqExact(repo_id, run_id, suite_id) FROM test_suite_results WHERE org_id LIKE 'c6a38355%') AS suite_keys_raw;
`
)

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

// formExecRun runs every statement of the FORM text as the text holds it: the pieces between ";\n", each named by its
// first line "-- S<n> ...". A piece with no SQL line (only comments) is not a statement.
func formExecRun(ctx context.Context, t *testing.T, conn driver.Conn, name, text, wantSHA string) map[string]formExecResult {
	t.Helper()
	sum := sha256.Sum256([]byte(text))
	if got := hex.EncodeToString(sum[:]); got != wantSHA {
		t.Fatalf("%s: sha256 of the embedded text = %s, want %s (the .sql file)", name, got, wantSHA)
	}
	results := map[string]formExecResult{}
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
		rows, err := conn.Query(ctx, statement)
		if err != nil {
			t.Fatalf("%s %s: %v", name, id, err)
		}
		result := formExecResult{columns: rows.Columns()}
		types := rows.ColumnTypes()
		for rows.Next() {
			dest := make([]any, len(types))
			for index, columnType := range types {
				dest[index] = reflect.New(columnType.ScanType()).Interface()
			}
			if err := rows.Scan(dest...); err != nil {
				t.Fatalf("%s %s: scan: %v", name, id, err)
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
		if err := rows.Err(); err != nil {
			t.Fatalf("%s %s: rows: %v", name, id, err)
		}
		_ = rows.Close()
		t.Logf("%s %s columns %v", name, id, result.columns)
		for index, row := range result.rows {
			t.Logf("%s %s row %d %v", name, id, index, row)
		}
		results[id] = result
	}
	return results
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

func formExecH8(eventID string) string {
	sum := sha256.Sum256([]byte(eventID))
	return hex.EncodeToString(sum[:])[:8]
}

// CHAOS-7829 fixture. Org c6a38355-... is the one org the FORM reads; org d6a38355-... holds the same event id at three
// occurred_at values and must not count. Event a: two occurred_at, one subject, and an older version of one key (FINAL
// keeps the newer last_synced). Event b: one occurred_at, two subjects. Event c: two occurred_at, two subjects, one row
// last synced before the Go cut. Event d: a jira content-hash id.
func TestFormExecFcdevCHAOS7829(t *testing.T) {
	ctx, conn := formExecOpen(t)
	const org, other = "c6a38355-7829-4000-8000-000000000001", "d6a38355-7829-4000-8000-000000000002"
	insert := func(orgID, provider, subject, occurred, lastSynced, eventID string) {
		formExecExec(ctx, t, conn, fmt.Sprintf(`INSERT INTO project_membership_transitions (org_id, repo_id, subject_kind, subject_id, provider, from_project_id, to_project_id, from_project_key, to_project_key, actor, occurred_at, last_synced, event_id) VALUES ('%s', '00000000-0000-0000-0000-000000000000', 'work_item', '%s', '%s', '', 'P1', '', '', '', '%s', '%s', '%s')`,
			orgID, subject, provider, occurred, lastSynced, eventID))
	}
	insert(org, "linear", "linear:FX-1", "2026-09-01 10:00:00.000", "2026-09-01 12:00:00.000", "linear:hist-a")
	insert(org, "linear", "linear:FX-1", "2026-09-01 10:00:00.000", "2026-09-03 12:00:00.000", "linear:hist-a")
	insert(org, "linear", "linear:FX-1", "2026-09-01 10:00:00.250", "2026-09-02 12:00:00.000", "linear:hist-a")
	insert(org, "linear", "linear:FX-2", "2026-09-05 08:00:00.000", "2026-09-06 00:00:00.000", "linear:hist-b")
	insert(org, "linear", "linear:FX-3", "2026-09-05 08:00:00.000", "2026-09-06 00:00:00.000", "linear:hist-b")
	insert(org, "linear", "linear:FX-4", "2026-09-07 08:00:00.000", "2026-08-20 00:00:00.000", "linear:hist-c")
	insert(org, "linear", "linear:FX-5", "2026-09-07 08:00:00.000", "2026-09-08 00:00:00.000", "linear:hist-c")
	insert(org, "linear", "linear:FX-5", "2026-09-07 09:00:00.000", "2026-09-08 00:00:00.000", "linear:hist-c")
	insert(org, "jira", "jira:FX-6", "2026-09-09 00:00:00.000", "2026-09-10 00:00:00.000", "abc123contenthash")
	for _, occurred := range []string{"2026-09-01 10:00:00.000", "2026-09-01 11:00:00.000", "2026-09-01 12:00:00.000"} {
		insert(other, "linear", "linear:FX-1", occurred, "2026-09-02 00:00:00.000", "linear:hist-a")
	}

	results := formExecRun(ctx, t, conn, "FORM-7829-1", formExec7829SQL, formExec7829SHA256)
	if len(results) != 5 {
		t.Fatalf("ran %d statements, want 5 (S0..S4)", len(results))
	}
	formExecWant(t, results, "S0", 0, "orgs", "1")
	formExecWantRange(t, results, "S0", 0, "raw_rows", 8, 9)
	formExecWantRange(t, results, "S0", 0, "raw_linear_rows", 7, 8)

	formExecWantRows(t, results, "S1", 2)
	formExecWant(t, results, "S1", 0, "provider", "jira")
	formExecWant(t, results, "S1", 0, "events", "1")
	formExecWant(t, results, "S1", 0, "events_multi_occ", "0")
	formExecWant(t, results, "S1", 1, "provider", "linear")
	formExecWant(t, results, "S1", 1, "events", "3")
	formExecWant(t, results, "S1", 1, "events_multi_occ", "2")
	formExecWant(t, results, "S1", 1, "multi_occ_native_id", "2")
	formExecWant(t, results, "S1", 1, "events_multi_subject", "2")
	formExecWant(t, results, "S1", 1, "rows_of_multi_occ", "5")

	formExecWantRows(t, results, "S2", 2)
	formExecWant(t, results, "S2", 0, "events_multi_occ_raw", "0")
	formExecWant(t, results, "S2", 1, "events_multi_occ_raw", "2")
	formExecWantRange(t, results, "S2", 1, "raw_rows_of_multi_occ", 5, 6)

	formExecWantRows(t, results, "S3", 2)
	h8 := map[string]string{formExecH8("linear:hist-a"): "a", formExecH8("linear:hist-c"): "c"}
	for row := 0; row < 2; row++ {
		switch h8[formExecCell(t, results, "S3", row, "event_h8")] {
		case "a":
			formExecWant(t, results, "S3", row, "row_count", "2")
			formExecWant(t, results, "S3", row, "occurred_values", "2")
			formExecWant(t, results, "S3", row, "subjects", "1")
			formExecWant(t, results, "S3", row, "native_id", "1")
			formExecWant(t, results, "S3", row, "occ_span_ms", "250")
			formExecWant(t, results, "S3", row, "min_occurred_utc", "2026-09-01 10:00:00.000")
			formExecWant(t, results, "S3", row, "min_last_synced_utc", "2026-09-02 12:00:00.000")
			formExecWant(t, results, "S3", row, "max_last_synced_utc", "2026-09-03 12:00:00.000")
			formExecWant(t, results, "S3", row, "rows_ls_before_cut", "0")
			formExecWant(t, results, "S3", row, "rows_ls_after_cut", "2")
		case "c":
			formExecWant(t, results, "S3", row, "row_count", "3")
			formExecWant(t, results, "S3", row, "occurred_values", "2")
			formExecWant(t, results, "S3", row, "subjects", "2")
			formExecWant(t, results, "S3", row, "occ_span_ms", "3600000")
			formExecWant(t, results, "S3", row, "min_last_synced_utc", "2026-08-20 00:00:00.000")
			formExecWant(t, results, "S3", row, "rows_ls_before_cut", "1")
			formExecWant(t, results, "S3", row, "rows_ls_after_cut", "2")
		default:
			t.Errorf("S3 row %d: event_h8 %s is not event a or c", row, formExecCell(t, results, "S3", row, "event_h8"))
		}
	}

	formExecWantRows(t, results, "S4", 4)
	keys := make([]string, 0, 4)
	for row := 0; row < len(results["S4"].rows); row++ {
		event := h8[formExecCell(t, results, "S4", row, "event_h8")]
		occurred := formExecCell(t, results, "S4", row, "occurred_utc")
		keys = append(keys, event+" "+occurred)
		if event == "a" && occurred == "2026-09-01 10:00:00.000" {
			formExecWant(t, results, "S4", row, "row_count", "1")
			formExecWant(t, results, "S4", row, "max_last_synced_utc", "2026-09-03 12:00:00.000")
		}
		if event == "c" && occurred == "2026-09-07 08:00:00.000" {
			formExecWant(t, results, "S4", row, "row_count", "2")
			formExecWant(t, results, "S4", row, "subjects", "2")
		}
	}
	sort.Strings(keys)
	if got, want := strings.Join(keys, ","), "a 2026-09-01 10:00:00.000,a 2026-09-01 10:00:00.250,c 2026-09-07 08:00:00.000,c 2026-09-07 09:00:00.000"; got != want {
		t.Errorf("S4 keys = %s, want %s", got, want)
	}
}

type formExecCase struct {
	name, status, duration string
}

type formExecSuite struct {
	name, started string
	failed        int
	cases         []formExecCase
}

// CHAOS-14259 fixture (repo uuids and run ids are synthetic). Runs of the github repo of org c6a38355-...:
//
//	r1 (attempt 2): matrix legs. Leg B: shards b1, b2 and an exact total bt (bt last synced older). Leg A: shards a1, a2
//	   and a total at whose pkg1 suite disagrees. Newest completeness row names aggregate_mismatch.
//	r2: two equal timed copies c1, c2, no completeness row.
//	r3: an exact total r3t with a coverage row (never dropped), complete after FINAL.
//	r6: slot t at attempts 1 and 2, slot u at attempt 1 equal to t@1 only (not the newest t).
//	r7: a total of two legs that share a case identity (not a partition).
//	r8: two equal copies without case durations (not timed).
//
// r4 (a gitlab repo of the same org) and r5 (another org) hold an exact total and must not count.
func TestFormExecFcdevCHAOS14259(t *testing.T) {
	ctx, conn := formExecOpen(t)
	const (
		org, other            = "c6a38355-1425-4000-8000-000000000001", "e6a38355-1425-4000-8000-000000000002"
		github, gitlab, alien = "00000000-0000-4000-8000-00000000a001", "00000000-0000-4000-8000-00000000a002", "00000000-0000-4000-8000-00000000a003"
		newLS, oldLS, copyLS  = "2026-11-21 00:00:00.000", "2026-11-10 00:00:00.000", "2026-11-01 00:00:00.000"
	)
	formExecExec(ctx, t, conn, `INSERT INTO repos (id, repo, org_id, provider, created_at, last_synced) VALUES ('`+github+`', 'fixture/github', '`+org+`', 'github', '2026-01-01 00:00:00', '2026-01-01 00:00:00'), ('`+gitlab+`', 'fixture/gitlab', '`+org+`', 'gitlab', '2026-01-01 00:00:00', '2026-01-01 00:00:00'), ('`+alien+`', 'fixture/other', '`+other+`', 'github', '2026-01-01 00:00:00', '2026-01-01 00:00:00')`)

	var suiteValues, caseValues []string
	orgSuites := 0
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
				caseValues = append(caseValues, fmt.Sprintf("('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', %s, 0, NULL, NULL, NULL, 0, '%s')",
					orgID, repo, run, suiteID, testCase.name, testCase.name, suite.name, testCase.status, testCase.duration, lastSynced))
			}
		}
	}
	const legB, legA = "2026-11-01 10:00:00.000", "2026-11-01 11:00:00.000"
	pkg1B := formExecSuite{"pkg1", legB, 0, []formExecCase{{"x1", "passed", "0.5"}}}
	pkg2B := formExecSuite{"pkg2", legB, 0, []formExecCase{{"x2", "passed", "0.5"}}}
	pkg1A := formExecSuite{"pkg1", legA, 0, []formExecCase{{"x1", "passed", "0.7"}}}
	pkg1AFailed := formExecSuite{"pkg1", legA, 1, []formExecCase{{"x1", "failed", "0.7"}}}
	pkg2A := formExecSuite{"pkg2", legA, 0, []formExecCase{{"x2", "passed", "0.7"}}}
	slot(org, github, "r1", "b1", 2, newLS, pkg1B)
	slot(org, github, "r1", "b2", 2, newLS, pkg2B)
	slot(org, github, "r1", "bt", 2, oldLS, pkg1B, pkg2B)
	slot(org, github, "r1", "a1", 2, newLS, pkg1A)
	slot(org, github, "r1", "a2", 2, newLS, pkg2A)
	slot(org, github, "r1", "at", 2, newLS, pkg1AFailed, pkg2A)

	pkg3 := formExecSuite{"pkg3", "2026-10-31 10:00:00.000", 0, []formExecCase{{"y1", "passed", "0.2"}}}
	slot(org, github, "r2", "c1", 1, copyLS, pkg3)
	slot(org, github, "r2", "c2", 1, copyLS, pkg3)

	pkg4 := formExecSuite{"pkg4", "2026-11-02 10:00:00.000", 0, []formExecCase{{"z1", "passed", "0.1"}}}
	pkg5 := formExecSuite{"pkg5", "2026-11-02 10:00:00.000", 0, []formExecCase{{"z2", "passed", "0.1"}}}
	slot(org, github, "r3", "r3a", 1, newLS, pkg4)
	slot(org, github, "r3", "r3b", 1, newLS, pkg5)
	slot(org, github, "r3", "r3t", 1, newLS, pkg4, pkg5)
	slot(org, gitlab, "r4", "g1", 1, newLS, pkg4)
	slot(org, gitlab, "r4", "g2", 1, newLS, pkg5)
	slot(org, gitlab, "r4", "gt", 1, newLS, pkg4, pkg5)
	slot(other, alien, "r5", "x1", 1, newLS, pkg4)
	slot(other, alien, "r5", "x2", 1, newLS, pkg5)
	slot(other, alien, "r5", "xt", 1, newLS, pkg4, pkg5)

	pkg6 := formExecSuite{"pkg6", "2026-11-03 10:00:00.000", 0, []formExecCase{{"w1", "passed", "0.3"}}}
	pkg6Failed := formExecSuite{"pkg6", "2026-11-03 10:00:00.000", 1, []formExecCase{{"w1", "failed", "0.3"}}}
	slot(org, github, "r6", "t", 1, newLS, pkg6)
	slot(org, github, "r6", "u", 1, newLS, pkg6)
	slot(org, github, "r6", "t", 2, newLS, pkg6Failed)

	slot(org, github, "r7", "p1", 1, newLS, formExecSuite{"pkg7", "2026-11-04 10:00:00.000", 0, []formExecCase{{"v1", "passed", "0.4"}}})
	slot(org, github, "r7", "p2", 1, newLS, formExecSuite{"pkg7", "2026-11-04 11:00:00.000", 0, []formExecCase{{"v1", "passed", "0.4"}}})
	slot(org, github, "r7", "tot", 1, newLS,
		formExecSuite{"pkg7", "2026-11-04 10:00:00.000", 0, []formExecCase{{"v1", "passed", "0.4"}}},
		formExecSuite{"pkg7", "2026-11-04 11:00:00.000", 0, []formExecCase{{"v1", "passed", "0.4"}}})

	pkg8 := formExecSuite{"pkg8", "2026-11-05 10:00:00.000", 0, []formExecCase{{"u1", "passed", "NULL"}}}
	slot(org, github, "r8", "d1", 1, newLS, pkg8)
	slot(org, github, "r8", "d2", 1, newLS, pkg8)

	formExecExec(ctx, t, conn, `INSERT INTO test_suite_results (org_id, repo_id, run_id, suite_id, suite_name, framework, environment, total_count, passed_count, failed_count, skipped_count, error_count, quarantined_count, retried_count, duration_seconds, started_at, finished_at, team_id, service_id, last_synced, report_slot, run_attempt) VALUES `+strings.Join(suiteValues, ", "))
	formExecExec(ctx, t, conn, `INSERT INTO test_case_results (org_id, repo_id, run_id, suite_id, case_id, case_name, class_name, status, duration_seconds, retry_attempt, failure_message, failure_type, stack_trace, is_quarantined, last_synced) VALUES `+strings.Join(caseValues, ", "))
	// An older version of b1's suite row: a second part, FINAL keeps the newer row.
	formExecExec(ctx, t, conn, `INSERT INTO test_suite_results (org_id, repo_id, run_id, suite_id, suite_name, framework, environment, total_count, passed_count, failed_count, skipped_count, error_count, quarantined_count, retried_count, duration_seconds, started_at, finished_at, team_id, service_id, last_synced, report_slot, run_attempt) VALUES ('`+org+`', '`+github+`', 'r1', 'b1@2/0-pkg1', 'pkg1', 'go', NULL, 9, 9, 0, 0, 0, 0, 0, 9.5, '`+legB+`', '`+legB+`', NULL, NULL, '2026-11-09 00:00:00.000', 'b1', 2)`)
	formExecExec(ctx, t, conn, `INSERT INTO coverage_snapshots (org_id, repo_id, run_id, snapshot_id, report_format, lines_total, lines_covered, last_synced, report_slot, run_attempt) VALUES ('`+org+`', '`+github+`', 'r3', 'cov-r3t', 'lcov', 10, 7, '`+newLS+`', 'r3t', 1)`)
	completeness := `INSERT INTO testops_run_report_completeness (org_id, repo_id, run_id, run_attempt, provider, complete, reasons, reports_expected, reports_admitted, last_synced, verdict_version) VALUES `
	formExecExec(ctx, t, conn, completeness+`('`+org+`', '`+github+`', 'r1', 1, 'github', 1, [], 6, 6, '`+newLS+`', 4398046512104), ('`+org+`', '`+github+`', 'r1', 2, 'github', 0, ['aggregate_mismatch'], 6, 6, '`+newLS+`', 2000), ('`+other+`', '`+alien+`', 'r5', 1, 'github', 0, ['aggregate_mismatch'], 3, 3, '`+newLS+`', 1000)`)
	formExecExec(ctx, t, conn, completeness+`('`+org+`', '`+github+`', 'r3', 1, 'github', 0, ['aggregate_mismatch'], 3, 3, '`+newLS+`', 1000)`)
	formExecExec(ctx, t, conn, completeness+`('`+org+`', '`+github+`', 'r3', 1, 'github', 1, [], 3, 3, '`+newLS+`', 4398046513104)`)

	results := formExecRun(ctx, t, conn, "FORM-14259-SIZE-1", formExec14259SQL, formExec14259SHA256)
	if len(results) != 4 {
		t.Fatalf("ran %d statements, want 4 (S0..S3)", len(results))
	}
	suites := strconv.Itoa(orgSuites)
	formExecWant(t, results, "S0", 0, "suite_orgs", "1")
	formExecWant(t, results, "S0", 0, "repo_orgs", "1")
	formExecWant(t, results, "S0", 0, "completeness_orgs", "1")
	formExecWant(t, results, "S0", 0, "github_repos", "1")
	formExecWantRange(t, results, "S0", 0, "suite_rows_raw", int64(orgSuites), int64(orgSuites)+1)
	formExecWantRange(t, results, "S0", 0, "slot_suite_rows_raw", int64(orgSuites), int64(orgSuites)+1)
	formExecWant(t, results, "S0", 0, "coverage_rows_raw", "1")

	formExecWant(t, results, "S1", 0, "u_runs", "5")
	formExecWant(t, results, "S1", 0, "u_slots", "14")
	formExecWant(t, results, "S1", 0, "u_runs_marked_mismatch", "1")
	formExecWant(t, results, "S1", 0, "u_runs_complete", "1")
	formExecWant(t, results, "S1", 0, "u_runs_no_row", "3")
	formExecWant(t, results, "S1", 0, "u_slots_stale", "1")
	formExecWant(t, results, "S1", 0, "u_slots_before_14045_commit", "3")
	formExecWant(t, results, "S1", 0, "u_min_ls_utc", "2026-11-01 00:00:00.000")
	formExecWant(t, results, "S1", 0, "u_max_ls_utc", "2026-11-21 00:00:00.000")

	formExecWant(t, results, "S2", 0, "l_runs", "2")
	formExecWant(t, results, "S2", 0, "l_slots", "3")
	formExecWant(t, results, "S2", 0, "l_runs_aggregate", "1")
	formExecWant(t, results, "S2", 0, "l_runs_copy", "1")
	formExecWant(t, results, "S2", 0, "l_runs_marked_mismatch", "1")
	formExecWant(t, results, "S2", 0, "l_runs_complete", "0")
	formExecWant(t, results, "S2", 0, "l_runs_no_row", "1")
	formExecWant(t, results, "S2", 0, "l_slots_stale", "1")
	formExecWant(t, results, "S2", 0, "l_slots_before_14045_commit", "3")
	formExecWant(t, results, "S2", 0, "l_slots_not_in_u", "0")

	formExecWant(t, results, "S3", 0, "suite_rows_final", suites)
	formExecWant(t, results, "S3", 0, "suite_keys_raw", suites)
}
