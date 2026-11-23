//go:build integration

package chmigrate_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
)

// Throwaway exec proof of a prod read form: not for merge. sizingForm is the form file byte for byte.
// The cost gate cap literals of the form (30M rows, 4 GiB on disk).
const (
	rowsCap  = "> 30000000"
	bytesCap = "> 4294967296"
)

const sizingFormSHA256 = "706d1c5d4de6737e2ea24faec8e28b482db70423d5183083de1f7b774fc394d2"

const sizingForm = `-- SIZING-13902-4 = SIZING-13902-3 (f38fec483020) + an engine cost gate in each heavy statement, lane l13902-form4, 2026-11-23.
-- Prod: NOT EXECUTED. Executed once on CI (synthetic rows, schema at the chain head): EXEC-PROOF-13902-4.md.
-- Edits vs -3 (the -3 edits G1-G4 and the -2 edits E1-E9 stay, see those files):
--   H1 (acp P2-1) stmt0, stmt1, stmt2 and stmt3 each start with WITH (SELECT throwIf(...)) AS cost_gate. The gate sums the
--      active-part rows and bytes on disk (system.parts) of the tables THAT statement reads, and throws 'STOP-COST-GATE'
--      over 30M rows or over 4 GiB. It is a scalar subquery: the engine runs it once, at query analysis, before any table
--      read of the statement (a bare throwIf term in a WHERE is not constant-folded and runs per block at execution).
--      stmt0 uses cost_gate inside each of its scalar subqueries (they also run at analysis, so the gate must come first);
--      stmt1 to stmt3 use it in the outer WHERE. The gate reads only system.parts (metadata) and system.one.
--   H2 S0 stays INFO: R, B, P per table = the inputs of the read_rows WANT (SIZING-13902-4.md). The stop is the gate (H1).
--   H3 (acp P3-1) STOP before stmt2 and stmt3 when stmt1 live_repo_days is over 5M in either row (the JOIN memory).
-- Run rule: each statement once, in order. A SQL error (STOP-COST-GATE included) = STOP. A timeout (30 s) = STOP, no re-run.
-- A STOP rule or a read_rows WANT miss = STOP (acp reads).

-- S0 INFO cost inputs. WANT 3 rows (one per table), table_rows > 0 in each. Unit: all orgs (upper bound for the org reads).
SELECT table, sum(rows) AS table_rows, sum(bytes_on_disk) AS disk_bytes, count() AS active_parts
FROM system.parts
WHERE active AND database = currentDatabase()
  AND table IN ('ci_pipeline_runs', 'cicd_metrics_daily', 'testops_pipeline_metrics_daily')
GROUP BY table
ORDER BY table;

-- stmt0 ORG-1. WANT raw_orgs = 1, cicd_orgs = 1, testops_orgs = 1. Gate tables: raw + cicd + testops.
WITH (SELECT throwIf(
    (SELECT sum(rows) FROM system.parts WHERE active AND database = currentDatabase()
      AND table IN ('ci_pipeline_runs', 'cicd_metrics_daily', 'testops_pipeline_metrics_daily')) > 30000000
    OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = currentDatabase()
      AND table IN ('ci_pipeline_runs', 'cicd_metrics_daily', 'testops_pipeline_metrics_daily')) > 4294967296,
    'STOP-COST-GATE')) AS cost_gate
SELECT
  (SELECT uniqExact(org_id) FROM ci_pipeline_runs WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0) AS raw_orgs,
  (SELECT uniqExact(org_id) FROM cicd_metrics_daily WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0) AS cicd_orgs,
  (SELECT uniqExact(org_id) FROM testops_pipeline_metrics_daily WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0) AS testops_orgs;

-- stmt1 RANGE of the stored live repo-days (= queue-table rows a full-range recompute rewrites). Gate tables: cicd + testops.
-- WANT live_repo_days > 0 in both rows. STOP before stmt2/3 when live_repo_days is over 5M in either row. INFO first_day,
-- last_day, days. The gate sits in the first branch: the engine analyzes the whole UNION ALL before it reads a table.
WITH (SELECT throwIf(
    (SELECT sum(rows) FROM system.parts WHERE active AND database = currentDatabase()
      AND table IN ('cicd_metrics_daily', 'testops_pipeline_metrics_daily')) > 30000000
    OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = currentDatabase()
      AND table IN ('cicd_metrics_daily', 'testops_pipeline_metrics_daily')) > 4294967296,
    'STOP-COST-GATE')) AS cost_gate
SELECT 'cicd' AS t, count() AS live_repo_days, min(day) AS first_day, max(day) AS last_day, uniqExact(day) AS days
FROM (SELECT repo_id, day, argMax(retracted, computed_at) AS r FROM cicd_metrics_daily
      WHERE startsWith(org_id, 'c6a38355') GROUP BY repo_id, day) AS cicd_keys WHERE r = 0 AND cost_gate = 0
UNION ALL
SELECT 'testops' AS t, count() AS live_repo_days, min(day) AS first_day, max(day) AS last_day, uniqExact(day) AS days
FROM (SELECT repo_id, day, argMax(retracted, computed_at) AS r FROM testops_pipeline_metrics_daily
      WHERE startsWith(org_id, 'c6a38355') GROUP BY repo_id, day) AS testops_keys WHERE r = 0;

-- stmt2 CICD. Raw = the cicd source (cicd/clickhouse.go:51-57): finished_at day, status not deleted, not running/queued.
-- Gate tables: raw + cicd. Run only if stmt1 live_repo_days is 5M or less in both rows.
-- WANT live_repo_days > 0, with_raw > 0, ctrl_checked > 0, ctrl_match = ctrl_checked.
-- WANT no_raw = 0 (a live key with no raw run gets a RETRACTION row at recompute, cicd/fence.go:66-69, :84-93).
-- WANT move_repo_days >= 1 (0 = NOT EXERCISED for cicd). WANT old_match = old_checked (a miss = acp reads).
-- INFO value_move (stored values that change), to_null, move_days, first/last, move_after_roll, move_raw_changed.
WITH (SELECT throwIf(
    (SELECT sum(rows) FROM system.parts WHERE active AND database = currentDatabase()
      AND table IN ('ci_pipeline_runs', 'cicd_metrics_daily')) > 30000000
    OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = currentDatabase()
      AND table IN ('ci_pipeline_runs', 'cicd_metrics_daily')) > 4294967296,
    'STOP-COST-GATE')) AS cost_gate
SELECT count() AS live_repo_days, countIf(ifNull(raw.runs, 0) > 0) AS with_raw,
  countIf(ifNull(raw.runs, 0) = 0) AS no_raw,
  minIf(s.day, ifNull(raw.runs, 0) = 0) AS no_raw_first_day, maxIf(s.day, ifNull(raw.runs, 0) = 0) AS no_raw_last_day,
  countIf(raw.rr > 0) AS move_repo_days, uniqExactIf(s.day, raw.rr > 0) AS move_days,
  minIf(s.day, raw.rr > 0) AS move_first_day, maxIf(s.day, raw.rr > 0) AS move_last_day,
  countIf(raw.rr > 0 AND raw.n_new = 0) AS to_null,
  countIf(raw.rr > 0 AND s.c >= toDateTime('2026-11-23 02:58:22', 'UTC')) AS move_after_roll,
  countIf(raw.rr > 0 AND raw.ls >= s.c) AS move_raw_changed,
  countIf(raw.rr > 0 AND raw.n_old > 0 AND s.c < toDateTime('2026-11-23 02:58:22', 'UTC') AND raw.ls < s.c) AS old_checked,
  countIf(raw.rr > 0 AND raw.n_old > 0 AND s.c < toDateTime('2026-11-23 02:58:22', 'UTC') AND raw.ls < s.c
          AND abs(ifNull(s.q, -1e9) - raw.sum_old / raw.n_old) <= 1e-6 * greatest(1, raw.sum_old / raw.n_old)) AS old_match,
  countIf(raw.rr > 0 AND raw.n_old > 0 AND s.c < toDateTime('2026-11-23 02:58:22', 'UTC') AND raw.ls < s.c
          AND (raw.n_new = 0
               OR abs(raw.sum_old / raw.n_old - raw.sum_new / raw.n_new) > 1e-6 * greatest(1, raw.sum_new / raw.n_new))) AS value_move,
  countIf(ifNull(raw.rr, 0) = 0 AND raw.n_old > 0 AND raw.ls < s.c) AS ctrl_checked,
  countIf(ifNull(raw.rr, 0) = 0 AND raw.n_old > 0 AND raw.ls < s.c
          AND abs(ifNull(s.q, -1e9) - raw.sum_old / raw.n_old) <= 1e-6 * greatest(1, raw.sum_old / raw.n_old)) AS ctrl_match
FROM (SELECT repo_id, day, max(computed_at) AS c, argMax(retracted, computed_at) AS r,
        tupleElement(argMax(tuple(avg_queue_minutes), computed_at), 1) AS q
      FROM cicd_metrics_daily WHERE startsWith(org_id, 'c6a38355') GROUP BY repo_id, day) AS s
LEFT JOIN (
  SELECT repo_id, toDate(assumeNotNull(finished_at)) AS day, count() AS runs, max(last_synced) AS ls,
    countIf(retry_count > 0 AND queued_at IS NOT NULL AND started_at >= queued_at) AS rr,
    countIf(queued_at IS NOT NULL AND started_at >= queued_at) AS n_old,
    sumIf(dateDiff('millisecond', queued_at, started_at) / 60000, queued_at IS NOT NULL AND started_at >= queued_at) AS sum_old,
    countIf(retry_count = 0 AND queued_at IS NOT NULL AND started_at >= queued_at) AS n_new,
    sumIf(dateDiff('millisecond', queued_at, started_at) / 60000, retry_count = 0 AND queued_at IS NOT NULL AND started_at >= queued_at) AS sum_new
  FROM ci_pipeline_runs FINAL
  WHERE startsWith(org_id, 'c6a38355') AND finished_at IS NOT NULL
    AND lowerUTF8(trimBoth(ifNull(status, ''))) NOT IN ('deleted', 'running', 'queued')
  GROUP BY repo_id, day
) AS raw ON raw.repo_id = s.repo_id AND raw.day = s.day
WHERE s.r = 0 AND cost_gate = 0;

-- stmt3 TESTOPS. Raw = the testops pipeline source (testops_native_clickhouse.go:126-130): started_at day, status not deleted.
-- Old sample = the raw arm text before the fix (queryapi/testopspooled/testopspooled.go:100-101 minus the retry_count guard).
-- Gate tables: raw + testops. Run only if stmt1 live_repo_days is 5M or less in both rows.
-- WANT / INFO: as stmt2 (no_raw keys are retired by supersedeStaleTeamKeys, testops_native_executor.go:169-185).
WITH (SELECT throwIf(
    (SELECT sum(rows) FROM system.parts WHERE active AND database = currentDatabase()
      AND table IN ('ci_pipeline_runs', 'testops_pipeline_metrics_daily')) > 30000000
    OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = currentDatabase()
      AND table IN ('ci_pipeline_runs', 'testops_pipeline_metrics_daily')) > 4294967296,
    'STOP-COST-GATE')) AS cost_gate
SELECT count() AS live_repo_days, countIf(ifNull(raw.runs, 0) > 0) AS with_raw,
  countIf(ifNull(raw.runs, 0) = 0) AS no_raw,
  minIf(s.day, ifNull(raw.runs, 0) = 0) AS no_raw_first_day, maxIf(s.day, ifNull(raw.runs, 0) = 0) AS no_raw_last_day,
  countIf(raw.rr > 0) AS move_repo_days, uniqExactIf(s.day, raw.rr > 0) AS move_days,
  minIf(s.day, raw.rr > 0) AS move_first_day, maxIf(s.day, raw.rr > 0) AS move_last_day,
  countIf(raw.rr > 0 AND raw.n_new = 0) AS to_null,
  countIf(raw.rr > 0 AND s.c >= toDateTime('2026-11-23 02:58:22', 'UTC')) AS move_after_roll,
  countIf(raw.rr > 0 AND raw.ls >= s.c) AS move_raw_changed,
  countIf(raw.rr > 0 AND raw.n_old > 0 AND s.c < toDateTime('2026-11-23 02:58:22', 'UTC') AND raw.ls < s.c) AS old_checked,
  countIf(raw.rr > 0 AND raw.n_old > 0 AND s.c < toDateTime('2026-11-23 02:58:22', 'UTC') AND raw.ls < s.c
          AND abs(ifNull(s.q, -1e9) - raw.sum_old / raw.n_old) <= 1e-6 * greatest(1, raw.sum_old / raw.n_old)) AS old_match,
  countIf(raw.rr > 0 AND raw.n_old > 0 AND s.c < toDateTime('2026-11-23 02:58:22', 'UTC') AND raw.ls < s.c
          AND (raw.n_new = 0
               OR abs(raw.sum_old / raw.n_old - raw.sum_new / raw.n_new) > 1e-6 * greatest(1, raw.sum_new / raw.n_new))) AS value_move,
  countIf(ifNull(raw.rr, 0) = 0 AND raw.n_old > 0 AND raw.ls < s.c) AS ctrl_checked,
  countIf(ifNull(raw.rr, 0) = 0 AND raw.n_old > 0 AND raw.ls < s.c
          AND abs(ifNull(s.q, -1e9) - raw.sum_old / raw.n_old) <= 1e-6 * greatest(1, raw.sum_old / raw.n_old)) AS ctrl_match
FROM (SELECT repo_id, day, max(computed_at) AS c, argMax(retracted, computed_at) AS r,
        tupleElement(argMax(tuple(avg_queue_seconds), computed_at), 1) AS q
      FROM testops_pipeline_metrics_daily WHERE startsWith(org_id, 'c6a38355') GROUP BY repo_id, day) AS s
LEFT JOIN (
  SELECT repo_id, day, count() AS runs, max(run_ls) AS ls, countIf(retry_count > 0 AND qs IS NOT NULL) AS rr,
    count(qs) AS n_old, sum(qs) AS sum_old,
    countIf(retry_count = 0 AND qs IS NOT NULL) AS n_new, sumIf(qs, retry_count = 0) AS sum_new
  FROM (
    SELECT repo_id, toDate(started_at) AS day, last_synced AS run_ls, retry_count,
      if(queue_seconds >= 0, queue_seconds,
         if(started_at >= queued_at, dateDiff('millisecond', queued_at, started_at) / 1000, NULL)) AS qs
    FROM ci_pipeline_runs FINAL
    WHERE startsWith(org_id, 'c6a38355') AND lowerUTF8(trimBoth(ifNull(status, ''))) != 'deleted'
  ) AS run_rows
  GROUP BY repo_id, day
) AS raw ON raw.repo_id = s.repo_id AND raw.day = s.day
WHERE s.r = 0 AND cost_gate = 0;
`

// Synthetic rows. Org c6a38355-0000-..., other org b0000000-0000-... (filter control). Merges are stopped and each
// version of a key is its own INSERT, so keys 11-10, 11-14 and 11-15 hold 2 unmerged versions when the form runs.
// Days: 11-10 re-run move (old rule), 11-11 control (+ a deleted re-run that must not count), 11-12 live key with no raw,
// 11-13 re-run with raw synced after the compute, 11-14 control whose NEWEST version is NULL, 11-15 retracted key,
// 11-22 re-run-only day computed in the rollout window.
const sizingFixture = `
SYSTEM STOP MERGES cicd_metrics_daily;
SYSTEM STOP MERGES testops_pipeline_metrics_daily;
INSERT INTO ci_pipeline_runs (org_id, repo_id, run_id, provider, status, queued_at, started_at, finished_at, last_synced, retry_count, queue_seconds) VALUES
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', 'run-a', 'github_actions', 'success', '2026-11-10 10:00:00', '2026-11-10 10:02:00', '2026-11-10 10:10:00', '2026-11-11 00:00:00', 0, 120),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', 'run-b', 'github_actions', 'success', '2026-11-10 09:00:00', '2026-11-10 09:10:00', '2026-11-10 09:30:00', '2026-11-11 00:00:00', 1, 600),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', 'run-c', 'github_actions', 'success', '2026-11-11 10:00:00', '2026-11-11 10:03:00', '2026-11-11 10:20:00', '2026-11-11 12:00:00', 0, NULL),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', 'run-del', 'github_actions', ' Deleted', '2026-11-11 08:00:00', '2026-11-11 08:15:00', '2026-11-11 08:30:00', '2026-11-11 12:00:00', 1, 900),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', 'run-e', 'github_actions', 'success', '2026-11-13 10:00:00', '2026-11-13 10:01:00', '2026-11-13 10:10:00', '2026-11-15 00:00:00', 1, 60),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', 'run-f', 'github_actions', 'success', '2026-11-13 11:00:00', '2026-11-13 11:02:00', '2026-11-13 11:10:00', '2026-11-15 00:00:00', 0, 120),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', 'run-g', 'github_actions', 'success', '2026-11-14 10:00:00', '2026-11-14 10:02:00', '2026-11-14 10:10:00', '2026-11-14 12:00:00', 0, 120),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000002', 'run-d', 'github_actions', 'success', '2026-11-22 08:00:00', '2026-11-22 08:05:00', '2026-11-22 08:20:00', '2026-11-23 01:00:00', 2, 300),
('b0000000-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', 'run-x', 'github_actions', 'success', '2026-11-10 10:00:00', '2026-11-10 10:30:00', '2026-11-10 10:40:00', '2026-11-11 00:00:00', 1, 1800);
INSERT INTO cicd_metrics_daily (org_id, repo_id, day, pipelines_count, success_rate, avg_queue_minutes, computed_at, retracted) VALUES
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', '2026-11-10', 2, 1, 99, '2026-11-10 23:00:00', 0),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', '2026-11-11', 1, 1, 3, '2026-11-12 01:00:00', 0),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', '2026-11-12', 1, 1, 4, '2026-11-13 01:00:00', 0),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', '2026-11-13', 2, 1, 1.5, '2026-11-14 01:00:00', 0),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', '2026-11-14', 1, 1, 2, '2026-11-15 01:00:00', 0),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000002', '2026-11-15', 1, 1, 1, '2026-11-16 01:00:00', 0),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000002', '2026-11-22', 1, 1, NULL, '2026-11-23 03:00:00', 0),
('b0000000-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', '2026-11-10', 1, 1, 30, '2026-11-11 01:00:00', 0);
INSERT INTO cicd_metrics_daily (org_id, repo_id, day, pipelines_count, success_rate, avg_queue_minutes, computed_at, retracted) VALUES
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', '2026-11-10', 2, 1, 6, '2026-11-11 01:00:00', 0),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', '2026-11-14', 1, 1, NULL, '2026-11-16 01:00:00', 0),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000002', '2026-11-15', 0, 0, NULL, '2026-11-17 01:00:00', 1);
INSERT INTO testops_pipeline_metrics_daily (org_id, repo_id, day, pipelines_count, avg_queue_seconds, computed_at, retracted) VALUES
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', '2026-11-10', 2, 999, '2026-11-10 23:00:00', 0),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', '2026-11-11', 1, 180, '2026-11-12 01:00:00', 0),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', '2026-11-12', 1, 240, '2026-11-13 01:00:00', 0),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', '2026-11-13', 2, 90, '2026-11-14 01:00:00', 0),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', '2026-11-14', 1, 120, '2026-11-15 01:00:00', 0),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000002', '2026-11-15', 1, 60, '2026-11-16 01:00:00', 0),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000002', '2026-11-22', 1, NULL, '2026-11-23 03:00:00', 0),
('b0000000-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', '2026-11-10', 1, 1800, '2026-11-11 01:00:00', 0);
INSERT INTO testops_pipeline_metrics_daily (org_id, repo_id, day, pipelines_count, avg_queue_seconds, computed_at, retracted) VALUES
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', '2026-11-10', 2, 360, '2026-11-11 01:00:00', 0),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', '2026-11-14', 1, NULL, '2026-11-16 01:00:00', 0),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000002', '2026-11-15', 0, NULL, '2026-11-17 01:00:00', 1);
`

type formStatement struct {
	label string
	text  string
}

// splitForm cuts the form at lines whose code part ends with ";", keeping each statement's comment lines.
func splitForm(body string) []formStatement {
	var out []formStatement
	var chunk []string
	for _, line := range strings.Split(body, "\n") {
		chunk = append(chunk, line)
		code := strings.TrimSpace(chmigrate.StripLineComments(line))
		if !strings.HasSuffix(code, ";") {
			continue
		}
		text := strings.TrimSuffix(strings.TrimRight(strings.Join(chunk, "\n"), " \t\n"), ";")
		label := ""
		for _, l := range chunk {
			if t := strings.TrimSpace(l); strings.HasPrefix(t, "-- S0") || strings.HasPrefix(t, "-- stmt") {
				label = strings.Fields(strings.TrimPrefix(t, "-- "))[0]
				break
			}
		}
		out = append(out, formStatement{label: label, text: text})
		chunk = nil
	}
	return out
}

func renderCell(v any) string {
	value := reflect.ValueOf(v)
	for value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return "NULL"
		}
		value = value.Elem()
	}
	if ts, ok := value.Interface().(time.Time); ok {
		return ts.UTC().Format("2006-01-02")
	}
	return fmt.Sprint(value.Interface())
}

func TestSizingReadFormExecutesAtTheChainHead(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	sum := sha256.Sum256([]byte(sizingForm))
	if got := hex.EncodeToString(sum[:]); got != sizingFormSHA256 {
		t.Fatalf("form sha256 = %s, want %s", got, sizingFormSHA256)
	}
	statements := splitForm(sizingForm)
	labels := make([]string, 0, len(statements))
	for _, s := range statements {
		labels = append(labels, s.label)
	}
	if strings.Join(labels, ",") != "S0,stmt0,stmt1,stmt2,stmt3" {
		t.Fatalf("form statements = %v", labels)
	}
	reads := map[string][]string{
		"stmt0": {"ci_pipeline_runs", "cicd_metrics_daily", "testops_pipeline_metrics_daily"},
		"stmt1": {"cicd_metrics_daily", "testops_pipeline_metrics_daily"},
		"stmt2": {"ci_pipeline_runs", "cicd_metrics_daily"},
		"stmt3": {"ci_pipeline_runs", "testops_pipeline_metrics_daily"},
	}
	// Each heavy statement holds exactly one gate (one rows cap, one bytes cap) over exactly the tables it reads.
	for _, s := range statements {
		wantCaps := 1
		if s.label == "S0" {
			wantCaps = 0
		}
		if n := strings.Count(s.text, rowsCap); n != wantCaps {
			t.Fatalf("%s: %d rows caps %q, want %d", s.label, n, rowsCap, wantCaps)
		}
		if n := strings.Count(s.text, bytesCap); n != wantCaps {
			t.Fatalf("%s: %d bytes caps %q, want %d", s.label, n, bytesCap, wantCaps)
		}
		if s.label == "S0" {
			continue
		}
		in := "table IN ('" + strings.Join(reads[s.label], "', '") + "')"
		if n := strings.Count(s.text, in); n != 2 {
			t.Fatalf("%s: gate table list %s found %d times, want 2", s.label, in, n)
		}
		if n := strings.Count(s.text, "'STOP-COST-GATE'"); n != 1 {
			t.Fatalf("%s: %d STOP-COST-GATE throwIf, want 1", s.label, n)
		}
	}

	scratch := scratchDatabase(t)
	conn := openDatabase(t, scratch.URI, "")
	db, _, err := chmigrate.NewConnDB(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}
	baseline, err := chmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	chain, err := chmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	result, err := chmigrate.Upgrade(ctx, db, baseline, chain)
	if err != nil {
		t.Fatalf("real chain: %v", err)
	}
	if result.Head < "170_" {
		t.Fatalf("schema head %q, want 170 or later", result.Head)
	}
	t.Logf("schema head %s", result.Head)

	for _, statement := range chmigrate.SplitStatements(sizingFixture) {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("fixture: %v\n%s", err, statement)
		}
	}

	// 7 keys per table, 3 of them with 2 unmerged versions.
	for _, table := range []string{"cicd_metrics_daily", "testops_pipeline_metrics_daily"} {
		var physical, keys, multi uint64
		if err := conn.QueryRow(ctx, "SELECT count(), uniqExact(repo_id, day), "+
			"(SELECT count() FROM (SELECT repo_id, day FROM "+table+" WHERE startsWith(org_id, 'c6a38355') GROUP BY repo_id, day HAVING count() = 2) AS k) "+
			"FROM "+table+" WHERE startsWith(org_id, 'c6a38355')").Scan(&physical, &keys, &multi); err != nil {
			t.Fatal(err)
		}
		t.Logf("%s versions: physical=%d keys=%d keys_with_2_versions=%d", table, physical, keys, multi)
		if physical != 10 || keys != 7 || multi != 3 {
			t.Fatalf("%s: physical=%d keys=%d keys_with_2_versions=%d, want 10, 7, 3", table, physical, keys, multi)
		}
	}
	// Discriminator: on the 11-14 key a plain argMax reads the older non-NULL value, the form's tuple read the newest NULL.
	for table, column := range map[string]string{"cicd_metrics_daily": "avg_queue_minutes", "testops_pipeline_metrics_daily": "avg_queue_seconds"} {
		var plain, newest string
		if err := conn.QueryRow(ctx, "SELECT ifNull(toString(argMax("+column+", computed_at)), 'NULL'), "+
			"ifNull(toString(tupleElement(argMax(tuple("+column+"), computed_at), 1)), 'NULL') FROM "+table+
			" WHERE org_id = 'c6a38355-0000-4000-8000-000000000000' AND repo_id = '00000000-0000-4000-8000-000000000001' AND day = '2026-11-14'").
			Scan(&plain, &newest); err != nil {
			t.Fatal(err)
		}
		t.Logf("%s 11-14 key: plain argMax=%s tuple argMax=%s", table, plain, newest)
		if plain == "NULL" || newest != "NULL" {
			t.Fatalf("%s 11-14 key: plain argMax=%s tuple argMax=%s, want non-NULL and NULL", table, plain, newest)
		}
	}

	want := map[string][]map[string]string{
		"stmt0": {{"raw_orgs": "1", "cicd_orgs": "1", "testops_orgs": "1"}},
		"stmt1": {
			{"t": "cicd", "live_repo_days": "6", "first_day": "2026-11-10", "last_day": "2026-11-22", "days": "6"},
			{"t": "testops", "live_repo_days": "6", "first_day": "2026-11-10", "last_day": "2026-11-22", "days": "6"},
		},
	}
	// ctrl_checked 2 = 11-11 + 11-14. ctrl_match 1: the 11-14 stored value is the newest row's NULL, not the older 2 min / 120 s.
	moveWant := map[string]string{
		"live_repo_days": "6", "with_raw": "5", "no_raw": "1", "no_raw_first_day": "2026-11-12", "no_raw_last_day": "2026-11-12",
		"move_repo_days": "3", "move_days": "3", "move_first_day": "2026-11-10", "move_last_day": "2026-11-22", "to_null": "1",
		"move_after_roll": "1", "move_raw_changed": "1", "old_checked": "1", "old_match": "1", "value_move": "1",
		"ctrl_checked": "2", "ctrl_match": "1",
	}
	want["stmt2"] = []map[string]string{moveWant}
	want["stmt3"] = []map[string]string{moveWant}

	tableRows := map[string]uint64{}
	tableParts := map[string]uint64{}
	for _, statement := range statements {
		var readRows uint64
		qctx := clickhouse.Context(ctx, clickhouse.WithProgress(func(p *clickhouse.Progress) { readRows += p.Rows }))
		rows, err := conn.Query(qctx, statement.text)
		if err != nil {
			t.Fatalf("%s: SQL error: %v", statement.label, err)
		}
		columns := rows.Columns()
		types := rows.ColumnTypes()
		var got []map[string]string
		for rows.Next() {
			cells := make([]any, len(types))
			for i, ct := range types {
				cells[i] = reflect.New(ct.ScanType()).Interface()
			}
			if err := rows.Scan(cells...); err != nil {
				t.Fatalf("%s: scan: %v", statement.label, err)
			}
			row := map[string]string{}
			parts := make([]string, 0, len(columns))
			for i, name := range columns {
				row[name] = renderCell(cells[i])
				parts = append(parts, name+"="+row[name])
			}
			t.Logf("%s ROW %s", statement.label, strings.Join(parts, " "))
			got = append(got, row)
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("%s: SQL error: %v", statement.label, err)
		}
		_ = rows.Close()

		if statement.label == "S0" {
			if len(got) != 3 {
				t.Fatalf("S0: %d rows, want 3", len(got))
			}
			for _, row := range got {
				var n uint64
				if _, err := fmt.Sscan(row["table_rows"], &n); err != nil || n == 0 {
					t.Fatalf("S0: table %s table_rows %q, want > 0", row["table"], row["table_rows"])
				}
				tableRows[row["table"]] = n
				var p uint64
				if _, err := fmt.Sscan(row["active_parts"], &p); err != nil || p == 0 {
					t.Fatalf("S0: table %s active_parts %q, want > 0", row["table"], row["active_parts"])
				}
				tableParts[row["table"]] = p
			}
			t.Logf("S0 read_rows=%d", readRows)
			continue
		}

		// md WANT: read_rows <= 1.1 x G + 2 x P + 2 (the gate lists P parts twice, plus one-row system.one reads).
		var g, p uint64
		for _, table := range reads[statement.label] {
			g += tableRows[table]
			p += tableParts[table]
		}
		limit := 1.1*float64(g) + 2*float64(p) + 2
		t.Logf("%s read_rows=%d gate_rows=%d gate_parts=%d limit=%.1f ratio=%.2f", statement.label, readRows, g, p, limit, float64(readRows)/float64(g))
		if 10*readRows > 11*g+20*p+20 {
			t.Errorf("%s: read_rows %d over 1.1 x G + 2 x P + 2 = %.1f (G=%d P=%d)", statement.label, readRows, limit, g, p)
		}

		expected := want[statement.label]
		if len(got) != len(expected) {
			t.Errorf("%s: %d rows, want %d", statement.label, len(got), len(expected))
			continue
		}
		for i, row := range expected {
			actual := got[i]
			if key, ok := row["t"]; ok {
				for _, candidate := range got {
					if candidate["t"] == key {
						actual = candidate
					}
				}
			}
			for name, value := range row {
				if actual[name] != value {
					t.Errorf("%s row %d: %s = %s, want %s", statement.label, i, name, actual[name], value)
				}
			}
		}
	}

	// Gate fires: each heavy statement with ONE cap literal lowered to 0 (rest byte for byte) must stop with
	// STOP-COST-GATE, and the query log must type it ExceptionBeforeStart (stopped at analysis, before any table read).
	type fired struct{ label, cap, comment string }
	var fires []fired
	for _, statement := range statements[1:] {
		for _, c := range []struct{ name, literal string }{{"rows", rowsCap}, {"bytes", bytesCap}} {
			lowered := strings.Replace(statement.text, c.literal, "> 0", 1)
			comment := fmt.Sprintf("sizing13902-gate-%s-%s-%d", statement.label, c.name, time.Now().UnixNano())
			var readRows uint64
			qctx := clickhouse.Context(ctx,
				clickhouse.WithSettings(clickhouse.Settings{"log_comment": comment}),
				clickhouse.WithProgress(func(p *clickhouse.Progress) { readRows += p.Rows }))
			rows, err := conn.Query(qctx, lowered)
			if err == nil {
				for rows.Next() {
				}
				err = rows.Err()
				_ = rows.Close()
			}
			if err == nil || !strings.Contains(err.Error(), "STOP-COST-GATE") {
				t.Errorf("GATE %s %s cap lowered: err = %v, want STOP-COST-GATE", statement.label, c.name, err)
				continue
			}
			t.Logf("GATE %s %s cap lowered: fired, client read_rows=%d, error: %s", statement.label, c.name, readRows, strings.SplitN(err.Error(), "\n", 2)[0])
			fires = append(fires, fired{statement.label, c.name, comment})
		}
	}
	if err := conn.Exec(ctx, "SYSTEM FLUSH LOGS"); err != nil {
		t.Fatalf("the query log is not readable by this user: %v", err)
	}
	for _, f := range fires {
		var types []string
		var logRead uint64
		rows, err := conn.Query(ctx, `SELECT toString(type), read_rows FROM system.query_log
			WHERE log_comment = ? AND type != 'QueryStart' ORDER BY event_time_microseconds`, f.comment)
		if err != nil {
			t.Fatalf("GATE %s %s: query log: %v", f.label, f.cap, err)
		}
		for rows.Next() {
			var typ string
			var r uint64
			if err := rows.Scan(&typ, &r); err != nil {
				t.Fatalf("GATE %s %s: query log scan: %v", f.label, f.cap, err)
			}
			types = append(types, typ)
			logRead += r
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("GATE %s %s: query log: %v", f.label, f.cap, err)
		}
		_ = rows.Close()
		t.Logf("GATE %s %s query_log types=%v read_rows=%d", f.label, f.cap, types, logRead)
		if len(types) != 1 || types[0] != "ExceptionBeforeStart" {
			t.Errorf("GATE %s %s: query_log types %v, want [ExceptionBeforeStart]", f.label, f.cap, types)
		}
	}
	if len(fires) != 8 {
		t.Errorf("gate fired %d times, want 8 (4 heavy statements x 2 caps)", len(fires))
	}
}

// Second throwaway exec proof: the read-back form of the recompute card. readbackForm is READBACK-13902-5.sql byte for byte.
const readbackFormSHA256 = "8a95fef669a888ca108a35736b7057e9ed36fcc757be14cebc7cb867f6adc30e"

const readbackForm = `-- READBACK-13902-5 = READBACK-13902-4 (17d5b2baef68) with the rows caps at the FORM-TEMPLATE-3 budget. Lane l13902-cutfix
-- (lead fcsup), 2026-11-23. ClickHouse. Shape: FORM-TEMPLATE-3 (pm-guard, 6b9e73a3c45a) (a) + pass-path budget.
-- Prod: NOT EXECUTED. CI exec: EXEC-PROOF-13902-READBACK-5.md. Fill rule, work math, WANT lines: READBACK-13902-5.md.
-- Card: CARD-13902-recompute-3 (30f972dfa124) step 0 (BEFORE, can-fail proof) and step 4 (AFTER the pass 1 recompute).
-- -3 and -4 are SUPERSEDED: do not run them.
-- Edit vs -4 (the only one):
--   B1 Rows cap of stmt0..stmt2 30000000 -> 20000000 = the FORM-TEMPLATE-3 pass-path budget. Every table is read once per
--      statement (k = 1: stmt0 3 scalar subqueries, one table each; stmt1 ci_pipeline_runs FINAL + cicd_metrics_daily;
--      stmt2 ci_pipeline_runs FINAL + testops_pipeline_metrics_daily; no CTE, no self-join), so the plain system.parts sum
--      IS the work sum. Bytes cap 4 GiB and S0 parts cap 100000 kept.
-- Edit of -4 vs -3 (kept):
--   T1 The cut literal '2026-11-23 03:08:08' (stmt1 and stmt2: stale_rerun, stale_live, rewritten, retracted_after_cut)
--      is the placeholder T0_UTC in double braces (8 places). pm fills it with the card T0 (md fill rule). Why: a row an
--      old-image pod wrote after 03:08:08 but before T0 counted as "rewritten" under -3 and could give a false pass (the
--      STEP 323 drain-0 read skipped Terminating pods; old go-heavy grace 7260 s). With the cut = T0, every row computed
--      before the recompute start counts as stale; only rows written at or after T0 count as rewritten.
-- Edits of -3 vs -2 (kept; K2 now reads: the cut is T0, not a literal; K1 rows cap now 20M, B1):
--   K1 Engine cost gate in EVERY statement: stmt0..stmt2 WITH (SELECT throwIf(...)) AS cost_gate over exactly the tables
--      the statement reads, caps 20M rows (B1), 4 GiB on disk; S0 gated on system.one (system.parts part count of its 3 tables
--      over 100000 = STOP). Error text STOP-COST-GATE in each.
--   Status literals = the writer literals: 'deleted' testops/status_export.go:15, 'running' / 'queued' :21-22,
--      NotOpenRunSQL :36 (at ops 340c3e05).
--   K2 Literals: org prefix c6a38355, day range 2026-04-17..2026-11-22 (= the run range of the card). The cut is T0 (T1):
--      a row computed at or after T0 is written by the recompute or a new-image pod (the card s4a check before T0 proves
--      no old pod runs then); a row before T0 is stale.
--   K3 LEFT JOIN (was INNER): live keys with no raw run today are counted (no_raw_live), not dropped. ifNull on every raw
--      column, so the result does not depend on join_use_nulls.
--   K4 New lines: stale_rerun (lead WANT: re-run keys computed before the cut = 0), stale_live (every live key in range),
--      rewritten, retracted_after_cut, rerun_keys, to_null_checked. Aliases on every derived table.
-- Run rule: each statement once, in order. A SQL error (STOP-COST-GATE included) = STOP. A timeout (30 s) = STOP, no re-run.
-- read_rows WANT after each of stmt0..stmt2: read_rows <= 1.1 x G + 2 x P + 2 (G, P = S0 rows, parts of the gate tables).
--   CI pass path (B1): G + 2 x P + 1 - O <= query_log read_rows <= G + (1 + s) x (2 x P + 1), s = 3 (stmt0) else 0,
--   O = rows outside the day range (a part wholly outside it is pruned). Work math per statement: md.
-- New rule = cicd/compute.go:158-165 (retry_count = 0, queued_at set, started - queued >= 0), testops/compute.go:637-644
-- (retry_count = 0, explicit queue_seconds >= 0 else started - queued >= 0); mean :698-701. Sources: cicd/clickhouse.go:51-57,
-- testops_native_clickhouse.go:126-130. All at ops 340c3e05. Tolerance |stored - new| <= 1e-6 x max(1, new).
-- chg = a raw run of the key synced at/after the stored compute: left out of the value checks, counted as INFO.

-- S0 INFO cost inputs. WANT 3 rows (one per table), table_rows > 0 in each. Unit: all orgs (upper bound for the org reads).
-- Gate on system.one: the part count of the 3 tables (the rows this statement reads) over 100000 = STOP-COST-GATE.
WITH (SELECT throwIf(
    (SELECT count() FROM system.parts WHERE active AND database = currentDatabase()
      AND table IN ('ci_pipeline_runs', 'cicd_metrics_daily', 'testops_pipeline_metrics_daily')) > 100000,
    'STOP-COST-GATE') FROM system.one) AS cost_gate
SELECT table, sum(rows) AS table_rows, sum(bytes_on_disk) AS disk_bytes, count() AS active_parts
FROM system.parts
WHERE active AND database = currentDatabase()
  AND table IN ('ci_pipeline_runs', 'cicd_metrics_daily', 'testops_pipeline_metrics_daily') AND cost_gate = 0
GROUP BY table
ORDER BY table;

-- stmt0 ORG-1. WANT raw_orgs = 1, cicd_orgs = 1, testops_orgs = 1. Gate tables: raw + cicd + testops.
WITH (SELECT throwIf(
    (SELECT sum(rows) FROM system.parts WHERE active AND database = currentDatabase()
      AND table IN ('ci_pipeline_runs', 'cicd_metrics_daily', 'testops_pipeline_metrics_daily')) > 20000000
    OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = currentDatabase()
      AND table IN ('ci_pipeline_runs', 'cicd_metrics_daily', 'testops_pipeline_metrics_daily')) > 4294967296,
    'STOP-COST-GATE')) AS cost_gate
SELECT
  (SELECT uniqExact(org_id) FROM ci_pipeline_runs WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0) AS raw_orgs,
  (SELECT uniqExact(org_id) FROM cicd_metrics_daily WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0) AS cicd_orgs,
  (SELECT uniqExact(org_id) FROM testops_pipeline_metrics_daily WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0) AS testops_orgs;

-- stmt1 CICD (avg_queue_minutes, pooled_queue_minutes), days 2026-04-17..2026-11-22. Gate tables: raw + cicd.
-- WANT (after the recompute): keys_checked > 0, ctrl_checked > 0, stale_rerun = 0, mismatch = 0, pooled_mismatch = 0,
-- ctrl_mismatch = 0, retracted_rerun = 0, no_raw_live = 0. acp reads if > 0: stale_live, retracted_after_cut, pooled_unchecked.
-- INFO rerun_keys, rewritten, to_null_checked, raw_changed_after. BEFORE the recompute it must FAIL: stale_rerun > 0, mismatch > 0.
WITH (SELECT throwIf(
    (SELECT sum(rows) FROM system.parts WHERE active AND database = currentDatabase()
      AND table IN ('ci_pipeline_runs', 'cicd_metrics_daily')) > 20000000
    OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = currentDatabase()
      AND table IN ('ci_pipeline_runs', 'cicd_metrics_daily')) > 4294967296,
    'STOP-COST-GATE')) AS cost_gate
SELECT countIf(rr > 0) AS rerun_keys, countIf(rr > 0 AND NOT chg) AS keys_checked,
  countIf(rr > 0 AND c < toDateTime('{{T0_UTC}}', 'UTC')) AS stale_rerun,
  countIf(r = 0 AND c < toDateTime('{{T0_UTC}}', 'UTC')) AS stale_live,
  countIf(c >= toDateTime('{{T0_UTC}}', 'UTC')) AS rewritten,
  countIf(rr > 0 AND r = 1) AS retracted_rerun,
  countIf(r = 1 AND c >= toDateTime('{{T0_UTC}}', 'UTC')) AS retracted_after_cut,
  countIf(r = 0 AND runs = 0) AS no_raw_live, countIf(chg) AS raw_changed_after,
  countIf(rr > 0 AND NOT chg AND r = 0 AND NOT ok) AS mismatch,
  countIf(rr > 0 AND NOT chg AND r = 0 AND n_new = 0) AS to_null_checked,
  countIf(rr > 0 AND NOT chg AND r = 0 AND m = 1 AND np != n_new) AS pooled_mismatch,
  countIf(rr > 0 AND NOT chg AND r = 0 AND m != 1) AS pooled_unchecked,
  countIf(rr = 0 AND runs > 0 AND NOT chg AND r = 0) AS ctrl_checked,
  countIf(rr = 0 AND runs > 0 AND NOT chg AND r = 0 AND NOT ok) AS ctrl_mismatch
FROM (
  SELECT s.c AS c, s.r AS r, s.m AS m, s.np AS np, ifNull(raw.runs, 0) AS runs, ifNull(raw.rr, 0) AS rr,
    ifNull(raw.n_new, 0) AS n_new, ifNull(raw.ls >= s.c, 0) AS chg,
    if(ifNull(raw.n_new, 0) = 0, isNull(s.q),
       abs(ifNull(s.q, -1e9) - raw.sum_new / raw.n_new) <= 1e-6 * greatest(1, raw.sum_new / raw.n_new)) AS ok
  FROM (SELECT repo_id, day, max(computed_at) AS c, argMax(retracted, computed_at) AS r,
          tupleElement(argMax(tuple(avg_queue_minutes), computed_at), 1) AS q,
          argMax(pooled_samples_stored, computed_at) AS m,
          argMax(length(arrayFilter(x -> isNotNull(x), pooled_queue_minutes)), computed_at) AS np
        FROM cicd_metrics_daily
        WHERE startsWith(org_id, 'c6a38355') AND day >= toDate('2026-04-17') AND day <= toDate('2026-11-22')
        GROUP BY repo_id, day) AS s
  LEFT JOIN (
    SELECT repo_id, toDate(assumeNotNull(finished_at)) AS day, count() AS runs, max(last_synced) AS ls,
      countIf(retry_count > 0 AND queued_at IS NOT NULL AND started_at >= queued_at) AS rr,
      countIf(retry_count = 0 AND queued_at IS NOT NULL AND started_at >= queued_at) AS n_new,
      sumIf(dateDiff('millisecond', queued_at, started_at) / 60000, retry_count = 0 AND queued_at IS NOT NULL AND started_at >= queued_at) AS sum_new
    FROM ci_pipeline_runs FINAL
    WHERE startsWith(org_id, 'c6a38355') AND finished_at IS NOT NULL
      AND lowerUTF8(trimBoth(ifNull(status, ''))) NOT IN ('deleted', 'running', 'queued')
      AND toDate(assumeNotNull(finished_at)) >= toDate('2026-04-17') AND toDate(assumeNotNull(finished_at)) <= toDate('2026-11-22')
    GROUP BY repo_id, day
  ) AS raw ON raw.repo_id = s.repo_id AND raw.day = s.day
) AS cicd_k
WHERE cost_gate = 0;

-- stmt2 TESTOPS (avg_queue_seconds, pooled_queues), days 2026-04-17..2026-11-22. Gate tables: raw + testops.
-- WANT / INFO / must-fail-before: as stmt1.
WITH (SELECT throwIf(
    (SELECT sum(rows) FROM system.parts WHERE active AND database = currentDatabase()
      AND table IN ('ci_pipeline_runs', 'testops_pipeline_metrics_daily')) > 20000000
    OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = currentDatabase()
      AND table IN ('ci_pipeline_runs', 'testops_pipeline_metrics_daily')) > 4294967296,
    'STOP-COST-GATE')) AS cost_gate
SELECT countIf(rr > 0) AS rerun_keys, countIf(rr > 0 AND NOT chg) AS keys_checked,
  countIf(rr > 0 AND c < toDateTime('{{T0_UTC}}', 'UTC')) AS stale_rerun,
  countIf(r = 0 AND c < toDateTime('{{T0_UTC}}', 'UTC')) AS stale_live,
  countIf(c >= toDateTime('{{T0_UTC}}', 'UTC')) AS rewritten,
  countIf(rr > 0 AND r = 1) AS retracted_rerun,
  countIf(r = 1 AND c >= toDateTime('{{T0_UTC}}', 'UTC')) AS retracted_after_cut,
  countIf(r = 0 AND runs = 0) AS no_raw_live, countIf(chg) AS raw_changed_after,
  countIf(rr > 0 AND NOT chg AND r = 0 AND NOT ok) AS mismatch,
  countIf(rr > 0 AND NOT chg AND r = 0 AND n_new = 0) AS to_null_checked,
  countIf(rr > 0 AND NOT chg AND r = 0 AND m = 1 AND np != n_new) AS pooled_mismatch,
  countIf(rr > 0 AND NOT chg AND r = 0 AND m != 1) AS pooled_unchecked,
  countIf(rr = 0 AND runs > 0 AND NOT chg AND r = 0) AS ctrl_checked,
  countIf(rr = 0 AND runs > 0 AND NOT chg AND r = 0 AND NOT ok) AS ctrl_mismatch
FROM (
  SELECT s.c AS c, s.r AS r, s.m AS m, s.np AS np, ifNull(raw.runs, 0) AS runs, ifNull(raw.rr, 0) AS rr,
    ifNull(raw.n_new, 0) AS n_new, ifNull(raw.ls >= s.c, 0) AS chg,
    if(ifNull(raw.n_new, 0) = 0, isNull(s.q),
       abs(ifNull(s.q, -1e9) - raw.sum_new / raw.n_new) <= 1e-6 * greatest(1, raw.sum_new / raw.n_new)) AS ok
  FROM (SELECT repo_id, day, max(computed_at) AS c, argMax(retracted, computed_at) AS r,
          tupleElement(argMax(tuple(avg_queue_seconds), computed_at), 1) AS q,
          argMax(pooled_samples_stored, computed_at) AS m,
          argMax(length(arrayFilter(x -> isNotNull(x), pooled_queues)), computed_at) AS np
        FROM testops_pipeline_metrics_daily
        WHERE startsWith(org_id, 'c6a38355') AND day >= toDate('2026-04-17') AND day <= toDate('2026-11-22')
        GROUP BY repo_id, day) AS s
  LEFT JOIN (
    SELECT repo_id, day, count() AS runs, max(run_ls) AS ls, countIf(retry_count > 0 AND qs IS NOT NULL) AS rr,
      countIf(retry_count = 0 AND qs IS NOT NULL) AS n_new, sumIf(qs, retry_count = 0) AS sum_new
    FROM (
      SELECT repo_id, toDate(started_at) AS day, last_synced AS run_ls, retry_count,
        if(queue_seconds >= 0, queue_seconds,
           if(started_at >= queued_at, dateDiff('millisecond', queued_at, started_at) / 1000, NULL)) AS qs
      FROM ci_pipeline_runs FINAL
      WHERE startsWith(org_id, 'c6a38355') AND lowerUTF8(trimBoth(ifNull(status, ''))) != 'deleted'
        AND toDate(started_at) >= toDate('2026-04-17') AND toDate(started_at) <= toDate('2026-11-22')
    ) AS run_rows
    GROUP BY repo_id, day
  ) AS raw ON raw.repo_id = s.repo_id AND raw.day = s.day
) AS testops_k
WHERE cost_gate = 0;
`

// Synthetic rows. Org c6a38355-0000-..., other org b0000000-... (filter control). Keys in the form range, per table:
// 11-10 re-run move (old 6 min / 360 s, new 2 / 120), 11-11 control (+ a deleted re-run that must not count),
// 11-12 re-run-only day (new NULL), 11-13 re-run whose raw syncs after every compute (chg), 11-14 control, 11-15 live key
// with no raw, 11-22 re-run-only day computed in the rollout window (old value), 11-21 re-run key a new-image pod wrote
// after 03:08:08 but before T0 (new value: stale under the T0 cut). 04-16 = a re-run key outside the range. Merges are stopped.
const readbackFixtureBefore = `
SYSTEM STOP MERGES ci_pipeline_runs;
SYSTEM STOP MERGES cicd_metrics_daily;
SYSTEM STOP MERGES testops_pipeline_metrics_daily;
INSERT INTO ci_pipeline_runs (org_id, repo_id, run_id, provider, status, queued_at, started_at, finished_at, last_synced, retry_count, queue_seconds) VALUES
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', 'rb-a', 'github_actions', 'success', '2026-11-10 10:00:00', '2026-11-10 10:02:00', '2026-11-10 10:10:00', '2026-11-10 12:00:00', 0, 120),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', 'rb-b', 'github_actions', 'success', '2026-11-10 09:00:00', '2026-11-10 09:10:00', '2026-11-10 09:30:00', '2026-11-10 12:00:00', 1, 600),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', 'rb-c', 'github_actions', 'success', '2026-11-11 10:00:00', '2026-11-11 10:03:00', '2026-11-11 10:20:00', '2026-11-11 12:00:00', 0, NULL),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', 'rb-del', 'github_actions', ' Deleted', '2026-11-11 08:00:00', '2026-11-11 08:15:00', '2026-11-11 08:30:00', '2026-11-11 12:00:00', 1, 900),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', 'rb-h', 'github_actions', 'failure', '2026-11-12 08:00:00', '2026-11-12 08:05:00', '2026-11-12 08:20:00', '2026-11-12 12:00:00', 2, 300),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', 'rb-e', 'github_actions', 'success', '2026-11-13 10:00:00', '2026-11-13 10:01:00', '2026-11-13 10:10:00', '2026-11-25 00:00:00', 1, 60),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', 'rb-f', 'github_actions', 'success', '2026-11-13 11:00:00', '2026-11-13 11:02:00', '2026-11-13 11:10:00', '2026-11-25 00:00:00', 0, 120),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000002', 'rb-g', 'github_actions', 'success', '2026-11-14 10:00:00', '2026-11-14 10:02:00', '2026-11-14 10:10:00', '2026-11-14 12:00:00', 0, 120),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000003', 'rb-i', 'github_actions', 'success', '2026-11-21 10:00:00', '2026-11-21 10:01:00', '2026-11-21 10:10:00', '2026-11-21 12:00:00', 0, 60),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000003', 'rb-j', 'github_actions', 'success', '2026-11-21 09:00:00', '2026-11-21 09:10:00', '2026-11-21 09:30:00', '2026-11-21 12:00:00', 1, 600),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000003', 'rb-k', 'github_actions', 'success', '2026-11-22 08:00:00', '2026-11-22 08:05:00', '2026-11-22 08:20:00', '2026-11-22 12:00:00', 1, 300),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', 'rb-o', 'github_actions', 'success', '2026-04-16 10:00:00', '2026-04-16 10:07:00', '2026-04-16 10:20:00', '2026-04-16 12:00:00', 1, 420),
('b0000000-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', 'rb-x', 'github_actions', 'success', '2026-11-10 10:00:00', '2026-11-10 10:30:00', '2026-11-10 10:40:00', '2026-11-10 12:00:00', 1, 1800);
INSERT INTO cicd_metrics_daily (org_id, repo_id, day, pipelines_count, success_rate, avg_queue_minutes, computed_at, retracted, pooled_samples_stored, pooled_queue_minutes) VALUES
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', '2026-11-10', 2, 1, 6, '2026-11-11 01:00:00', 0, 1, [2, 10]),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', '2026-11-11', 1, 1, 3, '2026-11-12 01:00:00', 0, 1, [3]),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', '2026-11-12', 1, 0, 5, '2026-11-13 01:00:00', 0, 1, [5]),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', '2026-11-13', 2, 1, 1.5, '2026-11-14 01:00:00', 0, 1, [1, 2]),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000002', '2026-11-14', 1, 1, 2, '2026-11-15 01:00:00', 0, 1, [2]),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000002', '2026-11-15', 1, 1, 4, '2026-11-16 01:00:00', 0, 1, [4]),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000003', '2026-11-21', 2, 1, 1, '2026-11-23 04:00:00', 0, 1, [1, NULL]),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000003', '2026-11-22', 1, 1, 5, '2026-11-23 03:00:00', 0, 0, []),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', '2026-04-16', 1, 1, 7, '2026-04-17 01:00:00', 0, 1, [7]),
('b0000000-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', '2026-11-10', 1, 1, 30, '2026-11-11 01:00:00', 0, 1, [30]);
INSERT INTO testops_pipeline_metrics_daily (org_id, repo_id, day, pipelines_count, avg_queue_seconds, computed_at, retracted, pooled_samples_stored, pooled_queues) VALUES
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', '2026-11-10', 2, 360, '2026-11-11 01:00:00', 0, 1, [120, 600]),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', '2026-11-11', 1, 180, '2026-11-12 01:00:00', 0, 1, [180]),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', '2026-11-12', 1, 300, '2026-11-13 01:00:00', 0, 1, [300]),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', '2026-11-13', 2, 90, '2026-11-14 01:00:00', 0, 1, [60, 120]),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000002', '2026-11-14', 1, 120, '2026-11-15 01:00:00', 0, 1, [120]),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000002', '2026-11-15', 1, 240, '2026-11-16 01:00:00', 0, 1, [240]),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000003', '2026-11-21', 2, 60, '2026-11-23 04:00:00', 0, 1, [60, NULL]),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000003', '2026-11-22', 1, 300, '2026-11-23 03:00:00', 0, 0, []),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', '2026-04-16', 1, 420, '2026-04-17 01:00:00', 0, 1, [420]),
('b0000000-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', '2026-11-10', 1, 1800, '2026-11-11 01:00:00', 0, 1, [1800]);
`

// The simulated recompute (2026-11-24 10:00): one new-rule row per key of the range, a retraction row for the key with no
// raw run (cicd/fence.go:84-93), nothing for the key outside the range or the other org.
const readbackFixtureRecompute = `
INSERT INTO cicd_metrics_daily (org_id, repo_id, day, pipelines_count, success_rate, avg_queue_minutes, computed_at, retracted, pooled_samples_stored, pooled_queue_minutes) VALUES
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', '2026-11-10', 2, 1, 2, '2026-11-24 10:00:00', 0, 1, [2, NULL]),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', '2026-11-11', 1, 1, 3, '2026-11-24 10:00:00', 0, 1, [3]),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', '2026-11-12', 1, 0, NULL, '2026-11-24 10:00:00', 0, 1, [NULL]),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', '2026-11-13', 2, 1, 2, '2026-11-24 10:00:00', 0, 1, [NULL, 2]),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000002', '2026-11-14', 1, 1, 2, '2026-11-24 10:00:00', 0, 1, [2]),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000002', '2026-11-15', 0, 0, NULL, '2026-11-24 10:00:00', 1, 0, []),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000003', '2026-11-21', 2, 1, 1, '2026-11-24 10:00:00', 0, 1, [1, NULL]),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000003', '2026-11-22', 1, 1, NULL, '2026-11-24 10:00:00', 0, 1, [NULL]);
INSERT INTO testops_pipeline_metrics_daily (org_id, repo_id, day, pipelines_count, avg_queue_seconds, computed_at, retracted, pooled_samples_stored, pooled_queues) VALUES
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', '2026-11-10', 2, 120, '2026-11-24 10:00:00', 0, 1, [120, NULL]),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', '2026-11-11', 1, 180, '2026-11-24 10:00:00', 0, 1, [180]),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', '2026-11-12', 1, NULL, '2026-11-24 10:00:00', 0, 1, [NULL]),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', '2026-11-13', 2, 120, '2026-11-24 10:00:00', 0, 1, [NULL, 120]),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000002', '2026-11-14', 1, 120, '2026-11-24 10:00:00', 0, 1, [120]),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000002', '2026-11-15', 0, NULL, '2026-11-24 10:00:00', 1, 0, []),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000003', '2026-11-21', 2, 60, '2026-11-24 10:00:00', 0, 1, [60, NULL]),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000003', '2026-11-22', 1, NULL, '2026-11-24 10:00:00', 0, 1, [NULL]);
`

// The card T0 as pm records it: T0_UTC.txt, one line 'YYYY-MM-DD HH:MM:SS' UTC (chunk-start.sh:63-66), before the
// simulated recompute (2026-11-24 10:00).
const readbackT0File = "2026-11-24 09:00:00\n"

// Old-pod case: a re-run key (repo 4, 11-20) that an old-image pod stored at 2026-11-23 05:00 (after the -3 cut 03:08:08,
// before T0) with the old value, whose raw run synced after that stamp (06:00: chg, so mismatch leaves it out), and that the
// recompute did not rewrite. WANT it counted stale (stale_rerun 1 = STOP). Under the -3 cut it counted as rewritten and
// every card WANT held: a false pass.
const readbackFixtureOldPodAfterCut = `
INSERT INTO ci_pipeline_runs (org_id, repo_id, run_id, provider, status, queued_at, started_at, finished_at, last_synced, retry_count, queue_seconds) VALUES
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000004', 'rb-m', 'github_actions', 'success', '2026-11-20 10:00:00', '2026-11-20 10:05:00', '2026-11-20 10:20:00', '2026-11-23 06:00:00', 1, 300);
INSERT INTO cicd_metrics_daily (org_id, repo_id, day, pipelines_count, success_rate, avg_queue_minutes, computed_at, retracted, pooled_samples_stored, pooled_queue_minutes) VALUES
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000004', '2026-11-20', 1, 1, 5, '2026-11-23 05:00:00', 0, 1, [5]);
INSERT INTO testops_pipeline_metrics_daily (org_id, repo_id, day, pipelines_count, avg_queue_seconds, computed_at, retracted, pooled_samples_stored, pooled_queues) VALUES
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000004', '2026-11-20', 1, 300, '2026-11-23 05:00:00', 0, 1, [300]);
`

func TestReadbackReadFormFailsBeforeAndPassesAfterARecompute(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	sum := sha256.Sum256([]byte(readbackForm))
	if got := hex.EncodeToString(sum[:]); got != readbackFormSHA256 {
		t.Fatalf("readback form sha256 = %s, want %s", got, readbackFormSHA256)
	}
	// Fill rule (READBACK-13902-4.md): every T0 placeholder := the one line of T0_UTC.txt, nothing else changes.
	const placeholder = "{{T0_UTC}}"
	if n := strings.Count(readbackForm, placeholder); n != 8 {
		t.Fatalf("readback form: %d T0 placeholders, want 8", n)
	}
	if strings.Contains(chmigrate.StripLineComments(readbackForm), "03:08:08") {
		t.Fatalf("readback form: the -3 cut literal is still in the SQL")
	}
	fill := func(t0 string) string {
		filled := strings.ReplaceAll(readbackForm, placeholder, t0)
		if strings.Contains(filled, "{{") || strings.Count(filled, "toDateTime('"+t0+"', 'UTC')") != 8 {
			t.Fatalf("readback form fill with %q: placeholder left or cut count != 8", t0)
		}
		return filled
	}
	t0 := strings.TrimSpace(readbackT0File)
	t.Logf("T0 fill %q", t0)
	statements := splitForm(fill(t0))
	// The same form filled with the -3 literal cut: the -3 SQL, for the old-pod contrast run.
	statementsCut3 := splitForm(fill("2026-11-23 03:08:08"))
	labels := make([]string, 0, len(statements))
	for _, s := range statements {
		labels = append(labels, s.label)
	}
	if strings.Join(labels, ",") != "S0,stmt0,stmt1,stmt2" {
		t.Fatalf("readback form statements = %v", labels)
	}
	reads := map[string][]string{
		"S0":    {"ci_pipeline_runs", "cicd_metrics_daily", "testops_pipeline_metrics_daily"},
		"stmt0": {"ci_pipeline_runs", "cicd_metrics_daily", "testops_pipeline_metrics_daily"},
		"stmt1": {"ci_pipeline_runs", "cicd_metrics_daily"},
		"stmt2": {"ci_pipeline_runs", "testops_pipeline_metrics_daily"},
	}
	// Every statement holds exactly one gate: S0 one part-count cap on system.one, the others one rows budget (20M,
	// FORM-TEMPLATE-3; every table read once, k = 1) + one bytes cap.
	const partsCap = "> 100000"
	const readbackRowsCap = "> 20000000"
	gateCaps := func(label string) []struct{ name, literal string } {
		if label == "S0" {
			return []struct{ name, literal string }{{"parts", partsCap}}
		}
		return []struct{ name, literal string }{{"rows", readbackRowsCap}, {"bytes", bytesCap}}
	}
	// s = scalar subqueries that read a table (each can run the gate once more).
	readbackScalars := map[string]uint64{"stmt0": 3}
	for _, s := range statements {
		wantRowsBytes, wantParts := 1, 0
		if s.label == "S0" {
			wantRowsBytes, wantParts = 0, 1
		}
		// Count caps in the SQL only: the header comments ride in the S0 chunk ("30000000 -> 20000000" holds "> 20000000").
		code := chmigrate.StripLineComments(s.text)
		if n := strings.Count(code, readbackRowsCap); n != wantRowsBytes {
			t.Fatalf("%s: %d rows budget caps %q, want %d", s.label, n, readbackRowsCap, wantRowsBytes)
		}
		if n := strings.Count(code, rowsCap); n != 0 {
			t.Fatalf("%s: %d old rows caps %q, want 0", s.label, n, rowsCap)
		}
		if s.label != "S0" {
			if n := uint64(strings.Count(code, "AND cost_gate = 0) AS ")); n != readbackScalars[s.label] {
				t.Fatalf("%s: %d scalar subqueries use cost_gate, want %d", s.label, n, readbackScalars[s.label])
			}
		}
		if n := strings.Count(code, bytesCap); n != wantRowsBytes {
			t.Fatalf("%s: %d bytes caps %q, want %d", s.label, n, bytesCap, wantRowsBytes)
		}
		if n := strings.Count(code, partsCap); n != wantParts {
			t.Fatalf("%s: %d parts caps %q, want %d", s.label, n, partsCap, wantParts)
		}
		in := "table IN ('" + strings.Join(reads[s.label], "', '") + "')"
		if n := strings.Count(s.text, in); n != 2 {
			t.Fatalf("%s: gate table list %s found %d times, want 2", s.label, in, n)
		}
		if n := strings.Count(s.text, "'STOP-COST-GATE'"); n != 1 {
			t.Fatalf("%s: %d STOP-COST-GATE throwIf, want 1", s.label, n)
		}
	}

	scratch := scratchDatabase(t)
	conn := openDatabase(t, scratch.URI, "")
	db, _, err := chmigrate.NewConnDB(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}
	baseline, err := chmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	chain, err := chmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	result, err := chmigrate.Upgrade(ctx, db, baseline, chain)
	if err != nil {
		t.Fatalf("real chain: %v", err)
	}
	if result.Head < "170_" {
		t.Fatalf("schema head %q, want 170 or later", result.Head)
	}
	t.Logf("schema head %s", result.Head)

	exec := func(fixture string) {
		for _, statement := range chmigrate.SplitStatements(fixture) {
			if err := conn.Exec(ctx, statement); err != nil {
				t.Fatalf("fixture: %v\n%s", err, statement)
			}
		}
	}
	versions := func(phase string, wantPhysical, wantKeys, wantMulti uint64) {
		for _, table := range []string{"cicd_metrics_daily", "testops_pipeline_metrics_daily"} {
			var physical, keys, multi uint64
			if err := conn.QueryRow(ctx, "SELECT count(), uniqExact(repo_id, day), "+
				"(SELECT count() FROM (SELECT repo_id, day FROM "+table+" WHERE startsWith(org_id, 'c6a38355') GROUP BY repo_id, day HAVING count() = 2) AS k) "+
				"FROM "+table+" WHERE startsWith(org_id, 'c6a38355')").Scan(&physical, &keys, &multi); err != nil {
				t.Fatal(err)
			}
			t.Logf("%s %s versions: physical=%d keys=%d keys_with_2_versions=%d", phase, table, physical, keys, multi)
			if physical != wantPhysical || keys != wantKeys || multi != wantMulti {
				t.Fatalf("%s %s: physical=%d keys=%d keys_with_2_versions=%d, want %d, %d, %d", phase, table, physical, keys, multi, wantPhysical, wantKeys, wantMulti)
			}
		}
	}

	// run executes every statement once, asserts each value and the read_rows bound, and returns S0's R and P per table.
	run := func(phase string, statements []formStatement, want map[string]map[string]string) (map[string]uint64, map[string]uint64) {
		tableRows := map[string]uint64{}
		tableParts := map[string]uint64{}
		comments := map[string]string{}
		for _, statement := range statements {
			var readRows uint64
			comments[statement.label] = fmt.Sprintf("readback13902-pass-%s-%d", statement.label, time.Now().UnixNano())
			qctx := clickhouse.Context(ctx,
				clickhouse.WithSettings(clickhouse.Settings{"log_comment": comments[statement.label]}),
				clickhouse.WithProgress(func(p *clickhouse.Progress) { readRows += p.Rows }))
			rows, err := conn.Query(qctx, statement.text)
			if err != nil {
				t.Fatalf("%s %s: SQL error: %v", phase, statement.label, err)
			}
			columns := rows.Columns()
			types := rows.ColumnTypes()
			var got []map[string]string
			for rows.Next() {
				cells := make([]any, len(types))
				for i, ct := range types {
					cells[i] = reflect.New(ct.ScanType()).Interface()
				}
				if err := rows.Scan(cells...); err != nil {
					t.Fatalf("%s %s: scan: %v", phase, statement.label, err)
				}
				row := map[string]string{}
				parts := make([]string, 0, len(columns))
				for i, name := range columns {
					row[name] = renderCell(cells[i])
					parts = append(parts, name+"="+row[name])
				}
				t.Logf("%s %s ROW %s", phase, statement.label, strings.Join(parts, " "))
				got = append(got, row)
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("%s %s: SQL error: %v", phase, statement.label, err)
			}
			_ = rows.Close()

			if statement.label == "S0" {
				if len(got) != 3 {
					t.Fatalf("%s S0: %d rows, want 3", phase, len(got))
				}
				for _, row := range got {
					var n, p uint64
					if _, err := fmt.Sscan(row["table_rows"], &n); err != nil || n == 0 {
						t.Fatalf("%s S0: table %s table_rows %q, want > 0", phase, row["table"], row["table_rows"])
					}
					if _, err := fmt.Sscan(row["active_parts"], &p); err != nil || p == 0 {
						t.Fatalf("%s S0: table %s active_parts %q, want > 0", phase, row["table"], row["active_parts"])
					}
					tableRows[row["table"]] = n
					tableParts[row["table"]] = p
				}
				t.Logf("%s S0 read_rows=%d", phase, readRows)
				continue
			}

			var g, p uint64
			for _, table := range reads[statement.label] {
				g += tableRows[table]
				p += tableParts[table]
			}
			limit := 1.1*float64(g) + 2*float64(p) + 2
			t.Logf("%s %s read_rows=%d gate_rows=%d gate_parts=%d limit=%.1f", phase, statement.label, readRows, g, p, limit)
			if 10*readRows > 11*g+20*p+20 {
				t.Errorf("%s %s: read_rows %d over 1.1 x G + 2 x P + 2 = %.1f (G=%d P=%d)", phase, statement.label, readRows, limit, g, p)
			}
			if len(got) != 1 {
				t.Errorf("%s %s: %d rows, want 1", phase, statement.label, len(got))
				continue
			}
			for name, value := range want[statement.label] {
				if got[0][name] != value {
					t.Errorf("%s %s: %s = %s, want %s", phase, statement.label, name, got[0][name], value)
				}
			}
			for _, name := range columns {
				if _, ok := want[statement.label][name]; !ok {
					t.Errorf("%s %s: column %s has no WANT in the cell", phase, statement.label, name)
				}
			}
		}

		// PASS path (FORM-TEMPLATE-3): the scan count from query_log. WANT [QueryFinish] and
		// G + 2 x P + 1 - O <= read_rows <= G + (1 + s) x (2 x P + 1); O = rows of the read tables outside the day range
		// (a part wholly outside it is pruned; stmt0 has no day filter: O = 0).
		outside := map[string]string{
			"stmt1": "(SELECT count() FROM cicd_metrics_daily WHERE day < '2026-04-17' OR day > '2026-11-22') + " +
				"(SELECT count() FROM ci_pipeline_runs WHERE finished_at IS NULL OR toDate(assumeNotNull(finished_at)) < '2026-04-17' OR toDate(assumeNotNull(finished_at)) > '2026-11-22')",
			"stmt2": "(SELECT count() FROM testops_pipeline_metrics_daily WHERE day < '2026-04-17' OR day > '2026-11-22') + " +
				"(SELECT count() FROM ci_pipeline_runs WHERE toDate(started_at) < '2026-04-17' OR toDate(started_at) > '2026-11-22')",
		}
		if err := conn.Exec(ctx, "SYSTEM FLUSH LOGS"); err != nil {
			t.Fatalf("the query log is not readable by this user: %v", err)
		}
		for _, statement := range statements[1:] {
			var g, p, o uint64
			for _, table := range reads[statement.label] {
				g += tableRows[table]
				p += tableParts[table]
			}
			if q, ok := outside[statement.label]; ok {
				if err := conn.QueryRow(ctx, "SELECT "+q).Scan(&o); err != nil {
					t.Fatalf("%s PASS %s: outside rows: %v", phase, statement.label, err)
				}
			}
			s := readbackScalars[statement.label]
			var types []string
			var logRead uint64
			rows, err := conn.Query(ctx, `SELECT toString(type), read_rows FROM system.query_log
				WHERE log_comment = ? AND type != 'QueryStart' ORDER BY event_time_microseconds`, comments[statement.label])
			if err != nil {
				t.Fatalf("%s PASS %s: query log: %v", phase, statement.label, err)
			}
			for rows.Next() {
				var typ string
				var r uint64
				if err := rows.Scan(&typ, &r); err != nil {
					t.Fatalf("%s PASS %s: query log scan: %v", phase, statement.label, err)
				}
				types = append(types, typ)
				logRead += r
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("%s PASS %s: query log: %v", phase, statement.label, err)
			}
			_ = rows.Close()
			low, high := g+2*p+1-o, g+(1+s)*(2*p+1)
			t.Logf("%s PASS %s query_log types=%v read_rows=%d G=%d P=%d O=%d s=%d band=[%d, %d]", phase, statement.label, types, logRead, g, p, o, s, low, high)
			if len(types) != 1 || types[0] != "QueryFinish" {
				t.Errorf("%s PASS %s: query_log types %v, want [QueryFinish]", phase, statement.label, types)
			}
			if logRead < low || logRead > high {
				t.Errorf("%s PASS %s: query_log read_rows %d outside [%d, %d]", phase, statement.label, logRead, low, high)
			}
		}
		return tableRows, tableParts
	}

	orgWant := map[string]string{"raw_orgs": "1", "cicd_orgs": "1", "testops_orgs": "1"}

	// BEFORE: the stored rows hold the old rule. The form must fail: stale_rerun > 0, mismatch > 0. Cut = T0: every stored
	// row is before T0, the 11-21 key written after 03:08:08 included (it was "rewritten" under -3).
	exec(readbackFixtureBefore)
	versions("before", 9, 9, 0)
	before := map[string]string{
		"rerun_keys": "5", "keys_checked": "4", "stale_rerun": "5", "stale_live": "8", "rewritten": "0",
		"retracted_rerun": "0", "retracted_after_cut": "0", "no_raw_live": "1", "raw_changed_after": "1",
		"mismatch": "3", "to_null_checked": "2", "pooled_mismatch": "2", "pooled_unchecked": "1",
		"ctrl_checked": "2", "ctrl_mismatch": "0",
	}
	run("before", statements, map[string]map[string]string{"stmt0": orgWant, "stmt1": before, "stmt2": before})

	// AFTER the simulated recompute: every WANT of the card holds.
	exec(readbackFixtureRecompute)
	versions("after", 17, 9, 8)
	for table, column := range map[string]string{"cicd_metrics_daily": "avg_queue_minutes", "testops_pipeline_metrics_daily": "avg_queue_seconds"} {
		var plain, newest string
		if err := conn.QueryRow(ctx, "SELECT ifNull(toString(argMax("+column+", computed_at)), 'NULL'), "+
			"ifNull(toString(tupleElement(argMax(tuple("+column+"), computed_at), 1)), 'NULL') FROM "+table+
			" WHERE org_id = 'c6a38355-0000-4000-8000-000000000000' AND repo_id = '00000000-0000-4000-8000-000000000001' AND day = '2026-11-12'").
			Scan(&plain, &newest); err != nil {
			t.Fatal(err)
		}
		t.Logf("after %s 11-12 key: plain argMax=%s tuple argMax=%s", table, plain, newest)
		if plain == "NULL" || newest != "NULL" {
			t.Fatalf("after %s 11-12 key: plain argMax=%s tuple argMax=%s, want non-NULL and NULL", table, plain, newest)
		}
	}
	after := map[string]string{
		"rerun_keys": "5", "keys_checked": "4", "stale_rerun": "0", "stale_live": "0", "rewritten": "8",
		"retracted_rerun": "0", "retracted_after_cut": "1", "no_raw_live": "0", "raw_changed_after": "1",
		"mismatch": "0", "to_null_checked": "2", "pooled_mismatch": "0", "pooled_unchecked": "0",
		"ctrl_checked": "2", "ctrl_mismatch": "0",
	}
	_, parts := run("after", statements, map[string]map[string]string{"stmt0": orgWant, "stmt1": after, "stmt2": after})

	// Gate fires: ONE cap literal lowered to 0 per run. WANT STOP-COST-GATE, query_log [ExceptionBeforeStart] and
	// query_log read_rows <= 2 x P + 1 (system.parts twice + system.one: no data-table row read).
	type fired struct {
		label, cap, comment string
		p                   uint64
	}
	var fires []fired
	for _, statement := range statements {
		var p uint64
		for _, table := range reads[statement.label] {
			p += parts[table]
		}
		for _, c := range gateCaps(statement.label) {
			lowered := strings.Replace(statement.text, c.literal, "> 0", 1)
			comment := fmt.Sprintf("readback13902-gate-%s-%s-%d", statement.label, c.name, time.Now().UnixNano())
			var readRows uint64
			qctx := clickhouse.Context(ctx,
				clickhouse.WithSettings(clickhouse.Settings{"log_comment": comment}),
				clickhouse.WithProgress(func(pr *clickhouse.Progress) { readRows += pr.Rows }))
			rows, err := conn.Query(qctx, lowered)
			if err == nil {
				for rows.Next() {
				}
				err = rows.Err()
				_ = rows.Close()
			}
			if err == nil || !strings.Contains(err.Error(), "STOP-COST-GATE") {
				t.Errorf("GATE %s %s cap lowered: err = %v, want STOP-COST-GATE", statement.label, c.name, err)
				continue
			}
			t.Logf("GATE %s %s cap lowered: fired, client read_rows=%d, error: %s", statement.label, c.name, readRows, strings.SplitN(err.Error(), "\n", 2)[0])
			fires = append(fires, fired{statement.label, c.name, comment, p})
		}
	}
	if err := conn.Exec(ctx, "SYSTEM FLUSH LOGS"); err != nil {
		t.Fatalf("the query log is not readable by this user: %v", err)
	}
	for _, f := range fires {
		var types []string
		var logRead uint64
		rows, err := conn.Query(ctx, `SELECT toString(type), read_rows FROM system.query_log
			WHERE log_comment = ? AND type != 'QueryStart' ORDER BY event_time_microseconds`, f.comment)
		if err != nil {
			t.Fatalf("GATE %s %s: query log: %v", f.label, f.cap, err)
		}
		for rows.Next() {
			var typ string
			var r uint64
			if err := rows.Scan(&typ, &r); err != nil {
				t.Fatalf("GATE %s %s: query log scan: %v", f.label, f.cap, err)
			}
			types = append(types, typ)
			logRead += r
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("GATE %s %s: query log: %v", f.label, f.cap, err)
		}
		_ = rows.Close()
		t.Logf("GATE %s %s query_log types=%v read_rows=%d bound=%d", f.label, f.cap, types, logRead, 2*f.p+1)
		if len(types) != 1 || types[0] != "ExceptionBeforeStart" {
			t.Errorf("GATE %s %s: query_log types %v, want [ExceptionBeforeStart]", f.label, f.cap, types)
		}
		if logRead > 2*f.p+1 {
			t.Errorf("GATE %s %s: query_log read_rows %d over 2 x P + 1 = %d", f.label, f.cap, logRead, 2*f.p+1)
		}
	}
	if len(fires) != 7 {
		t.Errorf("gate fired %d times, want 7 (S0 x 1 cap + 3 statements x 2 caps)", len(fires))
	}

	// Old-pod case (after the gate fires, so their part bounds hold): an old-image row stored after 03:08:08, before T0, not
	// rewritten, raw synced after its stamp. T0 cut: stale_rerun 1, stale_live 1 = STOP. -3 cut: stale_rerun 0, rewritten 9,
	// mismatch 0 = every card WANT holds (the false pass this revision closes).
	exec(readbackFixtureOldPodAfterCut)
	versions("oldpod", 18, 10, 8)
	oldPod := map[string]string{
		"rerun_keys": "6", "keys_checked": "4", "stale_rerun": "1", "stale_live": "1", "rewritten": "8",
		"retracted_rerun": "0", "retracted_after_cut": "1", "no_raw_live": "0", "raw_changed_after": "2",
		"mismatch": "0", "to_null_checked": "2", "pooled_mismatch": "0", "pooled_unchecked": "0",
		"ctrl_checked": "2", "ctrl_mismatch": "0",
	}
	run("oldpod T0", statements, map[string]map[string]string{"stmt0": orgWant, "stmt1": oldPod, "stmt2": oldPod})
	oldPodCut3 := map[string]string{}
	for k, v := range oldPod {
		oldPodCut3[k] = v
	}
	oldPodCut3["stale_rerun"], oldPodCut3["stale_live"], oldPodCut3["rewritten"] = "0", "0", "9"
	run("oldpod cut3", statementsCut3, map[string]map[string]string{"stmt0": orgWant, "stmt1": oldPodCut3, "stmt2": oldPodCut3})
}

// Third throwaway exec proof: SIZING-14285-5 (CHAOS-14285 sizing). sizing14285Form is SIZING-14285-5.sql byte for byte.
const sizing14285FormSHA256 = "13cebc9f7b6bd1a9350669a9ffebdd5f7c0bc5fe738a10108e010fb27bddff4b"

const sizing14285Form = `-- SIZING-14285-5 = SIZING-14285-4 (0e668c288a69) with a later old-rule cut. FORM-TEMPLATE-3 (pm-guard, 6b9e73a3c45a) shape
--   (a) + pass-path budget. ClickHouse, SELECT only. Lane l13902-cutfix (lead fcsup), 2026-11-23. Item 3 (CHAOS-14285) of
--   CARD-13902-recompute-3 (30f972dfa124) P6. Chain: SIZING-14285-2 406b0f9dd2d7 -> -3 0b545d0c495b -> -4 0e668c288a69
--   (CI exec, acp READ-FORM-14285-4 429a7a26eed0: FORM NOT OK, P2-1 cut) -> this file. -3 and -4 are SUPERSEDED: do not run.
-- Prod: NOT EXECUTED. CI exec: EXEC-PROOF-14285-5.md. md: SIZING-14285-5.md.
-- Edits vs -4 (acp READ-FORM-14285-4):
--   C1 (P2-1) Cut '2026-11-23 03:08:08' -> '2026-11-23 05:10:00' UTC in stmt1, stmt2, stmt3. Why: the STEP 323 drain-0 read
--      at 03:08:08Z (G3-read-s323-25-1.sh ef39b1593a28:11) skipped Terminating pods. The first new pod started 02:58:22Z
--      (RECORD-323 8ff7c60b985f); an old-image go-heavy pod (grace 7260 s) can write until 02:58:22Z + 7260 s = 04:59:22Z.
--      05:10:00 = that + margin: no old-image pod writes at or after it. A cut too early hid old-pod rows with c >= cut and
--      ls >= c in no count; a later cut cannot make a wrong count (a new-rule row before the cut matches, never doubled).
--   C2 (P2-1) stmt2 and stmt3 new INFO after_raw_changed (c >= cut, ls >= c) and after_doubled_raw_changed (same AND
--      sl > rl; > 0 = acp reads): the after side has no hidden class now.
--   C3 (P3-3) Cite pin wording: the live workers pin is 340c3e05 (STEP 323). Cites are read at a7febdf50002; acp checked
--      git diff a7febdf50002 340c3e05 over every cited path = only testops files: the same lines hold at 340c3e05.
--   No other SQL change: gates, caps, k weights, scans and every other column are the -4 text.
-- Edits of -4 vs -3 (FORM-TEMPLATE-3, kept):
--   M1 Budget gate: each statement's rows cap is the PASS-PATH BUDGET 20M rows read. The gate sums the system.parts rows
--      of each table it reads times k = the scans of that table in the statement (stmt5 git_pull_requests 2, stmt6
--      git_commit_stats 2 + git_commits 2, stmt7 git_commit_stats 2, else 1): sum(rows x k) over 20M or sum(bytes on disk
--      x k) over 4 GiB = STOP-COST-GATE. All orgs (an upper bound of the org reads). The gates read ONLY system.parts and
--      system.one. No CTE: the only WITH is the scalar gate; it reads metadata only, at each scope that uses it.
--   M2 stmt6: the 3 git_commits FINAL scalar subqueries (count, min day, max day) are one derived table cf, read once:
--      git_commits scans 4 -> 2. Same outputs, same names.
--   M3 read_rows WANT: read_rows <= 1.1 x G + (1 + s) x (2 x P + 1), G = sum of rows x k (S0), P = S0 parts of the gate
--      tables, s = scalar subqueries that read a table (stmt0 7, stmt6 3, else 0). CI asserts the exact scan count:
--      G + 2 x P + 1 <= query_log read_rows <= G + (1 + s) x (2 x P + 1).
-- Edits of -3 vs -2 (kept):
--   L1 Engine cost gate in EVERY statement (now the M1 budget gate), cost_gate = 0 in the outer WHERE and in every scalar
--      subquery that reads a table.
--   L2 New S0, gated on system.one (part count of its 7 tables over 100000 = STOP-COST-GATE): one row per table read here.
--   L3 No placeholder: the old-rule cut placeholder of -2 (ROLL_END_14285_UTC) is a literal, now '2026-11-23 05:10:00' UTC
--      (C1). ops 340c3e05 (STEP 323) holds ceee575af360 (merge-base --is-ancestor rc 0); the COMBINED-8 build 5aa4b332 does
--      not (rc 1). A row computed before the cut can be old rule (raw read) or new rule (an earlier fixed build matches: it
--      lands in match, never in doubled). A row at or after it is new rule (C1). Check: an after-cut row that does not match
--      the recompute = after_mismatch > 0: STOP, acp reads; an after-cut row with raw synced after it = after_raw_changed
--      (INFO), after_doubled_raw_changed (acp reads).
--   L4 Cites read with "git show a7febdf50002:<path>"; the live STEP 323 workers pin is 340c3e05 and the cited lines are the
--      same there (C3). "git diff 84d6c394e23f a7febdf50002" is empty for repouser/, repo_user_commit_native_executor.go,
--      icfinalize/executor.go, teamkeytables/teamkeytables.go, providersync/github_commit_stats_route.go, baseline/head.json.
--   L5 stmt3: new INFO doubled_commit_first_day / doubled_commit_last_day (range input of the decision rule).
-- Run rule: each statement once, in order. A SQL error (STOP-COST-GATE included) = STOP. A timeout (30 s) = STOP, no re-run.
-- read_rows WANT: M3. Work math per statement: md.
-- -2 notes (kept; paths at a7febdf50002, same lines at 340c3e05: C3):
--   F1 stmt0: prs_orgs / reviews_orgs 0 = INFO (stmt5 arm empty: no_raw can only grow), not a STOP. > 1 = STOP.
--   F2/F3 every derived table has an alias; every outer name is alias.<col>.
--   F4 stmt3 groups by the full commit_metrics key (repo_id, commit_hash, day, author_email): head.json:251 ORDER BY
--      (org_id, repo_id, day, author_email, commit_hash). INFO commit_rhd_keys < stored_commits = one commit stored under
--      2+ identity keys.
--   F5 doubled_raw_changed (stmt2, stmt3) > 0 = acp reads before a "no rerun" ruling.
--   F6 stmt7 OLD RULE vs NEW RULE on the same raw rows NOW. Old rule = the raw join of
--      ceee575af360^:internal/jobs/metrics/daily/repouser/clickhouse.go:87-89 ("FROM git_commits AS c LEFT JOIN
--      git_commit_stats AS s"). oldrule_under_commits = 0 is a hard control (raw rows hold every FINAL row, LOC terms >= 0).
--   F8 minIf/maxIf of a Date with no matching row = 1970-01-01 (= none). trimBoth trims ' ' only; Go strings.TrimSpace
--      (repouser/identity.go:35-43) trims all Unicode space: stmt4 INFO only. last_synced = the writer's normalizedAt
--      (providersync/github_commit_stats_route.go:185), set before the INSERT: a row with ls < c inserted after the
--      compute read is possible in a short gap (mostly under). computed_at = executor.nowUTC() after the reads
--      (repo_user_commit_native_executor.go:99-104 vs :164-165): same gap. Retire rows: marker column "retracted"
--      (teamkeytables/teamkeytables.go:380 repo, :295 user), version = snapshot (executor :99, :248-249).
--   F9 keys_checked / commits_checked / after_checked = 0 = NOT EXERCISED (not a "no rerun"): acp reads.
-- Run window: STEP 323 workers (340c3e05, holds 9d88395188e6 + ceee575af360) live, and OUTSIDE any recompute of these
--   tables (card daily-start run, partition-recompute). The normal nightly run is allowed: its keys are after-cut keys or
--   are left out by ls >= c. Schema: head.json 'create' lines :251 :346 :351 :361 :366 :531 :696; migrations 145-170 touch
--   only repo_metrics_daily here (161:34-36 pooled_*, not read; 164:16 retracted, read).
-- Defect: before ceee575af360 (#6672) commitStatsQuery read git_commits / git_commit_stats raw (both ReplacingMergeTree(
--   last_synced), head.json:346, :351). A commit with 2 unmerged versions joined every stat row twice; a stat row with 2
--   versions counted twice. The job STORED that LOC. Now: "git_commits AS c FINAL" / "git_commit_stats AS s FINAL"
--   (repouser/clickhouse.go:77-92).
-- Why a compare: the old unmerged state is not stored and merges removed most duplicates. So: newest stored row vs the same
--   value recomputed NOW by the code rule. stored > recomputed = the doubled case (this defect). stored < recomputed = raw
--   arrived after the compute (INFO, not this defect).
-- Code rule (recompute), repouser/ at a7febdf50002 (same lines at 340c3e05: C3):
--   R1 day = UTC day of git_commits.committer_when, [00:00, +24h) (clickhouse.go:90; compute.go:23-26; executor
--      repo_user_commit_native_executor.go:94-95). committer_when is DateTime64(3, 'UTC') (head.json:351).
--   R2 one aggregate per (repo_id, hash) (compute.go:213-228). LOC of a commit = sum over its stat rows of max(0, additions)
--      + max(0, deletions) (compute.go:229-230, :139). No stat row = 0 (LEFT JOIN clickhouse.go:88; derefInt32 :131-132).
--   R3 repo_metrics_daily.total_loc_touched = sum of the LOC of the day's commits (compute.go:249-254, :515-521; writer
--      clickhouse.go:538-541). commits_count = distinct commits of the day (compute.go:252, :520): NOT doubled = control
--      cell count_mismatch.
--   R4 commit_metrics.total_loc = LOC of the commit (compute.go:654-663; writer clickhouse.go:736-737). Size bucket
--      literals 'small' (<= 50) / 'medium' (<= 300) / 'large' = CommitSizeBucket compute.go:12-20.
--   R5 user_metrics_daily.loc_added + loc_deleted = LOC of the identity's commits of the day (compute.go:249-254, :464-467);
--      identity = lower(trim(author_email)), else trim(author_name), else the literal 'unknown' (identity.go:35-43;
--      executor :166).
-- Not checked (derived): avg_commit_size_loc, large_commit_ratio, large_commits_count, size_bucket (INFO bucket_move),
--   rework_churn_ratio_30d, single_owner_file_ratio_30d, bus_factor, code_ownership_gini (30-day window: executor :96,
--   :104). A commit doubled at the day's compute makes total_loc_touched of THAT day mismatch; a 30-day value made wrong by
--   a duplicate that existed only at a LATER day's compute is not found (CARD ADDENDUM-2 R2).
-- Exclusion: a key with a raw commit or stat row synced at/after the stored compute (ls >= c) is left out of every check
--   and counted as INFO raw_changed_after (before the cut) or after_raw_changed (at/after the cut, C2).
-- argMax: every stored value read here is NOT Nullable (UInt32 / UInt8 / LowCardinality(String)): plain argMax(v,
--   computed_at) = the newest row's value. computed_at is DateTime('UTC'), second precision (clickhouse.go:413-419).
-- LEFT JOIN miss: every test goes through ifNull first: no dependence on join_use_nulls (no SETTINGS).

-- S0 cost inputs. WANT 7 rows (one per table read by this form), table_rows > 0 in each (a wrong name gives no row).
-- Gate on system.one: the part count of the 7 tables over 100000 = STOP-COST-GATE.
WITH (SELECT throwIf(
    (SELECT count() FROM system.parts WHERE active AND database = currentDatabase()
      AND table IN ('commit_metrics', 'git_commit_stats', 'git_commits', 'git_pull_request_reviews', 'git_pull_requests', 'repo_metrics_daily', 'user_metrics_daily')) > 100000,
    'STOP-COST-GATE') FROM system.one) AS cost_gate
SELECT table, sum(rows) AS table_rows, sum(bytes_on_disk) AS disk_bytes, count() AS active_parts
FROM system.parts
WHERE active AND database = currentDatabase()
  AND table IN ('commit_metrics', 'git_commit_stats', 'git_commits', 'git_pull_request_reviews', 'git_pull_requests', 'repo_metrics_daily', 'user_metrics_daily') AND cost_gate = 0
GROUP BY table
ORDER BY table;

-- stmt0 ORG-1 over every table read. WANT 1 in each column; > 1 = STOP. F1: prs_orgs / reviews_orgs 0 = INFO; 0 in any
--   other column = STOP. Gate tables: all 7.
WITH (SELECT throwIf(
    (SELECT sum(rows) FROM system.parts WHERE active AND database = currentDatabase()
      AND table IN ('commit_metrics', 'git_commit_stats', 'git_commits', 'git_pull_request_reviews', 'git_pull_requests', 'repo_metrics_daily', 'user_metrics_daily')) > 20000000
    OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = currentDatabase()
      AND table IN ('commit_metrics', 'git_commit_stats', 'git_commits', 'git_pull_request_reviews', 'git_pull_requests', 'repo_metrics_daily', 'user_metrics_daily')) > 4294967296,
    'STOP-COST-GATE')) AS cost_gate
SELECT
  (SELECT uniqExact(org_id) FROM git_commits WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0) AS commits_orgs,
  (SELECT uniqExact(org_id) FROM git_commit_stats WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0) AS stats_orgs,
  (SELECT uniqExact(org_id) FROM git_pull_requests WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0) AS prs_orgs,
  (SELECT uniqExact(org_id) FROM git_pull_request_reviews WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0) AS reviews_orgs,
  (SELECT uniqExact(org_id) FROM repo_metrics_daily WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0) AS repo_orgs,
  (SELECT uniqExact(org_id) FROM user_metrics_daily WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0) AS user_orgs,
  (SELECT uniqExact(org_id) FROM commit_metrics WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0) AS commit_orgs;

-- stmt1 RANGE of the stored live repo-days of repo_metrics_daily. Gate tables: repo_metrics_daily.
-- WANT live_repo_days > 0; before_roll_repo_days > 0 (0 = nothing stored before the cut: NOT EXERCISED).
-- INFO first_day, last_day, days, after_roll_repo_days.
WITH (SELECT throwIf(
    (SELECT sum(rows) FROM system.parts WHERE active AND database = currentDatabase()
      AND table IN ('repo_metrics_daily')) > 20000000
    OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = currentDatabase()
      AND table IN ('repo_metrics_daily')) > 4294967296,
    'STOP-COST-GATE')) AS cost_gate
SELECT count() AS live_repo_days, min(s.day) AS first_day, max(s.day) AS last_day, uniqExact(s.day) AS days,
  countIf(s.c < toDateTime('2026-11-23 05:10:00', 'UTC')) AS before_roll_repo_days,
  countIf(s.c >= toDateTime('2026-11-23 05:10:00', 'UTC')) AS after_roll_repo_days
FROM (SELECT repo_id, day, max(computed_at) AS c, argMax(retracted, computed_at) AS r FROM repo_metrics_daily
      WHERE startsWith(org_id, 'c6a38355') GROUP BY repo_id, day) AS s
WHERE s.r = 0 AND cost_gate = 0;

-- stmt2 REPO (the gate). Stored newest live repo-day vs recompute R1-R3. Gate tables: commits + stats + repo.
-- Shapes (all over live stored keys, WHERE s.r = 0; the raw side has no filter but the org):
--   checked = raw commit on the day, computed before the cut, no raw sync at/after the compute.
--   after   = raw commit on the day, computed at/after the cut, no raw sync at/after the compute.
-- WANT live_repo_days > 0; with_commits > 0; keys_checked > 0; match_keys > 0 (the compare can match).
--   F9: keys_checked = 0 or after_checked = 0 = NOT EXERCISED: acp reads.
-- WANT after_checked > 0 and after_mismatch = 0 (a miss = STOP, acp reads). WANT count_mismatch = 0 (a miss = acp reads).
-- GATE doubled_repo_days, affected_days (0 + stmt3 doubled_commits 0 = NO rerun for item 3).
-- INFO doubled_first_day/last_day, ratio2_repo_days, loc_excess, under_repo_days, raw_changed_after, doubled_raw_changed
--   (F5), no_commit_now (stored commits_count > 0, no raw commit on the day today).
-- INFO after_raw_changed (computed at/after the cut, raw synced at/after the compute: left out of after_checked, C2);
--   after_doubled_raw_changed (same AND stored > recomputed) > 0 = acp reads.
WITH (SELECT throwIf(
    (SELECT sum(rows) FROM system.parts WHERE active AND database = currentDatabase()
      AND table IN ('git_commit_stats', 'git_commits', 'repo_metrics_daily')) > 20000000
    OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = currentDatabase()
      AND table IN ('git_commit_stats', 'git_commits', 'repo_metrics_daily')) > 4294967296,
    'STOP-COST-GATE')) AS cost_gate
SELECT count() AS live_repo_days,
  countIf(ifNull(raw.n, 0) > 0) AS with_commits,
  countIf(ifNull(raw.n, 0) = 0 AND s.sn > 0) AS no_commit_now,
  countIf(ifNull(raw.n, 0) > 0 AND s.c < toDateTime('2026-11-23 05:10:00', 'UTC') AND raw.ls >= s.c) AS raw_changed_after,
  countIf(ifNull(raw.n, 0) > 0 AND s.c < toDateTime('2026-11-23 05:10:00', 'UTC') AND raw.ls >= s.c
          AND s.sl > raw.rl) AS doubled_raw_changed,
  countIf(ifNull(raw.n, 0) > 0 AND s.c < toDateTime('2026-11-23 05:10:00', 'UTC') AND raw.ls < s.c) AS keys_checked,
  countIf(ifNull(raw.n, 0) > 0 AND s.c < toDateTime('2026-11-23 05:10:00', 'UTC') AND raw.ls < s.c
          AND s.sl = raw.rl) AS match_keys,
  countIf(ifNull(raw.n, 0) > 0 AND s.c < toDateTime('2026-11-23 05:10:00', 'UTC') AND raw.ls < s.c
          AND s.sl > raw.rl) AS doubled_repo_days,
  uniqExactIf(s.day, ifNull(raw.n, 0) > 0 AND s.c < toDateTime('2026-11-23 05:10:00', 'UTC') AND raw.ls < s.c
          AND s.sl > raw.rl) AS affected_days,
  minIf(s.day, ifNull(raw.n, 0) > 0 AND s.c < toDateTime('2026-11-23 05:10:00', 'UTC') AND raw.ls < s.c
          AND s.sl > raw.rl) AS doubled_first_day,
  maxIf(s.day, ifNull(raw.n, 0) > 0 AND s.c < toDateTime('2026-11-23 05:10:00', 'UTC') AND raw.ls < s.c
          AND s.sl > raw.rl) AS doubled_last_day,
  countIf(ifNull(raw.n, 0) > 0 AND s.c < toDateTime('2026-11-23 05:10:00', 'UTC') AND raw.ls < s.c
          AND raw.rl > 0 AND s.sl = 2 * raw.rl) AS ratio2_repo_days,
  sumIf(s.sl - raw.rl, ifNull(raw.n, 0) > 0 AND s.c < toDateTime('2026-11-23 05:10:00', 'UTC') AND raw.ls < s.c
          AND s.sl > raw.rl) AS loc_excess,
  countIf(ifNull(raw.n, 0) > 0 AND s.c < toDateTime('2026-11-23 05:10:00', 'UTC') AND raw.ls < s.c
          AND s.sl < raw.rl) AS under_repo_days,
  countIf(ifNull(raw.n, 0) > 0 AND s.c < toDateTime('2026-11-23 05:10:00', 'UTC') AND raw.ls < s.c
          AND s.sn != raw.n) AS count_mismatch,
  countIf(ifNull(raw.n, 0) > 0 AND s.c >= toDateTime('2026-11-23 05:10:00', 'UTC') AND raw.ls < s.c) AS after_checked,
  countIf(ifNull(raw.n, 0) > 0 AND s.c >= toDateTime('2026-11-23 05:10:00', 'UTC') AND raw.ls < s.c
          AND (s.sl != raw.rl OR s.sn != raw.n)) AS after_mismatch,
  countIf(ifNull(raw.n, 0) > 0 AND s.c >= toDateTime('2026-11-23 05:10:00', 'UTC') AND raw.ls >= s.c) AS after_raw_changed,
  countIf(ifNull(raw.n, 0) > 0 AND s.c >= toDateTime('2026-11-23 05:10:00', 'UTC') AND raw.ls >= s.c
          AND s.sl > raw.rl) AS after_doubled_raw_changed
FROM (SELECT repo_id, day, max(computed_at) AS c, argMax(retracted, computed_at) AS r,
        toInt64(argMax(total_loc_touched, computed_at)) AS sl, toInt64(argMax(commits_count, computed_at)) AS sn
      FROM repo_metrics_daily WHERE startsWith(org_id, 'c6a38355') GROUP BY repo_id, day) AS s
LEFT JOIN (
  SELECT cm.c_repo AS k_repo, cm.c_day AS k_day, count() AS n, sum(ifNull(st.loc, 0)) AS rl,
    max(greatest(cm.c_ls, ifNull(st.s_ls, cm.c_ls))) AS ls
  FROM (SELECT org_id AS c_org, repo_id AS c_repo, hash AS c_hash, toDate(committer_when) AS c_day, last_synced AS c_ls
        FROM git_commits FINAL WHERE startsWith(org_id, 'c6a38355')) AS cm
  LEFT JOIN (SELECT org_id AS s_org, repo_id AS s_repo, commit_hash AS s_hash,
               sum(greatest(0, additions)) + sum(greatest(0, deletions)) AS loc, max(last_synced) AS s_ls
             FROM git_commit_stats FINAL WHERE startsWith(org_id, 'c6a38355')
             GROUP BY org_id, repo_id, commit_hash) AS st
    ON st.s_org = cm.c_org AND st.s_repo = cm.c_repo AND st.s_hash = cm.c_hash
  GROUP BY cm.c_repo, cm.c_day
) AS raw ON raw.k_repo = s.repo_id AND raw.k_day = s.day
WHERE s.r = 0 AND cost_gate = 0;

-- stmt3 COMMIT. Stored newest commit_metrics row per (repo_id, commit_hash, day, author_email) (F4) vs recompute R1, R2, R4.
-- commit_metrics has no retracted column (head.json:251; writer clickhouse.go:736-737): every stored key counts.
-- Gate tables: commit_metrics + commits + stats. Shapes: checked / after as stmt2, on the commit key.
-- WANT stored_commits > 0; with_raw > 0; commits_checked > 0; match_commits > 0; after_checked > 0; after_mismatch = 0.
--   F9: commits_checked = 0 or after_checked = 0 = NOT EXERCISED: acp reads. F5: doubled_raw_changed > 0 = acp reads.
-- GATE doubled_commits. INFO commit_rhd_keys, doubled_commit_days, doubled_commit_repo_days, doubled_commit_first_day,
--   doubled_commit_last_day (L5), ratio2_commits, under_commits, bucket_move (stored size_bucket != bucket of the
--   recomputed LOC), large_flag_move (> 300 LOC flag of large_commits_count moves, compute.go:258), commit_no_raw (stored
--   key, no raw commit on that day: never retracted, a recompute leaves it), raw_changed_after, after_raw_changed (C2).
--   after_doubled_raw_changed > 0 = acp reads (C2).
WITH (SELECT throwIf(
    (SELECT sum(rows) FROM system.parts WHERE active AND database = currentDatabase()
      AND table IN ('commit_metrics', 'git_commit_stats', 'git_commits')) > 20000000
    OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = currentDatabase()
      AND table IN ('commit_metrics', 'git_commit_stats', 'git_commits')) > 4294967296,
    'STOP-COST-GATE')) AS cost_gate
SELECT count() AS stored_commits,
  uniqExact(tuple(s.repo_id, s.commit_hash, s.day)) AS commit_rhd_keys,
  countIf(ifNull(raw.present, 0) = 1) AS with_raw,
  countIf(ifNull(raw.present, 0) = 0) AS commit_no_raw,
  countIf(ifNull(raw.present, 0) = 1 AND s.c < toDateTime('2026-11-23 05:10:00', 'UTC') AND raw.ls >= s.c) AS raw_changed_after,
  countIf(ifNull(raw.present, 0) = 1 AND s.c < toDateTime('2026-11-23 05:10:00', 'UTC') AND raw.ls >= s.c
          AND s.sl > raw.rl) AS doubled_raw_changed,
  countIf(ifNull(raw.present, 0) = 1 AND s.c < toDateTime('2026-11-23 05:10:00', 'UTC') AND raw.ls < s.c) AS commits_checked,
  countIf(ifNull(raw.present, 0) = 1 AND s.c < toDateTime('2026-11-23 05:10:00', 'UTC') AND raw.ls < s.c
          AND s.sl = raw.rl) AS match_commits,
  countIf(ifNull(raw.present, 0) = 1 AND s.c < toDateTime('2026-11-23 05:10:00', 'UTC') AND raw.ls < s.c
          AND s.sl > raw.rl) AS doubled_commits,
  uniqExactIf(s.day, ifNull(raw.present, 0) = 1 AND s.c < toDateTime('2026-11-23 05:10:00', 'UTC') AND raw.ls < s.c
          AND s.sl > raw.rl) AS doubled_commit_days,
  uniqExactIf(tuple(s.repo_id, s.day), ifNull(raw.present, 0) = 1 AND s.c < toDateTime('2026-11-23 05:10:00', 'UTC')
          AND raw.ls < s.c AND s.sl > raw.rl) AS doubled_commit_repo_days,
  minIf(s.day, ifNull(raw.present, 0) = 1 AND s.c < toDateTime('2026-11-23 05:10:00', 'UTC') AND raw.ls < s.c
          AND s.sl > raw.rl) AS doubled_commit_first_day,
  maxIf(s.day, ifNull(raw.present, 0) = 1 AND s.c < toDateTime('2026-11-23 05:10:00', 'UTC') AND raw.ls < s.c
          AND s.sl > raw.rl) AS doubled_commit_last_day,
  countIf(ifNull(raw.present, 0) = 1 AND s.c < toDateTime('2026-11-23 05:10:00', 'UTC') AND raw.ls < s.c
          AND raw.rl > 0 AND s.sl = 2 * raw.rl) AS ratio2_commits,
  countIf(ifNull(raw.present, 0) = 1 AND s.c < toDateTime('2026-11-23 05:10:00', 'UTC') AND raw.ls < s.c
          AND s.sl < raw.rl) AS under_commits,
  countIf(ifNull(raw.present, 0) = 1 AND s.c < toDateTime('2026-11-23 05:10:00', 'UTC') AND raw.ls < s.c
          AND s.sl > raw.rl AND s.sb != multiIf(raw.rl <= 50, 'small', raw.rl <= 300, 'medium', 'large')) AS bucket_move,
  countIf(ifNull(raw.present, 0) = 1 AND s.c < toDateTime('2026-11-23 05:10:00', 'UTC') AND raw.ls < s.c
          AND s.sl > 300 AND raw.rl <= 300) AS large_flag_move,
  countIf(ifNull(raw.present, 0) = 1 AND s.c >= toDateTime('2026-11-23 05:10:00', 'UTC') AND raw.ls < s.c) AS after_checked,
  countIf(ifNull(raw.present, 0) = 1 AND s.c >= toDateTime('2026-11-23 05:10:00', 'UTC') AND raw.ls < s.c
          AND s.sl != raw.rl) AS after_mismatch,
  countIf(ifNull(raw.present, 0) = 1 AND s.c >= toDateTime('2026-11-23 05:10:00', 'UTC') AND raw.ls >= s.c) AS after_raw_changed,
  countIf(ifNull(raw.present, 0) = 1 AND s.c >= toDateTime('2026-11-23 05:10:00', 'UTC') AND raw.ls >= s.c
          AND s.sl > raw.rl) AS after_doubled_raw_changed
FROM (SELECT repo_id, commit_hash, day, max(computed_at) AS c, toInt64(argMax(total_loc, computed_at)) AS sl,
        argMax(size_bucket, computed_at) AS sb
      FROM commit_metrics WHERE startsWith(org_id, 'c6a38355') GROUP BY repo_id, commit_hash, day, author_email) AS s
LEFT JOIN (
  SELECT cm.c_repo AS k_repo, cm.c_hash AS k_hash, cm.c_day AS k_day, toUInt8(1) AS present,
    toInt64(ifNull(st.loc, 0)) AS rl, greatest(cm.c_ls, ifNull(st.s_ls, cm.c_ls)) AS ls
  FROM (SELECT org_id AS c_org, repo_id AS c_repo, hash AS c_hash, toDate(committer_when) AS c_day, last_synced AS c_ls
        FROM git_commits FINAL WHERE startsWith(org_id, 'c6a38355')) AS cm
  LEFT JOIN (SELECT org_id AS s_org, repo_id AS s_repo, commit_hash AS s_hash,
               sum(greatest(0, additions)) + sum(greatest(0, deletions)) AS loc, max(last_synced) AS s_ls
             FROM git_commit_stats FINAL WHERE startsWith(org_id, 'c6a38355')
             GROUP BY org_id, repo_id, commit_hash) AS st
    ON st.s_org = cm.c_org AND st.s_repo = cm.c_repo AND st.s_hash = cm.c_hash
) AS raw ON raw.k_repo = s.repo_id AND raw.k_hash = s.commit_hash AND raw.k_day = s.day
WHERE cost_gate = 0;

-- stmt4 USER (INFO only, no decision). Stored newest live user_metrics_daily row per (repo_id, author_email, day) vs
--   recompute R1, R2, R5. No cut split: ic_finalize writes the SAME key again with a later computed_at and the values
--   passed through (icfinalize/executor.go:408-427). Gate tables: commits + stats + user_metrics_daily.
-- WANT live_user_keys > 0; user_checked > 0; user_match > 0. INFO with_commits, user_over, user_over_days, user_ratio2,
--   user_under, user_count_mismatch, raw_changed_after.
WITH (SELECT throwIf(
    (SELECT sum(rows) FROM system.parts WHERE active AND database = currentDatabase()
      AND table IN ('git_commit_stats', 'git_commits', 'user_metrics_daily')) > 20000000
    OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = currentDatabase()
      AND table IN ('git_commit_stats', 'git_commits', 'user_metrics_daily')) > 4294967296,
    'STOP-COST-GATE')) AS cost_gate
SELECT count() AS live_user_keys,
  countIf(ifNull(raw.n, 0) > 0) AS with_commits,
  countIf(ifNull(raw.n, 0) > 0 AND raw.ls >= s.c) AS raw_changed_after,
  countIf(ifNull(raw.n, 0) > 0 AND raw.ls < s.c) AS user_checked,
  countIf(ifNull(raw.n, 0) > 0 AND raw.ls < s.c AND s.sl = raw.rl) AS user_match,
  countIf(ifNull(raw.n, 0) > 0 AND raw.ls < s.c AND s.sl > raw.rl) AS user_over,
  uniqExactIf(s.day, ifNull(raw.n, 0) > 0 AND raw.ls < s.c AND s.sl > raw.rl) AS user_over_days,
  countIf(ifNull(raw.n, 0) > 0 AND raw.ls < s.c AND raw.rl > 0 AND s.sl = 2 * raw.rl) AS user_ratio2,
  countIf(ifNull(raw.n, 0) > 0 AND raw.ls < s.c AND s.sl < raw.rl) AS user_under,
  countIf(ifNull(raw.n, 0) > 0 AND raw.ls < s.c AND s.sn != raw.n) AS user_count_mismatch
FROM (SELECT repo_id, author_email, day, max(computed_at) AS c, argMax(retracted, computed_at) AS r,
        toInt64(argMax(loc_added, computed_at)) + toInt64(argMax(loc_deleted, computed_at)) AS sl,
        toInt64(argMax(commits_count, computed_at)) AS sn
      FROM user_metrics_daily WHERE startsWith(org_id, 'c6a38355') GROUP BY repo_id, author_email, day) AS s
LEFT JOIN (
  SELECT cm.c_repo AS k_repo, cm.c_who AS k_who, cm.c_day AS k_day, count() AS n, sum(ifNull(st.loc, 0)) AS rl,
    max(greatest(cm.c_ls, ifNull(st.s_ls, cm.c_ls))) AS ls
  FROM (SELECT org_id AS c_org, repo_id AS c_repo, hash AS c_hash, toDate(committer_when) AS c_day, last_synced AS c_ls,
          if(trimBoth(ifNull(author_email, '')) != '', lowerUTF8(trimBoth(ifNull(author_email, ''))),
             if(trimBoth(ifNull(author_name, '')) != '', trimBoth(ifNull(author_name, '')), 'unknown')) AS c_who
        FROM git_commits FINAL WHERE startsWith(org_id, 'c6a38355')) AS cm
  LEFT JOIN (SELECT org_id AS s_org, repo_id AS s_repo, commit_hash AS s_hash,
               sum(greatest(0, additions)) + sum(greatest(0, deletions)) AS loc, max(last_synced) AS s_ls
             FROM git_commit_stats FINAL WHERE startsWith(org_id, 'c6a38355')
             GROUP BY org_id, repo_id, commit_hash) AS st
    ON st.s_org = cm.c_org AND st.s_repo = cm.c_repo AND st.s_hash = cm.c_hash
  GROUP BY cm.c_repo, cm.c_who, cm.c_day
) AS raw ON raw.k_repo = s.repo_id AND raw.k_who = s.author_email AND raw.k_day = s.day
WHERE s.r = 0 AND cost_gate = 0;

-- stmt5 ERASE (card R1 for this family). A live stored repo-day with no commit, no pull request (created or merged on the
--   day) and no review (submitted on the day) is not produced by Compute (repoSet, compute.go:500-506; reviews make a user
--   row, :361-367) and gets a row that retires the key at recompute (executor :241-253). Raw arms = the loader windows
--   (clickhouse.go:90, :178-179, :248); reviews read raw as the loader does (:247).
-- Gate tables: commits + reviews + pull requests + repo. git_pull_requests is read twice (created arm, merged arm).
-- WANT live_repo_days > 0; with_raw > 0; no_raw = 0 (> 0 = pm + acp read before any recompute of those days).
-- INFO no_raw_first_day, no_raw_last_day.
WITH (SELECT throwIf(
    (SELECT sum(rows * multiIf(table = 'git_pull_requests', 2, 1)) FROM system.parts WHERE active AND database = currentDatabase()
      AND table IN ('git_commits', 'git_pull_request_reviews', 'git_pull_requests', 'repo_metrics_daily')) > 20000000
    OR (SELECT sum(bytes_on_disk * multiIf(table = 'git_pull_requests', 2, 1)) FROM system.parts WHERE active AND database = currentDatabase()
      AND table IN ('git_commits', 'git_pull_request_reviews', 'git_pull_requests', 'repo_metrics_daily')) > 4294967296,
    'STOP-COST-GATE')) AS cost_gate
SELECT count() AS live_repo_days,
  countIf(ifNull(a.k, 0) > 0) AS with_raw,
  countIf(ifNull(a.k, 0) = 0) AS no_raw,
  minIf(s.day, ifNull(a.k, 0) = 0) AS no_raw_first_day, maxIf(s.day, ifNull(a.k, 0) = 0) AS no_raw_last_day
FROM (SELECT repo_id, day, argMax(retracted, computed_at) AS r FROM repo_metrics_daily
      WHERE startsWith(org_id, 'c6a38355') GROUP BY repo_id, day) AS s
LEFT JOIN (
  SELECT u.a_repo AS r_repo, u.a_day AS r_day, count() AS k FROM (
    SELECT repo_id AS a_repo, toDate(committer_when) AS a_day FROM git_commits FINAL
    WHERE startsWith(org_id, 'c6a38355')
    UNION ALL
    SELECT repo_id AS a_repo, toDate(created_at) AS a_day FROM git_pull_requests FINAL
    WHERE startsWith(org_id, 'c6a38355')
    UNION ALL
    SELECT repo_id AS a_repo, toDate(assumeNotNull(merged_at)) AS a_day FROM git_pull_requests FINAL
    WHERE startsWith(org_id, 'c6a38355') AND merged_at IS NOT NULL
    UNION ALL
    SELECT repo_id AS a_repo, toDate(submitted_at) AS a_day FROM git_pull_request_reviews
    WHERE startsWith(org_id, 'c6a38355')
  ) AS u GROUP BY u.a_repo, u.a_day
) AS a ON a.r_repo = s.repo_id AND a.r_day = s.day
WHERE s.r = 0 AND cost_gate = 0;

-- stmt6 RAW SPAN and unmerged versions NOW. Gate tables: commits + stats, each read twice (raw + FINAL; M2).
-- WANT commit_rows_final > 0; stat_rows_final > 0 (the compare sees data).
-- WANT raw_first_day <= stmt1 first_day (else old stored days lost their raw rows: see stmt2 no_commit_now, stmt5 no_raw).
-- INFO commit_versions_unmerged, stat_versions_unmerged: > 0 = the duplicate state still occurs today.
WITH (SELECT throwIf(
    (SELECT sum(rows * multiIf(table = 'git_commit_stats', 2, table = 'git_commits', 2, 1)) FROM system.parts WHERE active AND database = currentDatabase()
      AND table IN ('git_commit_stats', 'git_commits')) > 20000000
    OR (SELECT sum(bytes_on_disk * multiIf(table = 'git_commit_stats', 2, table = 'git_commits', 2, 1)) FROM system.parts WHERE active AND database = currentDatabase()
      AND table IN ('git_commit_stats', 'git_commits')) > 4294967296,
    'STOP-COST-GATE')) AS cost_gate
SELECT
  (SELECT count() FROM git_commits WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0) AS commit_rows_raw,
  cf.f_rows AS commit_rows_final,
  (SELECT count() FROM git_commit_stats WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0) AS stat_rows_raw,
  (SELECT count() FROM git_commit_stats FINAL WHERE startsWith(org_id, 'c6a38355') AND cost_gate = 0) AS stat_rows_final,
  commit_rows_raw - commit_rows_final AS commit_versions_unmerged,
  stat_rows_raw - stat_rows_final AS stat_versions_unmerged,
  cf.f_first AS raw_first_day, cf.f_last AS raw_last_day
FROM (SELECT count() AS f_rows, min(toDate(committer_when)) AS f_first, max(toDate(committer_when)) AS f_last
      FROM git_commits FINAL WHERE startsWith(org_id, 'c6a38355')) AS cf
WHERE cost_gate = 0;

-- stmt7 OLD RULE vs NEW RULE NOW (F6), per (repo_id, hash) of every raw commit of the org, no day filter.
--   old = the pre-fix join: raw git_commits x raw git_commit_stats, LOC as compute.go:229-230. new = git_commit_stats FINAL
--   (one version per (org, repo, hash, file_path), head.json:346). Gate tables: commits + stats (stats read twice).
-- WANT commits_now > 0; oldrule_same_commits > 0; oldrule_under_commits = 0 (old >= new always: > 0 = the SQL is wrong,
--   STOP, acp reads). INFO oldrule_over_commits (> 0 = the doubling is live today), oldrule_ratio2_commits.
WITH (SELECT throwIf(
    (SELECT sum(rows * multiIf(table = 'git_commit_stats', 2, 1)) FROM system.parts WHERE active AND database = currentDatabase()
      AND table IN ('git_commit_stats', 'git_commits')) > 20000000
    OR (SELECT sum(bytes_on_disk * multiIf(table = 'git_commit_stats', 2, 1)) FROM system.parts WHERE active AND database = currentDatabase()
      AND table IN ('git_commit_stats', 'git_commits')) > 4294967296,
    'STOP-COST-GATE')) AS cost_gate
SELECT count() AS commits_now,
  countIf(o.o_old = o.o_new) AS oldrule_same_commits,
  countIf(o.o_old > o.o_new) AS oldrule_over_commits,
  countIf(o.o_new > 0 AND o.o_old = 2 * o.o_new) AS oldrule_ratio2_commits,
  countIf(o.o_old < o.o_new) AS oldrule_under_commits
FROM (
  SELECT oc.k_repo AS o_repo, oc.k_hash AS o_hash, oc.old_loc AS o_old, ifNull(nc.new_loc, 0) AS o_new
  FROM (
    SELECT c.c_repo AS k_repo, c.c_hash AS k_hash,
      sum(greatest(0, ifNull(s.s_add, 0))) + sum(greatest(0, ifNull(s.s_del, 0))) AS old_loc
    FROM (SELECT org_id AS c_org, repo_id AS c_repo, hash AS c_hash FROM git_commits
          WHERE startsWith(org_id, 'c6a38355')) AS c
    LEFT JOIN (SELECT org_id AS s_org, repo_id AS s_repo, commit_hash AS s_hash, additions AS s_add, deletions AS s_del
               FROM git_commit_stats WHERE startsWith(org_id, 'c6a38355')) AS s
      ON s.s_org = c.c_org AND s.s_repo = c.c_repo AND s.s_hash = c.c_hash
    GROUP BY c.c_repo, c.c_hash
  ) AS oc
  LEFT JOIN (
    SELECT repo_id AS n_repo, commit_hash AS n_hash, sum(greatest(0, additions)) + sum(greatest(0, deletions)) AS new_loc
    FROM git_commit_stats FINAL WHERE startsWith(org_id, 'c6a38355') GROUP BY repo_id, commit_hash
  ) AS nc ON nc.n_repo = oc.k_repo AND nc.n_hash = oc.k_hash
) AS o
WHERE cost_gate = 0;
`

// Synthetic rows. Org c6a38355-0000-..., other org b0000000-... (filter control). Merges are stopped and each version of a
// key is its own INSERT. Commits: hash-01 (11-10, 2 unmerged commit versions: old rule doubles its LOC, stored doubled),
// hash-02 + hash-03 (11-11 control; hash-03 has no stat row and its identity is the trimmed author name), hash-04 (11-12, a
// stat version synced after the compute: left out), hash-05 (11-13, stored under the recompute, a negative additions term),
// hash-06 (11-14, 2 stat versions, stored doubled over 300 LOC), hash-08 (11-22, computed after the 05:10:00 cut, new rule).
// Stored keys with no raw commit: hash-07 / R2 11-15 (a PR created that day), R2 11-16 (nothing: no_raw), R2 11-17 (a
// review), R2 11-18 (a PR merged that day). R3 11-19 = retracted key.
const sizing14285Fixture = `
SYSTEM STOP MERGES git_commits;
SYSTEM STOP MERGES git_commit_stats;
SYSTEM STOP MERGES git_pull_requests;
SYSTEM STOP MERGES git_pull_request_reviews;
SYSTEM STOP MERGES repo_metrics_daily;
SYSTEM STOP MERGES user_metrics_daily;
SYSTEM STOP MERGES commit_metrics;
INSERT INTO git_commits (org_id, repo_id, hash, author_name, author_email, committer_when, last_synced) VALUES
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', 'hash-01', 'Dev One', 'Dev1@Example.com ', '2026-11-10 10:00:00', '2026-11-10 12:00:00'),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', 'hash-02', 'Dev Two', 'dev2@example.com', '2026-11-11 09:00:00', '2026-11-11 10:00:00'),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', 'hash-03', ' Dev Three ', NULL, '2026-11-11 15:00:00', '2026-11-11 16:00:00'),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', 'hash-04', 'Dev One', 'dev1@example.com', '2026-11-12 10:00:00', '2026-11-12 11:00:00'),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', 'hash-05', 'Dev Two', 'dev2@example.com', '2026-11-13 10:00:00', '2026-11-13 11:00:00'),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000002', 'hash-06', 'Dev Three', 'dev3@example.com', '2026-11-14 10:00:00', '2026-11-14 11:00:00'),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000003', 'hash-08', 'Dev Four', 'dev4@example.com', '2026-11-22 10:00:00', '2026-11-22 11:00:00'),
('b0000000-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000002', 'hash-09', 'Dev Other', 'other@example.com', '2026-11-16 10:00:00', '2026-11-16 11:00:00');
INSERT INTO git_commits (org_id, repo_id, hash, author_name, author_email, committer_when, last_synced) VALUES
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', 'hash-01', 'Dev One', 'Dev1@Example.com ', '2026-11-10 10:00:00', '2026-11-10 13:00:00'),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000003', 'hash-08', 'Dev Four', 'dev4@example.com', '2026-11-22 10:00:00', '2026-11-22 12:00:00');
INSERT INTO git_commit_stats (org_id, repo_id, commit_hash, file_path, additions, deletions, last_synced) VALUES
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', 'hash-01', 'f1.go', 10, 5, '2026-11-10 12:00:00'),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', 'hash-01', 'f2.go', 20, 0, '2026-11-10 12:00:00'),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', 'hash-02', 'f1.go', 3, 2, '2026-11-11 10:00:00'),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', 'hash-04', 'f1.go', 100, 0, '2026-11-12 11:00:00'),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', 'hash-05', 'f1.go', 6, 4, '2026-11-13 11:00:00'),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', 'hash-05', 'f2.go', -3, 0, '2026-11-13 11:00:00'),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000002', 'hash-06', 'f1.go', 200, 0, '2026-11-14 11:00:00'),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000003', 'hash-08', 'f1.go', 8, 2, '2026-11-22 11:00:00'),
('b0000000-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', 'hash-01', 'f1.go', 500, 500, '2026-11-10 12:00:00');
INSERT INTO git_commit_stats (org_id, repo_id, commit_hash, file_path, additions, deletions, last_synced) VALUES
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', 'hash-04', 'f1.go', 100, 0, '2026-11-14 00:00:00'),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000002', 'hash-06', 'f1.go', 200, 0, '2026-11-14 12:00:00');
INSERT INTO git_pull_requests (org_id, repo_id, number, created_at, merged_at, last_synced) VALUES
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000002', 1, '2026-11-15 09:00:00', NULL, '2026-11-15 10:00:00'),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000002', 2, '2026-11-01 09:00:00', NULL, '2026-11-01 10:00:00'),
('b0000000-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000002', 3, '2026-11-16 09:00:00', NULL, '2026-11-16 10:00:00');
INSERT INTO git_pull_requests (org_id, repo_id, number, created_at, merged_at, last_synced) VALUES
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000002', 2, '2026-11-01 09:00:00', '2026-11-18 09:00:00', '2026-11-18 10:00:00');
INSERT INTO git_pull_request_reviews (org_id, repo_id, number, review_id, reviewer, state, submitted_at, last_synced) VALUES
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000002', 1, 'review-1', 'dev1', 'APPROVED', '2026-11-17 14:00:00', '2026-11-17 15:00:00'),
('b0000000-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000002', 3, 'review-2', 'other', 'APPROVED', '2026-11-16 14:00:00', '2026-11-16 15:00:00');
INSERT INTO repo_metrics_daily (org_id, repo_id, day, commits_count, total_loc_touched, computed_at, retracted) VALUES
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', '2026-11-10', 1, 70, '2026-11-11 01:00:00', 0),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', '2026-11-11', 2, 999, '2026-11-11 20:00:00', 0),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', '2026-11-12', 1, 200, '2026-11-13 01:00:00', 0),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', '2026-11-13', 2, 4, '2026-11-14 01:00:00', 0),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000002', '2026-11-14', 1, 400, '2026-11-15 01:00:00', 0),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000002', '2026-11-15', 1, 7, '2026-11-16 01:00:00', 0),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000002', '2026-11-16', 0, 0, '2026-11-17 01:00:00', 0),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000002', '2026-11-17', 0, 0, '2026-11-18 01:00:00', 0),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000002', '2026-11-18', 0, 0, '2026-11-19 01:00:00', 0),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000003', '2026-11-19', 1, 3, '2026-11-19 23:00:00', 0),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000003', '2026-11-22', 1, 10, '2026-11-23 06:00:00', 0),
('b0000000-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', '2026-11-10', 1, 999, '2026-11-11 01:00:00', 0);
INSERT INTO repo_metrics_daily (org_id, repo_id, day, commits_count, total_loc_touched, computed_at, retracted) VALUES
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', '2026-11-11', 2, 5, '2026-11-12 01:00:00', 0),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000003', '2026-11-19', 0, 0, '2026-11-20 01:00:00', 1);
INSERT INTO commit_metrics (org_id, repo_id, commit_hash, day, author_email, total_loc, size_bucket, computed_at) VALUES
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', 'hash-01', '2026-11-10', 'dev1@example.com', 70, 'medium', '2026-11-11 01:00:00'),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', 'hash-01', '2026-11-10', 'dev1-old@example.com', 70, 'medium', '2026-11-11 00:30:00'),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', 'hash-02', '2026-11-11', 'dev2@example.com', 50, 'small', '2026-11-11 20:00:00'),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', 'hash-03', '2026-11-11', 'Dev Three', 0, 'small', '2026-11-12 01:00:00'),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', 'hash-04', '2026-11-12', 'dev1@example.com', 200, 'medium', '2026-11-13 01:00:00'),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', 'hash-05', '2026-11-13', 'dev2@example.com', 4, 'small', '2026-11-14 01:00:00'),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000002', 'hash-06', '2026-11-14', 'dev3@example.com', 400, 'large', '2026-11-15 01:00:00'),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000002', 'hash-07', '2026-11-15', 'dev3@example.com', 7, 'small', '2026-11-16 01:00:00'),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000003', 'hash-08', '2026-11-22', 'dev4@example.com', 10, 'small', '2026-11-23 06:00:00'),
('b0000000-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', 'hash-01', '2026-11-10', 'dev1@example.com', 999, 'large', '2026-11-11 01:00:00');
INSERT INTO commit_metrics (org_id, repo_id, commit_hash, day, author_email, total_loc, size_bucket, computed_at) VALUES
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', 'hash-02', '2026-11-11', 'dev2@example.com', 5, 'small', '2026-11-12 01:00:00');
INSERT INTO user_metrics_daily (org_id, repo_id, day, author_email, commits_count, loc_added, loc_deleted, computed_at, retracted) VALUES
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', '2026-11-10', 'dev1@example.com', 1, 60, 10, '2026-11-11 01:00:00', 0),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', '2026-11-11', 'dev2@example.com', 1, 3, 2, '2026-11-12 01:00:00', 0),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', '2026-11-11', 'Dev Three', 1, 0, 0, '2026-11-12 01:00:00', 0),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', '2026-11-12', 'dev1@example.com', 1, 200, 0, '2026-11-13 01:00:00', 0),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', '2026-11-13', 'dev2@example.com', 2, 4, 0, '2026-11-14 01:00:00', 0),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000002', '2026-11-14', 'dev3@example.com', 1, 400, 0, '2026-11-15 01:00:00', 0),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000002', '2026-11-15', 'dev3@example.com', 1, 7, 0, '2026-11-16 01:00:00', 0),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000003', '2026-11-19', 'dev4@example.com', 1, 3, 0, '2026-11-19 23:00:00', 0),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000003', '2026-11-22', 'dev4@example.com', 1, 8, 2, '2026-11-23 06:00:00', 0),
('b0000000-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000001', '2026-11-10', 'dev1@example.com', 1, 999, 0, '2026-11-11 01:00:00', 0);
INSERT INTO user_metrics_daily (org_id, repo_id, day, author_email, commits_count, loc_added, loc_deleted, computed_at, retracted) VALUES
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000003', '2026-11-19', 'dev4@example.com', 0, 0, 0, '2026-11-20 01:00:00', 1);
`

// Mutation sub-run rows (acp READ-FORM-14285-4 P2-2, P2-1), repo 3, added after the main run and the gate fires:
// 11-22 hash-08 stored again at 07:00 (after the cut) with old-rule LOC 20 = 2 x 10: after_mismatch 1.
// 11-21 hash-10 stored at 06:30 (after the cut) with LOC 20, raw synced at 08:00: after_raw_changed 1,
// after_doubled_raw_changed 1. 11-20 hash-11 stored at 03:40 by an old pod (after the -4 cut 03:08:08, before 05:10:00)
// with LOC 20, raw synced at 04:10 (the acp P2-1 case: in no count under -4): raw_changed_after +1, doubled_raw_changed +1.
const sizing14285Mutation = `
INSERT INTO git_commits (org_id, repo_id, hash, author_name, author_email, committer_when, last_synced) VALUES
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000003', 'hash-10', 'Dev Four', 'dev4@example.com', '2026-11-21 10:00:00', '2026-11-23 08:00:00'),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000003', 'hash-11', 'Dev Four', 'dev4@example.com', '2026-11-20 10:00:00', '2026-11-23 04:10:00');
INSERT INTO git_commit_stats (org_id, repo_id, commit_hash, file_path, additions, deletions, last_synced) VALUES
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000003', 'hash-10', 'f1.go', 6, 4, '2026-11-23 08:00:00'),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000003', 'hash-11', 'f1.go', 5, 5, '2026-11-23 04:10:00');
INSERT INTO repo_metrics_daily (org_id, repo_id, day, commits_count, total_loc_touched, computed_at, retracted) VALUES
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000003', '2026-11-22', 1, 20, '2026-11-23 07:00:00', 0),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000003', '2026-11-21', 1, 20, '2026-11-23 06:30:00', 0),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000003', '2026-11-20', 1, 20, '2026-11-23 03:40:00', 0);
INSERT INTO commit_metrics (org_id, repo_id, commit_hash, day, author_email, total_loc, size_bucket, computed_at) VALUES
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000003', 'hash-08', '2026-11-22', 'dev4@example.com', 20, 'small', '2026-11-23 07:00:00'),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000003', 'hash-10', '2026-11-21', 'dev4@example.com', 20, 'small', '2026-11-23 06:30:00'),
('c6a38355-0000-4000-8000-000000000000', '00000000-0000-4000-8000-000000000003', 'hash-11', '2026-11-20', 'dev4@example.com', 20, 'small', '2026-11-23 03:40:00');
`

func TestSizing14285ReadFormExecutesAndEveryGateFires(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	sum := sha256.Sum256([]byte(sizing14285Form))
	if got := hex.EncodeToString(sum[:]); got != sizing14285FormSHA256 {
		t.Fatalf("SIZING-14285-5 form sha256 = %s, want %s", got, sizing14285FormSHA256)
	}
	// The cut: one literal, 05:10:00 UTC, in every cut test of the SQL; the -4 cut 03:08:08 only in comments.
	code14285 := chmigrate.StripLineComments(sizing14285Form)
	if n := strings.Count(code14285, "toDateTime('2026-11-23 05:10:00', 'UTC')"); n != 35 {
		t.Fatalf("SIZING-14285-5: %d cut literals in the SQL, want 35", n)
	}
	if n := strings.Count(code14285, "toDateTime("); n != 35 || strings.Contains(code14285, "03:08:08") {
		t.Fatalf("SIZING-14285-5: %d toDateTime in the SQL (want 35) or the -4 cut is still in the SQL", n)
	}
	statements := splitForm(sizing14285Form)
	labels := make([]string, 0, len(statements))
	for _, s := range statements {
		labels = append(labels, s.label)
	}
	if strings.Join(labels, ",") != "S0,stmt0,stmt1,stmt2,stmt3,stmt4,stmt5,stmt6,stmt7" {
		t.Fatalf("SIZING-14285-5 statements = %v", labels)
	}
	// Table scans per statement (table -> k = times read). The gate list = the keys, sorted; G = sum of k x table rows.
	all := map[string]uint64{"commit_metrics": 1, "git_commit_stats": 1, "git_commits": 1, "git_pull_request_reviews": 1,
		"git_pull_requests": 1, "repo_metrics_daily": 1, "user_metrics_daily": 1}
	reads := map[string]map[string]uint64{
		"S0":    all,
		"stmt0": all,
		"stmt1": {"repo_metrics_daily": 1},
		"stmt2": {"git_commit_stats": 1, "git_commits": 1, "repo_metrics_daily": 1},
		"stmt3": {"commit_metrics": 1, "git_commit_stats": 1, "git_commits": 1},
		"stmt4": {"git_commit_stats": 1, "git_commits": 1, "user_metrics_daily": 1},
		"stmt5": {"git_commits": 1, "git_pull_request_reviews": 1, "git_pull_requests": 2, "repo_metrics_daily": 1},
		"stmt6": {"git_commit_stats": 2, "git_commits": 2},
		"stmt7": {"git_commit_stats": 2, "git_commits": 1},
	}
	gateList := func(label string) string {
		names := make([]string, 0, len(reads[label]))
		for name := range reads[label] {
			names = append(names, name)
		}
		sort.Strings(names)
		return "table IN ('" + strings.Join(names, "', '") + "')"
	}
	const partsCap = "> 100000"
	const budgetRowsCap = "> 20000000"
	gateCaps := func(label string) []struct{ name, literal string } {
		if label == "S0" {
			return []struct{ name, literal string }{{"parts", partsCap}}
		}
		return []struct{ name, literal string }{{"rows", budgetRowsCap}, {"bytes", bytesCap}}
	}
	// The budget gate weighs each table's system.parts rows and bytes by k, so it sums every scan of the statement.
	weight := func(label string) string {
		names := make([]string, 0, len(reads[label]))
		for name, k := range reads[label] {
			if k > 1 {
				names = append(names, name)
			}
		}
		if len(names) == 0 {
			return ""
		}
		sort.Strings(names)
		arms := make([]string, 0, len(names))
		for _, name := range names {
			arms = append(arms, fmt.Sprintf("table = '%s', %d", name, reads[label][name]))
		}
		return " * multiIf(" + strings.Join(arms, ", ") + ", 1)"
	}
	// s = scalar subqueries that read a table: each scope that uses cost_gate can run the gate (2 x P + 1 rows) once.
	scalarScopes := map[string]uint64{"stmt0": 7, "stmt6": 3}
	// Every statement holds exactly one gate over exactly the tables it reads.
	for _, s := range statements {
		wantRowsBytes, wantParts := 1, 0
		if s.label == "S0" {
			wantRowsBytes, wantParts = 0, 1
		}
		if n := strings.Count(s.text, budgetRowsCap); n != wantRowsBytes {
			t.Fatalf("%s: %d rows budget caps %q, want %d", s.label, n, budgetRowsCap, wantRowsBytes)
		}
		if n := strings.Count(s.text, rowsCap); n != 0 {
			t.Fatalf("%s: %d old rows caps %q, want 0", s.label, n, rowsCap)
		}
		if s.label != "S0" {
			for _, sum := range []string{"(SELECT sum(rows" + weight(s.label) + ") FROM system.parts", "(SELECT sum(bytes_on_disk" + weight(s.label) + ") FROM system.parts"} {
				if n := strings.Count(s.text, sum); n != 1 {
					t.Fatalf("%s: budget sum %q found %d times, want 1", s.label, sum, n)
				}
			}
			if n := uint64(strings.Count(chmigrate.StripLineComments(s.text), "AND cost_gate = 0) AS ")); n != scalarScopes[s.label] {
				t.Fatalf("%s: %d scalar subqueries use cost_gate, want %d", s.label, n, scalarScopes[s.label])
			}
		}
		if n := strings.Count(s.text, bytesCap); n != wantRowsBytes {
			t.Fatalf("%s: %d bytes caps %q, want %d", s.label, n, bytesCap, wantRowsBytes)
		}
		if n := strings.Count(s.text, partsCap); n != wantParts {
			t.Fatalf("%s: %d parts caps %q, want %d", s.label, n, partsCap, wantParts)
		}
		if n := strings.Count(s.text, gateList(s.label)); n != 2 {
			t.Fatalf("%s: gate table list %s found %d times, want 2", s.label, gateList(s.label), n)
		}
		if n := strings.Count(s.text, "table IN ("); n != 2 {
			t.Fatalf("%s: %d table lists, want 2", s.label, n)
		}
		if n := strings.Count(s.text, "'STOP-COST-GATE'"); n != 1 {
			t.Fatalf("%s: %d STOP-COST-GATE throwIf, want 1", s.label, n)
		}
	}

	scratch := scratchDatabase(t)
	conn := openDatabase(t, scratch.URI, "")
	db, _, err := chmigrate.NewConnDB(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}
	baseline, err := chmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	chain, err := chmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	result, err := chmigrate.Upgrade(ctx, db, baseline, chain)
	if err != nil {
		t.Fatalf("real chain: %v", err)
	}
	if result.Head < "170_" {
		t.Fatalf("schema head %q, want 170 or later", result.Head)
	}
	t.Logf("schema head %s", result.Head)
	var version string
	if err := conn.QueryRow(ctx, "SELECT version()").Scan(&version); err != nil {
		t.Fatal(err)
	}
	t.Logf("clickhouse version %s", version)

	for _, statement := range chmigrate.SplitStatements(sizing14285Fixture) {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("fixture: %v\n%s", err, statement)
		}
	}

	want := map[string]map[string]string{
		"stmt0": {"commits_orgs": "1", "stats_orgs": "1", "prs_orgs": "1", "reviews_orgs": "1", "repo_orgs": "1",
			"user_orgs": "1", "commit_orgs": "1"},
		"stmt1": {"live_repo_days": "10", "first_day": "2026-11-10", "last_day": "2026-11-22", "days": "10",
			"before_roll_repo_days": "9", "after_roll_repo_days": "1"},
		"stmt2": {"live_repo_days": "10", "with_commits": "6", "no_commit_now": "1", "raw_changed_after": "1",
			"doubled_raw_changed": "1", "keys_checked": "4", "match_keys": "1", "doubled_repo_days": "2", "affected_days": "2",
			"doubled_first_day": "2026-11-10", "doubled_last_day": "2026-11-14", "ratio2_repo_days": "2", "loc_excess": "235",
			"under_repo_days": "1", "count_mismatch": "1", "after_checked": "1", "after_mismatch": "0",
			"after_raw_changed": "0", "after_doubled_raw_changed": "0"},
		"stmt3": {"stored_commits": "9", "commit_rhd_keys": "8", "with_raw": "8", "commit_no_raw": "1", "raw_changed_after": "1",
			"doubled_raw_changed": "1", "commits_checked": "6", "match_commits": "2", "doubled_commits": "3",
			"doubled_commit_days": "2", "doubled_commit_repo_days": "2", "doubled_commit_first_day": "2026-11-10",
			"doubled_commit_last_day": "2026-11-14", "ratio2_commits": "3", "under_commits": "1", "bucket_move": "3",
			"large_flag_move": "1", "after_checked": "1", "after_mismatch": "0", "after_raw_changed": "0",
			"after_doubled_raw_changed": "0"},
		"stmt4": {"live_user_keys": "8", "with_commits": "7", "raw_changed_after": "1", "user_checked": "6", "user_match": "3",
			"user_over": "2", "user_over_days": "2", "user_ratio2": "2", "user_under": "1", "user_count_mismatch": "1"},
		"stmt5": {"live_repo_days": "10", "with_raw": "9", "no_raw": "1", "no_raw_first_day": "2026-11-16",
			"no_raw_last_day": "2026-11-16"},
		"stmt6": {"commit_rows_raw": "9", "commit_rows_final": "7", "stat_rows_raw": "10", "stat_rows_final": "8",
			"commit_versions_unmerged": "2", "stat_versions_unmerged": "2", "raw_first_day": "2026-11-10",
			"raw_last_day": "2026-11-22"},
		"stmt7": {"commits_now": "7", "oldrule_same_commits": "3", "oldrule_over_commits": "4", "oldrule_ratio2_commits": "4",
			"oldrule_under_commits": "0"},
	}

	tableRows := map[string]uint64{}
	tableParts := map[string]uint64{}
	passComments := map[string]string{}
	for _, statement := range statements {
		var readRows uint64
		passComments[statement.label] = fmt.Sprintf("sizing14285-pass-%s-%d", statement.label, time.Now().UnixNano())
		qctx := clickhouse.Context(ctx,
			clickhouse.WithSettings(clickhouse.Settings{"log_comment": passComments[statement.label]}),
			clickhouse.WithProgress(func(p *clickhouse.Progress) { readRows += p.Rows }))
		rows, err := conn.Query(qctx, statement.text)
		if err != nil {
			t.Fatalf("%s: SQL error: %v", statement.label, err)
		}
		columns := rows.Columns()
		types := rows.ColumnTypes()
		var got []map[string]string
		for rows.Next() {
			cells := make([]any, len(types))
			for i, ct := range types {
				cells[i] = reflect.New(ct.ScanType()).Interface()
			}
			if err := rows.Scan(cells...); err != nil {
				t.Fatalf("%s: scan: %v", statement.label, err)
			}
			row := map[string]string{}
			parts := make([]string, 0, len(columns))
			for i, name := range columns {
				row[name] = renderCell(cells[i])
				parts = append(parts, name+"="+row[name])
			}
			t.Logf("%s ROW %s", statement.label, strings.Join(parts, " "))
			got = append(got, row)
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("%s: SQL error: %v", statement.label, err)
		}
		_ = rows.Close()

		if statement.label == "S0" {
			if len(got) != len(all) {
				t.Fatalf("S0: %d rows, want %d (one per table read)", len(got), len(all))
			}
			for _, row := range got {
				if _, ok := all[row["table"]]; !ok {
					t.Fatalf("S0: unexpected table %q", row["table"])
				}
				var n, b, p uint64
				for name, dst := range map[string]*uint64{"table_rows": &n, "disk_bytes": &b, "active_parts": &p} {
					if _, err := fmt.Sscan(row[name], dst); err != nil || *dst == 0 {
						t.Fatalf("S0: table %s %s %q, want > 0", row["table"], name, row[name])
					}
				}
				tableRows[row["table"]] = n
				tableParts[row["table"]] = p
			}
			t.Logf("S0 read_rows=%d", readRows)
			continue
		}

		t.Logf("%s client read_rows=%d", statement.label, readRows)
		if len(got) != 1 {
			t.Errorf("%s: %d rows, want 1", statement.label, len(got))
			continue
		}
		for name, value := range want[statement.label] {
			if got[0][name] != value {
				t.Errorf("%s: %s = %s, want %s", statement.label, name, got[0][name], value)
			}
		}
		for _, name := range columns {
			if _, ok := want[statement.label][name]; !ok {
				t.Errorf("%s: column %s has no WANT in the cell", statement.label, name)
			}
		}
	}

	// PASS path (caps at the real values): the scan count. WANT G + 2 x P + 1 <= query_log read_rows <= G + (1 + s) x (2 x P + 1),
	// G = sum of k x table rows (every scan of the statement), P = parts of the gate tables, s = scalar gate scopes.
	if err := conn.Exec(ctx, "SYSTEM FLUSH LOGS"); err != nil {
		t.Fatalf("the query log is not readable by this user: %v", err)
	}
	for _, statement := range statements[1:] {
		var g, p uint64
		for table, k := range reads[statement.label] {
			g += k * tableRows[table]
			p += tableParts[table]
		}
		s := scalarScopes[statement.label]
		var types []string
		var logRead uint64
		rows, err := conn.Query(ctx, `SELECT toString(type), read_rows FROM system.query_log
			WHERE log_comment = ? AND type != 'QueryStart' ORDER BY event_time_microseconds`, passComments[statement.label])
		if err != nil {
			t.Fatalf("PASS %s: query log: %v", statement.label, err)
		}
		for rows.Next() {
			var typ string
			var r uint64
			if err := rows.Scan(&typ, &r); err != nil {
				t.Fatalf("PASS %s: query log scan: %v", statement.label, err)
			}
			types = append(types, typ)
			logRead += r
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("PASS %s: query log: %v", statement.label, err)
		}
		_ = rows.Close()
		low, high := g+2*p+1, g+(1+s)*(2*p+1)
		t.Logf("PASS %s query_log types=%v read_rows=%d G=%d P=%d s=%d band=[%d, %d]", statement.label, types, logRead, g, p, s, low, high)
		if len(types) != 1 || types[0] != "QueryFinish" {
			t.Errorf("PASS %s: query_log types %v, want [QueryFinish]", statement.label, types)
		}
		if logRead < low || logRead > high {
			t.Errorf("PASS %s: query_log read_rows %d outside [G + 2 x P + 1, G + (1 + s) x (2 x P + 1)] = [%d, %d]", statement.label, logRead, low, high)
		}
	}

	// Gate fires: ONE cap literal lowered to 0 per run, rest byte for byte. WANT STOP-COST-GATE (code 395), query_log
	// [ExceptionBeforeStart] and query_log read_rows <= 2 x P + 1 (system.parts twice + system.one: no data-table row).
	type fired struct {
		label, cap, comment string
		p                   uint64
	}
	var fires []fired
	for _, statement := range statements {
		var p uint64
		for table := range reads[statement.label] {
			p += tableParts[table]
		}
		for _, c := range gateCaps(statement.label) {
			lowered := strings.Replace(statement.text, c.literal, "> 0", 1)
			comment := fmt.Sprintf("sizing14285-gate-%s-%s-%d", statement.label, c.name, time.Now().UnixNano())
			var readRows uint64
			qctx := clickhouse.Context(ctx,
				clickhouse.WithSettings(clickhouse.Settings{"log_comment": comment}),
				clickhouse.WithProgress(func(pr *clickhouse.Progress) { readRows += pr.Rows }))
			rows, err := conn.Query(qctx, lowered)
			if err == nil {
				for rows.Next() {
				}
				err = rows.Err()
				_ = rows.Close()
			}
			if err == nil || !strings.Contains(err.Error(), "STOP-COST-GATE") || !strings.Contains(err.Error(), "395") {
				t.Errorf("GATE %s %s cap lowered: err = %v, want code 395 STOP-COST-GATE", statement.label, c.name, err)
				continue
			}
			t.Logf("GATE %s %s cap lowered: fired, client read_rows=%d, error: %s", statement.label, c.name, readRows, strings.SplitN(err.Error(), "\n", 2)[0])
			fires = append(fires, fired{statement.label, c.name, comment, p})
		}
	}
	if err := conn.Exec(ctx, "SYSTEM FLUSH LOGS"); err != nil {
		t.Fatalf("the query log is not readable by this user: %v", err)
	}
	for _, f := range fires {
		var types []string
		var logRead uint64
		rows, err := conn.Query(ctx, `SELECT toString(type), read_rows FROM system.query_log
			WHERE log_comment = ? AND type != 'QueryStart' ORDER BY event_time_microseconds`, f.comment)
		if err != nil {
			t.Fatalf("GATE %s %s: query log: %v", f.label, f.cap, err)
		}
		for rows.Next() {
			var typ string
			var r uint64
			if err := rows.Scan(&typ, &r); err != nil {
				t.Fatalf("GATE %s %s: query log scan: %v", f.label, f.cap, err)
			}
			types = append(types, typ)
			logRead += r
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("GATE %s %s: query log: %v", f.label, f.cap, err)
		}
		_ = rows.Close()
		t.Logf("GATE %s %s query_log types=%v read_rows=%d bound=%d", f.label, f.cap, types, logRead, 2*f.p+1)
		if len(types) != 1 || types[0] != "ExceptionBeforeStart" {
			t.Errorf("GATE %s %s: query_log types %v, want [ExceptionBeforeStart]", f.label, f.cap, types)
		}
		if logRead > 2*f.p+1 {
			t.Errorf("GATE %s %s: query_log read_rows %d over 2 x P + 1 = %d", f.label, f.cap, logRead, 2*f.p+1)
		}
	}
	if len(fires) != 17 {
		t.Errorf("gate fired %d times, want 17 (S0 x 1 cap + 8 statements x 2 caps)", len(fires))
	}

	// Mutation sub-run (after the pass path and the fires, so their counts hold): the after-cut controls fire.
	for _, statement := range chmigrate.SplitStatements(sizing14285Mutation) {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("mutation fixture: %v\n%s", err, statement)
		}
	}
	mutationWant := map[string]map[string]string{
		"stmt2": {"live_repo_days": "12", "with_commits": "8", "no_commit_now": "1", "raw_changed_after": "2",
			"doubled_raw_changed": "2", "keys_checked": "4", "match_keys": "1", "doubled_repo_days": "2", "affected_days": "2",
			"doubled_first_day": "2026-11-10", "doubled_last_day": "2026-11-14", "ratio2_repo_days": "2", "loc_excess": "235",
			"under_repo_days": "1", "count_mismatch": "1", "after_checked": "1", "after_mismatch": "1",
			"after_raw_changed": "1", "after_doubled_raw_changed": "1"},
		"stmt3": {"stored_commits": "11", "commit_rhd_keys": "10", "with_raw": "10", "commit_no_raw": "1",
			"raw_changed_after": "2", "doubled_raw_changed": "2", "commits_checked": "6", "match_commits": "2",
			"doubled_commits": "3", "doubled_commit_days": "2", "doubled_commit_repo_days": "2",
			"doubled_commit_first_day": "2026-11-10", "doubled_commit_last_day": "2026-11-14", "ratio2_commits": "3",
			"under_commits": "1", "bucket_move": "3", "large_flag_move": "1", "after_checked": "1", "after_mismatch": "1",
			"after_raw_changed": "1", "after_doubled_raw_changed": "1"},
	}
	mutated := 0
	for _, statement := range statements {
		want, ok := mutationWant[statement.label]
		if !ok {
			continue
		}
		mutated++
		rows, err := conn.Query(ctx, statement.text)
		if err != nil {
			t.Fatalf("MUTATION %s: SQL error: %v", statement.label, err)
		}
		columns := rows.Columns()
		types := rows.ColumnTypes()
		var got []map[string]string
		for rows.Next() {
			cells := make([]any, len(types))
			for i, ct := range types {
				cells[i] = reflect.New(ct.ScanType()).Interface()
			}
			if err := rows.Scan(cells...); err != nil {
				t.Fatalf("MUTATION %s: scan: %v", statement.label, err)
			}
			row := map[string]string{}
			parts := make([]string, 0, len(columns))
			for i, name := range columns {
				row[name] = renderCell(cells[i])
				parts = append(parts, name+"="+row[name])
			}
			t.Logf("MUTATION %s ROW %s", statement.label, strings.Join(parts, " "))
			got = append(got, row)
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("MUTATION %s: SQL error: %v", statement.label, err)
		}
		_ = rows.Close()
		if len(got) != 1 {
			t.Errorf("MUTATION %s: %d rows, want 1", statement.label, len(got))
			continue
		}
		for name, value := range want {
			if got[0][name] != value {
				t.Errorf("MUTATION %s: %s = %s, want %s", statement.label, name, got[0][name], value)
			}
		}
		for _, name := range columns {
			if _, ok := want[name]; !ok {
				t.Errorf("MUTATION %s: column %s has no WANT in the cell", statement.label, name)
			}
		}
	}
	if mutated != 2 {
		t.Errorf("mutation sub-run ran %d statements, want 2 (stmt2, stmt3)", mutated)
	}
}
