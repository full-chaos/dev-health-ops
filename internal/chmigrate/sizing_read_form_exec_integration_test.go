//go:build integration

package chmigrate_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"reflect"
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

// Second throwaway exec proof: the read-back form of the recompute card. readbackForm is READBACK-13902-3.sql byte for byte.
const readbackFormSHA256 = "b8dacbfe036b8586a39de567785bbe987a2c19d0c44576956e6fd65f62820b29"

const readbackForm = `-- READBACK-13902-3 = READBACK-13902-2 (49b02f5013a6, acp FORM copy) in FORM-TEMPLATE-1 (pm-guard, e220dd03e6fc) shape (a)
-- ClickHouse, lane l13902-card2, 2026-11-23. Template example: SIZING-13902-4 706d1c5d4de6.
-- Prod: NOT EXECUTED. Executed on CI (synthetic rows, before and after a simulated recompute, CH 26.7; prod CH 26.6 = same
-- major, SIZING-13902-4.out): EXEC-PROOF-13902-READBACK-3.md. This revision supersedes sha12 e6c31770951b (S0 had no gate).
-- Card: CARD-13902-recompute-2.md section 6. Run AFTER the pass 1 recompute (and once BEFORE it, as the can-fail proof).
-- Edits vs -2:
--   K1 Engine cost gate in EVERY statement: stmt0..stmt2 WITH (SELECT throwIf(...)) AS cost_gate over exactly the tables
--      the statement reads, caps 30M rows, 4 GiB on disk; S0 gated on system.one (system.parts part count of its 3 tables
--      over 100000 = STOP). Error text STOP-COST-GATE in each.
--   Status literals = the writer literals: 'deleted' testops/status_export.go:15, 'running' / 'queued' :21-22,
--      NotOpenRunSQL :36 (at ops 340c3e05).
--   K2 No placeholder: org prefix c6a38355, day range 2026-04-17..2026-11-22 (= the run range of the card), and the old-image
--      cut '2026-11-23 03:08:08' UTC are literals. The cut = STEP 323 old-pod drain 0 (RECORD-323 8ff7c60b985f G3 line):
--      a row computed at or after it is from a new-image pod (fix 9d88395188e6 in it); a row before it may be old-rule.
--   K3 LEFT JOIN (was INNER): live keys with no raw run today are counted (no_raw_live), not dropped. ifNull on every raw
--      column, so the result does not depend on join_use_nulls.
--   K4 New lines: stale_rerun (lead WANT: re-run keys computed before the cut = 0), stale_live (every live key in range),
--      rewritten, retracted_after_cut, rerun_keys, to_null_checked. Aliases on every derived table.
-- Run rule: each statement once, in order. A SQL error (STOP-COST-GATE included) = STOP. A timeout (30 s) = STOP, no re-run.
-- read_rows WANT after each of stmt0..stmt2: read_rows <= 1.1 x G + 2 x P + 2 (G, P = S0 rows, parts of the gate tables).
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
      AND table IN ('ci_pipeline_runs', 'cicd_metrics_daily', 'testops_pipeline_metrics_daily')) > 30000000
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
      AND table IN ('ci_pipeline_runs', 'cicd_metrics_daily')) > 30000000
    OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = currentDatabase()
      AND table IN ('ci_pipeline_runs', 'cicd_metrics_daily')) > 4294967296,
    'STOP-COST-GATE')) AS cost_gate
SELECT countIf(rr > 0) AS rerun_keys, countIf(rr > 0 AND NOT chg) AS keys_checked,
  countIf(rr > 0 AND c < toDateTime('2026-11-23 03:08:08', 'UTC')) AS stale_rerun,
  countIf(r = 0 AND c < toDateTime('2026-11-23 03:08:08', 'UTC')) AS stale_live,
  countIf(c >= toDateTime('2026-11-23 03:08:08', 'UTC')) AS rewritten,
  countIf(rr > 0 AND r = 1) AS retracted_rerun,
  countIf(r = 1 AND c >= toDateTime('2026-11-23 03:08:08', 'UTC')) AS retracted_after_cut,
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
      AND table IN ('ci_pipeline_runs', 'testops_pipeline_metrics_daily')) > 30000000
    OR (SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = currentDatabase()
      AND table IN ('ci_pipeline_runs', 'testops_pipeline_metrics_daily')) > 4294967296,
    'STOP-COST-GATE')) AS cost_gate
SELECT countIf(rr > 0) AS rerun_keys, countIf(rr > 0 AND NOT chg) AS keys_checked,
  countIf(rr > 0 AND c < toDateTime('2026-11-23 03:08:08', 'UTC')) AS stale_rerun,
  countIf(r = 0 AND c < toDateTime('2026-11-23 03:08:08', 'UTC')) AS stale_live,
  countIf(c >= toDateTime('2026-11-23 03:08:08', 'UTC')) AS rewritten,
  countIf(rr > 0 AND r = 1) AS retracted_rerun,
  countIf(r = 1 AND c >= toDateTime('2026-11-23 03:08:08', 'UTC')) AS retracted_after_cut,
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
// with no raw, 11-22 re-run-only day computed in the rollout window before the cut (old value), 11-21 re-run key a new-image
// pod wrote after the cut (new value). 04-16 = a re-run key outside the range. Merges are stopped.
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

func TestReadbackReadFormFailsBeforeAndPassesAfterARecompute(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	sum := sha256.Sum256([]byte(readbackForm))
	if got := hex.EncodeToString(sum[:]); got != readbackFormSHA256 {
		t.Fatalf("readback form sha256 = %s, want %s", got, readbackFormSHA256)
	}
	statements := splitForm(readbackForm)
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
	// Every statement holds exactly one gate: S0 one part-count cap on system.one, the others one rows + one bytes cap.
	const partsCap = "> 100000"
	gateCaps := func(label string) []struct{ name, literal string } {
		if label == "S0" {
			return []struct{ name, literal string }{{"parts", partsCap}}
		}
		return []struct{ name, literal string }{{"rows", rowsCap}, {"bytes", bytesCap}}
	}
	for _, s := range statements {
		wantRowsBytes, wantParts := 1, 0
		if s.label == "S0" {
			wantRowsBytes, wantParts = 0, 1
		}
		if n := strings.Count(s.text, rowsCap); n != wantRowsBytes {
			t.Fatalf("%s: %d rows caps %q, want %d", s.label, n, rowsCap, wantRowsBytes)
		}
		if n := strings.Count(s.text, bytesCap); n != wantRowsBytes {
			t.Fatalf("%s: %d bytes caps %q, want %d", s.label, n, bytesCap, wantRowsBytes)
		}
		if n := strings.Count(s.text, partsCap); n != wantParts {
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
	run := func(phase string, want map[string]map[string]string) (map[string]uint64, map[string]uint64) {
		tableRows := map[string]uint64{}
		tableParts := map[string]uint64{}
		for _, statement := range statements {
			var readRows uint64
			qctx := clickhouse.Context(ctx, clickhouse.WithProgress(func(p *clickhouse.Progress) { readRows += p.Rows }))
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
		return tableRows, tableParts
	}

	orgWant := map[string]string{"raw_orgs": "1", "cicd_orgs": "1", "testops_orgs": "1"}

	// BEFORE: the stored rows hold the old rule. The form must fail: stale_rerun > 0, mismatch > 0.
	exec(readbackFixtureBefore)
	versions("before", 9, 9, 0)
	before := map[string]string{
		"rerun_keys": "5", "keys_checked": "4", "stale_rerun": "4", "stale_live": "7", "rewritten": "1",
		"retracted_rerun": "0", "retracted_after_cut": "0", "no_raw_live": "1", "raw_changed_after": "1",
		"mismatch": "3", "to_null_checked": "2", "pooled_mismatch": "2", "pooled_unchecked": "1",
		"ctrl_checked": "2", "ctrl_mismatch": "0",
	}
	run("before", map[string]map[string]string{"stmt0": orgWant, "stmt1": before, "stmt2": before})

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
	_, parts := run("after", map[string]map[string]string{"stmt0": orgWant, "stmt1": after, "stmt2": after})

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
}
