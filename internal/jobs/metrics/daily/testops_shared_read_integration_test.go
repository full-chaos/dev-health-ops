//go:build integration

package daily

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// testops_test and testops_risk run in the same pre_bridge pass for the same
// (org, repo, day), and both need the same test metric record, which is the
// only thing either of them derives from test_case_results. That table has no
// time column in its sorting key, so every read of it scans the repo's whole
// case history. This test drives the pass through computeNativeFamilies, the
// loop production runs, and pins two things:
//
//   - how many queries touch test_case_results and how many rows they read,
//     taken from system.query_log, so a regression to one read per family (or
//     one per sub-question) fails on a number rather than on a timing;
//   - every byte both families write, against a golden captured from the
//     implementation where each family read on its own, so sharing the read
//     cannot move an output.
//
// On top of the differential fixture's duplicates and version ties, the seed
// adds rows that only a wrong shared read would get wrong:
//
//   - a second repo in the partition whose only case rows are historical
//     failures: it must still produce no rows at all, so a combined read may
//     not count a history-only name as seen today;
//   - a late superseding copy of one of today's case rows, inserted after
//     everything else, which changes the failure recurrence denominator;
//   - the same repo_id under another org and a sibling repo in the same org,
//     each carrying many case rows, so a case-side read that is not scoped to
//     (org, repo) shows up in read_rows, and in the golden if it leaks;
//   - a team whose repo pattern matches the partition repo, so the team id is
//     written through each family's own resolver.
func TestTestopsTestAndRiskReadCaseResultsOnceAgainstRealClickHouse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	clickhouseInstance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer clickhouseInstance.Close(context.Background())
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(clickhouseInstance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	for _, statement := range append(testopsDifferentialSchema(), testopsRiskOutputSchema()...) {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	// Merges stay stopped so FINAL has real duplicates to resolve and the
	// part layout, and with it read_rows, is the same on every run.
	for _, table := range []string{"ci_pipeline_runs", "test_suite_results", "test_case_results", "coverage_snapshots"} {
		if err := conn.Exec(ctx, "SYSTEM STOP MERGES "+table); err != nil {
			t.Fatalf("stop merges %s: %v", table, err)
		}
		defer func(table string) {
			resumeCtx, cancelResume := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancelResume()
			if err := conn.Exec(resumeCtx, "SYSTEM START MERGES "+table); err != nil {
				t.Errorf("resume merges %s: %v", table, err)
			}
		}(table)
	}

	const orgID = "00000000-0000-4000-8000-000000000009"
	const otherOrgID = "00000000-0000-4000-8000-00000000000f"
	repoID := uuid.MustParse("00000000-0000-4000-8000-0000000000a1")
	historyOnlyRepoID := uuid.MustParse("00000000-0000-4000-8000-0000000000b2")
	siblingRepoID := uuid.MustParse("00000000-0000-4000-8000-0000000000c3")
	day := time.Date(2026, 8, 27, 0, 0, 0, 0, time.UTC)

	seedTestopsDifferentialFixture(ctx, t, conn, orgID, repoID, day)
	seedTestopsSharedReadHostility(ctx, t, conn, testopsSharedReadSeed{
		orgID: orgID, otherOrgID: otherOrgID, repoID: repoID,
		historyOnlyRepoID: historyOnlyRepoID, siblingRepoID: siblingRepoID, day: day,
	})

	testExecutor, err := NewTestopsTestExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	riskExecutor, err := NewTestopsRiskExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	handler := &PartitionHandler{nativeFamiliesNow: func() time.Time { return time.Now().UTC() }}
	if err := handler.SetNativeFamilies(map[string]NativeFamilyExecutor{
		"testops_test": testExecutor,
		"testops_risk": riskExecutor,
	}); err != nil {
		t.Fatal(err)
	}
	run := Run{ID: "run-1", OrganizationID: orgID, TargetDay: day}
	partition := Partition{ID: "partition-1", RunID: "run-1", RepoIDs: []RepositoryID{
		RepositoryID(repoID.String()), RepositoryID(historyOnlyRepoID.String()),
	}}

	firstPass := measureCaseResultsReads(ctx, t, conn, func() {
		if err := handler.computeNativeFamilies(ctx, run, partition); err != nil {
			t.Fatalf("computeNativeFamilies: %v", err)
		}
	})
	for _, line := range firstPass.perQuery {
		t.Logf("test_case_results read: %s", line)
	}
	t.Logf("test_case_results: %d queries, %d rows read", firstPass.queries, firstPass.readRows)

	// One read per repo in the partition: testops_test and testops_risk need
	// the same per-case aggregate for the same (org, repo, day), and both of
	// its halves come out of a single scan.
	wantCaseReadQueries := uint64(len(partition.RepoIDs))
	if firstPass.queries != wantCaseReadQueries {
		t.Errorf("one partition pass ran %d queries against test_case_results for %d repos, want %d",
			firstPass.queries, len(partition.RepoIDs), wantCaseReadQueries)
	}
	// Exact, because merges are stopped and the part layout is fixed. Each
	// family reading on its own, with separate case-group and history
	// queries, read 136 rows here across 8 queries.
	const wantCaseReadRows = 64
	if firstPass.readRows != wantCaseReadRows {
		t.Errorf("one partition pass read %d rows through test_case_results queries, want %d",
			firstPass.readRows, wantCaseReadRows)
	}

	assertTestopsOutputsMatchGolden(ctx, t, conn)

	// A retried partition is a new pass and must see the table as it is then,
	// so nothing may carry a read from one pass into the next.
	secondPass := measureCaseResultsReads(ctx, t, conn, func() {
		if err := handler.computeNativeFamilies(ctx, run, partition); err != nil {
			t.Fatalf("second computeNativeFamilies: %v", err)
		}
	})
	if secondPass.queries != firstPass.queries || secondPass.readRows != firstPass.readRows {
		t.Errorf("second pass read test_case_results %d times (%d rows), first pass %d times (%d rows): "+
			"each pass must do its own read", secondPass.queries, secondPass.readRows, firstPass.queries, firstPass.readRows)
	}
}

type testopsSharedReadSeed struct {
	orgID, otherOrgID                        string
	repoID, historyOnlyRepoID, siblingRepoID uuid.UUID
	day                                      time.Time
}

func seedTestopsSharedReadHostility(ctx context.Context, t *testing.T, conn driver.Conn, seed testopsSharedReadSeed) {
	t.Helper()
	inDay := seed.day.Add(9 * time.Hour)
	historyDay := seed.day.AddDate(0, 0, -10).Add(9 * time.Hour)
	synced := seed.day.Add(23 * time.Hour)

	if err := conn.Exec(ctx, `INSERT INTO teams (id, name, members, repo_patterns, org_id) VALUES (?, ?, ?, ?, ?)`,
		"team-a", "Team A", []string{}, []string{seed.repoID.String()}, seed.orgID); err != nil {
		t.Fatalf("seed teams: %v", err)
	}

	insertSuite := func(orgID string, repoID uuid.UUID, runID, suiteID string, started time.Time) {
		t.Helper()
		if err := conn.Exec(ctx, `INSERT INTO test_suite_results
(repo_id, run_id, suite_id, suite_name, total_count, passed_count, failed_count, skipped_count,
 error_count, quarantined_count, duration_seconds, started_at, finished_at,
 team_id, service_id, org_id, last_synced)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			repoID, runID, suiteID, "suite", uint32(4), uint32(2), uint32(2), uint32(0),
			uint32(0), uint32(0), 30.0, started, started.Add(time.Minute),
			nil, nil, orgID, synced,
		); err != nil {
			t.Fatalf("seed test_suite_results %s/%s: %v", runID, suiteID, err)
		}
	}
	insertCases := func(orgID string, repoID uuid.UUID, runID, suiteID, namePrefix, status string, count int) {
		t.Helper()
		batch, err := conn.PrepareBatch(ctx, `INSERT INTO test_case_results
(repo_id, run_id, suite_id, case_id, case_name, status, retry_attempt, org_id, last_synced)`)
		if err != nil {
			t.Fatalf("prepare test_case_results batch: %v", err)
		}
		for index := 0; index < count; index++ {
			if err := batch.Append(repoID, runID, suiteID, fmt.Sprintf("%s-case-%d", namePrefix, index),
				fmt.Sprintf("%s-%d", namePrefix, index), status, uint32(0), orgID, synced); err != nil {
				t.Fatalf("append test_case_results row: %v", err)
			}
		}
		if err := batch.Send(); err != nil {
			t.Fatalf("send test_case_results batch: %v", err)
		}
	}

	// History-only repo inside the partition.
	insertSuite(seed.orgID, seed.historyOnlyRepoID, "run-b-hist", "suite-b-hist", historyDay)
	insertCases(seed.orgID, seed.historyOnlyRepoID, "run-b-hist", "suite-b-hist", "history-only", "failed", 3)

	// Same repo_id under another org, reusing the partition repo's run and
	// suite ids so only the org predicate separates them.
	insertSuite(seed.otherOrgID, seed.repoID, "run-1", "suite-1", inDay)
	insertSuite(seed.otherOrgID, seed.repoID, "run-hist", "suite-hist", historyDay)
	insertCases(seed.otherOrgID, seed.repoID, "run-1", "suite-1", "recurring", "failed", 400)
	insertCases(seed.otherOrgID, seed.repoID, "run-hist", "suite-hist", "clean", "failed", 400)

	// Sibling repo in the same org, outside the partition.
	insertSuite(seed.orgID, seed.siblingRepoID, "run-1", "suite-1", inDay)
	insertCases(seed.orgID, seed.siblingRepoID, "run-1", "suite-1", "sibling", "failed", 400)

	// Late superseding copy of today's "clean" case, landing after everything
	// else in its own part.
	if err := conn.Exec(ctx, `INSERT INTO test_case_results
(repo_id, run_id, suite_id, case_id, case_name, status, retry_attempt, org_id, last_synced)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		seed.repoID, "run-1", "suite-1", "c4", "clean", "failed", uint32(1), seed.orgID, synced.Add(time.Hour),
	); err != nil {
		t.Fatalf("seed late test_case_results row: %v", err)
	}
}

type caseResultsReadMeasurement struct {
	queries  uint64
	readRows uint64
	perQuery []string
}

// measureCaseResultsReads brackets action with query_log flushes and returns
// every finished query in that window whose table list includes
// test_case_results.
func measureCaseResultsReads(ctx context.Context, t *testing.T, conn driver.Conn, action func()) caseResultsReadMeasurement {
	t.Helper()
	if err := conn.Exec(ctx, "SYSTEM FLUSH LOGS"); err != nil {
		t.Fatalf("flush logs: %v", err)
	}
	var sinceMicros int64
	if err := conn.QueryRow(ctx, "SELECT toUnixTimestamp64Micro(now64(6))").Scan(&sinceMicros); err != nil {
		t.Fatalf("read server clock: %v", err)
	}
	action()
	if err := conn.Exec(ctx, "SYSTEM FLUSH LOGS"); err != nil {
		t.Fatalf("flush logs: %v", err)
	}
	rows, err := conn.Query(ctx, `
SELECT read_rows, replaceRegexpAll(substring(query, 1, 160), '\\s+', ' ')
FROM system.query_log
WHERE type = 'QueryFinish'
  AND toUnixTimestamp64Micro(event_time_microseconds) >= ?
  AND has(tables, concat(currentDatabase(), '.test_case_results'))
ORDER BY event_time_microseconds`, sinceMicros)
	if err != nil {
		t.Fatalf("read query_log: %v", err)
	}
	defer rows.Close()
	var measurement caseResultsReadMeasurement
	for rows.Next() {
		var readRows uint64
		var text string
		if err := rows.Scan(&readRows, &text); err != nil {
			t.Fatalf("scan query_log: %v", err)
		}
		measurement.queries++
		measurement.readRows += readRows
		measurement.perQuery = append(measurement.perQuery, fmt.Sprintf("%d rows: %s", readRows, text))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate query_log: %v", err)
	}
	if measurement.queries == 0 {
		t.Fatal("query_log recorded no test_case_results query for the pass: the measurement is vacuous")
	}
	return measurement
}

const testopsSharedReadGoldenPath = "testdata/testops_test_and_risk_outputs.golden"

// assertTestopsOutputsMatchGolden renders every row both families wrote,
// computed_at aside, as ClickHouse's own text form of the tuple, so a float
// that moves in its last digit changes the golden.
func assertTestopsOutputsMatchGolden(ctx context.Context, t *testing.T, conn driver.Conn) {
	t.Helper()
	var rendered strings.Builder
	for _, table := range []string{
		"testops_test_metrics_daily",
		"testops_release_confidence",
		"testops_quality_drag",
		"testops_pipeline_stability",
	} {
		rows, err := conn.Query(ctx, fmt.Sprintf(
			`SELECT toString(tuple(* EXCEPT (computed_at))) AS line FROM %s ORDER BY repo_id, day, line`, table))
		if err != nil {
			t.Fatalf("render %s: %v", table, err)
		}
		count := 0
		for rows.Next() {
			var line string
			if err := rows.Scan(&line); err != nil {
				rows.Close()
				t.Fatalf("scan %s: %v", table, err)
			}
			fmt.Fprintf(&rendered, "%s\t%s\n", table, line)
			count++
		}
		iterErr := rows.Err()
		rows.Close()
		if iterErr != nil {
			t.Fatalf("iterate %s: %v", table, iterErr)
		}
		if count != 1 {
			t.Fatalf("%s holds %d rows, want exactly 1: the partition repo writes one row and the "+
				"history-only repo writes none", table, count)
		}
	}
	got := rendered.String()
	if !strings.Contains(got, "team-a") {
		t.Fatalf("no written row carries the resolver's team id, so team resolution is not under test:\n%s", got)
	}

	want, err := os.ReadFile(testopsSharedReadGoldenPath)
	if errors.Is(err, os.ErrNotExist) {
		if err := os.MkdirAll(filepath.Dir(testopsSharedReadGoldenPath), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(testopsSharedReadGoldenPath, []byte(got), 0o644); err != nil {
			t.Fatal(err)
		}
		t.Fatalf("golden %s was missing and has been written; review it and re-run", testopsSharedReadGoldenPath)
	}
	if err != nil {
		t.Fatal(err)
	}
	if string(want) != got {
		t.Fatalf("testops_test/testops_risk outputs moved:\n--- golden\n%s\n--- got\n%s", want, got)
	}
}

// testopsRiskOutputSchema is the DDL for the three testops_risk tables, in the
// production column shapes the risk writer targets.
func testopsRiskOutputSchema() []string {
	return []string{
		`CREATE TABLE testops_release_confidence (
    repo_id UUID, day Date, confidence_score Float64, pipeline_success_factor Float64,
    test_pass_factor Float64, coverage_factor Float64, flake_penalty Float64, regression_penalty Float64,
    factors_json String DEFAULT '{}', team_id Nullable(String), service_id Nullable(String),
    org_id LowCardinality(String) DEFAULT '', computed_at DateTime('UTC')
) ENGINE MergeTree PARTITION BY toYYYYMM(day) ORDER BY (repo_id, day)`,
		`CREATE TABLE testops_quality_drag (
    repo_id UUID, day Date, drag_hours Nullable(Float64), failure_rework_hours Nullable(Float64),
    flake_investigation_hours Nullable(Float64), queue_wait_hours Nullable(Float64), retry_overhead_hours Nullable(Float64),
    factors_json String DEFAULT '{}', team_id Nullable(String), service_id Nullable(String),
    org_id LowCardinality(String) DEFAULT '', computed_at DateTime('UTC')
) ENGINE MergeTree PARTITION BY toYYYYMM(day) ORDER BY (repo_id, day)`,
		`CREATE TABLE testops_pipeline_stability (
    repo_id UUID, day Date, stability_index Float64, success_rate_7d Float64, success_rate_trend Float64,
    failure_clustering_score Float64, median_recovery_time_seconds Nullable(Float64),
    team_id Nullable(String), service_id Nullable(String), org_id LowCardinality(String) DEFAULT '',
    computed_at DateTime('UTC')
) ENGINE MergeTree PARTITION BY toYYYYMM(day) ORDER BY (repo_id, day)`,
	}
}
