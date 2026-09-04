//go:build integration

package daily

import (
	"context"
	"testing"
	"time"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// benchmarkingSchema is the production shape of the tables this family touches:
// `repos` (for the org anchor), one source metric table, and the two output
// tables the assertions read. The other four outputs share the same writer path
// and are covered by the compute-side oracle.
var benchmarkingSchema = []string{
	`CREATE TABLE repos (
    id UUID, org_id String, repo String, settings String, provider String,
    last_synced DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (org_id, id)`,
	`CREATE TABLE testops_pipeline_metrics_daily (
    repo_id UUID, day Date, team_id Nullable(String), service_id Nullable(String),
    success_rate Float64, failure_rate Float64, rerun_rate Float64,
    median_duration_seconds Float64, p95_duration_seconds Float64,
    avg_queue_seconds Float64, p95_queue_seconds Float64,
    computed_at DateTime('UTC'), org_id String
) ENGINE = MergeTree PARTITION BY toYYYYMM(day) ORDER BY (org_id, repo_id, day)`,
	`CREATE TABLE testops_metric_baselines (
    metric_name LowCardinality(String), scope_type LowCardinality(String), scope_key String,
    period_start Date, period_end Date, rolling_window_days UInt16,
    current_value Float64, baseline_value Float64, percentile_rank Float64,
    p25_value Float64, p50_value Float64, p75_value Float64, p90_value Float64,
    sample_size UInt32, org_id LowCardinality(String) DEFAULT '', computed_at DateTime('UTC')
) ENGINE MergeTree PARTITION BY toYYYYMM(period_end)
  ORDER BY (metric_name, scope_type, scope_key, period_end, rolling_window_days)`,
	`CREATE TABLE testops_maturity_bands (
    metric_name LowCardinality(String), scope_type LowCardinality(String), scope_key String,
    period_start Date, period_end Date, value Float64, percentile_rank Float64,
    maturity_band LowCardinality(String), confidence Float64,
    org_id LowCardinality(String) DEFAULT '', computed_at DateTime('UTC')
) ENGINE MergeTree PARTITION BY toYYYYMM(period_end)
  ORDER BY (metric_name, scope_type, scope_key, period_end)`,
}

// TestBenchmarkingComputesOncePerOrgNotOncePerPartition is the proof of this
// family's single deliberate divergence from Python, and it is the ONLY layer
// that can show it: the compute-side golden never sees partition fan-out at all.
//
// Python's run_benchmarking_for_day takes no repo_id but is called from every
// repo partition, so an org with N repos writes N identical row sets into six
// append-only tables each night. The native executor anchors on the org's
// lexicographically-first repository id and no-ops on every other partition.
//
// The org here has THREE repos. Running all three partitions must leave exactly
// ONE row set behind -- under Python's behaviour it would be three.
func TestBenchmarkingComputesOncePerOrgNotOncePerPartition(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	for _, statement := range benchmarkingSchema {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}

	const (
		org = "00000000-0000-4000-8000-0000000000a0"
		// repo1 sorts FIRST, so it is the anchor partition.
		repo1 = "00000000-0000-4000-8000-000000000001"
		repo2 = "00000000-0000-4000-8000-000000000002"
		repo3 = "00000000-0000-4000-8000-000000000003"
	)

	if err := conn.Exec(ctx, `
INSERT INTO repos (id, org_id, repo, settings, provider, last_synced) VALUES
(toUUID('`+repo1+`'), '`+org+`', 'a/one',   '{}', 'github', '2026-08-24 00:00:00.000'),
(toUUID('`+repo2+`'), '`+org+`', 'a/two',   '{}', 'github', '2026-08-24 00:00:00.000'),
(toUUID('`+repo3+`'), '`+org+`', 'a/three', '{}', 'github', '2026-08-24 00:00:00.000')
`); err != nil {
		t.Fatal(err)
	}

	// Enough history for the 30-day baseline window across all three repos.
	for dayOffset := 0; dayOffset < 12; dayOffset++ {
		day := time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC).AddDate(0, 0, -dayOffset)
		for index, repo := range []string{repo1, repo2, repo3} {
			value := 0.5 + float64(index)*0.1 + float64(dayOffset)*0.01
			if err := conn.Exec(ctx, `
INSERT INTO testops_pipeline_metrics_daily
(repo_id, day, team_id, service_id, success_rate, failure_rate, rerun_rate,
 median_duration_seconds, p95_duration_seconds, avg_queue_seconds, p95_queue_seconds,
 computed_at, org_id) VALUES
(toUUID(?), ?, NULL, NULL, ?, 0.1, 0.05, 100.0, 200.0, 10.0, 20.0, ?, ?)`,
				repo, day, value, day, org,
			); err != nil {
				t.Fatal(err)
			}
		}
	}

	executor, err := NewBenchmarkingExecutor(conn, nil)
	if err != nil {
		t.Fatal(err)
	}
	targetDay := time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)
	run := Run{ID: "run-a", OrganizationID: org, TargetDay: targetDay}

	// Run EVERY partition, exactly as the fan-out would.
	written := map[string]int{}
	for _, repo := range []string{repo1, repo2, repo3} {
		rows, err := executor.ComputeFamily(ctx, run, Partition{
			ID: "partition-" + repo, RunID: "run-a",
			RepoIDs: []RepositoryID{RepositoryID(repo)},
		})
		if err != nil {
			t.Fatalf("partition %s: %v", repo, err)
		}
		written[repo] = rows
	}

	if written[repo1] == 0 {
		t.Error("the anchor partition (lexicographically-first repo) wrote nothing")
	}
	if written[repo2] != 0 || written[repo3] != 0 {
		t.Errorf(
			"non-anchor partitions wrote rows (%d, %d) -- the once-per-org anchor is not "+
				"holding, and this family would multiply its output by the repo count",
			written[repo2], written[repo3],
		)
	}

	// The durable proof: exactly one row set in the table, not three.
	var baselineRows, distinctComputedAt uint64
	if err := conn.QueryRow(ctx,
		`SELECT count(), uniqExact(computed_at) FROM testops_metric_baselines WHERE org_id = ?`, org,
	).Scan(&baselineRows, &distinctComputedAt); err != nil {
		t.Fatal(err)
	}
	if baselineRows == 0 {
		t.Fatal("no baseline rows written at all")
	}
	if distinctComputedAt != 1 {
		t.Errorf(
			"testops_metric_baselines carries %d distinct computed_at values for one org/day -- "+
				"expected exactly 1, so more than one partition computed",
			distinctComputedAt,
		)
	}

	// Scope keys must be repo ids (scope_type 'repo'), which is what the
	// executed-proof readback's dead-id oracle cross-checks.
	var strayScopeKeys uint64
	if err := conn.QueryRow(ctx, `
SELECT count() FROM testops_metric_baselines
WHERE org_id = ? AND scope_type = 'repo'
  AND scope_key NOT IN (?, ?, ?)`, org, repo1, repo2, repo3,
	).Scan(&strayScopeKeys); err != nil {
		t.Fatal(err)
	}
	if strayScopeKeys != 0 {
		t.Errorf("%d baseline row(s) carry a scope_key that is not one of the org's repos", strayScopeKeys)
	}

	// Maturity bands are derived from the baselines in the same call, so a
	// mismatch here means the two collections came from different runs.
	var bandRows uint64
	if err := conn.QueryRow(ctx,
		`SELECT count() FROM testops_maturity_bands WHERE org_id = ?`, org,
	).Scan(&bandRows); err != nil {
		t.Fatal(err)
	}
	if bandRows != baselineRows {
		t.Errorf("maturity_bands has %d rows but baselines has %d -- they are emitted 1:1", bandRows, baselineRows)
	}
}
