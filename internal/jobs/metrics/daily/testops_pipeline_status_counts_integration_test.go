//go:build integration

package daily

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// The daily pipeline row on a real ClickHouse puts every run of the day in exactly one bucket, so pipelines_count is
// the sum of the six, and counts the unfinished runs whose row was last synced more than the open-run lookback before
// computed_at as stale. A deleted run is in no bucket.
func TestTestopsPipelineRowCountsEveryRunInOneBucketAndTheStaleUnfinishedRuns(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	inst, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer inst.Close(context.Background())
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(inst.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	chschema.Apply(ctx, t, inst)

	const org = "00000000-0000-4000-8000-0000000140a0"
	repo := uuid.MustParse("00000000-0000-4000-8000-0000000140a1")
	day := time.Date(2026, 11, 16, 0, 0, 0, 0, time.UTC)
	computedAt := time.Date(2026, 11, 18, 3, 0, 0, 0, time.UTC)
	cutoff := computedAt.Add(-6 * time.Hour)
	runs := []struct{ status, lastSynced string }{
		{"success", "2026-11-17 10:00:00"},
		{"failure", "2026-11-17 10:00:00"},
		{"timed_out", "2026-11-17 10:00:00"},
		{"cancelled", "2026-11-17 10:00:00"},
		{"skipped", "2026-11-17 10:00:00"},
		{"running", cutoff.Add(-time.Second).Format(time.DateTime)}, // stale
		{"queued", "2026-11-16 12:00:00"},                           // stale
		{"running", cutoff.Format(time.DateTime)},                   // at the cutoff: not stale
		{"queued", "2026-11-18 02:00:00"},                           // fresh
		{"stale", "2026-11-16 12:00:00"},
		{"completed", "2026-11-16 12:00:00"},
		{"canceling", "2026-11-16 12:00:00"},
		{"some_new_provider_state", "2026-11-16 12:00:00"},
		{"deleted", "2026-11-16 12:00:00"},
	}
	for index, run := range runs {
		if err := conn.Exec(ctx, fmt.Sprintf(`INSERT INTO ci_pipeline_runs (repo_id, run_id, status, started_at, last_synced, org_id)
VALUES ('%s', 'r%02d', '%s', toDateTime64('2026-11-16 09:00:00', 3, 'UTC'), toDateTime64('%s', 3, 'UTC'), '%s')`,
			repo, index, run.status, run.lastSynced, org)); err != nil {
			t.Fatal(err)
		}
	}
	if err := conn.Exec(ctx, fmt.Sprintf(`INSERT INTO ci_pipeline_runs (repo_id, run_id, status, started_at, last_synced, org_id)
VALUES ('%s', 'r-null', NULL, toDateTime64('2026-11-16 09:00:00', 3, 'UTC'), toDateTime64('2026-11-16 12:00:00', 3, 'UTC'), '%s')`,
		repo, org)); err != nil {
		t.Fatal(err)
	}

	pipeline, err := NewTestopsPipelineExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	pipeline.nowUTC = func() time.Time { return computedAt }
	run := Run{OrganizationID: org, TargetDay: day}
	partition := Partition{ID: "00000000-0000-4000-8000-0000000140c1", RunID: "00000000-0000-4000-8000-0000000140c0",
		RepoIDs: []RepositoryID{RepositoryID(repo.String())}}
	if _, err := pipeline.ComputeFamily(ctx, run, partition); err != nil {
		t.Fatalf("testops pipeline: %v", err)
	}

	var pipelines, success, failure, cancelled uint32
	var skipped, unfinished, other, stale *uint32
	if err := conn.QueryRow(ctx, `SELECT pipelines_count, success_count, failure_count, cancelled_count,
       skipped_count, unfinished_count, other_count, unfinished_stale_count
FROM testops_pipeline_metrics_daily WHERE org_id = ? AND repo_id = ?`, org, repo).Scan(
		&pipelines, &success, &failure, &cancelled, &skipped, &unfinished, &other, &stale); err != nil {
		t.Fatal(err)
	}
	if skipped == nil || unfinished == nil || other == nil || stale == nil {
		t.Fatalf("a new row reads NULL: skipped %v unfinished %v other %v stale %v", skipped, unfinished, other, stale)
	}
	got := [8]uint32{pipelines, success, failure, cancelled, *skipped, *unfinished, *other, *stale}
	// 14 counted runs (the deleted one is out): 1 success, 2 failure, 1 cancelled, 1 skipped, 4 unfinished,
	// 5 other (stale, completed, canceling, an unknown status, NULL); 2 unfinished are stale.
	if want := [8]uint32{14, 1, 2, 1, 1, 4, 5, 2}; got != want {
		t.Fatalf("pipelines, success, failure, cancelled, skipped, unfinished, other, stale = %v, want %v", got, want)
	}
	if sum := success + failure + cancelled + *skipped + *unfinished + *other; sum != pipelines {
		t.Fatalf("the six buckets sum to %d, pipelines_count %d", sum, pipelines)
	}
}
