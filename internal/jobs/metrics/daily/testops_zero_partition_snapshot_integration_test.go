//go:build integration

package daily

// A run of the whole organization with NO partition (its discovery found no repository: the finalize of such a run now
// reaches the run-level retraction with an empty list of repositories) and the snapshot marker of testops_test_metrics_daily.
// An empty list must never read as "the organization owns nothing": the keys of a repository the organization still holds are
// left alone; the keys of a repository it no longer holds retire. Real ClickHouse; the end of the run goes through
// RunStaleKeyRetractor, the code the finalize calls.

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

type zeroPartitionKey struct {
	rows      uint64
	retracted uint8
	newest    time.Time
}

func zeroPartitionState(t *testing.T, ctx context.Context, conn driver.Conn, org string, repo uuid.UUID, day time.Time) zeroPartitionKey {
	t.Helper()
	var got zeroPartitionKey
	if err := conn.QueryRow(ctx, `SELECT count(), argMax(retracted, computed_at), max(computed_at) FROM testops_test_metrics_daily
WHERE org_id = ? AND repo_id = ? AND day = ?`, org, repo, day).Scan(&got.rows, &got.retracted, &got.newest); err != nil {
		t.Fatal(err)
	}
	t.Logf("ZEROPARTITION STATE org=%s repo=%s day=%s %+v", org, repo, day.Format(time.DateOnly), got)
	return got
}

func TestAWholeOrganizationRunWithNoPartitionRetiresOnlyTheKeysOfRepositoriesTheOrganizationNoLongerHolds(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)
	day := sharedScopeDay
	nextDay := day.AddDate(0, 0, 1)
	stored := day.Add(30 * time.Hour)
	clock := stored.Add(time.Hour)
	exec := func(what, query string, args ...any) {
		t.Helper()
		if err := conn.Exec(ctx, query, args...); err != nil {
			t.Fatalf("%s: %v", what, err)
		}
	}
	exec("stop merges", "SYSTEM STOP MERGES testops_test_metrics_daily")
	repository := func(org string, id uuid.UUID, name string) {
		t.Helper()
		exec("insert repo", "INSERT INTO repos (id, repo, org_id, provider, last_synced) VALUES (?, ?, ?, ?, ?)", id, name, org, "github", stored)
	}
	live := func(org string, repo uuid.UUID, on time.Time) {
		t.Helper()
		exec("stored testops row", `INSERT INTO testops_test_metrics_daily (repo_id, day, total_cases, passed_count, failed_count, pass_rate,
  failure_rate, flake_rate, total_suites, flake_cases, distinct_cases, team_id, service_id, org_id, computed_at)
VALUES (?, ?, 4, 2, 2, 0.5, 0.5, 0.5, 1, 1, 2, 'team-1', 'svc-1', ?, ?)`, repo, on, org, stored)
	}
	run := func(org string) {
		t.Helper()
		got := Run{ID: uuid.NewString(), OrganizationID: org, TargetDay: day, FullOrg: true}
		if len(got.DiscoveredRepoIDs) != 0 {
			t.Fatal("the run must have no partition")
		}
		endStaleKeyRun(t, ctx, conn, got, clock)
	}

	// (a) + (b): the organization holds R1 (not in any partition: the run has none) and no longer holds R2.
	t.Run("a_held_repository_keeps_its_key_and_a_gone_repository_retires", func(t *testing.T) {
		const org = "00000000-0000-4000-8000-0000007f0001"
		held := uuid.MustParse("13928000-0000-4000-8000-0000000007a1")
		gone := uuid.MustParse("13928000-0000-4000-8000-0000000007a2")
		repository(org, held, "acme/held")
		live(org, held, day)
		live(org, gone, day)
		run(org)
		if got := zeroPartitionState(t, ctx, conn, org, held, day); got.rows != 1 || got.retracted != 0 {
			t.Errorf("(a) the repository the organization holds lost its key to a run with no partition: %+v, want its one live row untouched", got)
		}
		if got := zeroPartitionState(t, ctx, conn, org, gone, day); got.rows != 2 || got.retracted != 1 || !got.newest.Equal(clock) {
			t.Errorf("(b) the repository the organization no longer holds: %+v, want a marker at the run's clock %s", got, clock)
		}
	})

	// (c): the organization holds no repository at all: its stale key retires and nothing else is touched.
	t.Run("an_organization_with_no_repository_retires_its_stale_keys_and_nothing_else", func(t *testing.T) {
		const org = "00000000-0000-4000-8000-0000007f0002"
		const bystander = "00000000-0000-4000-8000-0000007f0003"
		gone := uuid.MustParse("13928000-0000-4000-8000-0000000007b1")
		other := uuid.MustParse("13928000-0000-4000-8000-0000000007b2")
		live(org, gone, day)
		live(org, gone, nextDay)
		live(bystander, other, day)
		repository(bystander, other, "acme/bystander")
		run(org)
		if got := zeroPartitionState(t, ctx, conn, org, gone, day); got.rows != 2 || got.retracted != 1 {
			t.Errorf("(c) the stale key of the organization with no repository: %+v, want a marker", got)
		}
		if got := zeroPartitionState(t, ctx, conn, org, gone, nextDay); got.rows != 1 || got.retracted != 0 {
			t.Errorf("(c) the same repository on another day: %+v, want its one live row untouched (the run is of one day)", got)
		}
		if got := zeroPartitionState(t, ctx, conn, bystander, other, day); got.rows != 1 || got.retracted != 0 {
			t.Errorf("(c) another organization's key: %+v, want its one live row untouched", got)
		}
	})
}
