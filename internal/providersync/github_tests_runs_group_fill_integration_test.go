//go:build integration

package providersync

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// runsGroupPostgres claims the fixture unit as a github/cicd unit over 2026-07-22 12:00..20:00 (no UTC midnight) on a
// migrated real Postgres and returns the repository (lease + chunk store), the session and the pool.
func runsGroupPostgres(t *testing.T, ctx context.Context) (*PostgresRepository, *LeaseSession, *pgxpool.Pool) {
	t.Helper()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeContext, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if closeErr := instance.Close(closeContext); closeErr != nil {
			t.Errorf("terminate PostgreSQL: %v", closeErr)
		}
	})
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	createProviderSyncFixture(t, ctx, pool)
	seedProviderSyncFixture(t, ctx, pool)
	if _, err := pool.Exec(ctx, `UPDATE public.sync_run_units SET dataset_key='cicd', processor_flags='{"sync_git":true}'::jsonb,
		since_at='2026-07-22T12:00:00Z', before_at='2026-07-22T20:00:00Z' WHERE id=$1`, firstUnitID); err != nil {
		t.Fatal(err)
	}
	repository, err := NewPostgresRepository(pool)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	claim, err := repository.Claim(ctx, ClaimRequest{
		UnitID: firstUnitID, OrgID: "org-acme", Owner: uuid.NewString(), Now: now,
		LeaseDuration: 10 * time.Minute, AllowExpiredRecovery: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	return repository, &LeaseSession{Repository: repository, Claim: claim, LeaseDuration: 10 * time.Minute, Deadline: now.Add(time.Hour)}, pool
}

func storedChunkCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool) int {
	t.Helper()
	var stored int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM public.sync_run_unit_effect_chunks WHERE sync_run_unit_id=$1::uuid`,
		firstUnitID).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	return stored
}

// TestGitHubTestsRunsPhaseFillsChunkGroups pins CHAOS-14408 on real Postgres: a runs page of 100 small runs (3 jobs
// each) commits in fewer than 10 chunk groups, each run read once and written once. The base commits one group per run.
func TestGitHubTestsRunsPhaseFillsChunkGroups(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	repository, session, pool := runsGroupPostgres(t, ctx)
	doer, sink := newRunsGroupDoer(t, 100, 3), newRunsGroupSink()
	stops := runUntilDone(t, runsGroupExecutor(time.Now().UTC(), GitHubTestsRouteHandler{}, repository, sink, doer, nil),
		session, cicdDescriptor(t))
	assertEachRunOnce(t, sink, doer, 100)
	for id, gets := range doer.jobGets {
		if gets != 1 {
			t.Fatalf("run %s jobs read %d times", id, gets)
		}
	}
	stored := storedChunkCount(t, ctx, pool)
	t.Logf("GROUPS: 100 runs x 3 jobs -> %d chunk groups with pipeline rows, %d stored chunks, %d attempts",
		sink.pipelineWrites, stored, len(stops))
	if sink.pipelineWrites >= 10 {
		t.Fatalf("a runs page of 100 small runs committed in %d chunk groups, want fewer than 10", sink.pipelineWrites)
	}
}

// A sink failure inside a coalesced group on real Postgres: the group was prepared whole before its first write, so the
// next attempt drains it from the store (no GitHub read again) and every row lands exactly once.
func TestGitHubTestsRunsPhaseCrashInsideACoalescedGroupDrainsOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	repository, session, pool := runsGroupPostgres(t, ctx)
	doer, sink := newRunsGroupDoer(t, 100, 3), newRunsGroupSink()
	sink.failJobWrite = 2
	executor := runsGroupExecutor(time.Now().UTC(), GitHubTestsRouteHandler{}, repository, sink, doer, nil)
	if _, err := executor.Execute(ctx, session, cicdDescriptor(t)); err == nil || errors.As(err, new(ChunkContinuationError)) {
		t.Fatalf("attempt 1 err=%v, want the sink failure", err)
	}
	checkpoint, err := repository.LoadChunkCheckpoint(ctx, session.Claim, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if checkpoint.PreparedChunks != 2 || checkpoint.NextOrdinal != 1 {
		t.Fatalf("after the crash prepared=%d next=%d, want 2 1 (the second group prepared whole, not committed)",
			checkpoint.PreparedChunks, checkpoint.NextOrdinal)
	}
	runUntilDone(t, executor, session, cicdDescriptor(t))
	assertEachRunOnce(t, sink, doer, 100)
	// Run 66 arrived, did not fit the second group and started its flush: it was never held or prepared, so it is the
	// one run read again. The 33 runs of the prepared group drain without GitHub.
	for index := 0; index < 100; index++ {
		want := 1
		if index == 66 {
			want = 2
		}
		if gets := doer.jobGets[runsGroupRunID(index)]; gets != want {
			t.Fatalf("run %d jobs read %d times, want %d", index, gets, want)
		}
	}
	t.Logf("CRASH: %d stored chunks after recovery", storedChunkCount(t, ctx, pool))
}
