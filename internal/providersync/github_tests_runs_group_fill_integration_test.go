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

// TestGitHubTestsRunsPhaseFillsChunkGroups pins CHAOS-14408 on real Postgres: a runs page of 100 small runs (12 jobs
// each) commits in 3 chunk groups (41 + 41 + 18 at the 500-row bound), each run read once and written once. The base commits one group per run.
func TestGitHubTestsRunsPhaseFillsChunkGroups(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	repository, session, pool := runsGroupPostgres(t, ctx)
	doer, sink := newRunsGroupDoer(t, 100, 12), newRunsGroupSink()
	stops := runUntilDone(t, runsGroupExecutor(time.Now().UTC(), GitHubTestsRouteHandler{CoalesceRuns: true}, repository, sink, doer, nil),
		session, cicdDescriptor(t))
	assertEachRunOnce(t, sink, doer, 100)
	for id, gets := range doer.jobGets {
		if gets != 1 {
			t.Fatalf("run %s jobs read %d times", id, gets)
		}
	}
	stored := storedChunkCount(t, ctx, pool)
	t.Logf("GROUPS: 100 runs x 12 jobs -> %d chunk groups with pipeline rows, %d stored chunks, %d attempts",
		sink.pipelineWrites, stored, len(stops))
	if sink.pipelineWrites != 3 || stored != 4 {
		t.Fatalf("a runs page of 100 runs x 12 jobs committed in %d chunk groups (%d stored chunks), want 3 (41 + 41 + 18) and 4 with the final chunk", sink.pipelineWrites, stored)
	}
}

// A sink failure inside a coalesced group on real Postgres: the group was prepared whole before its first write and the
// run whose arrival started the flush is staged as its own group, so the next attempt drains both from the store (no
// GitHub read again) and every row lands exactly once.
func TestGitHubTestsRunsPhaseCrashInsideACoalescedGroupDrainsOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	repository, session, pool := runsGroupPostgres(t, ctx)
	doer, sink := newRunsGroupDoer(t, 100, 12), newRunsGroupSink()
	sink.failJobWrite = 2
	executor := runsGroupExecutor(time.Now().UTC(), GitHubTestsRouteHandler{CoalesceRuns: true}, repository, sink, doer, nil)
	if _, err := executor.Execute(ctx, session, cicdDescriptor(t)); err == nil || errors.As(err, new(ChunkContinuationError)) {
		t.Fatalf("attempt 1 err=%v, want the sink failure", err)
	}
	checkpoint, err := repository.LoadChunkCheckpoint(ctx, session.Claim, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if checkpoint.PreparedChunks != 3 || checkpoint.NextOrdinal != 1 {
		t.Fatalf("after the crash prepared=%d next=%d, want 3 1 (the second group prepared whole, not committed, and run 82 "+
			"whose arrival started its flush staged as its own group)", checkpoint.PreparedChunks, checkpoint.NextOrdinal)
	}
	runUntilDone(t, executor, session, cicdDescriptor(t))
	assertEachRunOnce(t, sink, doer, 100)
	for index := 0; index < 100; index++ {
		if gets := doer.jobGets[runsGroupRunID(index)]; gets != 1 {
			t.Fatalf("run %d jobs read %d times, want once (prepared groups drain without GitHub)", index, gets)
		}
	}
	t.Logf("CRASH: %d stored chunks after recovery", storedChunkCount(t, ctx, pool))
}
