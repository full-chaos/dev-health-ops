//go:build integration

package daily

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// The finalize_failed blocked marker of a run clears once a LATER run of the
// same org-day that covers its scope succeeded (or is a reopen of a succeeded
// run). Cover: the later run is full-org, or its succeeded partitions hold
// every repository id of the failed run (on a reopened later run: partitions
// that are not failed_permanent). Each subtest seeds its own
// organization on one migrated schema.

type coverPartition struct {
	repos  []string
	status string
}

type coverSeed struct {
	t    *testing.T
	ctx  context.Context
	pool *pgxpool.Pool
	org  string
	base time.Time
	runs int
}

const coverDay = "2026-09-20"

// run inserts one run of the organization; minute orders created_at.
func (s *coverSeed) run(day, status, finalization string, fullOrg bool, minute int, partitions ...coverPartition) string {
	s.t.Helper()
	s.runs++
	runID := uuid.NewString()
	at := s.base.Add(time.Duration(minute) * time.Minute)
	if _, err := s.pool.Exec(s.ctx, `INSERT INTO daily_metrics_runs
        (id,org_id,target_day,generation,status,finalization_status,created_at,updated_at,full_org)
        VALUES ($1::uuid,$2::uuid,$3::date,$4,$5,$6,$7,$7,$8)`,
		runID, s.org, day, "post-sync:cover-"+runID, status, finalization, at, fullOrg); err != nil {
		s.t.Fatal(err)
	}
	for ordinal, partition := range partitions {
		repos, err := json.Marshal(partition.repos)
		if err != nil {
			s.t.Fatal(err)
		}
		var reason *string
		if partition.status == "failed_permanent" {
			value := "ambiguous_refused"
			reason = &value
		}
		if _, err := s.pool.Exec(s.ctx, `INSERT INTO daily_metrics_partitions
            (id,run_id,ordinal,repo_ids,status,failure_reason,attempt_count,created_at,updated_at)
            VALUES ($1::uuid,$2::uuid,$3,$4::json,$5,$6,1,$7,$7)`,
			uuid.NewString(), runID, ordinal, string(repos), partition.status, reason, at); err != nil {
			s.t.Fatal(err)
		}
	}
	return runID
}

// reopen turns a succeeded run back into the state a finalize-redrive or a
// partition recompute leaves: status running with its provenance row.
func (s *coverSeed) reopen(runID, actor string) {
	s.t.Helper()
	switch actor {
	case "finalize-redrive":
		if _, err := s.pool.Exec(s.ctx, `INSERT INTO daily_metrics_finalize_redrive_events
            (id,run_id,org_id,target_day,prior_status,prior_finalization_status,actor,reason,nonce)
            VALUES ($1::uuid,$2::uuid,$3::uuid,$4::date,'succeeded','succeeded','finalize-redrive','cover test',$5)`,
			uuid.NewString(), runID, s.org, coverDay, uuid.NewString()); err != nil {
			s.t.Fatal(err)
		}
	case "partition-recompute":
		if _, err := s.pool.Exec(s.ctx, `INSERT INTO daily_metrics_partition_recompute_events
            (id,run_id,org_id,target_day,family,prior_status,prior_generation,actor,reason,nonce)
            VALUES ($1::uuid,$2::uuid,$3::uuid,$4::date,'cicd','succeeded','post-sync:cover','partition-recompute','cover test',$5)`,
			uuid.NewString(), runID, s.org, coverDay, uuid.NewString()); err != nil {
			s.t.Fatal(err)
		}
	default:
		s.t.Fatalf("unknown reopen actor %q", actor)
	}
	if _, err := s.pool.Exec(s.ctx, `UPDATE daily_metrics_runs SET status = 'running', finalization_status = 'pending' WHERE id = $1::uuid`, runID); err != nil {
		s.t.Fatal(err)
	}
}

func (s *coverSeed) reconcile(store *PostgresStore) BlockedReconcileOutcome {
	s.t.Helper()
	outcome, err := store.ReconcileBlockedRuns(s.ctx, s.org)
	if err != nil {
		s.t.Fatal(err)
	}
	return outcome
}

func (s *coverSeed) reason(runID string) string {
	s.t.Helper()
	var reason *string
	if err := s.pool.QueryRow(s.ctx, `SELECT blocked_reason FROM daily_metrics_runs WHERE id = $1::uuid`, runID).Scan(&reason); err != nil {
		s.t.Fatal(err)
	}
	return deref(reason)
}

func TestFinalizeFailedBlockedMarkerCover(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	createDailyTables(t, ctx, pool)
	store, err := NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 9, 20, 6, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return base.Add(24 * time.Hour) }

	const (
		r1 = "00000000-0000-4000-8000-0000001412a1"
		r2 = "00000000-0000-4000-8000-0000001412a2"
		r3 = "00000000-0000-4000-8000-0000001412a3"
	)
	ok := func(repos ...string) coverPartition { return coverPartition{repos: repos, status: "succeeded"} }
	seed := func(t *testing.T) *coverSeed {
		return &coverSeed{t: t, ctx: ctx, pool: pool, org: uuid.NewString(), base: base}
	}
	expect := func(t *testing.T, s *coverSeed, runID, want, why string) {
		t.Helper()
		if got := s.reason(runID); got != want {
			t.Fatalf("blocked_reason = %q, want %q: %s", got, want, why)
		}
	}

	t.Run("ALaterRunCoveringTheRepositoriesClearsARepositoryScopedFinalizeFailedRun", func(t *testing.T) {
		s := seed(t)
		failed := s.run(coverDay, "failed", "failed", false, 0, ok(r1, r2))
		later := s.run(coverDay, "succeeded", "succeeded", false, 10, ok(r1), ok(r2, r3))
		outcome := s.reconcile(store)
		t.Logf("failed=%q later=%q marked=%d blocked=%d", s.reason(failed), s.reason(later), outcome.Marked, outcome.Blocked)
		expect(t, s, failed, "", "the later run's succeeded partitions hold every repository of the failed run")
		if outcome.Blocked != 0 {
			t.Fatalf("blocked = %d, want 0", outcome.Blocked)
		}
	})

	t.Run("AFullOrgLaterRunClearsAFullOrgFinalizeFailedRun", func(t *testing.T) {
		s := seed(t)
		failed := s.run(coverDay, "failed", "failed", true, 0, ok(r1))
		s.run(coverDay, "succeeded", "succeeded", true, 10, ok(r1))
		s.reconcile(store)
		expect(t, s, failed, "", "a later full-org run of the day succeeded")
	})

	t.Run("AReopenedLaterRunStillCovers", func(t *testing.T) {
		for _, actor := range []string{"finalize-redrive", "partition-recompute"} {
			t.Run(actor, func(t *testing.T) {
				s := seed(t)
				failed := s.run(coverDay, "failed", "failed", true, 0, ok(r1))
				later := s.run(coverDay, "succeeded", "succeeded", true, 10, ok(r1))
				s.reopen(later, actor)
				outcome := s.reconcile(store)
				t.Logf("%s: failed=%q marked=%d blocked=%d", actor, s.reason(failed), outcome.Marked, outcome.Blocked)
				expect(t, s, failed, "", "a reopen of a succeeded later run keeps it covering")
			})
		}
	})

	t.Run("ARepositoryScopedLaterRunReopenedWithPendingPartitionsKeepsTheClearAndRaisesNoAlert", func(t *testing.T) {
		s := seed(t)
		failed := s.run(coverDay, "failed", "failed", false, 0, ok(r1, r2))
		s.reconcile(store)
		expect(t, s, failed, BlockedReasonFinalizeFailed, "no later run yet")
		later := s.run(coverDay, "succeeded", "succeeded", false, 10, ok(r1), ok(r2))
		cleared := s.reconcile(store)
		expect(t, s, failed, "", "the later run covers before the reopen")
		s.reopen(later, "partition-recompute")
		if _, err := pool.Exec(ctx, `UPDATE daily_metrics_partitions SET status = 'pending' WHERE run_id = $1::uuid`, later); err != nil {
			t.Fatal(err)
		}
		outcome := s.reconcile(store)
		t.Logf("before the reopen cleared=%d; in the recompute window marked=%d blocked=%d failed=%q",
			cleared.Cleared, outcome.Marked, outcome.Blocked, s.reason(failed))
		expect(t, s, failed, "", "a reopened later run covers while its partitions are pending")
		if cleared.Cleared != 1 || outcome.Marked != 0 || outcome.Blocked != 0 {
			t.Fatalf("cleared=%d then marked=%d blocked=%d, want 1 then 0/0: the recompute window must not raise the alert",
				cleared.Cleared, outcome.Marked, outcome.Blocked)
		}
	})

	t.Run("StaysBlockedWhileTheLaterRunIsNotSucceeded", func(t *testing.T) {
		for _, state := range [][2]string{{"pending", "pending"}, {"running", "pending"}, {"running", "failed"}} {
			s := seed(t)
			failed := s.run(coverDay, "failed", "failed", true, 0, ok(r1))
			s.run(coverDay, state[0], state[1], true, 10, ok(r1))
			s.reconcile(store)
			expect(t, s, failed, BlockedReasonFinalizeFailed, "a later run that never succeeded covers nothing ("+state[0]+"/"+state[1]+")")
		}
	})

	t.Run("StaysBlockedWhenOnlyAnOlderRunSucceeded", func(t *testing.T) {
		s := seed(t)
		s.run(coverDay, "succeeded", "succeeded", true, 0, ok(r1))
		failed := s.run(coverDay, "failed", "failed", true, 10, ok(r1))
		s.reconcile(store)
		expect(t, s, failed, BlockedReasonFinalizeFailed, "the newer run of the day failed; an older success does not cover it")
	})

	t.Run("StaysBlockedWhenTheLaterRunIsRepositoryScopedNarrower", func(t *testing.T) {
		s := seed(t)
		fullOrg := s.run(coverDay, "failed", "failed", true, 0, ok(r1, r2))
		s.run(coverDay, "succeeded", "succeeded", false, 10, ok(r1, r2))
		scoped := s.run("2026-09-21", "failed", "failed", false, 0, ok(r1, r2))
		s.run("2026-09-21", "succeeded", "succeeded", false, 10, ok(r1))
		s.reconcile(store)
		expect(t, s, fullOrg, BlockedReasonFinalizeFailed, "a repository-scoped run never covers a full-org run")
		expect(t, s, scoped, BlockedReasonFinalizeFailed, "the later run misses a repository of the failed run")
	})

	t.Run("StaysBlockedWhenTheLaterRunsPartitionForARepositoryIsFailedPermanent", func(t *testing.T) {
		s := seed(t)
		failed := s.run(coverDay, "failed", "failed", false, 0, ok(r1, r2))
		later := s.run(coverDay, "succeeded", "succeeded", false, 10, ok(r1), coverPartition{repos: []string{r2}, status: "failed_permanent"})
		s.reopen(later, "partition-recompute")
		s.reconcile(store)
		expect(t, s, failed, BlockedReasonFinalizeFailed, "the later run did not succeed on repository r2")
	})

	t.Run("StaysBlockedWhenTheLaterRunEndsFinalizeFailed", func(t *testing.T) {
		s := seed(t)
		first := s.run(coverDay, "failed", "failed", true, 0, ok(r1))
		second := s.run(coverDay, "failed", "failed", true, 10, ok(r1))
		outcome := s.reconcile(store)
		expect(t, s, first, BlockedReasonFinalizeFailed, "the later run failed too")
		expect(t, s, second, BlockedReasonFinalizeFailed, "the later run is itself finalize_failed")
		if outcome.Blocked != 2 {
			t.Fatalf("blocked = %d, want 2", outcome.Blocked)
		}
	})

	t.Run("ANoRepositoriesLaterRunDoesNotCover", func(t *testing.T) {
		s := seed(t)
		failed := s.run(coverDay, "failed", "failed", true, 0, ok(r1))
		s.run(coverDay, "no_repositories", "succeeded", true, 10)
		s.reconcile(store)
		expect(t, s, failed, BlockedReasonFinalizeFailed, "a no_repositories run computes nothing")
	})

	t.Run("AnotherDayOrAnotherOrganizationDoesNotClear", func(t *testing.T) {
		s := seed(t)
		failed := s.run(coverDay, "failed", "failed", true, 0, ok(r1))
		s.run("2026-09-21", "succeeded", "succeeded", true, 10, ok(r1))
		other := &coverSeed{t: t, ctx: ctx, pool: pool, org: uuid.NewString(), base: base}
		other.run(coverDay, "succeeded", "succeeded", true, 10, ok(r1))
		s.reconcile(store)
		expect(t, s, failed, BlockedReasonFinalizeFailed, "only the same organization and day can cover")
	})

	t.Run("APartitionReasonIsNotClearedByALaterRun", func(t *testing.T) {
		s := seed(t)
		blocked := s.run(coverDay, "running", "pending", true, 0, ok(r1), coverPartition{repos: []string{r2}, status: "failed_permanent"})
		s.run(coverDay, "succeeded", "succeeded", true, 10, ok(r1, r2))
		s.reconcile(store)
		expect(t, s, blocked, BlockedReasonPartialPartitionsPermanent, "the cover rule sits on the finalize arm only")
	})
}
