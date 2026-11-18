//go:build integration

package daily

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// CHAOS-4290, #2241 r3 Finding 2.
//
// The finalize policy says a deterministically failing native family exhausts
// its River attempts and is then RECORDED by CHAOS-5040's blocked marker. That
// was false: reconcileBlockedRunsSQL fired only on a failed_permanent
// PARTITION, and a stranded finalize has none -- every partition succeeded and
// only the cross-partition step failed. blocked_at stayed NULL, so the run was
// invisible to the sweep built to surface wedged runs.
//
// CORRECTED after the confirmation pass. The first version keyed on
// finalization_status='failed' alone, which is what ReleaseFinalize writes after
// ANY failed attempt and which ClaimFinalize treats as claimable -- so the
// marker fired on healthy RETRYING runs, and never on stranded ones, because a
// terminally failed run also has status='failed' and was excluded by the query's
// status='running' scope. Exactly backwards.
//
// AND THE FIRST FIXTURE COULD NOT HAVE CAUGHT THAT. It inserted one row in the
// retryable state and asserted the marker fired on it. That proves the predicate
// FIRES; it cannot prove the predicate DISTINGUISHES, because the state it must
// NOT match was never in the fixture.
//
// So this fixture now carries BOTH: an attempt-1 retryable row and an exhausted
// terminal one. The assertion is that the marker fires on the terminal row and
// NOT on the retryable one -- which is the only shape that can fail if the
// predicate inverts again.
func TestAFailedFinalizeReachesTheBlockedMarker(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
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

	const (
		orgID        = "00000000-0000-4000-8000-000000000041"
		terminalRun  = "00000000-0000-4000-8000-000000000042"
		retryableRun = "00000000-0000-4000-8000-000000000043"
		pendingRun   = "00000000-0000-4000-8000-000000000044"
	)
	now := time.Date(2026, 9, 4, 18, 0, 0, 0, time.UTC)

	// TERMINAL: retries exhausted. status AND finalization_status both 'failed',
	// which is what FailFinalizePermanently writes on the final River attempt.
	// This is the only row that may be marked.
	if _, err := pool.Exec(ctx, `INSERT INTO daily_metrics_runs
        (id,org_id,target_day,generation,status,finalization_status,created_at,updated_at)
        VALUES ($1,$2,'2026-09-04','daily-v1','failed','failed',$3,$3)`,
		terminalRun, orgID, now); err != nil {
		t.Fatal(err)
	}
	// RETRYABLE: the state ReleaseFinalize writes after ANY failed attempt --
	// status still 'running', finalization_status 'failed'. ClaimFinalize will
	// claim this again, so marking it would flag a healthy retrying run.
	//
	// THIS ROW IS THE POINT OF THE TEST. The previous fixture contained only
	// this shape and asserted the marker FIRED on it, which is how an inverted
	// predicate passed.
	if _, err := pool.Exec(ctx, `INSERT INTO daily_metrics_runs
        (id,org_id,target_day,generation,status,finalization_status,created_at,updated_at)
        VALUES ($1,$2,'2026-09-03','daily-v1','running','failed',$3,$3)`,
		retryableRun, orgID, now); err != nil {
		t.Fatal(err)
	}
	// PENDING: never attempted. Neither arm should match it.
	if _, err := pool.Exec(ctx, `INSERT INTO daily_metrics_runs
        (id,org_id,target_day,generation,status,finalization_status,created_at,updated_at)
        VALUES ($1,$2,'2026-09-02','daily-v1','running','pending',$3,$3)`,
		pendingRun, orgID, now); err != nil {
		t.Fatal(err)
	}
	for index, runID := range []string{terminalRun, retryableRun, pendingRun} {
		if _, err := pool.Exec(ctx, `INSERT INTO daily_metrics_partitions
            (id,run_id,ordinal,repo_ids,status,attempt_count,created_at,updated_at)
            VALUES ($1,$2,0,'[]'::jsonb,'succeeded',1,$3,$3)`,
			"00000000-0000-4000-8000-00000000005"+string(rune('0'+index)), runID, now); err != nil {
			t.Fatal(err)
		}
	}

	store, err := NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	store.now = func() time.Time { return now }

	outcome, err := store.ReconcileBlockedRuns(ctx, orgID)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Marked != 1 {
		t.Fatalf("marked %d run(s), want exactly 1 -- ONLY the terminal run. Marking "+
			"more means the predicate cannot tell an exhausted finalize from a "+
			"retrying one", outcome.Marked)
	}

	// r2 finding #2 (CHAOS-4290): the WRITE side above marks the terminal run
	// even though its status is 'failed', not 'running' -- but outcome.Blocked
	// (the gauge) and BlockedRuns (the workerctl operator readback) each ran
	// their OWN separate query, still scoped to status='running' alone, before
	// this fix. A marker nothing can read back is exactly as invisible as no
	// marker at all.
	if outcome.Blocked != 1 {
		t.Fatalf("ReconcileBlockedRuns outcome.Blocked = %d, want 1 -- the gauge query "+
			"must count a status='failed' finalize_blocked run, not only status='running' ones",
			outcome.Blocked)
	}
	blockedRuns, err := store.BlockedRuns(ctx, orgID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(blockedRuns) != 1 || blockedRuns[0].RunID != terminalRun {
		t.Fatalf("BlockedRuns = %+v, want exactly the terminal run %s -- the operator "+
			"readback must surface a finalize_blocked run the same way it surfaces a "+
			"partition-blocked one", blockedRuns, terminalRun)
	}
	if blockedRuns[0].Reason != BlockedReasonFinalizeFailed {
		t.Fatalf("BlockedRuns[0].Reason = %q, want %q", blockedRuns[0].Reason, BlockedReasonFinalizeFailed)
	}

	var blockedAt *time.Time
	var reason *string
	if err := pool.QueryRow(ctx,
		`SELECT blocked_at, blocked_reason FROM daily_metrics_runs WHERE id = $1`,
		terminalRun).Scan(&blockedAt, &reason); err != nil {
		t.Fatal(err)
	}
	if blockedAt == nil {
		t.Fatal("blocked_at is NULL for a TERMINALLY failed finalize with every " +
			"partition succeeded -- it is stranded and invisible to the sweep")
	}
	if reason == nil || *reason != BlockedReasonFinalizeFailed {
		t.Fatalf("blocked_reason = %v, want %q", reason, BlockedReasonFinalizeFailed)
	}

	// THE ASSERTION THE OLD FIXTURE COULD NOT MAKE.
	for _, unmarked := range []struct{ id, why string }{
		{retryableRun, "a RETRYABLE finalize failure (status='running', " +
			"finalization_status='failed') -- ClaimFinalize will claim it again, so " +
			"marking it flags a healthy run and, since CompleteFinalize does not " +
			"clear blocked_at, the marker would survive the successful retry forever"},
		{pendingRun, "a run whose finalization was never attempted"},
	} {
		var got *time.Time
		if err := pool.QueryRow(ctx,
			`SELECT blocked_at FROM daily_metrics_runs WHERE id = $1`,
			unmarked.id).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != nil {
			t.Errorf("blocked_at is set on %s", unmarked.why)
		}
	}

	// And the retryable run must still be CLAIMABLE -- the marker must not have
	// changed its eligibility, only its visibility.
	store2, err := NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	store2.now = func() time.Time { return now }
	claim, err := store2.ClaimFinalize(ctx, retryableRun)
	if err != nil {
		t.Fatal(err)
	}
	if claim == nil {
		t.Fatal("the retryable run is no longer claimable -- the reconcile pass " +
			"changed retry eligibility, which it must never do")
	}
}

// completionWriteDownStore is a real store whose completion write always fails.
type completionWriteDownStore struct {
	*PostgresStore
}

func (*completionWriteDownStore) CompleteFinalize(context.Context, FinalizeClaim) error {
	return ErrUnavailable
}

// A finalize whose completion write fails on every attempt, the last one too,
// must end the run where the blocked marker sees it. After a retried attempt the
// run stays claimable and unmarked.
func TestACompletionWriteLostOnEveryAttemptReachesTheBlockedMarker(t *testing.T) {
	defer restoreRecognisedFinalizeFamilies(pythonRecognisedFinalizeFamilies)
	pythonRecognisedFinalizeFamilies = []string{}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
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

	const (
		runID       = "00000000-0000-4000-8000-000000000061"
		partitionID = "00000000-0000-4000-8000-000000000062"
		attempts    = 4
	)
	now := time.Now().UTC()
	if _, err := pool.Exec(ctx, `INSERT INTO daily_metrics_runs
        (id,org_id,target_day,generation,status,finalization_status,created_at,updated_at)
        VALUES ($1,$2,'2026-11-01','daily-v1','running','pending',$3,$3)`,
		runID, testOrgID, now); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO daily_metrics_partitions
        (id,run_id,ordinal,repo_ids,status,attempt_count,created_at,updated_at)
        VALUES ($1,$2,0,'[]'::jsonb,'succeeded',1,$3,$3)`,
		partitionID, runID, now); err != nil {
		t.Fatal(err)
	}
	base, err := NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	handler, err := NewFinalizeHandler(&completionWriteDownStore{PostgresStore: base})
	if err != nil {
		t.Fatal(err)
	}
	readRun := func() (string, string) {
		t.Helper()
		var status, finalization string
		if err := pool.QueryRow(ctx, `SELECT status, finalization_status FROM daily_metrics_runs WHERE id = $1`,
			runID).Scan(&status, &finalization); err != nil {
			t.Fatal(err)
		}
		return status, finalization
	}
	reconcile := func() BlockedReconcileOutcome {
		t.Helper()
		outcome, err := base.ReconcileBlockedRuns(ctx, testOrgID)
		if err != nil {
			t.Fatal(err)
		}
		return outcome
	}

	for attempt := 1; attempt <= attempts; attempt++ {
		execution := finalizeExecutionFor(runID)
		execution.Attempt = attempt
		execution.Definition.MaxAttempts = attempts
		if err := handler.Work(ctx, execution); err == nil {
			t.Fatalf("attempt %d: Work succeeded with the completion write down", attempt)
		}
		if attempt != 1 {
			continue
		}
		status, finalization := readRun()
		outcome := reconcile()
		t.Logf("after attempt 1 of %d: status=%s finalization=%s blocked=%d", attempts, status, finalization, outcome.Blocked)
		if status != "running" || finalization != "failed" || outcome.Blocked != 0 {
			t.Fatalf("after a retried attempt: status=%s finalization=%s blocked=%d, want running/failed/0",
				status, finalization, outcome.Blocked)
		}
	}

	status, finalization := readRun()
	outcome := reconcile()
	t.Logf("after attempt %d of %d: status=%s finalization=%s marked=%d blocked=%d",
		attempts, attempts, status, finalization, outcome.Marked, outcome.Blocked)
	if status != "failed" || finalization != "failed" {
		t.Fatalf("after the last attempt: status=%s finalization=%s, want failed/failed -- "+
			"the run has no job left and must not stay running", status, finalization)
	}
	if outcome.Marked != 1 || outcome.Blocked != 1 {
		t.Fatalf("reconcile after the last attempt: marked=%d blocked=%d, want 1/1", outcome.Marked, outcome.Blocked)
	}
	var reason *string
	if err := pool.QueryRow(ctx, `SELECT blocked_reason FROM daily_metrics_runs WHERE id = $1`, runID).Scan(&reason); err != nil {
		t.Fatal(err)
	}
	if reason == nil || *reason != BlockedReasonFinalizeFailed {
		t.Fatalf("blocked_reason = %v, want %q", reason, BlockedReasonFinalizeFailed)
	}
}

// The operator recovery of a run whose completion write was lost on the last
// attempt is a rerun of its day (`metrics daily-start --rerun-tag`): no redrive
// reopens a 'failed' run. The rerun's run reaches succeeded; the failed run
// keeps its row and its finalize_failed marker.
func TestARerunOfTheDayRecoversARunWhoseLastCompletionWriteWasLost(t *testing.T) {
	defer restoreRecognisedFinalizeFamilies(pythonRecognisedFinalizeFamilies)
	pythonRecognisedFinalizeFamilies = []string{}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	store, publisher, pool := rerunStore(t, ctx)
	const (
		day      = "2026-11-02"
		attempts = 4
	)
	repos := []RepositoryID{"00000000-0000-4000-8000-000000000071"}

	runToFinalize := func(tag string) string {
		t.Helper()
		started, err := store.StartManualDailyRerun(ctx, testOrgID, day, ManualDailyRerunGeneration(testOrgID, day, repos, tag), repos, publisher, tag)
		if err != nil || started.AlreadyStarted {
			t.Fatalf("StartManualDailyRerun(%s) = %+v, %v", tag, started, err)
		}
		if run, err := store.ClaimDispatch(ctx, started.RunID); err != nil || run == nil {
			t.Fatalf("ClaimDispatch(%s) = %v, %v", started.RunID, run, err)
		}
		rows, err := pool.Query(ctx, `SELECT id::text FROM daily_metrics_partitions WHERE run_id = $1::uuid`, started.RunID)
		if err != nil {
			t.Fatal(err)
		}
		var partitionIDs []string
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				t.Fatal(err)
			}
			partitionIDs = append(partitionIDs, id)
		}
		rows.Close()
		if len(partitionIDs) == 0 {
			t.Fatalf("run %s has no partition", started.RunID)
		}
		for _, id := range partitionIDs {
			claim, err := store.ClaimPartition(ctx, id)
			if err != nil || claim == nil {
				t.Fatalf("ClaimPartition(%s) = %v, %v", id, claim, err)
			}
			if err := store.CompletePartition(ctx, *claim, publisher); err != nil {
				t.Fatalf("CompletePartition(%s): %v", id, err)
			}
		}
		return started.RunID
	}
	readRun := func(runID string) (status, finalization string, reason *string) {
		t.Helper()
		if err := pool.QueryRow(ctx, `SELECT status, finalization_status, blocked_reason FROM daily_metrics_runs WHERE id = $1::uuid`,
			runID).Scan(&status, &finalization, &reason); err != nil {
			t.Fatal(err)
		}
		return status, finalization, reason
	}
	finalize := func(handler *FinalizeHandler, runID string, attempt int) error {
		execution := finalizeExecutionFor(runID)
		execution.Attempt = attempt
		execution.Definition.MaxAttempts = attempts
		return handler.Work(ctx, execution)
	}

	lost := runToFinalize("t0")
	down, err := NewFinalizeHandler(&completionWriteDownStore{PostgresStore: store})
	if err != nil {
		t.Fatal(err)
	}
	for attempt := 1; attempt <= attempts; attempt++ {
		if err := finalize(down, lost, attempt); err == nil {
			t.Fatalf("attempt %d: Work succeeded with the completion write down", attempt)
		}
	}
	if _, err := store.ReconcileBlockedRuns(ctx, testOrgID); err != nil {
		t.Fatal(err)
	}
	status, finalization, reason := readRun(lost)
	t.Logf("before the rerun: lost run status=%s finalization=%s blocked_reason=%v", status, finalization, deref(reason))
	if status != "failed" || finalization != "failed" || reason == nil || *reason != BlockedReasonFinalizeFailed {
		t.Fatalf("the lost run = %s/%s/%v, want failed/failed/%s", status, finalization, deref(reason), BlockedReasonFinalizeFailed)
	}

	rerun := runToFinalize("fix-1")
	up, err := NewFinalizeHandler(store)
	if err != nil {
		t.Fatal(err)
	}
	if err := finalize(up, rerun, 1); err != nil {
		t.Fatalf("the rerun's finalize: %v", err)
	}
	outcome, err := store.ReconcileBlockedRuns(ctx, testOrgID)
	if err != nil {
		t.Fatal(err)
	}
	rerunStatus, rerunFinalization, rerunReason := readRun(rerun)
	status, finalization, reason = readRun(lost)
	t.Logf("after the rerun: rerun status=%s finalization=%s blocked_reason=%v; lost run status=%s finalization=%s blocked_reason=%v; blocked=%d",
		rerunStatus, rerunFinalization, deref(rerunReason), status, finalization, deref(reason), outcome.Blocked)
	if rerunStatus != "succeeded" || rerunFinalization != "succeeded" || rerunReason != nil {
		t.Fatalf("the rerun = %s/%s/%v, want succeeded/succeeded/unmarked", rerunStatus, rerunFinalization, deref(rerunReason))
	}
	if status != "failed" || reason == nil || *reason != BlockedReasonFinalizeFailed || outcome.Blocked != 1 {
		t.Fatalf("the lost run after the rerun = %s/%v blocked=%d, want failed/%s and 1 blocked",
			status, deref(reason), outcome.Blocked, BlockedReasonFinalizeFailed)
	}
}
