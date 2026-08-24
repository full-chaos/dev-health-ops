//go:build integration

package jobruntime

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivermigrate"
)

// TestHandlerPanicIsRecoveredAsAJobFailureAndTheWorkerProcessSurvives is the
// verification CHAOS-4175's BudgetGuard chokepoint design leans on: a
// malformed-verdict invariant violation is meant to panic() (the Go idiom
// for "this should never happen"), and that choice is only safe if the job
// runtime demonstrably recovers a handler panic as an ordinary JOB failure
// rather than crashing the worker process (and every OTHER queue that
// process is running). Reading adapter.go's recover() is not proof; this
// drives a panicking handler through a REAL River client and PostgreSQL
// row, then proves the SAME running client/worker pool still processes a
// second, unrelated job afterward -- the process did not die.
func TestHandlerPanicIsRecoveredAsAJobFailureAndTheWorkerProcessSurvives(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	})
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if _, err := pool.Exec(ctx, "CREATE SCHEMA river"); err != nil {
		t.Fatal(err)
	}
	migrator, err := rivermigrate.New(riverpgxv5.New(pool), &rivermigrate.Config{Logger: logger, Schema: "river"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := migrator.Migrate(ctx, rivermigrate.DirectionUp, nil); err != nil {
		t.Fatal(err)
	}

	// The FIRST call panics (simulating a malformed-verdict invariant
	// violation at the chokepoint); every call after that succeeds. One
	// adapter instance, one registered worker, one running process --
	// exactly the shape "does a panic take down the whole worker" needs.
	var calls atomic.Int32
	claim := &recordingClaim{state: ClaimProceed}
	adapter := newRetentionAdapter(
		t,
		HandlerFunc[RetentionCleanupArgs](func(context.Context, *Execution[RetentionCleanupArgs]) error {
			if calls.Add(1) == 1 {
				panic("simulated chokepoint invariant violation")
			}
			return nil
		}),
		&recordingObserver{}, claim, &recordingLease{}, &bytes.Buffer{},
	)
	workers := river.NewWorkers()
	if err := river.AddWorkerSafely(workers, adapter); err != nil {
		t.Fatal(err)
	}
	client, err := river.NewClient(riverpgxv5.New(pool), &river.Config{
		FetchCooldown:     200 * time.Millisecond,
		FetchPollInterval: 200 * time.Millisecond,
		ID:                "panic-recovery-integration",
		Logger:            logger,
		Queues: map[string]river.QueueConfig{
			"retention": {MaxWorkers: 1},
		},
		Schema:   "river",
		TestOnly: true,
		Workers:  workers,
	})
	if err != nil {
		t.Fatal(err)
	}
	events, unsubscribe := client.Subscribe(river.EventKindJobFailed, river.EventKindJobCompleted)
	defer unsubscribe()
	if err := client.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		if err := client.StopAndCancel(stopCtx); err != nil {
			t.Errorf("stop River client: %v", err)
		}
	})

	panicking, err := client.Insert(ctx, retentionJob(t, 1).Args, &river.InsertOpts{
		MaxAttempts: 3, Priority: 3, Queue: "retention",
	})
	if err != nil {
		t.Fatal(err)
	}

	var sawPanicFailure bool
	var succeedingJobID int64
	var sawSecondJobComplete bool
	deadline := time.After(20 * time.Second)
	for !sawPanicFailure || succeedingJobID == 0 || !sawSecondJobComplete {
		select {
		case event := <-events:
			if event.Job == nil {
				continue
			}
			switch {
			case !sawPanicFailure && event.Kind == river.EventKindJobFailed && event.Job.ID == panicking.Job.ID:
				sawPanicFailure = true
				// PROOF 1: the job itself is a normal, retryable failure --
				// not lost, not stuck, not silently discarded.
				var storedAttempt int
				var storedState string
				if err := pool.QueryRow(ctx, `SELECT attempt, state::text FROM river.river_job WHERE id = $1`, panicking.Job.ID).
					Scan(&storedAttempt, &storedState); err != nil {
					t.Fatal(err)
				}
				if storedAttempt < 1 {
					t.Fatalf("stored attempt=%d, want at least 1 (the panicking attempt was recorded)", storedAttempt)
				}
				if storedState == "discarded" {
					t.Fatalf("stored state=%q, want a retryable state -- attempt 1 of 3 must not discard outright", storedState)
				}
				// PROOF 2 (the actual "worker process survives" claim): only
				// AFTER observing the panic was recovered as a normal
				// failure do we ask the SAME running client/worker to
				// process a second, distinct job. If the panic had escaped
				// Work() and crashed the pool/process, this insert (or the
				// completion event below) would never resolve.
				succeeding, insertErr := client.Insert(ctx, retentionJob(t, 1).Args, &river.InsertOpts{
					MaxAttempts: 3, Priority: 3, Queue: "retention",
				})
				if insertErr != nil {
					t.Fatal(insertErr)
				}
				succeedingJobID = succeeding.Job.ID
			case succeedingJobID != 0 && event.Kind == river.EventKindJobCompleted && event.Job.ID == succeedingJobID:
				sawSecondJobComplete = true
			}
		case <-deadline:
			t.Fatalf("timed out: sawPanicFailure=%v succeedingJobID=%d sawSecondJobComplete=%v -- "+
				"the panic may have escaped Work() and crashed the client, or the worker pool did not survive to process a job submitted after it",
				sawPanicFailure, succeedingJobID, sawSecondJobComplete)
		}
	}
}
