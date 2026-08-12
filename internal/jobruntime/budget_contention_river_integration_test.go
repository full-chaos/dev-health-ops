//go:build integration

package jobruntime

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivermigrate"
	"github.com/riverqueue/river/rivertype"
)

// TestBudgetContentionSnoozeIsAttemptNeutralInRiver executes the production
// adapter through a real River client and PostgreSQL row. Unit classification
// alone cannot prove River applies JobSnoozeError's attempt-neutral database
// transition.
func TestBudgetContentionSnoozeIsAttemptNeutralInRiver(t *testing.T) {
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
	migrator, err := rivermigrate.New(
		riverpgxv5.New(pool),
		&rivermigrate.Config{Logger: logger, Schema: "river"},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := migrator.Migrate(ctx, rivermigrate.DirectionUp, nil); err != nil {
		t.Fatal(err)
	}

	claim := &recordingClaim{state: ClaimProceed}
	adapter := newRetentionAdapter(
		t,
		HandlerFunc[RetentionCleanupArgs](func(context.Context, *Execution[RetentionCleanupArgs]) error {
			return BudgetContention(errors.New("provider budget contended"), 1500*time.Millisecond)
		}),
		&recordingObserver{}, claim, &recordingLease{}, &bytes.Buffer{},
	)
	workers := river.NewWorkers()
	if err := river.AddWorkerSafely(workers, adapter); err != nil {
		t.Fatal(err)
	}
	client, err := river.NewClient(riverpgxv5.New(pool), &river.Config{
		FetchCooldown:     500 * time.Millisecond,
		FetchPollInterval: 500 * time.Millisecond,
		ID:                "provider-budget-contention-integration",
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
	events, unsubscribe := client.Subscribe(river.EventKindJobFailed, river.EventKindJobSnoozed)
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

	inserted, err := client.Insert(ctx, retentionJob(t, 1).Args, &river.InsertOpts{
		MaxAttempts: 3,
		Priority:    3,
		Queue:       "retention",
	})
	if err != nil {
		t.Fatal(err)
	}
	var event *river.Event
	select {
	case event = <-events:
	case <-time.After(15 * time.Second):
		t.Fatal("River did not publish the snoozed event")
	}
	if event == nil || event.Job == nil || event.Job.ID != inserted.Job.ID {
		t.Fatalf("snooze event does not identify inserted job: %#v", event)
	}
	if event.Kind != river.EventKindJobSnoozed {
		t.Fatalf("River emitted %s instead of an attempt-neutral snooze", event.Kind)
	}
	if event.Job.Attempt != 0 || !riverSnoozeStateIsSchedulable(event.Job.State) {
		t.Fatalf("snooze event attempt/state=%d/%s, want 0 and scheduled or available",
			event.Job.Attempt, event.Job.State)
	}

	var storedAttempt int
	var storedState string
	var scheduledAt time.Time
	if err := pool.QueryRow(ctx, `
SELECT attempt, state::text, scheduled_at
FROM river.river_job WHERE id = $1`, inserted.Job.ID).Scan(
		&storedAttempt, &storedState, &scheduledAt,
	); err != nil {
		t.Fatal(err)
	}
	if storedAttempt != 0 || !riverSnoozeStateIsSchedulable(rivertype.JobState(storedState)) {
		t.Fatalf("stored attempt/state=%d/%s, want 0 and scheduled or available", storedAttempt, storedState)
	}
	if scheduledAt.IsZero() {
		t.Fatal("snoozed row has no scheduled_at")
	}
	if len(claim.completions) != 1 || claim.completions[0] != (Completion{
		Result: ResultRetry, Category: CategoryBudget,
	}) {
		t.Fatalf("domain completion=%#v, want one budget retry", claim.completions)
	}
}

func riverSnoozeStateIsSchedulable(state rivertype.JobState) bool {
	return state == rivertype.JobStateScheduled || state == rivertype.JobStateAvailable
}
