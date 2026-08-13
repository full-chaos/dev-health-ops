//go:build integration

package jobrescue

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchcontract"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivermigrate"
)

// This is the production failure class: a maintenance leader whose execution
// queues do not include sync still sees a globally stuck sync job. Without
// rescue coverage River v0.40 classifies the kind as unhandled and discards it
// before max attempts. With coverage it applies the kind policy and retries.
func TestPartialQueueClientDoesNotDiscardAnotherQueuesStuckJob(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
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

	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve test path")
	}
	contractRoot := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", "contracts", "jobs", "v1"))
	registry, err := jobruntime.Load(contractRoot)
	if err != nil {
		t.Fatal(err)
	}
	workers := river.NewWorkers()
	if _, err := RegisterMissingWorkers(workers, registry, nil); err != nil {
		t.Fatal(err)
	}
	client, err := river.NewClient(riverpgxv5.New(pool), &river.Config{
		ID:                   "partial-client-rescue-proof",
		JobTimeout:           10 * time.Millisecond,
		Logger:               logger,
		Queues:               map[string]river.QueueConfig{"unrelated": {MaxWorkers: 1}},
		RescueStuckJobsAfter: 20 * time.Millisecond,
		Schema:               "river",
		TestOnly:             true,
		Workers:              workers,
	})
	if err != nil {
		t.Fatal(err)
	}
	var jobID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO river.river_job (
			state, attempt, max_attempts, attempted_at, args, attempted_by,
			kind, queue, scheduled_at
		) VALUES (
			'running', 1, 5, now() - interval '2 hours', '{}', ARRAY['dead-client'],
			$1, 'sync', now() - interval '2 hours'
		) RETURNING id`, syncdispatchcontract.KindDispatchSyncRun).Scan(&jobID); err != nil {
		t.Fatal(err)
	}
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

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		var state string
		if err := pool.QueryRow(ctx, "SELECT state::text FROM river.river_job WHERE id = $1", jobID).Scan(&state); err != nil {
			t.Fatal(err)
		}
		if state != "running" {
			if state == "discarded" {
				t.Fatal("partial client discarded another queue's still-retryable job")
			}
			if state != "retryable" && state != "available" && state != "scheduled" {
				t.Fatalf("rescued job state = %s", state)
			}
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("River maintenance leader did not rescue the stuck job")
}
