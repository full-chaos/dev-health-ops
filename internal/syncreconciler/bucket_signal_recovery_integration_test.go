//go:build integration

package syncreconciler

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/syncrunrollup"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
	"github.com/jackc/pgx/v5/pgxpool"
)

type recordingBucketSweeps struct{ enqueued []string }

func (sweeps *recordingBucketSweeps) EnqueueBucketSweep(_ context.Context, orgID, provider, costClass string) error {
	sweeps.enqueued = append(sweeps.enqueued, orgID+"|"+provider+"|"+costClass)
	return nil
}

func (sweeps *recordingBucketSweeps) EnsureBucketSweep(context.Context, string, string, string) error {
	return nil
}

// TestB3UnacknowledgedBucketSignalIsRecoveredWithinTickPlus60s (CHAOS-14250 plan 3 Q5): a signal that reaches a
// sweep after its acknowledgement read (River drops its enqueue as the running job's duplicate) stays unacknowledged.
// On a fake clock, the materializer's tick enqueues the bucket's sweep once the signal is 60 s old, not before, and
// never for an acknowledged signal.
func TestB3UnacknowledgedBucketSignalIsRecoveredWithinTickPlus60s(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	instance := pgschema.Start(ctx, t)
	defer instance.Close(context.Background())
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	sweeps := &recordingBucketSweeps{}
	materializer, err := NewMaterializer(pool)
	if err != nil {
		t.Fatal(err)
	}
	materializer.WithBucketSweeps(sweeps)
	const org = "00000000-0000-4000-8000-000000142599"
	signaledAt := time.Date(2026, time.November, 20, 6, 0, 0, 0, time.UTC)
	generation, err := syncrunrollup.ReadBucketGeneration(ctx, pool, org, "github", "medium")
	if err != nil {
		t.Fatal(err)
	}
	if err := syncrunrollup.BumpBucketGeneration(ctx, pool, org, "github", "medium", signaledAt); err != nil {
		t.Fatal(err)
	}
	tick := func(at time.Time) int {
		t.Helper()
		before := len(sweeps.enqueued)
		if _, err := materializer.Step(ctx, at, at.Add(-15*time.Minute), 100); err != nil {
			t.Fatal(err)
		}
		return len(sweeps.enqueued) - before
	}
	early := tick(signaledAt.Add(59 * time.Second))
	due := tick(signaledAt.Add(60 * time.Second))
	signaled, err := syncrunrollup.AckBucketGeneration(ctx, pool, org, "github", "medium", generation+1)
	if err != nil || signaled {
		t.Fatalf("ack of the current generation: signaled=%v err=%v", signaled, err)
	}
	acknowledged := tick(signaledAt.Add(5 * time.Minute))
	t.Logf("unacknowledged signal: tick at +59s enqueued %d, at +60s %d (%v); after the ack, tick at +5m enqueued %d",
		early, due, sweeps.enqueued, acknowledged)
	if early != 0 || due != 1 || sweeps.enqueued[0] != org+"|github|medium" || acknowledged != 0 {
		t.Fatalf("recovery: +59s %d, +60s %d, acknowledged %d enqueues, want 0, 1, 0", early, due, acknowledged)
	}
}

// TestB3ParkCommittedDuringAMaterializerStepIsKept (CHAOS-14250 plan 3 class 1): the dispatch materialization reads its
// candidates in its statement snapshot, then takes each outbox row's lock for ON CONFLICT DO UPDATE and judges the
// row as it is then. A pass that parks its row after that snapshot, while the step waits for the row, keeps the park:
// the conflict gate, not only the candidates read, leaves parked rows alone.
func TestB3ParkCommittedDuringAMaterializerStepIsKept(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	instance := pgschema.Start(ctx, t)
	defer instance.Close(context.Background())
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	pgseed.EnsureSyncIntegration(ctx, t, pool, "org-materializer", "", "")
	materializer, err := NewMaterializer(pool)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, time.November, 20, 6, 0, 0, 0, time.UTC)
	const runID = "00000000-0000-4000-9146-000000000001"
	seedRun(t, ctx, pool, runID, "running", now.Add(-2*time.Hour))
	seedUnit(t, ctx, pool, "00000000-0000-4000-9147-000000000001", runID, "planned", nil, now.Add(-2*time.Hour))
	seedMaterializerDispatchedOutbox(t, ctx, pool, runID, "river-park-race", now.Add(-time.Hour))

	holder, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = holder.Rollback(ctx) }()
	if _, err := holder.Exec(ctx, `SELECT 1 FROM public.sync_dispatch_outbox WHERE sync_run_id = $1::uuid AND kind = 'dispatch_sync_run' FOR UPDATE`, runID); err != nil {
		t.Fatal(err)
	}
	stepDone := make(chan error, 1)
	go func() {
		_, err := materializer.Step(ctx, now, now.Add(-15*time.Minute), 100)
		stepDone <- err
	}()
	deadline := time.Now().Add(30 * time.Second)
	for {
		var waiting bool
		if err := pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_stat_activity
WHERE wait_event_type = 'Lock' AND query LIKE '%WITH active AS MATERIALIZED%')`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		select {
		case err := <-stepDone:
			t.Fatalf("the materializer step ended without waiting on the held outbox row (err %v)", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("the materializer step never waited on the held outbox row")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if _, err := holder.Exec(ctx, `UPDATE public.sync_dispatch_outbox SET last_error = 'bucket_parked'
WHERE sync_run_id = $1::uuid AND kind = 'dispatch_sync_run'`, runID); err != nil {
		t.Fatal(err)
	}
	if err := holder.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-stepDone; err != nil {
		t.Fatal(err)
	}
	var status string
	var lastError *string
	if err := pool.QueryRow(ctx, `SELECT status, last_error FROM public.sync_dispatch_outbox WHERE sync_run_id = $1::uuid AND kind = 'dispatch_sync_run'`,
		runID).Scan(&status, &lastError); err != nil {
		t.Fatal(err)
	}
	parked := "<nil>"
	if lastError != nil {
		parked = *lastError
	}
	t.Logf("park committed while the step waited on the row: status=%s last_error=%s", status, parked)
	if status != "dispatched" || lastError == nil || *lastError != "bucket_parked" {
		t.Fatalf("the step re-armed a row parked after its snapshot: status=%s last_error=%v, want dispatched / bucket_parked", status, lastError)
	}
}
