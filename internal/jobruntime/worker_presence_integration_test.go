//go:build integration

package jobruntime

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestWorkerPresenceCountsIndependentReplicasAndDrainExpiry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if _, err := pool.Exec(ctx, `
		CREATE TABLE public.worker_instances (
			instance_id uuid PRIMARY KEY,
			worker_group varchar(64) NOT NULL,
			queues json NOT NULL,
			state varchar(16) NOT NULL CHECK (state IN ('accepting', 'draining')),
			started_at timestamptz NOT NULL,
			heartbeat_at timestamptz NOT NULL,
			expires_at timestamptz NOT NULL
		)`); err != nil {
		t.Fatal(err)
	}
	queues := []string{"retention", "heartbeat", "coverage", "webhooks"}
	first, err := NewWorkerPresence(pool, "tenant-worker-a", queues, uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewWorkerPresence(pool, "tenant-worker-a", []string{"webhooks", "coverage", "heartbeat", "retention"}, uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	first.ttl = time.Minute
	second.ttl = time.Minute
	if err := first.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := second.Start(ctx); err != nil {
		t.Fatal(err)
	}
	assertWorkerPresenceSummary(t, ctx, pool, WorkerPresenceSummary{
		WorkerGroup: "tenant-worker-a", Queues: []string{"coverage", "heartbeat", "retention", "webhooks"}, Live: 2,
	})
	if err := first.BeginDrain(ctx); err != nil {
		t.Fatal(err)
	}
	assertWorkerPresenceSummary(t, ctx, pool, WorkerPresenceSummary{
		WorkerGroup: "tenant-worker-a", Queues: []string{"coverage", "heartbeat", "retention", "webhooks"}, Live: 2, Draining: 1,
	})
	if err := first.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	assertWorkerPresenceSummary(t, ctx, pool, WorkerPresenceSummary{
		WorkerGroup: "tenant-worker-a", Queues: []string{"coverage", "heartbeat", "retention", "webhooks"}, Live: 1,
	})
	if _, err := pool.Exec(ctx, `
		UPDATE public.worker_instances
		SET expires_at = statement_timestamp() - interval '1 second'
		WHERE instance_id = $1`, second.instanceID); err != nil {
		t.Fatal(err)
	}
	assertWorkerPresenceSummary(t, ctx, pool, WorkerPresenceSummary{})
	if err := second.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

func assertWorkerPresenceSummary(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	want WorkerPresenceSummary,
) {
	t.Helper()
	got, err := ReadWorkerPresence(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	if want.WorkerGroup == "" {
		if len(got) != 0 {
			t.Fatalf("presence = %#v, want none", got)
		}
		return
	}
	if len(got) != 1 || !reflect.DeepEqual(got[0], want) {
		t.Fatalf("presence = %#v, want %#v", got, want)
	}
}
