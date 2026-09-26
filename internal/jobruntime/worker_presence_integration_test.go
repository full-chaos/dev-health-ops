//go:build integration

package jobruntime

import (
	"context"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/workersignals"
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

// CHAOS-6920: a failed presence heartbeat is a counter on /metrics, not only a log line. The
// row is deleted under a running presence so its next renewal updates zero rows (ownership
// lost), which is the failure the prod sampler could not measure.
func TestWorkerPresenceHeartbeatFailureIsCounted(t *testing.T) {
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
	presence, err := NewWorkerPresence(pool, "tenant-worker-a", []string{"heartbeat"}, uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	presence.ttl = 300 * time.Millisecond // ticks every 100 ms
	if err := presence.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = presence.Shutdown(context.Background()) })

	heartbeatFailures := func() uint64 {
		var scraped strings.Builder
		if err := workersignals.MetricsSource().WritePrometheus(&scraped); err != nil {
			t.Fatal(err)
		}
		for _, line := range strings.Split(scraped.String(), "\n") {
			if value, ok := strings.CutPrefix(line, "worker_presence_heartbeat_failed_total "); ok {
				count, err := strconv.ParseUint(value, 10, 64)
				if err != nil {
					t.Fatal(err)
				}
				return count
			}
		}
		t.Fatal("no worker_presence_heartbeat_failed_total sample")
		return 0
	}
	before := heartbeatFailures()
	if _, err := pool.Exec(ctx, `DELETE FROM public.worker_instances WHERE instance_id = $1`, presence.instanceID); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(20 * time.Second)
	for heartbeatFailures() == before {
		if time.Now().After(deadline) {
			t.Fatalf("heartbeat failures stayed at %d for 20 s after the presence row was deleted: a failing "+
				"heartbeat is only a log line, not a counter (CHAOS-6920)", before)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
