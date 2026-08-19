//go:build integration

package riverstore_test

import (
	"context"
	"errors"
	"testing"
	"time"

	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestQueueTelemetrySamplerReadsPinnedRiverSchemaWithoutClaimingJobs(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer closeInstance(t, instance)

	adminPool := openPool(t, ctx, instance.URI)
	defer adminPool.Close()
	createRuntimeRoles(t, ctx, adminPool)
	if _, err := riverstore.ApplyPinnedMigrations(ctx, adminPool, riverstore.MigrationOptions{
		Schema: "river", DomainRole: domainRole, QueueRole: queueRole,
	}); err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC()
	insertTelemetryJob := func(state, queue, kind, args string, scheduledAt time.Time, attemptedBy []string) int64 {
		t.Helper()
		var id int64
		if err := adminPool.QueryRow(
			ctx,
			`INSERT INTO river.river_job
				(state, max_attempts, args, kind, queue, scheduled_at, attempted_by)
			 VALUES ($1, 3, $2::jsonb, $3, $4, $5, $6)
			 RETURNING id`,
			state,
			args,
			kind,
			queue,
			scheduledAt,
			attemptedBy,
		).Scan(&id); err != nil {
			t.Fatal(err)
		}
		return id
	}
	insertTelemetryJob("available", "heartbeat", "system.heartbeat", `{"contract_version":1}`, now.Add(-10*time.Minute), nil)
	futureID := insertTelemetryJob("available", "heartbeat", "system.heartbeat", `{"contract_version":1}`, now.Add(time.Hour), nil)
	insertTelemetryJob("available", "retention", "system.retention_cleanup", `{"contract_version":1}`, now.Add(-5*time.Minute), nil)
	insertTelemetryJob("running", "heartbeat", "system.heartbeat", `{"contract_version":1}`, now, []string{"client-ops"})
	insertTelemetryJob("running", "retention", "system.retention_cleanup", `{"contract_version":1}`, now, []string{"previous", "client-ops"})
	insertTelemetryJob("running", "retention", "system.retention_cleanup", `{"contract_version":1}`, now, []string{"client-ops"})
	insertTelemetryJob("running", "retention", "system.retention_cleanup", `{"contract_version":1}`, now, []string{"other-client"})
	insertTelemetryJob("available", "other-profile", "unknown.kind", `{"contract_version":99}`, now.Add(-24*time.Hour), nil)

	queueURI := roleURI(t, instance.URI, queueRole, queuePassword, "worker_test")
	queuePool := openPool(t, ctx, queueURI)
	defer queuePool.Close()
	sampler, err := riverstore.NewQueueTelemetrySampler(queuePool, riverstore.QueueTelemetryConfig{
		Schema:   "river",
		ClientID: "client-ops",
		Queues: []riverstore.QueueTelemetryQueue{
			{Name: "heartbeat", MaxWorkers: 2},
			{Name: "retention", MaxWorkers: 2},
		},
		Jobs: []riverstore.QueueTelemetryJob{
			{Queue: "heartbeat", Kind: "system.heartbeat", SupportedVersions: []int{1}},
			{Queue: "retention", Kind: "system.retention_cleanup", SupportedVersions: []int{1}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	snapshot, err := sampler.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.LocalRunning != 3 || snapshot.ExecutionSaturation != 0.75 {
		t.Fatalf("unexpected live snapshot scalars: %#v", snapshot)
	}
	available := make(map[string]int64, len(snapshot.Jobs))
	for _, job := range snapshot.Jobs {
		available[job.Queue+"/"+job.Kind] = job.Available
	}
	if available["heartbeat/system.heartbeat"] != 1 || available["retention/system.retention_cleanup"] != 1 {
		t.Fatalf("live available counts = %v", available)
	}
	ages := make(map[string]time.Duration, len(snapshot.Queues))
	for _, queue := range snapshot.Queues {
		ages[queue.Queue] = queue.OldestAvailableAge
	}
	if ages["heartbeat"] < 9*time.Minute || ages["heartbeat"] > 11*time.Minute ||
		ages["retention"] < 4*time.Minute || ages["retention"] > 6*time.Minute {
		t.Fatalf("live oldest ages = %v", ages)
	}
	capacities := make(map[string]riverstore.QueueCapacityTelemetry, len(snapshot.QueueCapacities))
	for _, queue := range snapshot.QueueCapacities {
		capacities[queue.Queue] = queue
	}
	if capacities["heartbeat"].Capacity != 2 || capacities["heartbeat"].Running != 1 ||
		capacities["heartbeat"].Saturation != 0.5 ||
		capacities["retention"].Capacity != 2 || capacities["retention"].Running != 2 ||
		capacities["retention"].Saturation != 1 {
		t.Fatalf("live queue capacities = %v", capacities)
	}
	if err := sampler.CheckAvailableContractVersions(ctx); err != nil {
		t.Fatalf("supported available contracts failed readiness: %v", err)
	}

	// Readiness checks every state=available row, even one not fetchable until
	// later, so a rollout cannot become incompatible when scheduled_at arrives.
	if _, err := adminPool.Exec(ctx, `UPDATE river.river_job SET args='{"contract_version":2}'::jsonb WHERE id=$1`, futureID); err != nil {
		t.Fatal(err)
	}
	if err := sampler.CheckAvailableContractVersions(ctx); err != riverstore.ErrUnsupportedAvailableContractVersion {
		t.Fatalf("unsupported future contract readiness error = %v", err)
	}

	// A JSON string is not an integer contract version even when its text is 1.
	if _, err := adminPool.Exec(ctx, `UPDATE river.river_job SET args='{"contract_version":"1"}'::jsonb WHERE id=$1`, futureID); err != nil {
		t.Fatal(err)
	}
	if err := sampler.CheckAvailableContractVersions(ctx); !errors.Is(err, riverstore.ErrUnsupportedAvailableContractVersion) {
		t.Fatalf("non-integer contract readiness error = %v", err)
	}

	if _, err := adminPool.Exec(ctx, `UPDATE river.river_job SET args='{"contract_version":1}'::jsonb, kind='unknown.kind' WHERE id=$1`, futureID); err != nil {
		t.Fatal(err)
	}
	if err := sampler.CheckAvailableContractVersions(ctx); !errors.Is(err, riverstore.ErrUnsupportedAvailableContractVersion) {
		t.Fatalf("unknown kind readiness error = %v", err)
	}
}

// TestQueueSaturationIsPerProcessAcrossReplicas is CHAOS-3867 evidence.
//
// The per-queue running count was fleet-wide while the capacity it is divided
// by is this process's MaxWorkers, so every replica of an N-replica group
// reported saturation of roughly N -- clamped to 1.0. The signal an operator
// uses to decide scale-out was therefore pegged at 100% exactly under
// scale-out, and the queue_saturation warning (threshold 0.9) fired
// permanently at N >= 2.
func TestQueueSaturationIsPerProcessAcrossReplicas(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer closeInstance(t, instance)

	adminPool := openPool(t, ctx, instance.URI)
	defer adminPool.Close()
	createRuntimeRoles(t, ctx, adminPool)
	if _, err := riverstore.ApplyPinnedMigrations(ctx, adminPool, riverstore.MigrationOptions{
		Schema: "river", DomainRole: domainRole, QueueRole: queueRole,
	}); err != nil {
		t.Fatal(err)
	}

	queuePool := openPool(t, ctx, roleURI(t, instance.URI, queueRole, queuePassword, "worker_test"))
	defer queuePool.Close()
	samplerFor := func(clientID string) *riverstore.QueueTelemetrySampler {
		t.Helper()
		sampler, err := riverstore.NewQueueTelemetrySampler(queuePool, riverstore.QueueTelemetryConfig{
			Schema:   "river",
			ClientID: clientID,
			Queues:   []riverstore.QueueTelemetryQueue{{Name: "retention", MaxWorkers: 2}},
			Jobs: []riverstore.QueueTelemetryJob{
				{Queue: "retention", Kind: "system.retention_cleanup", SupportedVersions: []int{1}},
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		return sampler
	}
	saturationFor := func(sampler *riverstore.QueueTelemetrySampler) (float64, int64) {
		t.Helper()
		snapshot, err := sampler.Snapshot(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, capacity := range snapshot.QueueCapacities {
			if capacity.Queue == "retention" {
				return capacity.Saturation, capacity.Running
			}
		}
		t.Fatal("retention queue missing from snapshot")
		return 0, 0
	}

	first, second := samplerFor("replica-1"), samplerFor("replica-2")

	// An idle two-replica group must not trip the 0.9 queue_saturation warning.
	for name, sampler := range map[string]*riverstore.QueueTelemetrySampler{
		"replica-1": first, "replica-2": second,
	} {
		if saturation, running := saturationFor(sampler); saturation != 0 || running != 0 {
			t.Fatalf("idle %s reported saturation=%v running=%d, want 0/0", name, saturation, running)
		}
	}

	// Each replica claims one of its two slots: 2 of 4 fleet slots busy, so
	// true utilization is 50%. Fleet-wide counting reported 2/2 = 1.0 on BOTH.
	insertRunningRetentionJob(t, ctx, adminPool, "replica-1")
	insertRunningRetentionJob(t, ctx, adminPool, "replica-2")

	for name, sampler := range map[string]*riverstore.QueueTelemetrySampler{
		"replica-1": first, "replica-2": second,
	} {
		saturation, running := saturationFor(sampler)
		if running != 1 {
			t.Fatalf("%s counted %d running jobs, want only its own 1", name, running)
		}
		if saturation != 0.5 {
			t.Fatalf("%s reported saturation=%v at 50%% true utilization, want 0.5", name, saturation)
		}
	}
}

func insertRunningRetentionJob(t *testing.T, ctx context.Context, pool *pgxpool.Pool, clientID string) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		INSERT INTO river.river_job
			(state, max_attempts, args, kind, queue, scheduled_at, attempted_by)
		VALUES ('running', 3, '{"contract_version":1}'::jsonb, 'system.retention_cleanup',
			'retention', now(), $1)`,
		[]string{clientID},
	); err != nil {
		t.Fatal(err)
	}
}
