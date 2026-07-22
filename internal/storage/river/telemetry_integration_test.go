//go:build integration

package riverstore_test

import (
	"context"
	"errors"
	"testing"
	"time"

	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
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
		Profile:  "ops",
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
	if snapshot.Profile != "ops" || snapshot.LocalRunning != 3 || snapshot.ExecutionSaturation != 0.75 {
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
