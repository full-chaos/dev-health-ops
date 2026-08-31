//go:build integration

package riverstore_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
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

	// CREATE ROLE is cluster-scoped, not database-scoped -- a scratch
	// database does not isolate it (CHAOS-4661). Deriving both role names
	// from this call's own database identity is what makes two successive
	// runs, and two concurrent lanes, collision-free.
	roleSuffix, err := containers.RoleSuffix(instance)
	if err != nil {
		t.Fatal(err)
	}
	dbName, err := containers.DatabaseName(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	domainRole := "worker_domain_runtime_" + roleSuffix
	queueRole := "worker_queue_runtime_" + roleSuffix

	adminPool := openPool(t, ctx, instance.URI)
	defer adminPool.Close()
	createRuntimeRoles(t, ctx, adminPool, domainRole, queueRole)
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

	queueURI := roleURI(t, instance.URI, queueRole, queuePassword, dbName)
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
	// Each refusal must also NAME the offending queue/kind/version, so an
	// operator reading the crash-loop log can tell which contract refused
	// instead of querying river_job by hand (CHAOS-3938).
	assertRefusalNames(t, ctx, sampler, "heartbeat/system.heartbeat@2")

	// A JSON string is not an integer contract version even when its text is 1.
	if _, err := adminPool.Exec(ctx, `UPDATE river.river_job SET args='{"contract_version":"1"}'::jsonb WHERE id=$1`, futureID); err != nil {
		t.Fatal(err)
	}
	assertRefusalNames(t, ctx, sampler, "heartbeat/system.heartbeat@none")

	// A JSON number that is not a canonical unsigned integer is still an
	// unsupported version, and must be NAMED. Emitting the raw text instead
	// would fail the Go re-validation and collapse the whole read into an
	// unreadable snapshot, taking the queue metrics down with it.
	// jsonb normalises numbers to numeric, so an exponent form like 1e3 is
	// stored as the canonical 1000; the forms that actually survive as
	// non-canonical are negatives and fractions.
	for _, malformed := range []string{"-1", "1.5"} {
		if _, err := adminPool.Exec(
			ctx,
			`UPDATE river.river_job SET args=jsonb_build_object('contract_version', $2::numeric) WHERE id=$1`,
			futureID, malformed,
		); err != nil {
			t.Fatal(err)
		}
		if _, err := sampler.Snapshot(ctx); err != nil {
			t.Fatalf("contract_version %s made the whole snapshot unreadable: %v", malformed, err)
		}
		assertRefusalNames(t, ctx, sampler, "heartbeat/system.heartbeat@invalid")
	}
	// An ABSENT contract_version is missing, not malformed. The two must stay
	// distinguishable: one means a producer that never stamped a version, the
	// other a producer that stamped a wrong one.
	if _, err := adminPool.Exec(ctx, `UPDATE river.river_job SET args='{}'::jsonb WHERE id=$1`, futureID); err != nil {
		t.Fatal(err)
	}
	assertRefusalNames(t, ctx, sampler, "heartbeat/system.heartbeat@none")

	if _, err := adminPool.Exec(ctx, `UPDATE river.river_job SET args='{"contract_version":1}'::jsonb, kind='unknown.kind' WHERE id=$1`, futureID); err != nil {
		t.Fatal(err)
	}
	assertRefusalNames(t, ctx, sampler, "heartbeat/unknown.kind@1")

	// A kind outside the telemetry label character class is a real row an
	// older producer could have written. It must still refuse, must not reach
	// the message verbatim, and must NOT cost the backlog metrics: SQL left()
	// counts characters while the Go re-validation counts bytes, so a
	// multi-byte kind is exactly the case where truncation alone diverges.
	if _, err := adminPool.Exec(
		ctx,
		`UPDATE river.river_job SET args='{"contract_version":1}'::jsonb, kind=$2 WHERE id=$1`,
		// A slash is the sharpest trigger: it is the label's own separator,
		// so an unsanitised kind containing one silently re-splits the label
		// into the wrong queue/kind pair before validation even runs.
		futureID, "legacy/cleanup kïnd",
	); err != nil {
		t.Fatal(err)
	}
	if _, err := sampler.Snapshot(ctx); err != nil {
		t.Fatalf("an out-of-class kind made the backlog metrics unavailable: %v", err)
	}
	// Per-COMPONENT replacement, not whole-label: the queue and the version are
	// in-vocabulary and survive, so the operator still learns where the row is
	// and what version it claims, and only the unusable kind is redacted. The
	// whole-label placeholder is the last resort for a label the query could
	// not produce at all, covered by the unit test.
	assertRefusalNames(t, ctx, sampler, "heartbeat/unprintable@1")
}

// assertRefusalNames requires the refusal to wrap the stable sentinel AND to
// carry exactly the offending queue/kind/version label.
func assertRefusalNames(
	t *testing.T,
	ctx context.Context,
	sampler *riverstore.QueueTelemetrySampler,
	want string,
) {
	t.Helper()
	err := sampler.CheckAvailableContractVersions(ctx)
	if !errors.Is(err, riverstore.ErrUnsupportedAvailableContractVersion) {
		t.Fatalf("readiness error = %v, want it to wrap the unsupported-contract sentinel", err)
	}
	var unsupported *riverstore.UnsupportedContractVersionError
	if !errors.As(err, &unsupported) {
		t.Fatalf("readiness error = %v, want an *UnsupportedContractVersionError", err)
	}
	if !reflect.DeepEqual(unsupported.Offenders, []string{want}) {
		t.Fatalf("refusal named %v, want [%s]", unsupported.Offenders, want)
	}
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("refusal message %q does not name %q", err.Error(), want)
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

	// CREATE ROLE is cluster-scoped, not database-scoped -- a scratch
	// database does not isolate it (CHAOS-4661). Deriving both role names
	// from this call's own database identity is what makes two successive
	// runs, and two concurrent lanes, collision-free.
	roleSuffix, err := containers.RoleSuffix(instance)
	if err != nil {
		t.Fatal(err)
	}
	dbName, err := containers.DatabaseName(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	domainRole := "worker_domain_runtime_" + roleSuffix
	queueRole := "worker_queue_runtime_" + roleSuffix

	adminPool := openPool(t, ctx, instance.URI)
	defer adminPool.Close()
	createRuntimeRoles(t, ctx, adminPool, domainRole, queueRole)
	if _, err := riverstore.ApplyPinnedMigrations(ctx, adminPool, riverstore.MigrationOptions{
		Schema: "river", DomainRole: domainRole, QueueRole: queueRole,
	}); err != nil {
		t.Fatal(err)
	}

	queuePool := openPool(t, ctx, roleURI(t, instance.URI, queueRole, queuePassword, dbName))
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
