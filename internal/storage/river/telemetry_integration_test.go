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
	dbName, err := containers.DatabaseName(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	domainRole, err := containers.RoleName("worker_domain_runtime", instance)
	if err != nil {
		t.Fatal(err)
	}
	queueRole, err := containers.RoleName("worker_queue_runtime", instance)
	if err != nil {
		t.Fatal(err)
	}

	adminPool := openPool(t, ctx, instance.URI)
	defer adminPool.Close()
	defer containers.DropRole(adminPool, domainRole, t.Logf)
	defer containers.DropRole(adminPool, queueRole, t.Logf)
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
	dbName, err := containers.DatabaseName(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	domainRole, err := containers.RoleName("worker_domain_runtime", instance)
	if err != nil {
		t.Fatal(err)
	}
	queueRole, err := containers.RoleName("worker_queue_runtime", instance)
	if err != nil {
		t.Fatal(err)
	}

	adminPool := openPool(t, ctx, instance.URI)
	defer adminPool.Close()
	defer containers.DropRole(adminPool, domainRole, t.Logf)
	defer containers.DropRole(adminPool, queueRole, t.Logf)
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

// TestQueueTelemetryContractVersionScanBoundsWorkToTheBacklogNotTheWholeTable
// is CHAOS-5615's part (b): before the supplemental index, the
// queued_contract_versions check's contract-version extraction had no index
// to use for state='available' rows in the configured queues, so it
// sequentially scanned the WHOLE river_job table -- every completed/
// discarded job ever run, not just the live backlog. On a restored backlog
// (11,428 available `metrics` rows, the live incident) that scan blew the
// query's timeout budget. This pins that river_job_available_contract_version_idx
// (added in ApplyPinnedMigrations) changes the plan from a sequential scan
// of the whole table to an index scan bounded to the matching subset, on a
// synthetic backlog sized past the live incident's own count plus 400k
// unrelated completed rows standing in for retained job history.
//
// It does NOT claim an index-ONLY scan: measured directly against Postgres
// 18 (both the pinned ghcr.io/full-chaos/postgres:18-alpine test image and
// upstream postgres:18), a btree index whose key includes a jsonb
// extraction expression is never used to serve that expression's value at
// execution time -- the planner always re-reads it from the heap even
// though the exact value is already sitting in the index, a real, verified
// limitation, not an implementation gap here. The win this index delivers
// is bounding which rows are visited at all (backlog-sized, not
// table-sized), not eliminating the per-row heap/args read.
//
// It does not exercise QueueTelemetrySampler directly (that type has no
// EXPLAIN seam); it proves the INDEX ITSELF is usable for the exact
// predicate shape unsupported_available's subquery in telemetry.go issues.
func TestQueueTelemetryContractVersionScanBoundsWorkToTheBacklogNotTheWholeTable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer closeInstance(t, instance)

	domainRole, err := containers.RoleName("worker_domain_runtime", instance)
	if err != nil {
		t.Fatal(err)
	}
	queueRole, err := containers.RoleName("worker_queue_runtime", instance)
	if err != nil {
		t.Fatal(err)
	}

	adminPool := openPool(t, ctx, instance.URI)
	defer adminPool.Close()
	defer containers.DropRole(adminPool, domainRole, t.Logf)
	defer containers.DropRole(adminPool, queueRole, t.Logf)
	createRuntimeRoles(t, ctx, adminPool, domainRole, queueRole)
	if _, err := riverstore.ApplyPinnedMigrations(ctx, adminPool, riverstore.MigrationOptions{
		Schema: "river", DomainRole: domainRole, QueueRole: queueRole,
	}); err != nil {
		t.Fatal(err)
	}

	// Sized past the live incident's own 11,428-row backlog.
	const backlogRows = 12_000
	if _, err := adminPool.Exec(ctx, `
		INSERT INTO river.river_job (state, max_attempts, args, kind, queue, scheduled_at)
		SELECT
			'available',
			3,
			jsonb_build_object(
				'contract_version', 1,
				'org_id', gen_random_uuid(),
				'payload', repeat('x', 512)
			),
			'metrics.compute',
			'metrics',
			now() - (generated.ordinal * interval '1 second')
		FROM generate_series(1, $1) AS generated(ordinal)`,
		backlogRows,
	); err != nil {
		t.Fatal(err)
	}
	// A second, unrelated queue proves the index (queue, kind, ...) leading
	// column is what makes the plan selective -- if the planner ever fell
	// back to scanning both queues' rows the row count doubles.
	if _, err := adminPool.Exec(ctx, `
		INSERT INTO river.river_job (state, max_attempts, args, kind, queue, scheduled_at, finalized_at)
		VALUES ('available', 3, '{"contract_version":1}'::jsonb, 'system.heartbeat', 'heartbeat', now(), NULL)`,
	); err != nil {
		t.Fatal(err)
	}
	// A real river_job table is never just its live backlog -- completed/
	// discarded rows accumulate for the whole retention window. A seq scan's
	// cost is proportional to the WHOLE relation, not the matched subset, so
	// without enough non-'available' filler the table is small enough that
	// Postgres correctly prefers a seq scan regardless of any index (measured
	// live: it does, on the backlog alone). This filler is what makes the
	// synthetic backlog representative of the incident's actual table shape,
	// not just its available-row count.
	const fillerRows = 400_000
	if _, err := adminPool.Exec(ctx, `
		INSERT INTO river.river_job (state, max_attempts, args, kind, queue, scheduled_at, finalized_at)
		SELECT
			'completed',
			3,
			jsonb_build_object('contract_version', 1, 'payload', repeat('y', 512)),
			'metrics.compute',
			'metrics',
			now() - (generated.ordinal * interval '1 second'),
			now() - (generated.ordinal * interval '1 second')
		FROM generate_series(1, $1) AS generated(ordinal)`,
		fillerRows,
	); err != nil {
		t.Fatal(err)
	}
	// CHAOS-5615 runbook step (c): ANALYZE after a restore/bulk load so the
	// planner's row-count estimate is fresh -- without this the planner can
	// choose the wrong plan (a sequential scan) regardless of which indexes
	// exist.
	if _, err := adminPool.Exec(ctx, "ANALYZE river.river_job"); err != nil {
		t.Fatal(err)
	}

	// The exact predicate shape unsupported_available's subquery in
	// telemetry.go issues: queue = ANY(configured queues), state='available',
	// projecting queue/kind/contract_version -- never args itself.
	var plan strings.Builder
	rows, err := adminPool.Query(ctx, `
		EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
		SELECT DISTINCT river_job.queue, river_job.kind, river_job.args ->> 'contract_version'
		FROM river.river_job
		WHERE river_job.queue = ANY($1::text[])
			AND river_job.state = 'available'`,
		[]string{"metrics"},
	)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		plan.WriteString(line)
		plan.WriteString("\n")
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	rows.Close()

	planText := plan.String()
	if !strings.Contains(planText, "Index Scan using river_job_available_contract_version_idx") {
		t.Fatalf("plan did not use the contract-version index at all:\n%s", planText)
	}
	if strings.Contains(planText, "Seq Scan on river_job") {
		t.Fatalf("plan fell back to a sequential scan over the %d-row table instead of the %d-row backlog:\n%s", backlogRows+fillerRows+1, backlogRows, planText)
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
