//go:build integration

package sync

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const occurrenceOrgID = "00000000-0000-4000-8000-0000000000aa"

// The reconcile-state columns and constraints (alembic 0051) come from the migrated schema: the
// check constraints are what prove the reconciler never writes an ambiguous lifecycle row.

type occurrenceFixture struct {
	pool       *pgxpool.Pool
	configID   string
	jobID      string
	occurrence Occurrence
}

func startOccurrencePostgres(t *testing.T) occurrenceFixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	t.Cleanup(cancel)
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
	pgschema.Apply(ctx, t, pool)

	const (
		configID = "00000000-0000-4000-8000-000000004001"
		jobID    = "00000000-0000-4000-8000-000000004002"
	)
	// planner_managed is TRUE so this file's reconciler fixtures exercise the materialization paths
	// they were written for (CHAOS-4174); source_id stays NULL (CHAOS-4604).
	if err := insertPlannerFixture(ctx, pool, plannerFixture{
		configID: configID, jobID: jobID, orgID: occurrenceOrgID, plannerManaged: true,
		createdAt: time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatal(err)
	}
	// The stub materializers report these ids as the plan they "created"; the migrated schema's
	// foreign keys need the job run and the sync run to exist for the occurrence to link to them.
	pgseed.JobRun(ctx, t, pool, "00000000-0000-4000-8000-00000000b001", jobID, 0, "")
	pgseed.EnsureSyncRun(ctx, t, pool, pgseed.SyncRun{ID: "00000000-0000-4000-8000-00000000b002", OrgID: occurrenceOrgID})
	scheduledFor := at("2026-07-24T01:00:00Z")
	occurrence := newOccurrence(configID, occurrenceOrgID, jobID, scheduledFor, scheduledFor, scheduledFor)
	if _, err := pool.Exec(ctx, `
INSERT INTO public.scheduled_sync_occurrences
    (occurrence_id, identity_version, org_id, sync_config_id, scheduled_job_id, scheduled_for, created_at)
VALUES ($1, $2, $3, $4::uuid, $5::uuid, $6, now())`,
		occurrence.ID, occurrence.IdentityVersion, occurrenceOrgID,
		configID, jobID, scheduledFor); err != nil {
		t.Fatal(err)
	}
	return occurrenceFixture{pool: pool, configID: configID, jobID: jobID, occurrence: occurrence}
}

type occurrenceState struct {
	Status       string
	AttemptCount int
	ErrorCode    *string
	NextAttempt  *time.Time
	JobRunID     *string
	SyncRunID    *string
}

func (fixture occurrenceFixture) state(t *testing.T, occurrenceID string) occurrenceState {
	t.Helper()
	var state occurrenceState
	if err := fixture.pool.QueryRow(context.Background(), `
SELECT reconcile_status, reconcile_attempt_count, reconcile_error_code,
       reconcile_next_attempt_at, job_run_id::text, sync_run_id::text
FROM public.scheduled_sync_occurrences
WHERE occurrence_id = $1`, occurrenceID).Scan(
		&state.Status, &state.AttemptCount, &state.ErrorCode,
		&state.NextAttempt, &state.JobRunID, &state.SyncRunID,
	); err != nil {
		t.Fatal(err)
	}
	return state
}

// countingMaterializer records invocations and can be told to fail.
type countingMaterializer struct {
	mu        sync.Mutex
	calls     int
	err       error
	jobRunID  string
	syncRunID string
	// writeRow proves a failed materialization's partial writes are discarded.
	writeRow bool
}

func (materializer *countingMaterializer) Materialize(
	ctx context.Context,
	tx pgx.Tx,
	occurrence PendingOccurrence,
) (PlanResult, error) {
	materializer.mu.Lock()
	materializer.calls++
	err := materializer.err
	materializer.mu.Unlock()
	if materializer.writeRow {
		if _, execErr := tx.Exec(ctx, `
INSERT INTO public.sync_configurations (id, org_id, name, provider, is_active, sync_options, created_at, updated_at)
VALUES (gen_random_uuid(), $1, 'partial-write-' || gen_random_uuid()::text, 'github', TRUE, '{}'::json, now(), now())`, occurrence.OrgID); execErr != nil {
			return PlanResult{}, execErr
		}
	}
	if err != nil {
		return PlanResult{}, err
	}
	return PlanResult{JobRunID: materializer.jobRunID, SyncRunID: materializer.syncRunID}, nil
}

func (materializer *countingMaterializer) count() int {
	materializer.mu.Lock()
	defer materializer.mu.Unlock()
	return materializer.calls
}

func TestOccurrenceReconcilerCompletesAPendingOccurrenceOnce(t *testing.T) {
	ctx := context.Background()
	fixture := startOccurrencePostgres(t)
	materializer := &countingMaterializer{
		jobRunID:  "00000000-0000-4000-8000-00000000b001",
		syncRunID: "00000000-0000-4000-8000-00000000b002",
	}
	reconciler, err := NewOccurrenceReconciler(fixture.pool, materializer)
	if err != nil {
		t.Fatal(err)
	}
	result, err := reconciler.Reconcile(ctx, at("2026-07-24T01:05:00Z"), 10)
	if err != nil {
		t.Fatal(err)
	}
	if result.Scanned != 1 || result.Completed != 1 {
		t.Fatalf("Reconcile() = %+v", result)
	}
	state := fixture.state(t, fixture.occurrence.ID)
	if state.Status != OccurrenceReconcileCompleted || state.JobRunID == nil || state.SyncRunID == nil {
		t.Fatalf("occurrence state = %+v", state)
	}
	if state.AttemptCount != 0 || state.ErrorCode != nil {
		t.Fatalf("completed occurrence kept failure state: %+v", state)
	}

	// A second pass must find nothing: the completed occurrence is no longer
	// due, so a repeated batch cannot re-materialize the same schedule.
	second, err := reconciler.Reconcile(ctx, at("2026-07-24T01:10:00Z"), 10)
	if err != nil {
		t.Fatal(err)
	}
	if second.Scanned != 0 || materializer.count() != 1 {
		t.Fatalf("second pass scanned=%d materialized=%d", second.Scanned, materializer.count())
	}
}

func TestOccurrenceReconcilerBacksOffThenQuarantines(t *testing.T) {
	ctx := context.Background()
	fixture := startOccurrencePostgres(t)
	materializer := &countingMaterializer{err: errors.New("planner exploded")}
	reconciler, err := NewOccurrenceReconciler(fixture.pool, materializer)
	if err != nil {
		t.Fatal(err)
	}

	// The ladder is 60s, 120s, 240s, 480s; the fifth attempt is terminal. Each
	// pass observes a clock past the previous deferral so the occurrence is due.
	wantBackoff := []time.Duration{
		60 * time.Second, 120 * time.Second, 240 * time.Second, 480 * time.Second,
	}
	now := at("2026-07-24T01:05:00Z")
	for attempt, want := range wantBackoff {
		result, err := reconciler.Reconcile(ctx, now, 10)
		if err != nil {
			t.Fatal(err)
		}
		if result.Retried != 1 {
			t.Fatalf("attempt %d: %+v", attempt+1, result)
		}
		state := fixture.state(t, fixture.occurrence.ID)
		if state.Status != OccurrenceReconcileRetry || state.AttemptCount != attempt+1 {
			t.Fatalf("attempt %d state = %+v", attempt+1, state)
		}
		if state.ErrorCode == nil || *state.ErrorCode != OccurrenceErrorPlannerError {
			t.Fatalf("attempt %d error code = %v", attempt+1, state.ErrorCode)
		}
		if state.NextAttempt == nil {
			t.Fatalf("attempt %d recorded no next attempt", attempt+1)
		}
		if delay := state.NextAttempt.Sub(now); delay != want {
			t.Fatalf("attempt %d backoff = %s, want %s", attempt+1, delay, want)
		}
		// A pass before the deferral expires must not touch the occurrence.
		early, err := reconciler.Reconcile(ctx, now.Add(want-time.Second), 10)
		if err != nil {
			t.Fatal(err)
		}
		if early.Scanned != 0 {
			t.Fatalf("attempt %d ran %d occurrences before the backoff expired", attempt+1, early.Scanned)
		}
		now = state.NextAttempt.Add(time.Second)
	}

	final, err := reconciler.Reconcile(ctx, now, 10)
	if err != nil {
		t.Fatal(err)
	}
	if final.Quarantined != 1 {
		t.Fatalf("final pass = %+v", final)
	}
	state := fixture.state(t, fixture.occurrence.ID)
	if state.Status != OccurrenceReconcileQuarantined ||
		state.ErrorCode == nil || *state.ErrorCode != OccurrenceErrorRetryExhausted ||
		state.NextAttempt != nil {
		t.Fatalf("quarantined state = %+v", state)
	}

	// A quarantined occurrence is terminal: it must never be picked up again,
	// which is what keeps a poison schedule from consuming the batch forever.
	after, err := reconciler.Reconcile(ctx, now.Add(time.Hour), 10)
	if err != nil {
		t.Fatal(err)
	}
	if after.Scanned != 0 {
		t.Fatalf("quarantined occurrence was re-claimed: %+v", after)
	}
}

func TestOccurrenceReconcilerDiscardsPartialWritesFromAFailedPlan(t *testing.T) {
	ctx := context.Background()
	fixture := startOccurrencePostgres(t)
	materializer := &countingMaterializer{err: errors.New("planner exploded"), writeRow: true}
	reconciler, err := NewOccurrenceReconciler(fixture.pool, materializer)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(ctx, at("2026-07-24T01:05:00Z"), 10); err != nil {
		t.Fatal(err)
	}
	var configurations int
	if err := fixture.pool.QueryRow(ctx,
		"SELECT count(*) FROM public.sync_configurations").Scan(&configurations); err != nil {
		t.Fatal(err)
	}
	if configurations != 1 {
		t.Fatalf("failed plan left %d configurations, want only the fixture row", configurations)
	}
	state := fixture.state(t, fixture.occurrence.ID)
	if state.Status != OccurrenceReconcileRetry || state.AttemptCount != 1 {
		t.Fatalf("failure was not recorded: %+v", state)
	}
}

func TestOccurrenceReconcilerQuarantinesAnIdentityConflictWithoutRetrying(t *testing.T) {
	ctx := context.Background()
	fixture := startOccurrencePostgres(t)
	// Repoint the occurrence at a different due time so the recomputed identity
	// no longer matches the stored key: the shape a repointed configuration or
	// a changed derivation would produce.
	if _, err := fixture.pool.Exec(ctx, `
UPDATE public.scheduled_sync_occurrences
SET scheduled_for = $1
WHERE occurrence_id = $2`, at("2026-07-24T02:00:00Z"), fixture.occurrence.ID); err != nil {
		t.Fatal(err)
	}
	materializer := &countingMaterializer{
		jobRunID:  "00000000-0000-4000-8000-00000000b001",
		syncRunID: "00000000-0000-4000-8000-00000000b002",
	}
	reconciler, err := NewOccurrenceReconciler(fixture.pool, materializer)
	if err != nil {
		t.Fatal(err)
	}
	result, err := reconciler.Reconcile(ctx, at("2026-07-24T03:00:00Z"), 10)
	if err != nil {
		t.Fatal(err)
	}
	if result.Quarantined != 1 {
		t.Fatalf("Reconcile() = %+v", result)
	}
	if materializer.count() != 0 {
		t.Fatal("a conflicting identity was materialized")
	}
	state := fixture.state(t, fixture.occurrence.ID)
	if state.Status != OccurrenceReconcileQuarantined ||
		state.ErrorCode == nil || *state.ErrorCode != OccurrenceErrorIdentityConflict {
		t.Fatalf("state = %+v", state)
	}
	// Identity conflict is not a failed attempt at the same work, so the
	// attempt count must stay where it was.
	if state.AttemptCount != 0 {
		t.Fatalf("identity conflict consumed an attempt: %+v", state)
	}
}

func TestOccurrenceReconcilerRecordsIneligibilityDistinctly(t *testing.T) {
	ctx := context.Background()
	fixture := startOccurrencePostgres(t)
	materializer := &countingMaterializer{
		err: fmt.Errorf("entitlement revoked: %w", ErrOccurrenceIneligible),
	}
	reconciler, err := NewOccurrenceReconciler(fixture.pool, materializer)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(ctx, at("2026-07-24T01:05:00Z"), 10); err != nil {
		t.Fatal(err)
	}
	state := fixture.state(t, fixture.occurrence.ID)
	if state.ErrorCode == nil || *state.ErrorCode != OccurrenceErrorIneligible {
		t.Fatalf("ineligible plan recorded %v", state.ErrorCode)
	}
}

// TestOccurrenceReconcilerQuarantinesInvalidPlanWithoutRetrying is the codex
// gate-round-6 fix: ErrInvalidPlan (an unsupported mode, a unit-cap
// overflow, a malformed manual selector) reproduces identically on every
// future attempt, so it must quarantine on the FIRST Reconcile pass exactly
// like an identity conflict -- never through deferOccurrence's
// retry-with-backoff ladder (60s/120s/240s/480s, ~15 minutes before
// quarantine), which only delays reaching the same terminal state fork 2
// already requires for a deterministic client-input error Python rejects
// synchronously.
func TestOccurrenceReconcilerQuarantinesInvalidPlanWithoutRetrying(t *testing.T) {
	ctx := context.Background()
	fixture := startOccurrencePostgres(t)
	materializer := &countingMaterializer{
		err: fmt.Errorf("invalid source_id: not-a-uuid: %w", ErrInvalidPlan),
	}
	reconciler, err := NewOccurrenceReconciler(fixture.pool, materializer)
	if err != nil {
		t.Fatal(err)
	}
	result, err := reconciler.Reconcile(ctx, at("2026-07-24T01:05:00Z"), 10)
	if err != nil {
		t.Fatal(err)
	}
	if result.Quarantined != 1 {
		t.Fatalf("Reconcile() = %+v, want quarantined on the first pass, not deferred", result)
	}
	state := fixture.state(t, fixture.occurrence.ID)
	if state.Status != OccurrenceReconcileQuarantined ||
		state.ErrorCode == nil || *state.ErrorCode != OccurrenceErrorInvalidPlan ||
		state.NextAttempt != nil {
		t.Fatalf("state = %+v", state)
	}
	// Not a failed attempt at doable work, same reasoning as the identity
	// conflict case: the occurrence's own data can never produce a valid
	// plan, so nothing was actually attempted and retried.
	if state.AttemptCount != 0 {
		t.Fatalf("invalid plan consumed an attempt: %+v", state)
	}
	// Quarantined is terminal: never picked up again.
	after, err := reconciler.Reconcile(ctx, at("2026-07-24T01:05:00Z").Add(time.Hour), 10)
	if err != nil {
		t.Fatal(err)
	}
	if after.Scanned != 0 {
		t.Fatalf("quarantined occurrence was re-claimed: %+v", after)
	}
}

// TestActiveActiveReconcilersMaterializeOnce is the active/active acceptance
// case: several replicas run the same batch concurrently and the occurrence is
// materialized exactly once.
func TestActiveActiveReconcilersMaterializeOnce(t *testing.T) {
	ctx := context.Background()
	fixture := startOccurrencePostgres(t)
	materializer := &countingMaterializer{
		jobRunID:  "00000000-0000-4000-8000-00000000b001",
		syncRunID: "00000000-0000-4000-8000-00000000b002",
	}

	const replicas = 5
	results := make([]OccurrenceReconcileResult, replicas)
	errs := make([]error, replicas)
	release := make(chan struct{})
	var finished sync.WaitGroup
	finished.Add(replicas)
	for index := 0; index < replicas; index++ {
		reconciler, err := NewOccurrenceReconciler(fixture.pool, materializer)
		if err != nil {
			t.Fatal(err)
		}
		go func(index int, reconciler *OccurrenceReconciler) {
			defer finished.Done()
			<-release
			results[index], errs[index] = reconciler.Reconcile(ctx, at("2026-07-24T01:05:00Z"), 10)
		}(index, reconciler)
	}
	close(release)
	finished.Wait()

	completed := 0
	for index, err := range errs {
		if err != nil {
			t.Fatalf("replica %d: %v", index, err)
		}
		completed += results[index].Completed
	}
	if completed != 1 {
		t.Fatalf("replicas completed the occurrence %d times", completed)
	}
	if calls := materializer.count(); calls != 1 {
		t.Fatalf("materializer ran %d times under active/active", calls)
	}
	state := fixture.state(t, fixture.occurrence.ID)
	if state.Status != OccurrenceReconcileCompleted {
		t.Fatalf("state = %+v", state)
	}
}

func TestOccurrenceReconcilerRejectsAMissingMaterializer(t *testing.T) {
	if _, err := NewOccurrenceReconciler(&pgxpool.Pool{}, nil); !errors.Is(err, ErrMaterializerUnavailable) {
		t.Fatalf("NewOccurrenceReconciler() = %v, want ErrMaterializerUnavailable", err)
	}
}

// A missing planner is not a failing occurrence. Consuming an attempt here
// would march healthy pending work toward quarantine for a reason that has
// nothing to do with it, so the occurrence must be left exactly as it was and
// the condition surfaced to the caller instead.
func TestUnavailableMaterializerLeavesTheOccurrenceUntouched(t *testing.T) {
	ctx := context.Background()
	fixture := startOccurrencePostgres(t)
	reconciler, err := NewOccurrenceReconciler(fixture.pool, NewUnavailableMaterializer())
	if err != nil {
		t.Fatal(err)
	}
	before := fixture.state(t, fixture.occurrence.ID)

	for attempt := 0; attempt < 3; attempt++ {
		if _, err := reconciler.Reconcile(ctx, at("2026-07-24T01:05:00Z"), 10); !errors.Is(
			err, ErrMaterializerUnavailable,
		) {
			t.Fatalf("attempt %d Reconcile() = %v, want ErrMaterializerUnavailable", attempt, err)
		}
	}

	after := fixture.state(t, fixture.occurrence.ID)
	if after.Status != before.Status || after.AttemptCount != before.AttemptCount {
		t.Fatalf("a missing planner mutated the occurrence: before=%+v after=%+v", before, after)
	}
	if after.Status != OccurrenceReconcilePending || after.AttemptCount != 0 {
		t.Fatalf("occurrence state = %+v, want an untouched pending occurrence", after)
	}
	if after.ErrorCode != nil {
		t.Fatalf("a missing planner recorded error code %v on the occurrence", *after.ErrorCode)
	}
}

// With no pending work a missing planner is not an error: an idle scheduler
// must not report itself broken.
func TestUnavailableMaterializerIsSilentWithNoPendingWork(t *testing.T) {
	ctx := context.Background()
	fixture := startOccurrencePostgres(t)
	if _, err := fixture.pool.Exec(
		ctx, "DELETE FROM public.scheduled_sync_occurrences",
	); err != nil {
		t.Fatal(err)
	}
	reconciler, err := NewOccurrenceReconciler(fixture.pool, NewUnavailableMaterializer())
	if err != nil {
		t.Fatal(err)
	}
	result, err := reconciler.Reconcile(ctx, at("2026-07-24T01:05:00Z"), 10)
	if err != nil {
		t.Fatalf("an idle scheduler reported %v", err)
	}
	if result.Scanned != 0 {
		t.Fatalf("Reconcile() = %+v", result)
	}
}
