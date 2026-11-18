//go:build integration

package daily

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

type sweepRunSeed struct {
	orgID, runID       string
	day                string
	finalizationStatus string
	updatedAt          time.Time
	partitions         []string
}

func seedSweepRun(t *testing.T, ctx context.Context, pool *pgxpool.Pool, seed sweepRunSeed) {
	t.Helper()
	if _, err := pool.Exec(ctx, `INSERT INTO daily_metrics_runs (id,org_id,target_day,generation,status,finalization_status,created_at,updated_at) VALUES ($1::uuid,$2,$3::date,'post-sync:' || $1::uuid::text,'running',$4,$5,$5)`,
		seed.runID, seed.orgID, seed.day, seed.finalizationStatus, seed.updatedAt); err != nil {
		t.Fatal(err)
	}
	for ordinal, status := range seed.partitions {
		var completed any
		if status == "succeeded" {
			completed = seed.updatedAt
		}
		if _, err := pool.Exec(ctx, `INSERT INTO daily_metrics_partitions (id,run_id,ordinal,repo_ids,status,attempt_count,completed_at,created_at,updated_at) VALUES ($1,$2,$3,'[]'::jsonb,$4,1,$5,$6,$6)`,
			uuid.NewString(), seed.runID, ordinal, status, completed, seed.updatedAt); err != nil {
			t.Fatal(err)
		}
	}
}

func finalizeRedriveRows(t *testing.T, ctx context.Context, pool *pgxpool.Pool, runID string) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM worker_job_outbox WHERE job_kind = 'metrics.daily_finalize' AND dedupe_key LIKE 'metrics.daily_finalize:redrive:' || $1 || ':%'`, runID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

// A run whose partitions all succeeded and whose finalize was never claimed
// waits for a metrics.daily_finalize job. Once that job is gone (River's
// cleaner deletes a finished job after its retention), the strand repair cannot
// select the run and only the operator CLI redrives it. The nightly dispatch of
// the organization must redrive its never-attempted stranded finalizes past the
// horizon, and nothing else.
func TestNightlyDispatchRedrivesItsOrganizationsStrandedFinalize(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	createDailyTables(t, ctx, pool)
	registry, err := jobruntime.Load(filepath.Join("..", "..", "..", "..", "contracts", "jobs", "v1"))
	if err != nil {
		t.Fatal(err)
	}
	publisher, err := NewPostgresPublisher(pool, dailyTestRegistry{production: registry})
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	old := now.Add(-48 * time.Hour)
	young := now.Add(-time.Hour)

	orgID, otherOrg := uuid.NewString(), uuid.NewString()
	stranded, youngRun, priorAttempt, unfinished, otherOrgRun := uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()
	for _, seed := range []sweepRunSeed{
		{orgID, stranded, "2026-11-01", "pending", old, []string{"succeeded", "succeeded"}},
		{orgID, youngRun, "2026-11-02", "pending", young, []string{"succeeded"}},
		{orgID, priorAttempt, "2026-11-03", "failed", old, []string{"succeeded"}},
		{orgID, unfinished, "2026-11-04", "pending", old, []string{"succeeded", "failed"}},
		{otherOrg, otherOrgRun, "2026-11-01", "pending", old, []string{"succeeded"}},
	} {
		seedSweepRun(t, ctx, pool, seed)
	}

	// The nightly run of the organization, dispatched through the real handler.
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	nightly, err := store.StartRunTx(ctx, tx, StartRunRequest{
		OrganizationID: orgID, TargetDay: time.Date(2026, 11, 18, 0, 0, 0, 0, time.UTC),
		Generation: "post-sync:" + uuid.NewString(), RepositoryIDs: []RepositoryID{RepositoryID(uuid.NewString())},
	}, publisher)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	handler, err := NewDispatcher(store, publisher, &fakeRepositoryDiscoverer{})
	if err != nil {
		t.Fatal(err)
	}
	execution := &jobruntime.Execution[jobruntime.DailyMetricsDispatchArgs]{
		OrganizationID: pointer(orgID),
		Attempt:        1,
		Envelope: jobcontract.Envelope{
			OrganizationID: pointer(orgID),
			Domain:         jobcontract.DomainLink{Type: "daily_metrics_run", ID: nightly.ID},
		},
		Args: jobruntime.DailyMetricsDispatchArgs{
			EnvelopeArgs: jobruntime.EnvelopeArgs[jobcontract.DailyMetricsDispatchPayload]{
				OrganizationID: pointer(orgID),
				Domain:         jobcontract.DomainLink{Type: "daily_metrics_run", ID: nightly.ID},
				Payload:        jobcontract.DailyMetricsDispatchPayload{RunID: nightly.ID},
			},
		},
	}
	if err := handler.Work(ctx, execution); err != nil {
		t.Fatalf("Work: %v", err)
	}

	for _, check := range []struct {
		name  string
		runID string
		want  int
	}{
		{"stranded never-attempted finalize past the horizon", stranded, 1},
		{"pending finalize inside the horizon", youngRun, 0},
		{"finalize with a prior attempt", priorAttempt, 0},
		{"run with an unfinished partition", unfinished, 0},
		{"another organization's run", otherOrgRun, 0},
		{"the nightly run itself", nightly.ID, 0},
	} {
		if got := finalizeRedriveRows(t, ctx, pool, check.runID); got != check.want {
			t.Errorf("%s: %d redrive finalize rows, want %d", check.name, got, check.want)
		}
	}

	// The redriven finalize can run: its claim succeeds and the run ends.
	claim, err := store.ClaimFinalize(ctx, stranded)
	if err != nil || claim == nil {
		t.Fatalf("ClaimFinalize of the redriven run: %v %v", claim, err)
	}
	if err := store.CompleteFinalize(ctx, *claim); err != nil {
		t.Fatal(err)
	}
	var status string
	if err := pool.QueryRow(ctx, `SELECT status FROM daily_metrics_runs WHERE id = $1`, stranded).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "succeeded" {
		t.Fatalf("redriven run status = %q, want succeeded", status)
	}

	// A second nightly pass publishes nothing again for runs already redriven
	// or still inside the horizon.
	if err := handler.Work(ctx, execution); err != nil {
		t.Fatalf("second Work: %v", err)
	}
	if got := finalizeRedriveRows(t, ctx, pool, stranded); got != 1 {
		t.Errorf("after a second pass the stranded run has %d redrive rows, want 1", got)
	}
}

// queuedTwinFinalizeFamily counts computes; the first one waits on release, so a
// second worker can try the same run while the first holds the lease.
type queuedTwinFinalizeFamily struct {
	computes atomic.Int32
	started  chan struct{}
	release  chan struct{}
}

func (family *queuedTwinFinalizeFamily) ComputeFinalizeFamily(ctx context.Context, _ Run) (int, error) {
	if family.computes.Add(1) == 1 {
		close(family.started)
		select {
		case <-family.release:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	return 0, nil
}

func dispatchExecution(orgID, runID string) *jobruntime.Execution[jobruntime.DailyMetricsDispatchArgs] {
	return &jobruntime.Execution[jobruntime.DailyMetricsDispatchArgs]{
		OrganizationID: pointer(orgID),
		Attempt:        1,
		Envelope: jobcontract.Envelope{
			OrganizationID: pointer(orgID),
			Domain:         jobcontract.DomainLink{Type: "daily_metrics_run", ID: runID},
		},
		Args: jobruntime.DailyMetricsDispatchArgs{
			EnvelopeArgs: jobruntime.EnvelopeArgs[jobcontract.DailyMetricsDispatchPayload]{
				OrganizationID: pointer(orgID),
				Domain:         jobcontract.DomainLink{Type: "daily_metrics_run", ID: runID},
				Payload:        jobcontract.DailyMetricsDispatchPayload{RunID: runID},
			},
		},
	}
}

func sweepFinalizeExecution(orgID, runID string) *jobruntime.Execution[jobruntime.DailyMetricsFinalizeArgs] {
	return &jobruntime.Execution[jobruntime.DailyMetricsFinalizeArgs]{
		OrganizationID: pointer(orgID),
		Attempt:        1,
		Envelope: jobcontract.Envelope{
			OrganizationID: pointer(orgID),
			Domain:         jobcontract.DomainLink{Type: "daily_metrics_run", ID: runID},
		},
		Args: jobruntime.DailyMetricsFinalizeArgs{
			EnvelopeArgs: jobruntime.EnvelopeArgs[jobcontract.DailyMetricsFinalizePayload]{
				OrganizationID: pointer(orgID),
				Domain:         jobcontract.DomainLink{Type: "daily_metrics_run", ID: runID},
				Payload:        jobcontract.DailyMetricsFinalizePayload{RunID: runID},
			},
		},
	}
}

// The sweep cannot see River: a finalize job still queued past the horizon gets
// a second one beside it. Two finalize jobs of one run, worked at the same time
// through the real handler, compute the run exactly once: the second claim
// meets the live lease and is told to retry, and its retry finds the run ended.
func TestAStillQueuedFinalizeAndItsRedriveComputeTheRunOnce(t *testing.T) {
	defer restoreRecognisedFinalizeFamilies(pythonRecognisedFinalizeFamilies)
	pythonRecognisedFinalizeFamilies = []string{"ic_finalize"}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	createDailyTables(t, ctx, pool)
	registry, err := jobruntime.Load(filepath.Join("..", "..", "..", "..", "contracts", "jobs", "v1"))
	if err != nil {
		t.Fatal(err)
	}
	publisher, err := NewPostgresPublisher(pool, dailyTestRegistry{production: registry})
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	orgID, stranded := uuid.NewString(), uuid.NewString()
	seedSweepRun(t, ctx, pool, sweepRunSeed{orgID, stranded, "2026-11-01", "pending", now.Add(-48 * time.Hour), []string{"succeeded"}})

	// The original finalize job: its canonical row is published and never run.
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := publisher.PublishFinalizeTx(ctx, tx, Run{ID: stranded, OrganizationID: orgID, Status: "running"}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	tx, err = pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	nightly, err := store.StartRunTx(ctx, tx, StartRunRequest{
		OrganizationID: orgID, TargetDay: time.Date(2026, 11, 18, 0, 0, 0, 0, time.UTC),
		Generation: "post-sync:" + uuid.NewString(), RepositoryIDs: []RepositoryID{RepositoryID(uuid.NewString())},
	}, publisher)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	dispatcher, err := NewDispatcher(store, publisher, &fakeRepositoryDiscoverer{})
	if err != nil {
		t.Fatal(err)
	}
	if err := dispatcher.Work(ctx, dispatchExecution(orgID, nightly.ID)); err != nil {
		t.Fatalf("dispatch Work: %v", err)
	}
	var finalizeRows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM worker_job_outbox WHERE job_kind = 'metrics.daily_finalize' AND args #>> '{domain,id}' = $1`, stranded).Scan(&finalizeRows); err != nil {
		t.Fatal(err)
	}
	if finalizeRows != 2 {
		t.Fatalf("finalize outbox rows of the run = %d, want 2 (the queued original and the redrive)", finalizeRows)
	}

	family := &queuedTwinFinalizeFamily{started: make(chan struct{}), release: make(chan struct{})}
	finalizer, err := NewFinalizeHandler(store)
	if err != nil {
		t.Fatal(err)
	}
	if err := finalizer.SetNativeFinalizeFamilies(map[string]NativeFinalizeFamilyExecutor{"ic_finalize": family}); err != nil {
		t.Fatal(err)
	}
	first := make(chan error, 1)
	go func() { first <- finalizer.Work(ctx, sweepFinalizeExecution(orgID, stranded)) }()
	<-family.started
	// The second job, while the first holds the lease: retry, no compute.
	if err := finalizer.Work(ctx, sweepFinalizeExecution(orgID, stranded)); !errors.Is(err, ErrLeaseActive) {
		t.Fatalf("second job while the first computes: %v, want a lease-active retry", err)
	}
	close(family.release)
	if err := <-first; err != nil {
		t.Fatalf("first job: %v", err)
	}
	// The second job's retry finds the run ended.
	if err := finalizer.Work(ctx, sweepFinalizeExecution(orgID, stranded)); err != nil {
		t.Fatalf("second job's retry: %v", err)
	}
	if got := family.computes.Load(); got != 1 {
		t.Fatalf("computes = %d, want exactly 1", got)
	}
	var status string
	if err := pool.QueryRow(ctx, `SELECT status FROM daily_metrics_runs WHERE id = $1`, stranded).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "succeeded" {
		t.Fatalf("run status = %q, want succeeded", status)
	}
}
