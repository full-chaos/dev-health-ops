//go:build integration

package daily

import (
	"context"
	"errors"
	"net/url"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
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

// One nightly pass reaches every stranded run of the organization, beyond one
// page of candidates, and a later pass does not publish a run again while its
// redriven finalize job is still waiting.
func TestNightlySweepReachesEveryStrandedRunOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
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
	orgID := uuid.NewString()
	const stranded = defaultFinalizeSweepLimit + 1
	old := time.Now().UTC().Add(-48 * time.Hour)
	if _, err := pool.Exec(ctx, `
INSERT INTO daily_metrics_runs (id,org_id,target_day,generation,status,finalization_status,created_at,updated_at)
SELECT gen_random_uuid(), $1::uuid, DATE '2026-01-01' + n, 'post-sync:sweep-' || n, 'running', 'pending', $2, $2
FROM generate_series(1, $3::int) AS n`, orgID, old, stranded); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO daily_metrics_partitions (id,run_id,ordinal,repo_ids,status,attempt_count,completed_at,created_at,updated_at)
SELECT gen_random_uuid(), run.id, 0, '[]'::jsonb, 'succeeded', 1, $2, $2, $2
FROM daily_metrics_runs AS run WHERE run.org_id = $1::uuid`, orgID, old); err != nil {
		t.Fatal(err)
	}

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
	dispatcher, err := NewDispatcher(store, publisher, &fakeRepositoryDiscoverer{})
	if err != nil {
		t.Fatal(err)
	}
	counts := func() (runs, rows int) {
		t.Helper()
		if err := pool.QueryRow(ctx, `
SELECT count(DISTINCT args #>> '{domain,id}'), count(*) FROM worker_job_outbox
WHERE job_kind = 'metrics.daily_finalize' AND dedupe_key LIKE 'metrics.daily_finalize:redrive:%'`).Scan(&runs, &rows); err != nil {
			t.Fatal(err)
		}
		return runs, rows
	}
	for pass := 1; pass <= 2; pass++ {
		if err := dispatcher.Work(ctx, dispatchExecution(orgID, nightly.ID)); err != nil {
			t.Fatalf("pass %d Work: %v", pass, err)
		}
		runs, rows := counts()
		t.Logf("pass %d: %d runs redriven, %d redrive rows", pass, runs, rows)
		if runs != stranded || rows != stranded {
			t.Fatalf("pass %d: %d runs with %d redrive rows, want %d runs with one row each", pass, runs, rows, stranded)
		}
	}
}

// completionFailingStore fails the first CompleteFinalize, as a lost database
// write would, and passes every other call to the real store.
type completionFailingStore struct {
	*PostgresStore
	failed atomic.Bool
}

func (store *completionFailingStore) CompleteFinalize(ctx context.Context, claim FinalizeClaim) error {
	if store.failed.CompareAndSwap(false, true) {
		return ErrUnavailable
	}
	return store.PostgresStore.CompleteFinalize(ctx, claim)
}

type countingFinalizeFamily struct{ computes atomic.Int32 }

func (family *countingFinalizeFamily) ComputeFinalizeFamily(context.Context, Run) (int, error) {
	family.computes.Add(1)
	return 0, nil
}

// A redriven twin adds no compute over what one finalize job already does: when
// a completion write is lost after a compute, one job's own retry computes
// again, and a pair of jobs computes exactly as often.
func TestARedriveTwinComputesNoMoreThanOneJobsRetry(t *testing.T) {
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
	base, err := NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	old := time.Now().UTC().Add(-48 * time.Hour)
	for _, testCase := range []struct {
		name string
		// jobs is how many finalize deliveries of the run are worked, one after
		// another, each until it returns nil.
		jobs int
	}{
		{name: "one job and its retry", jobs: 1},
		{name: "the job and its redriven twin", jobs: 2},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			orgID, runID := uuid.NewString(), uuid.NewString()
			seedSweepRun(t, ctx, pool, sweepRunSeed{orgID, runID, "2026-11-01", "pending", old, []string{"succeeded"}})
			store := &completionFailingStore{PostgresStore: base}
			family := &countingFinalizeFamily{}
			finalizer, err := NewFinalizeHandler(store)
			if err != nil {
				t.Fatal(err)
			}
			if err := finalizer.SetNativeFinalizeFamilies(map[string]NativeFinalizeFamilyExecutor{"ic_finalize": family}); err != nil {
				t.Fatal(err)
			}
			for job := 0; job < testCase.jobs; job++ {
				for attempt := 1; attempt <= 4; attempt++ {
					if err := finalizer.Work(ctx, sweepFinalizeExecution(orgID, runID)); err == nil {
						break
					}
				}
			}
			var status string
			if err := pool.QueryRow(ctx, `SELECT status FROM daily_metrics_runs WHERE id = $1`, runID).Scan(&status); err != nil {
				t.Fatal(err)
			}
			t.Logf("%s: computes=%d status=%s", testCase.name, family.computes.Load(), status)
			if status != "succeeded" || family.computes.Load() != 2 {
				t.Fatalf("computes=%d status=%s, want 2 computes (one lost completion) and succeeded", family.computes.Load(), status)
			}
		})
	}
}

// The sweep re-reads its whole predicate under the run's lock. A run selected
// as stranded can be reopened before the lock (a partition recompute leaves it
// fresh, a range redrive leaves an open event); the sweep must then publish
// nothing. The operator path keeps its own rule.
func TestSweepRechecksItsPredicateUnderTheRunLock(t *testing.T) {
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
	cutoff := now.Add(-finalizeStrandHorizon)
	orgID := uuid.NewString()

	for _, testCase := range []struct {
		name      string
		updatedAt time.Time
		openEvent bool
		cutoff    *time.Time
		want      bool
	}{
		{name: "still stranded", updatedAt: now.Add(-48 * time.Hour), cutoff: &cutoff, want: true},
		{name: "fresh again after a recompute", updatedAt: now.Add(-time.Minute), cutoff: &cutoff, want: false},
		{name: "reopened by a range redrive (open event)", updatedAt: now.Add(-48 * time.Hour), openEvent: true, cutoff: &cutoff, want: false},
		{name: "operator path keeps its rule", updatedAt: now.Add(-time.Minute), cutoff: nil, want: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			runID := uuid.NewString()
			seedSweepRun(t, ctx, pool, sweepRunSeed{orgID, runID, "2026-11-01", "pending", testCase.updatedAt, []string{"succeeded"}})
			if testCase.openEvent {
				if _, err := pool.Exec(ctx, `INSERT INTO daily_metrics_finalize_redrive_events (id,run_id,org_id,target_day,prior_status,prior_finalization_status,actor,reason,nonce) VALUES ($1,$2,$3,'2026-11-01','succeeded','succeeded','finalize-redrive','review',$4)`,
					uuid.NewString(), runID, orgID, uuid.NewString()); err != nil {
					t.Fatal(err)
				}
			}
			published, err := store.redriveOneStrandedFinalize(ctx, publisher, runID, uuid.NewString(), now, false, testCase.cutoff)
			if err != nil {
				t.Fatal(err)
			}
			if published != testCase.want || (finalizeRedriveRows(t, ctx, pool, runID) == 1) != testCase.want {
				t.Fatalf("published=%t rows=%d, want %t", published, finalizeRedriveRows(t, ctx, pool, runID), testCase.want)
			}
			var updatedAt time.Time
			if err := pool.QueryRow(ctx, `SELECT updated_at FROM daily_metrics_runs WHERE id = $1`, runID).Scan(&updatedAt); err != nil {
				t.Fatal(err)
			}
			restarted := !updatedAt.Equal(testCase.updatedAt) && !updatedAt.Before(now)
			if restarted != (testCase.want && testCase.cutoff != nil) {
				t.Fatalf("updated_at=%s, want the horizon restarted only by a sweep publish", updatedAt)
			}
		})
	}
}

// The sweep runs inside the worker's nightly dispatch, on the domain pool. Every
// table it reads or writes must be granted to that role by the pinned
// migrations (no grant is added by hand here): the whole dispatch, sweep
// included, runs as the domain role and redrives the stranded run.
func TestNightlySweepRunsUnderTheWorkerDomainRole(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	config, err := pgxpool.ParseConfig(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	config.MaxConns = 8
	admin, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	createDailyTables(t, ctx, admin)
	domainRole, err := containers.RoleName("sweep_domain", instance)
	if err != nil {
		t.Fatal(err)
	}
	queueRole, err := containers.RoleName("sweep_queue", instance)
	if err != nil {
		t.Fatal(err)
	}
	defer containers.DropRole(admin, domainRole, t.Logf)
	defer containers.DropRole(admin, queueRole, t.Logf)
	const password = "sweep_role_password"
	for _, role := range []string{domainRole, queueRole} {
		if _, err := admin.Exec(ctx, "CREATE ROLE "+role+" LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '"+password+"'"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := riverstore.ApplyPinnedMigrations(ctx, admin, riverstore.MigrationOptions{
		Schema: "river", DomainRole: domainRole, QueueRole: queueRole,
	}); err != nil {
		t.Fatal(err)
	}
	domainURI, err := url.Parse(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	domainURI.User = url.UserPassword(domainRole, password)
	domain, err := pgxpool.New(ctx, domainURI.String())
	if err != nil {
		t.Fatal(err)
	}
	defer domain.Close()

	registry, err := jobruntime.Load(filepath.Join("..", "..", "..", "..", "contracts", "jobs", "v1"))
	if err != nil {
		t.Fatal(err)
	}
	adminPublisher, err := NewPostgresPublisher(admin, dailyTestRegistry{production: registry})
	if err != nil {
		t.Fatal(err)
	}
	adminStore, err := NewPostgresStore(admin)
	if err != nil {
		t.Fatal(err)
	}
	orgID, stranded := uuid.NewString(), uuid.NewString()
	seedSweepRun(t, ctx, admin, sweepRunSeed{orgID, stranded, "2026-11-01", "pending", time.Now().UTC().Add(-48 * time.Hour), []string{"succeeded"}})
	tx, err := admin.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	nightly, err := adminStore.StartRunTx(ctx, tx, StartRunRequest{
		OrganizationID: orgID, TargetDay: time.Date(2026, 11, 18, 0, 0, 0, 0, time.UTC),
		Generation: "post-sync:" + uuid.NewString(), RepositoryIDs: []RepositoryID{RepositoryID(uuid.NewString())},
	}, adminPublisher)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	store, err := NewPostgresStore(domain)
	if err != nil {
		t.Fatal(err)
	}
	publisher, err := NewPostgresPublisher(domain, dailyTestRegistry{production: registry})
	if err != nil {
		t.Fatal(err)
	}
	if redriven, err := store.SweepStrandedFinalizeForOrg(ctx, publisher, orgID); err != nil || redriven != 1 {
		t.Fatalf("sweep as the domain role: redriven=%d err=%v, want 1 and no error", redriven, err)
	}
	// A second pass, through the whole dispatch as the domain role: nothing
	// fails and nothing is published again.
	dispatcher, err := NewDispatcher(store, publisher, &fakeRepositoryDiscoverer{})
	if err != nil {
		t.Fatal(err)
	}
	records := captureDefaultLog(t, func() {
		if err := dispatcher.Work(ctx, dispatchExecution(orgID, nightly.ID)); err != nil {
			t.Fatalf("dispatch Work as the domain role: %v", err)
		}
	})
	for _, record := range records {
		if record["msg"] == StrandedFinalizeSweepFailedLogMessage {
			t.Fatalf("the sweep failed under the domain role: %v", record)
		}
	}
	if got := finalizeRedriveRows(t, ctx, admin, stranded); got != 1 {
		t.Fatalf("redrive rows = %d, want 1", got)
	}
}

// steppingClock moves forward by one millisecond on every read, as the wall
// clock does between a pass's start and its later writes.
type steppingClock struct {
	mu  sync.Mutex
	now time.Time
}

func (clock *steppingClock) read() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	clock.now = clock.now.Add(time.Millisecond)
	return clock.now
}

func (clock *steppingClock) set(at time.Time) {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	clock.now = at
}

// A redriven run's horizon starts at its publish, not at the start of the pass
// that published it: a pass that starts exactly one horizon after the previous
// pass started must not publish the run again.
func TestSweepRestartsTheHorizonFromThePublish(t *testing.T) {
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
	start := time.Date(2026, 11, 18, 1, 0, 0, 0, time.UTC)
	clock := &steppingClock{now: start}
	store.now = clock.read
	orgID, runID := uuid.NewString(), uuid.NewString()
	seedSweepRun(t, ctx, pool, sweepRunSeed{orgID, runID, "2026-11-01", "pending", start.Add(-48 * time.Hour), []string{"succeeded"}})

	clock.set(start)
	if redriven, err := store.SweepStrandedFinalizeForOrg(ctx, publisher, orgID); err != nil || redriven != 1 {
		t.Fatalf("first pass: redriven=%d err=%v, want 1", redriven, err)
	}
	var publishedAt time.Time
	if err := pool.QueryRow(ctx, `SELECT updated_at FROM daily_metrics_runs WHERE id = $1`, runID).Scan(&publishedAt); err != nil {
		t.Fatal(err)
	}
	firstStart := start.Add(time.Millisecond)
	t.Logf("first pass started %s, publish stamped %s", firstStart.Format(time.RFC3339Nano), publishedAt.Format(time.RFC3339Nano))

	// The next pass starts exactly one horizon after the first pass started,
	// before one horizon after the publish.
	clock.set(firstStart.Add(finalizeStrandHorizon).Add(-time.Millisecond))
	if redriven, err := store.SweepStrandedFinalizeForOrg(ctx, publisher, orgID); err != nil || redriven != 0 {
		t.Fatalf("second pass one horizon after the first pass started: redriven=%d err=%v, want 0", redriven, err)
	}
	// One horizon after the publish, the run is redriven again.
	clock.set(publishedAt.Add(finalizeStrandHorizon))
	if redriven, err := store.SweepStrandedFinalizeForOrg(ctx, publisher, orgID); err != nil || redriven != 1 {
		t.Fatalf("pass one horizon after the publish: redriven=%d err=%v, want 1", redriven, err)
	}
	if got := finalizeRedriveRows(t, ctx, pool, runID); got != 2 {
		t.Fatalf("redrive rows = %d, want 2", got)
	}
}

// completionFailingNTimes fails the first n CompleteFinalize calls.
type completionFailingNTimes struct {
	*PostgresStore
	left atomic.Int32
}

func (store *completionFailingNTimes) CompleteFinalize(ctx context.Context, claim FinalizeClaim) error {
	if store.left.Add(-1) >= 0 {
		return ErrUnavailable
	}
	return store.PostgresStore.CompleteFinalize(ctx, claim)
}

// The bound when the first job loses every completion write (codex round 2,
// "missed in round 1"): the job alone computes once per attempt (4) and leaves
// the run running with finalization failed; with its redriven twin the run
// gets one more compute (5), and that one ends the run succeeded.
func TestARedriveTwinAfterAnExhaustedJobComputesOnceMore(t *testing.T) {
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
	base, err := NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	const attempts = 4
	for _, testCase := range []struct {
		name         string
		jobs         int
		wantComputes int32
		wantStatus   string
	}{
		{name: "one job exhausts its attempts", jobs: 1, wantComputes: attempts, wantStatus: "running"},
		{name: "the job exhausts its attempts, then its twin", jobs: 2, wantComputes: attempts + 1, wantStatus: "succeeded"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			orgID, runID := uuid.NewString(), uuid.NewString()
			seedSweepRun(t, ctx, pool, sweepRunSeed{orgID, runID, "2026-11-01", "pending", time.Now().UTC().Add(-48 * time.Hour), []string{"succeeded"}})
			store := &completionFailingNTimes{PostgresStore: base}
			store.left.Store(attempts)
			family := &countingFinalizeFamily{}
			finalizer, err := NewFinalizeHandler(store)
			if err != nil {
				t.Fatal(err)
			}
			if err := finalizer.SetNativeFinalizeFamilies(map[string]NativeFinalizeFamilyExecutor{"ic_finalize": family}); err != nil {
				t.Fatal(err)
			}
			for job := 0; job < testCase.jobs; job++ {
				for attempt := 1; attempt <= attempts; attempt++ {
					execution := sweepFinalizeExecution(orgID, runID)
					execution.Attempt = attempt
					execution.Definition.MaxAttempts = attempts
					if err := finalizer.Work(ctx, execution); err == nil {
						break
					}
				}
			}
			var status, finalization string
			if err := pool.QueryRow(ctx, `SELECT status, finalization_status FROM daily_metrics_runs WHERE id = $1`, runID).Scan(&status, &finalization); err != nil {
				t.Fatal(err)
			}
			t.Logf("%s: computes=%d status=%s finalization=%s", testCase.name, family.computes.Load(), status, finalization)
			if family.computes.Load() != testCase.wantComputes || status != testCase.wantStatus {
				t.Fatalf("computes=%d status=%s, want %d and %s", family.computes.Load(), status, testCase.wantComputes, testCase.wantStatus)
			}
		})
	}
}
