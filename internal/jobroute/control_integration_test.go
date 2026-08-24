//go:build integration

package jobroute

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

type integrationRegistry struct{ descriptor jobruntime.Descriptor }

func (registry integrationRegistry) Descriptor(kind string) (jobruntime.Descriptor, bool) {
	return registry.descriptor, kind == registry.descriptor.Kind
}

func (registry integrationRegistry) Descriptors() []jobruntime.Descriptor {
	return []jobruntime.Descriptor{registry.descriptor}
}

type idleQuiescer struct{}

func (idleQuiescer) Quiesce(context.Context, string) error { return nil }

func TestSyncProviderCanaryTransitionsFromSeededCeleryRoute(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("close PostgreSQL: %v", err)
		}
	}()
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, `
		CREATE TABLE public.worker_job_routes (
			job_kind text PRIMARY KEY, transport text NOT NULL, paused boolean NOT NULL,
			generation bigint NOT NULL, updated_at timestamptz NOT NULL
		);
		CREATE TABLE public.worker_job_outbox (
			id uuid PRIMARY KEY, job_kind text NOT NULL, status text NOT NULL
		);
		CREATE TABLE public.worker_job_runs (
			id uuid PRIMARY KEY, job_kind text NOT NULL, status text NOT NULL
		);
		CREATE TABLE public.sync_run_units (
			id uuid PRIMARY KEY, provider text NOT NULL, dataset_key text NOT NULL,
			status text NOT NULL, updated_at timestamptz NOT NULL,
			lease_expires_at timestamptz
		);
		INSERT INTO public.worker_job_routes
			(job_kind, transport, paused, generation, updated_at)
		VALUES ('sync.provider_unit', 'celery', FALSE, 1, statement_timestamp());
		INSERT INTO public.sync_run_units (id, provider, dataset_key, status, updated_at) VALUES
			('00000000-0000-4000-8000-000000000001', 'launchdarkly', 'feature-flags', 'planned', statement_timestamp()),
			('00000000-0000-4000-8000-000000000002', 'launchdarkly', 'feature-flags', 'retrying', statement_timestamp()),
			('00000000-0000-4000-8000-000000000003', 'github', 'commits', 'running', statement_timestamp())`); err != nil {
		t.Fatal(err)
	}
	celeryQuiescer, err := NewPostgresCelerySyncProviderQuiescer(pool)
	if err != nil {
		t.Fatal(err)
	}
	controller, err := NewControllerWithCeleryQuiescer(
		pool,
		integrationRegistry{jobruntime.Descriptor{
			Kind: "sync.provider_unit", Route: "river_canary", RollbackRoute: "celery",
		}},
		idleQuiescer{},
		celeryQuiescer,
	)
	if err != nil {
		t.Fatal(err)
	}
	state, err := controller.ApplyCheckedIn(ctx, "sync.provider_unit")
	if err != nil {
		t.Fatalf("ApplyCheckedIn(): %v", err)
	}
	if state.Transport != "river_canary" || state.Generation != 2 {
		t.Fatalf("canary state = %+v", state)
	}
	state, err = controller.Rollback(ctx, "sync.provider_unit")
	if err != nil {
		t.Fatalf("Rollback(): %v", err)
	}
	if state.Transport != "celery" || state.Generation != 3 {
		t.Fatalf("rollback state = %+v", state)
	}
	producer, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var transport string
	if err := producer.QueryRow(ctx, `
		SELECT transport FROM public.worker_job_routes
		WHERE job_kind = 'sync.provider_unit' FOR SHARE`).Scan(&transport); err != nil {
		t.Fatal(err)
	}
	if transport != "celery" {
		t.Fatalf("producer observed route %q", transport)
	}
	if _, err := producer.Exec(ctx, `
		INSERT INTO public.sync_run_units (id, provider, dataset_key, status, updated_at)
		VALUES ('00000000-0000-4000-8000-000000000004', 'launchdarkly', 'feature-flags', 'dispatching', statement_timestamp())`); err != nil {
		t.Fatal(err)
	}
	applyResult := make(chan error, 1)
	go func() {
		_, applyErr := controller.ApplyCheckedIn(ctx, "sync.provider_unit")
		applyResult <- applyErr
	}()
	waitForBlockedRouteUpdate(t, ctx, pool)
	if err := producer.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-applyResult; !errors.Is(err, ErrLiveClaims) {
		t.Fatalf("ApplyCheckedIn() with active Celery unit error = %v, want %v", err, ErrLiveClaims)
	}
	state, err = controller.Inspect(ctx, "sync.provider_unit")
	if err != nil {
		t.Fatal(err)
	}
	if state.Transport != "celery" || state.Generation != 3 {
		t.Fatalf("active unit changed route: %+v", state)
	}
}

func TestRollbackWaitsForProducerRouteLockThenRejectsStagedOutbox(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("close PostgreSQL: %v", err)
		}
	}()
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, `
		CREATE TABLE public.worker_job_routes (
			job_kind text PRIMARY KEY, transport text NOT NULL, paused boolean NOT NULL,
			generation bigint NOT NULL, updated_at timestamptz NOT NULL
		);
		CREATE TABLE public.worker_job_outbox (
			id uuid PRIMARY KEY, job_kind text NOT NULL, status text NOT NULL
		);
		CREATE TABLE public.worker_job_runs (
			id uuid PRIMARY KEY, job_kind text NOT NULL, status text NOT NULL
		);
		INSERT INTO public.worker_job_routes
			(job_kind, transport, paused, generation, updated_at)
		VALUES ('job.test', 'river_canary', FALSE, 1, statement_timestamp())`); err != nil {
		t.Fatal(err)
	}
	controller, err := NewController(pool, integrationRegistry{jobruntime.Descriptor{
		Kind: "job.test", Route: "river_canary", RollbackRoute: "celery",
	}}, idleQuiescer{})
	if err != nil {
		t.Fatal(err)
	}
	producer, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = producer.Rollback(ctx) }()
	var transport string
	if err := producer.QueryRow(ctx, `
		SELECT transport FROM public.worker_job_routes
		WHERE job_kind = 'job.test' FOR SHARE`).Scan(&transport); err != nil {
		t.Fatal(err)
	}
	if transport != "river_canary" {
		t.Fatalf("producer observed %q", transport)
	}

	result := make(chan error, 1)
	go func() {
		_, rollbackErr := controller.Rollback(ctx, "job.test")
		result <- rollbackErr
	}()
	waitForBlockedRouteUpdate(t, ctx, pool)
	if _, err := producer.Exec(ctx, `
		INSERT INTO public.worker_job_outbox (id, job_kind, status)
		VALUES ('00000000-0000-4000-8000-000000000001', 'job.test', 'pending')`); err != nil {
		t.Fatal(err)
	}
	if err := producer.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-result; !errors.Is(err, ErrPendingOutbox) {
		t.Fatalf("Rollback() error = %v", err)
	}
	state, err := controller.Inspect(ctx, "job.test")
	if err != nil {
		t.Fatal(err)
	}
	if state.Transport != "river_canary" || state.Generation != 1 {
		t.Fatalf("route changed despite staged work: %+v", state)
	}

	if _, err := pool.Exec(ctx, `
		DELETE FROM public.worker_job_outbox;
		UPDATE public.worker_job_routes
		SET transport = 'celery', generation = 2, updated_at = statement_timestamp()
		WHERE job_kind = 'job.test'`); err != nil {
		t.Fatal(err)
	}
	if _, err := controller.ApplyCheckedIn(ctx, "job.test"); !errors.Is(err, ErrCeleryQuiescenceMissing) {
		t.Fatalf("ApplyCheckedIn() without Celery quiescer error = %v", err)
	}
	state, err = controller.Inspect(ctx, "job.test")
	if err != nil {
		t.Fatal(err)
	}
	if state.Transport != "celery" || state.Generation != 2 {
		t.Fatalf("route changed without Celery quiescence: %+v", state)
	}

	celery := &observedQuiescer{}
	activationController, err := NewControllerWithCeleryQuiescer(
		pool,
		integrationRegistry{jobruntime.Descriptor{
			Kind: "job.test", Route: "river_canary", RollbackRoute: "celery",
		}},
		idleQuiescer{},
		celery,
	)
	if err != nil {
		t.Fatal(err)
	}
	producer, err = pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := producer.QueryRow(ctx, `
		SELECT transport FROM public.worker_job_routes
		WHERE job_kind = 'job.test' FOR SHARE`).Scan(&transport); err != nil {
		t.Fatal(err)
	}
	result = make(chan error, 1)
	go func() {
		_, applyErr := activationController.ApplyCheckedIn(ctx, "job.test")
		result <- applyErr
	}()
	waitForBlockedRouteUpdate(t, ctx, pool)
	if celery.calls.Load() != 0 {
		t.Fatal("Celery quiescence ran before the producer transaction completed")
	}
	if err := producer.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-result; err != nil {
		t.Fatalf("ApplyCheckedIn() with quiescence: %v", err)
	}
	if celery.calls.Load() != 1 {
		t.Fatalf("Celery quiescence calls = %d", celery.calls.Load())
	}
	state, err = activationController.Inspect(ctx, "job.test")
	if err != nil {
		t.Fatal(err)
	}
	if state.Transport != "river_canary" || state.Generation != 3 {
		t.Fatalf("route not activated after quiescence: %+v", state)
	}

	state, err = activationController.Rollback(ctx, "job.test")
	if err != nil {
		t.Fatalf("Rollback() after canary: %v", err)
	}
	if state.Transport != "celery" || state.Generation != 4 {
		t.Fatalf("route not restored after rollback: %+v", state)
	}
	celery.err = ErrUnavailable
	if _, err := activationController.ApplyCheckedIn(ctx, "job.test"); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("ApplyCheckedIn() unavailable Celery probe error = %v, want %v", err, ErrUnavailable)
	}
	state, err = activationController.Inspect(ctx, "job.test")
	if err != nil {
		t.Fatal(err)
	}
	if state.Transport != "celery" || state.Generation != 4 {
		t.Fatalf("unavailable probe changed route: %+v", state)
	}
}

type observedQuiescer struct {
	calls atomic.Int32
	err   error
}

func (quiescer *observedQuiescer) Quiesce(context.Context, string) error {
	quiescer.calls.Add(1)
	return quiescer.err
}

// TestRollbackSurfacesRiverQuiesceProbeFailureAsUnavailableNotLiveClaims
// reproduces CHAOS-3904: a River quiescence probe that fails because the
// database is unavailable (here, the river schema/table is simply absent)
// must surface as ErrUnavailable, not be relabelled ErrLiveClaims. Before the
// control.go fix, Rollback discarded the quiescer's error entirely and
// always returned ErrLiveClaims, which mapJobRouteError/IsPrecondition would
// have misdiagnosed as a precondition ("still has live claims, wait it out")
// instead of a backend outage.
func TestRollbackSurfacesRiverQuiesceProbeFailureAsUnavailableNotLiveClaims(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("close PostgreSQL: %v", err)
		}
	}()
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	// Deliberately do NOT create the river schema/table: the probe query
	// fails exactly as it would during a database outage.
	if _, err := pool.Exec(ctx, `
		CREATE TABLE public.worker_job_routes (
			job_kind text PRIMARY KEY, transport text NOT NULL, paused boolean NOT NULL,
			generation bigint NOT NULL, updated_at timestamptz NOT NULL
		);
		CREATE TABLE public.worker_job_outbox (
			id uuid PRIMARY KEY, job_kind text NOT NULL, status text NOT NULL
		);
		CREATE TABLE public.worker_job_runs (
			id uuid PRIMARY KEY, job_kind text NOT NULL, status text NOT NULL
		);
		INSERT INTO public.worker_job_routes
			(job_kind, transport, paused, generation, updated_at)
		VALUES ('job.river_probe_outage', 'river_canary', FALSE, 1, statement_timestamp())`); err != nil {
		t.Fatal(err)
	}
	quiescer, err := NewPostgresRiverQuiescer(pool, "river")
	if err != nil {
		t.Fatal(err)
	}
	// Directly proves the quiescer.go fix: the driver failure is wrapped,
	// not discarded for a bare sentinel.
	probeErr := quiescer.Quiesce(ctx, "job.river_probe_outage")
	if !errors.Is(probeErr, ErrUnavailable) {
		t.Fatalf("Quiesce() error = %v, want wrapped %v", probeErr, ErrUnavailable)
	}
	if errors.Is(probeErr, ErrLiveClaims) {
		t.Fatalf("Quiesce() error = %v, must not present as live claims", probeErr)
	}
	if probeErr.Error() == ErrUnavailable.Error() {
		t.Fatalf("Quiesce() error lost the driver cause: %v", probeErr)
	}

	controller, err := NewController(pool, integrationRegistry{jobruntime.Descriptor{
		Kind: "job.river_probe_outage", Route: "river_canary", RollbackRoute: "celery",
	}}, quiescer)
	if err != nil {
		t.Fatal(err)
	}
	state, err := controller.Rollback(ctx, "job.river_probe_outage")
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Rollback() error = %v, want wrapped %v", err, ErrUnavailable)
	}
	if errors.Is(err, ErrLiveClaims) {
		t.Fatalf("Rollback() misdiagnosed a database outage as live claims: %v", err)
	}
	if IsPrecondition(err) {
		t.Fatalf("Rollback() error classified as an operator precondition: %v", err)
	}
	if (state != State{}) {
		t.Fatalf("Rollback() returned a non-zero state on failure: %+v", state)
	}
	inspected, err := controller.Inspect(ctx, "job.river_probe_outage")
	if err != nil {
		t.Fatal(err)
	}
	if inspected.Transport != "river_canary" || inspected.Generation != 1 {
		t.Fatalf("route changed despite the outage: %+v", inspected)
	}
}

// TestRollbackStillReportsGenuineRiverLiveClaimsAsPrecondition is the negative
// control for the test above: a real live River claim must still surface as
// ErrLiveClaims (and therefore IsPrecondition) after the control.go fix. This
// proves the fix passes the underlying error through rather than blindly
// discarding every quiescer error.
func TestRollbackStillReportsGenuineRiverLiveClaimsAsPrecondition(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("close PostgreSQL: %v", err)
		}
	}()
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, `
		CREATE TABLE public.worker_job_routes (
			job_kind text PRIMARY KEY, transport text NOT NULL, paused boolean NOT NULL,
			generation bigint NOT NULL, updated_at timestamptz NOT NULL
		);
		CREATE TABLE public.worker_job_outbox (
			id uuid PRIMARY KEY, job_kind text NOT NULL, status text NOT NULL
		);
		CREATE TABLE public.worker_job_runs (
			id uuid PRIMARY KEY, job_kind text NOT NULL, status text NOT NULL
		);
		CREATE SCHEMA river;
		CREATE TABLE river.river_job (id bigint PRIMARY KEY, kind text NOT NULL, state text NOT NULL);
		INSERT INTO river.river_job (id, kind, state) VALUES (1, 'job.river_probe_live', 'running');
		INSERT INTO public.worker_job_routes
			(job_kind, transport, paused, generation, updated_at)
		VALUES ('job.river_probe_live', 'river_canary', FALSE, 1, statement_timestamp())`); err != nil {
		t.Fatal(err)
	}
	quiescer, err := NewPostgresRiverQuiescer(pool, "river")
	if err != nil {
		t.Fatal(err)
	}
	probeErr := quiescer.Quiesce(ctx, "job.river_probe_live")
	if !errors.Is(probeErr, ErrLiveClaims) {
		t.Fatalf("Quiesce() error = %v, want %v", probeErr, ErrLiveClaims)
	}

	controller, err := NewController(pool, integrationRegistry{jobruntime.Descriptor{
		Kind: "job.river_probe_live", Route: "river_canary", RollbackRoute: "celery",
	}}, quiescer)
	if err != nil {
		t.Fatal(err)
	}
	_, err = controller.Rollback(ctx, "job.river_probe_live")
	if !errors.Is(err, ErrLiveClaims) {
		t.Fatalf("Rollback() error = %v, want %v", err, ErrLiveClaims)
	}
	if !IsPrecondition(err) {
		t.Fatalf("Rollback() genuine live-claims error not classified as a precondition: %v", err)
	}
}

// TestRollbackSurfacesCelerySyncQuiesceProbeFailureAsUnavailableNotLiveClaims
// is the same reproduction as the River case above, for the second quiescer
// implementation named in CHAOS-3904's acceptance criteria.
func TestRollbackSurfacesCelerySyncQuiesceProbeFailureAsUnavailableNotLiveClaims(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("close PostgreSQL: %v", err)
		}
	}()
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	// Deliberately do NOT create public.sync_run_units: the probe query
	// fails exactly as it would during a database outage.
	if _, err := pool.Exec(ctx, `
		CREATE TABLE public.worker_job_routes (
			job_kind text PRIMARY KEY, transport text NOT NULL, paused boolean NOT NULL,
			generation bigint NOT NULL, updated_at timestamptz NOT NULL
		);
		CREATE TABLE public.worker_job_outbox (
			id uuid PRIMARY KEY, job_kind text NOT NULL, status text NOT NULL
		);
		CREATE TABLE public.worker_job_runs (
			id uuid PRIMARY KEY, job_kind text NOT NULL, status text NOT NULL
		);
		INSERT INTO public.worker_job_routes
			(job_kind, transport, paused, generation, updated_at)
		VALUES ('sync.provider_unit', 'river_canary', FALSE, 1, statement_timestamp())`); err != nil {
		t.Fatal(err)
	}
	quiescer, err := NewPostgresCelerySyncProviderQuiescer(pool)
	if err != nil {
		t.Fatal(err)
	}
	probeErr := quiescer.Quiesce(ctx, "sync.provider_unit")
	if !errors.Is(probeErr, ErrUnavailable) {
		t.Fatalf("Quiesce() error = %v, want wrapped %v", probeErr, ErrUnavailable)
	}
	if errors.Is(probeErr, ErrLiveClaims) {
		t.Fatalf("Quiesce() error = %v, must not present as live claims", probeErr)
	}
	if probeErr.Error() == ErrUnavailable.Error() {
		t.Fatalf("Quiesce() error lost the driver cause: %v", probeErr)
	}

	controller, err := NewController(pool, integrationRegistry{jobruntime.Descriptor{
		Kind: "sync.provider_unit", Route: "river_canary", RollbackRoute: "celery",
	}}, quiescer)
	if err != nil {
		t.Fatal(err)
	}
	state, err := controller.Rollback(ctx, "sync.provider_unit")
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Rollback() error = %v, want wrapped %v", err, ErrUnavailable)
	}
	if errors.Is(err, ErrLiveClaims) {
		t.Fatalf("Rollback() misdiagnosed a database outage as live claims: %v", err)
	}
	if IsPrecondition(err) {
		t.Fatalf("Rollback() error classified as an operator precondition: %v", err)
	}
	if (state != State{}) {
		t.Fatalf("Rollback() returned a non-zero state on failure: %+v", state)
	}
	inspected, err := controller.Inspect(ctx, "sync.provider_unit")
	if err != nil {
		t.Fatal(err)
	}
	if inspected.Transport != "river_canary" || inspected.Generation != 1 {
		t.Fatalf("route changed despite the outage: %+v", inspected)
	}
}

// celerySyncQuiescenceSchema is shared by the liveness-boundary tests below:
// TestRollbackStillReportsGenuineCelerySyncLiveClaimsAsPrecondition (fresh
// DISPATCHING, live RUNNING lease -- must still block) and its CHAOS-3929
// counterparts (stale orphaned DISPATCHING, expired RUNNING lease -- must not
// block). All four share one schema so the only thing that differs between
// them is the row shape under test.
const celerySyncQuiescenceSchema = `
	CREATE TABLE public.worker_job_routes (
		job_kind text PRIMARY KEY, transport text NOT NULL, paused boolean NOT NULL,
		generation bigint NOT NULL, updated_at timestamptz NOT NULL
	);
	CREATE TABLE public.worker_job_outbox (
		id uuid PRIMARY KEY, job_kind text NOT NULL, status text NOT NULL
	);
	CREATE TABLE public.worker_job_runs (
		id uuid PRIMARY KEY, job_kind text NOT NULL, status text NOT NULL
	);
	CREATE TABLE public.sync_run_units (
		id uuid PRIMARY KEY, provider text NOT NULL, dataset_key text NOT NULL,
		status text NOT NULL, updated_at timestamptz NOT NULL,
		lease_expires_at timestamptz
	);
	INSERT INTO public.worker_job_routes
		(job_kind, transport, paused, generation, updated_at)
	VALUES ('sync.provider_unit', 'river_canary', FALSE, 1, statement_timestamp())`

// TestRollbackStillReportsGenuineCelerySyncLiveClaimsAsPrecondition is the
// positive control for the Celery sync-provider quiescer: a fresh DISPATCHING
// row (well inside the stale-dispatch threshold) must still block the route
// move, because it can still be a live, unclaimed Celery message.
func TestRollbackStillReportsGenuineCelerySyncLiveClaimsAsPrecondition(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("close PostgreSQL: %v", err)
		}
	}()
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, celerySyncQuiescenceSchema+`;
		INSERT INTO public.sync_run_units (id, provider, dataset_key, status, updated_at) VALUES
			('00000000-0000-4000-8000-000000000005', 'launchdarkly', 'feature-flags', 'dispatching', statement_timestamp())`); err != nil {
		t.Fatal(err)
	}
	quiescer, err := NewPostgresCelerySyncProviderQuiescer(pool)
	if err != nil {
		t.Fatal(err)
	}
	probeErr := quiescer.Quiesce(ctx, "sync.provider_unit")
	if !errors.Is(probeErr, ErrLiveClaims) {
		t.Fatalf("Quiesce() error = %v, want %v", probeErr, ErrLiveClaims)
	}

	controller, err := NewController(pool, integrationRegistry{jobruntime.Descriptor{
		Kind: "sync.provider_unit", Route: "river_canary", RollbackRoute: "celery",
	}}, quiescer)
	if err != nil {
		t.Fatal(err)
	}
	_, err = controller.Rollback(ctx, "sync.provider_unit")
	if !errors.Is(err, ErrLiveClaims) {
		t.Fatalf("Rollback() error = %v, want %v", err, ErrLiveClaims)
	}
	if !IsPrecondition(err) {
		t.Fatalf("Rollback() genuine live-claims error not classified as a precondition: %v", err)
	}
}

// TestRollbackStillReportsGenuineRunningCelerySyncLeaseAsPrecondition is a
// second positive control: a RUNNING row with a lease that has not expired
// yet must still block, mirroring the dispatch-layer guard's capacity-
// consumer rule for RUNNING (sync/guard.py).
func TestRollbackStillReportsGenuineRunningCelerySyncLeaseAsPrecondition(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("close PostgreSQL: %v", err)
		}
	}()
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, celerySyncQuiescenceSchema+`;
		INSERT INTO public.sync_run_units (id, provider, dataset_key, status, updated_at, lease_expires_at) VALUES
			('00000000-0000-4000-8000-000000000006', 'launchdarkly', 'feature-flags', 'running', statement_timestamp() - interval '2 hours', statement_timestamp() + interval '10 minutes')`); err != nil {
		t.Fatal(err)
	}
	quiescer, err := NewPostgresCelerySyncProviderQuiescer(pool)
	if err != nil {
		t.Fatal(err)
	}
	probeErr := quiescer.Quiesce(ctx, "sync.provider_unit")
	if !errors.Is(probeErr, ErrLiveClaims) {
		t.Fatalf("Quiesce() error = %v, want %v", probeErr, ErrLiveClaims)
	}

	controller, err := NewController(pool, integrationRegistry{jobruntime.Descriptor{
		Kind: "sync.provider_unit", Route: "river_canary", RollbackRoute: "celery",
	}}, quiescer)
	if err != nil {
		t.Fatal(err)
	}
	_, err = controller.Rollback(ctx, "sync.provider_unit")
	if !errors.Is(err, ErrLiveClaims) {
		t.Fatalf("Rollback() error = %v, want %v", err, ErrLiveClaims)
	}
	if !IsPrecondition(err) {
		t.Fatalf("Rollback() genuine live-claims error not classified as a precondition: %v", err)
	}
}

// TestRollbackDoesNotBlockOnStaleOrphanedDispatchingRow reproduces CHAOS-3929
// directly: a DISPATCHING row that has sat untouched well past the
// stale-dispatch threshold, exactly the shape production hit when Celery
// workers were scaled to zero mid-cutover (zero lease owner, zero attempts,
// null heartbeat -- the only signal the Go quiescer has, updated_at, is
// stale). The route move it used to block must now succeed and actually
// reach the rollback transport.
func TestRollbackDoesNotBlockOnStaleOrphanedDispatchingRow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("close PostgreSQL: %v", err)
		}
	}()
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, celerySyncQuiescenceSchema+`;
		INSERT INTO public.sync_run_units (id, provider, dataset_key, status, updated_at) VALUES
			('00000000-0000-4000-8000-000000000007', 'launchdarkly', 'feature-flags', 'dispatching', statement_timestamp() - interval '2 hours')`); err != nil {
		t.Fatal(err)
	}
	quiescer, err := NewPostgresCelerySyncProviderQuiescer(pool)
	if err != nil {
		t.Fatal(err)
	}
	probeErr := quiescer.Quiesce(ctx, "sync.provider_unit")
	if probeErr != nil {
		t.Fatalf("Quiesce() error = %v, want nil for a stale orphaned dispatching row", probeErr)
	}

	controller, err := NewController(pool, integrationRegistry{jobruntime.Descriptor{
		Kind: "sync.provider_unit", Route: "river_canary", RollbackRoute: "celery",
	}}, quiescer)
	if err != nil {
		t.Fatal(err)
	}
	state, err := controller.Rollback(ctx, "sync.provider_unit")
	if err != nil {
		t.Fatalf("Rollback() error = %v, want nil", err)
	}
	if state.Transport != "celery" {
		t.Fatalf("Rollback() left transport = %q, want %q", state.Transport, "celery")
	}
	inspected, err := controller.Inspect(ctx, "sync.provider_unit")
	if err != nil {
		t.Fatal(err)
	}
	if inspected.Transport != "celery" {
		t.Fatalf("route did not actually move: %+v", inspected)
	}
}

// TestRollbackDoesNotBlockOnExpiredRunningLease is the RUNNING-side sibling
// of TestRollbackDoesNotBlockOnStaleOrphanedDispatchingRow: an explicitly
// expired lease proves the worker holding it is gone (mirrors sync/guard.py's
// capacity-consumer rule), so it must not block the route move either.
func TestRollbackDoesNotBlockOnExpiredRunningLease(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("close PostgreSQL: %v", err)
		}
	}()
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, celerySyncQuiescenceSchema+`;
		INSERT INTO public.sync_run_units (id, provider, dataset_key, status, updated_at, lease_expires_at) VALUES
			('00000000-0000-4000-8000-000000000008', 'launchdarkly', 'feature-flags', 'running', statement_timestamp() - interval '2 hours', statement_timestamp() - interval '10 minutes')`); err != nil {
		t.Fatal(err)
	}
	quiescer, err := NewPostgresCelerySyncProviderQuiescer(pool)
	if err != nil {
		t.Fatal(err)
	}
	probeErr := quiescer.Quiesce(ctx, "sync.provider_unit")
	if probeErr != nil {
		t.Fatalf("Quiesce() error = %v, want nil for an expired running lease", probeErr)
	}

	controller, err := NewController(pool, integrationRegistry{jobruntime.Descriptor{
		Kind: "sync.provider_unit", Route: "river_canary", RollbackRoute: "celery",
	}}, quiescer)
	if err != nil {
		t.Fatal(err)
	}
	state, err := controller.Rollback(ctx, "sync.provider_unit")
	if err != nil {
		t.Fatalf("Rollback() error = %v, want nil", err)
	}
	if state.Transport != "celery" {
		t.Fatalf("Rollback() left transport = %q, want %q", state.Transport, "celery")
	}
}

func waitForBlockedRouteUpdate(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	waitForLockWaiter(t, ctx, pool, "%worker_job_routes WHERE job_kind = $1%FOR UPDATE%", "rollback never blocked on producer route lock")
}

func waitForLockWaiter(t *testing.T, ctx context.Context, pool *pgxpool.Pool, queryPattern, timeoutMessage string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var waiting int
		if err := pool.QueryRow(ctx, `
			SELECT count(*) FROM pg_stat_activity
			WHERE wait_event_type = 'Lock'
			  AND query LIKE $1`, queryPattern,
		).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal(timeoutMessage)
}
