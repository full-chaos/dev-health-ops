package main

import (
	"context"
	"errors"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/platform/selfprobe"
)

// TestExecutionLivenessCatchesAWedgedDomainPoolAfterAdmission is the direct
// red-on-baseline reproduction of the CHAOS-4029 incident: on 2026-08-20 the
// domain pool moved 17 seconds after every worker had already passed
// preclaim readiness, and every job failed at PostgresIdempotency.Begin for
// two hours while /readyz kept answering 200, because nothing re-observed
// the process's own execution path after admission.
//
// This test proves the fix: idempotency_backend (synchronous) and
// execution_liveness (ticking self-probe) both start healthy once the
// process is admitted, both flip a running process to NOT ready once the
// domain pool wedges -- with NO restart and NO reconstruction of
// dependencies, exactly mirroring "the pool moved under a live process" --
// and both self-heal the moment the dependency recovers.
//
// Before the CHAOS-4029 fix (i.e. on origin/main, before this change),
// neither idempotency_backend nor execution_liveness exists as a check
// name, and domain_postgres alone does not reproduce this failure mode: it
// tests role POSTURE via a SELECT-shaped introspection query, not a live
// Begin/transaction round trip, so it stays green through exactly the
// class of failure this test injects. Running this test against that
// baseline fails immediately at the components[2].(*selfprobe.Monitor) type
// assertion below (the baseline's third component is preclaim-readiness,
// not a liveness monitor, since neither new check nor the monitor exists
// yet) -- which is the point: this is a genuinely new, previously-absent
// signal, not a restatement of an existing one.
func TestExecutionLivenessCatchesAWedgedDomainPoolAfterAdmission(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	queues := []string{"coverage", "heartbeat", "retention", "webhooks"}
	runtimeRegistry, err := jobruntime.Load(defaultContractRoot)
	if err != nil {
		t.Fatal(err)
	}
	database := &fakeWorkerDatabase{}
	sources := productionWorkerDependencySources
	sources.openDatabase = func(context.Context, config.Config) (workerDatabase, error) {
		return database, nil
	}
	// fakeWorkerDatabase is not *postgresWorkerDatabase, so the production
	// operational-family and River-process builders (which both need a real
	// pgxpool-backed database) cannot compose it -- neither concern is what
	// this test is about. Swap in fakes that report exactly the same
	// handlers/queues the production operational builder would for this
	// queue selection (proven by TestProductionOperationalBuilderConstructsNativeSyncCoverageRefresh
	// against the same registry and queue set), so queue_completeness and
	// job_registry both pass for real, and this test's readiness assertions
	// exercise the real registered CHAOS-4029 checks end-to-end.
	sources.buildOperational = fakeHandlerBuilder(
		"operational",
		mustSelectedQueueSpecs(t, runtimeRegistry, queues...),
		selectedQueueBudgets(queues, queues, map[string]int{
			"coverage": 1, "heartbeat": 1, "retention": 1, "webhooks": 4,
		})...,
	)
	sources.buildRiverProcess = fakeRiverProcessBuilder("river-worker")

	registry := health.NewRegistry(2 * time.Second)
	components, err := configureWorkerDependenciesWithSources(
		context.Background(),
		config.Config{
			Queues:                 queues,
			WorkerQueueConcurrency: map[string]int{"coverage": 1, "heartbeat": 1, "retention": 1, "webhooks": 4},
			RiverDatabaseSchema:    "river",
			DomainDatabaseMaxConns: 4,
			QueueDatabaseMaxConns:  2,
		},
		registry,
		sources,
	)
	if err != nil {
		t.Fatalf("configureWorkerDependenciesWithSources() error = %v", err)
	}

	// The execution_liveness monitor is the third registered component (see
	// the ordering asserted by TestCeleryRoutedHandlersCannotPassQueueCompleteness).
	// Shrink its sampling interval and staleness window from the production
	// defaults (20s / 60s) to real-but-small durations BEFORE Start, so this
	// test proves the same staleness mechanics selfprobe's own unit tests
	// prove with a fake clock, but end-to-end through the real wiring, in
	// well under a second of wall-clock time instead of a minute.
	livenessMonitor, ok := components[2].(*selfprobe.Monitor)
	if !ok {
		t.Fatalf("components[2] = %#v, want *selfprobe.Monitor", components[2])
	}
	livenessMonitor.SetInterval(20 * time.Millisecond)
	livenessMonitor.SetStaleness(80 * time.Millisecond)

	// Every registered component starts, exactly as the real lifecycle
	// runtime does before opening the readiness gate (cmd's shell always
	// runs Start on every returned component ahead of health.Gate.Start --
	// see internal/platform/lifecycle). river-workers is skipped: its
	// worker-presence wiring requires a real *postgresWorkerDatabase, which
	// is orthogonal to what this test proves (readiness signal behavior),
	// and fakeRiverProcessBuilder above already keeps its OWN Start a no-op
	// -- only the presence field, populated unconditionally in
	// configureWorkerDependenciesWithSources regardless of which River
	// process builder ran, needs a real database.
	for _, component := range components {
		if component.Name() == "river-workers" {
			continue
		}
		if err := component.Start(context.Background()); err != nil {
			t.Fatalf("start %s: %v", component.Name(), err)
		}
	}
	if err := (health.Gate{Registry: registry}).Start(context.Background()); err != nil {
		t.Fatalf("open readiness gate: %v", err)
	}

	// Admission: the process is healthy and ready, exactly like the four
	// workers that passed preclaim-readiness at 20:32:21 on 2026-08-20.
	status := registry.Readiness(context.Background())
	if !status.Ready {
		t.Fatalf("expected the freshly admitted worker to be ready, got %#v", status)
	}

	// The incident: the domain pool wedges AFTER admission, with the process
	// still running and never restarted -- pgbouncer-1 was recreated 17
	// seconds after the last successful claim, and nothing about the worker
	// process itself changed.
	database.setTxOpenerErr(errors.New("dependency_unavailable"))

	// idempotency_backend is synchronous and re-evaluated on every /readyz
	// poll (health.Registry.CheckRequired), so it must already be failing
	// on the very next poll -- no staleness window needed for this one.
	status = registry.Readiness(context.Background())
	if status.Ready {
		t.Fatal("expected readiness to fail once the domain pool wedges, got Ready=true")
	}
	if !slices.Contains(status.Failed, "idempotency_backend") {
		t.Fatalf("expected idempotency_backend to fail immediately, got failed=%v", status.Failed)
	}

	// execution_liveness is staleness-based (it proves the process's OWN
	// background loop is still pumping, not just that this one HTTP request
	// could reach the database) -- give its ticking self-probe time to
	// observe the same wedge and cross the (deliberately shrunk) staleness
	// window.
	deadline := time.Now().Add(2 * time.Second)
	for {
		status = registry.Readiness(context.Background())
		if slices.Contains(status.Failed, "execution_liveness") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("execution_liveness never failed after the domain pool wedged; failed=%v", status.Failed)
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Self-heal: the ticket requires recovery WITHOUT a restart the moment
	// the dependency comes back, mirroring "recovers on its own" from the
	// CHAOS-4029 acceptance criterion.
	database.setTxOpenerErr(nil)
	deadline = time.Now().Add(2 * time.Second)
	for {
		status = registry.Readiness(context.Background())
		if status.Ready {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("readiness did not self-heal after the domain pool recovered; failed=%v", status.Failed)
		}
		time.Sleep(5 * time.Millisecond)
	}

	for _, component := range components {
		if component.Name() == "river-workers" {
			continue
		}
		shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		if err := component.Shutdown(shutdownCtx); err != nil {
			t.Errorf("shutdown %s: %v", component.Name(), err)
		}
		cancel()
	}
}
