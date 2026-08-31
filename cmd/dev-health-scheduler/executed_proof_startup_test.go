package main

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	schedulersync "github.com/full-chaos/dev-health-ops/internal/scheduler/sync"
	"github.com/jackc/pgx/v5/pgxpool"
)

// stubExecutedProofLoader fails a fixed number of times, then succeeds.
type stubExecutedProofLoader struct {
	failuresBeforeSuccess int
	attempts              int
	loaded                bool
	// deadlines records the budget each attempt actually got, so the test can
	// prove every attempt is bounded rather than inheriting one shared clock.
	deadlines []time.Duration
}

func (loader *stubExecutedProofLoader) RefreshExecutedProof(ctx context.Context) error {
	loader.attempts++
	if deadline, ok := ctx.Deadline(); ok {
		loader.deadlines = append(loader.deadlines, time.Until(deadline).Round(time.Second))
	} else {
		loader.deadlines = append(loader.deadlines, 0)
	}
	if loader.attempts <= loader.failuresBeforeSuccess {
		return errors.New("timeout: context deadline exceeded")
	}
	loader.loaded = true
	return nil
}

func (loader *stubExecutedProofLoader) HasLoadedExecutedProof() bool { return loader.loaded }

// TestLoadExecutedProofAtStartupRetriesInsteadOfSettlingForEmptyDegraded is
// the CHAOS-4124 red control, expressed as the counterfactual the incident
// actually turned on.
//
// The scheduler restarted at 15:16Z, made ONE 30s evidence-load attempt
// against a database busy with recovery load, lost it, and installed an empty
// Degraded snapshot that blocked every non-waived route for eight hours. The
// database was not down -- the 23:53Z restart's identical first attempt
// succeeded. One attempt was the whole difference.
//
// failuresBeforeSuccess=1 is that exact shape: a single-attempt loader gives
// up (the first assertion, which is the pre-fix behavior) and a retrying one
// recovers on the very next try.
func TestLoadExecutedProofAtStartupRetriesInsteadOfSettlingForEmptyDegraded(t *testing.T) {
	slept := []time.Duration{}
	loader := &stubExecutedProofLoader{failuresBeforeSuccess: 1}
	if err := loadExecutedProofAtStartup(loader, func(d time.Duration) {
		slept = append(slept, d)
	}); err != nil {
		t.Fatalf("a load that succeeds on the second attempt still failed: %v", err)
	}
	if loader.attempts != 2 {
		t.Fatalf("attempts=%d, want exactly 2 -- one failure then one success, and no more",
			loader.attempts)
	}
	if !loader.HasLoadedExecutedProof() {
		t.Fatal("the loader reports evidence never loaded after a successful retry")
	}
	if len(slept) != 1 || slept[0] != executedProofStartupInitialBackoff {
		t.Fatalf("backoff schedule=%v, want exactly one %s wait", slept,
			executedProofStartupInitialBackoff)
	}
	for _, budget := range loader.deadlines {
		if budget <= 0 || budget > executedProofStartupAttemptBudget {
			t.Fatalf("attempt budgets=%v, want each within (0, %s] -- an unbounded "+
				"attempt is how one doomed statement held the whole startup",
				loader.deadlines, executedProofStartupAttemptBudget)
		}
	}
}

// TestLoadExecutedProofAtStartupStopsInsteadOfRetryingForever pins the other
// half. Trading CHAOS-4124's silent outage for a process that never finishes
// constructing is not an improvement: an unreachable database must end with a
// returned error, a loud log, and a scheduler that comes up and reports itself
// unhealthy, not with a hung boot.
func TestLoadExecutedProofAtStartupStopsInsteadOfRetryingForever(t *testing.T) {
	slept := []time.Duration{}
	loader := &stubExecutedProofLoader{failuresBeforeSuccess: 1 << 30}
	err := loadExecutedProofAtStartup(loader, func(d time.Duration) {
		slept = append(slept, d)
	})
	if err == nil {
		t.Fatal("a permanently failing load reported success")
	}
	if loader.attempts != executedProofStartupAttempts {
		t.Fatalf("attempts=%d, want the declared bound %d",
			loader.attempts, executedProofStartupAttempts)
	}
	if len(slept) != executedProofStartupAttempts-1 {
		t.Fatalf("waits=%d, want %d -- the last attempt must not sleep before giving up",
			len(slept), executedProofStartupAttempts-1)
	}
	// Backoff grows and is capped. An uncapped doubling would push total
	// startup delay past any sane deploy timeout on its own.
	var total time.Duration
	for index, wait := range slept {
		if wait > executedProofStartupMaxBackoff {
			t.Fatalf("wait %d = %s exceeds the %s cap", index, wait,
				executedProofStartupMaxBackoff)
		}
		total += wait
	}
	if total > executedProofStartupTotalBudget {
		t.Fatalf("total backoff %s exceeds the %s budget", total,
			executedProofStartupTotalBudget)
	}
	if loader.HasLoadedExecutedProof() {
		t.Fatal("the loader claims evidence loaded after only failures")
	}
}

// TestLoadExecutedProofAtStartupRefusesAnUnusableCaller keeps the helper from
// silently reporting success when it was handed nothing to do -- a nil loader
// returning nil would read exactly like a healthy load.
func TestLoadExecutedProofAtStartupRefusesAnUnusableCaller(t *testing.T) {
	if err := loadExecutedProofAtStartup(nil, func(time.Duration) {}); err == nil {
		t.Error("a nil loader reported a successful evidence load")
	}
	if err := loadExecutedProofAtStartup(&stubExecutedProofLoader{}, nil); err == nil {
		t.Error("a nil sleep function reported a successful evidence load")
	}
}

// evidenceReportingStepper is a stub occurrence stepper that also answers the
// CHAOS-4124 readiness question, so the composition can register the check.
type evidenceReportingStepper struct{ loaded *atomic.Bool }

func (evidenceReportingStepper) Reconcile(
	context.Context, time.Time, int,
) (schedulersync.OccurrenceReconcileResult, error) {
	return schedulersync.OccurrenceReconcileResult{}, nil
}

func (stepper evidenceReportingStepper) HasLoadedExecutedProof() bool {
	return stepper.loaded.Load()
}

// TestSchedulerReadinessClosesWhileExecutedProofHasNeverLoaded is the other
// half of the CHAOS-4124 fix, and the one that answers "why did nobody
// notice for eight hours".
//
// The scheduler was reporting READY the entire time. It completed every
// occurrence successfully; they just planned nothing. The gate's own failure
// was one ERROR line at 15:17 that nothing alerted on. So the fix is not only
// to retry harder -- it is to make the state observable where deploy tooling
// and operators already look.
//
// Both directions are asserted deliberately. Readiness must CLOSE while
// evidence has never loaded, and it must OPEN again the moment a refresh
// succeeds, with no restart: maybeRefreshExecutedProof keeps trying, so a
// check that latched closed would turn a transient database blip into a
// permanently unhealthy deployment.
func TestSchedulerReadinessClosesWhileExecutedProofHasNeverLoaded(t *testing.T) {
	loaded := &atomic.Bool{}
	database := &fakeSchedulerDatabase{pool: &pgxpool.Pool{}, coordinatorPool: &pgxpool.Pool{}}
	registry := health.NewRegistry(time.Second)
	component, err := buildSchedulerLoopWithSources(
		context.Background(),
		config.Config{RiverDatabaseSchema: "river"},
		registry,
		schedulerRuntimeSources{
			openDatabase: func(context.Context, config.Config) (schedulerDatabase, error) {
				return database, nil
			},
			newRepository: func(*pgxpool.Pool) (schedulersync.HandoffStepper, error) {
				return schedulerHandoffStepperFunc(func(
					context.Context, time.Time, int, schedulersync.Coordinator,
				) (schedulersync.HandoffResult, error) {
					return schedulersync.HandoffResult{}, nil
				}), nil
			},
			newCoordinator: schedulersync.NewOccurrenceCoordinator,
			newLoop:        schedulersync.NewLoop,
			newOccurrences: func(*pgxpool.Pool, *pgxpool.Pool, config.Config) (schedulersync.OccurrenceStepper, error) {
				return evidenceReportingStepper{loaded: loaded}, nil
			},
			newFixedLoop: func(*pgxpool.Pool, *health.Registry, *slog.Logger) (fixedScheduleRuntime, error) {
				return &fakeFixedLoop{}, nil
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := component.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = component.Shutdown(context.Background()) }()
	if err := (health.Gate{Registry: registry}).Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	status := registry.Readiness(context.Background())
	if status.Ready {
		t.Fatal("the scheduler reported READY while executed-proof evidence had never " +
			"loaded -- that is exactly the eight hours of CHAOS-4124, in which every " +
			"non-waived route was blocked and the process looked healthy")
	}
	if !slices.Contains(status.Failed, "executed_proof_evidence") {
		t.Fatalf("readiness closed for the wrong reason: %#v", status.Failed)
	}

	// A later refresh succeeds. Nothing restarts; readiness must reopen on
	// its own.
	loaded.Store(true)
	if status := registry.Readiness(context.Background()); !status.Ready {
		t.Fatalf("readiness stayed closed after evidence loaded: %#v", status)
	}
}
