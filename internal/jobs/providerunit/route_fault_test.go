package providerunit

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

// retryableCategory is the bounded category string jobruntime.Retryable
// renders. Asserting on it keeps the regression honest: a permanent or
// cancelling mark would change this value.
var retryableCategory = jobruntime.Retryable(errors.New("probe")).Error()

// TestUnroutableScopeNeverTerminalizesAsRouteDisabled is the TRD
// non-negotiable #3 regression: a provider unit delivered to River for a scope
// the Go descriptor does not serve must fail safe. Terminalizing it as a
// permanent route_disabled failure would silently discard real sync data
// whenever the Python producer gate is wider than the Go capability system.
func TestUnroutableScopeNeverTerminalizesAsRouteDisabled(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	for name, test := range map[string]struct {
		provider, dataset string
		costClass         providersync.CostClass
		switches          providersync.CompleteRouteSwitches
		descriptorPresent bool
	}{
		"descriptor absent for an unconfigured pair": {
			provider: "github", dataset: "feature-flags",
			costClass: providersync.CostMedium,
			// github has no feature-flags capability at all.
			descriptorPresent: false,
		},
		"descriptor present but not route ready": {
			provider: "pagerduty", dataset: "incidents",
			costClass:         providersync.CostLight,
			descriptorPresent: true,
		},
		"descriptor route ready but not enabled": {
			provider: "launchdarkly", dataset: "feature-flags",
			costClass: providersync.CostMedium,
			switches:  providersync.CompleteRouteSwitches{},
			// LaunchDarklyFeatureFlags is off.
			descriptorPresent: true,
		},
		"work-item alias held closed": {
			provider: "linear", dataset: "work-item-labels",
			costClass:         providersync.CostLight,
			switches:          providersync.CompleteRouteSwitches{LinearWorkItems: true},
			descriptorPresent: true,
		},
	} {
		test := test
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			unit := providerUnit()
			unit.Provider, unit.Dataset = test.provider, test.dataset
			unit.CostClass = test.costClass
			unit.DatasetOptions = map[string]any{}
			repository := newMemoryUnitRepository(unit)
			var faults []RouteFault
			handler := &Handler{
				Repository:    repository,
				Switches:      test.switches,
				LeaseDuration: time.Minute,
				Heartbeat:     10 * time.Second,
				Now:           func() time.Time { return now },
				BuildExecutor: func(
					*providersync.LeaseSession,
				) (providersync.CompleteRouteExecutor, error) {
					t.Fatal("an unroutable scope must never build an executor")
					return providersync.CompleteRouteExecutor{}, nil
				},
				OnRouteFault: func(fault RouteFault) {
					faults = append(faults, fault)
				},
			}

			err := handler.Work(
				context.Background(), providerExecution(unit, now, 1),
			)

			if !errors.Is(err, ErrRouteReconciliationRequired) {
				t.Fatalf("error=%v want %v", err, ErrRouteReconciliationRequired)
			}
			if err.Error() != retryableCategory {
				t.Fatalf("category=%q want %q", err.Error(), retryableCategory)
			}
			// The unit must remain live work, not a terminal failure.
			if repository.status != "dispatching" {
				t.Fatalf("unit status=%q want dispatching", repository.status)
			}
			if repository.failures != 0 {
				t.Fatalf("unit was terminalized %d times", repository.failures)
			}
			if repository.lastFailCategory == "route_disabled" {
				t.Fatal("unit terminalized as route_disabled")
			}
			if len(faults) != 1 {
				t.Fatalf("route faults=%v", faults)
			}
			fault := faults[0]
			if fault.Provider != test.provider || fault.Dataset != test.dataset ||
				fault.DescriptorPresent != test.descriptorPresent ||
				fault.RouteEnabled || !fault.Released || fault.Terminal {
				t.Fatalf("fault=%+v", fault)
			}
		})
	}
}

// TestRouteFaultAcrossEveryAttemptUpToTheCap is the post-cap stranding
// regression. River discards the job after the final attempt
// (max_attempts=5 in contracts/jobs/v1/registry.json), so releasing on that
// attempt would leave the unit `dispatching` with no live consumer. The
// producer outbox dedupes on `sync.provider_unit:<unit-id>`, so a stale
// redispatch reports "queued" without enqueueing anything and the sync run
// never finalizes. The terminal attempt must instead record an explicit
// durable reconciliation-required state.
func TestRouteFaultAcrossEveryAttemptUpToTheCap(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	const maxAttempts = 5
	unit := providerUnit()
	unit.Provider, unit.Dataset = "pagerduty", "incidents"
	unit.CostClass = providersync.CostLight
	unit.DatasetOptions = map[string]any{}
	repository := newMemoryUnitRepository(unit)
	var faults []RouteFault
	handler := &Handler{
		Repository:    repository,
		LeaseDuration: time.Minute,
		Heartbeat:     10 * time.Second,
		Now:           func() time.Time { return now },
		BuildExecutor: func(
			*providersync.LeaseSession,
		) (providersync.CompleteRouteExecutor, error) {
			t.Fatal("an unroutable scope must never build an executor")
			return providersync.CompleteRouteExecutor{}, nil
		},
		OnRouteFault: func(fault RouteFault) { faults = append(faults, fault) },
	}

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		execution := providerExecution(unit, now, attempt)
		execution.Definition.MaxAttempts = maxAttempts
		err := handler.Work(context.Background(), execution)
		if !errors.Is(err, ErrRouteReconciliationRequired) ||
			err.Error() != retryableCategory {
			t.Fatalf("attempt %d error=%v", attempt, err)
		}
		fault := faults[len(faults)-1]
		if fault.Attempt != attempt || fault.MaxAttempts != maxAttempts {
			t.Fatalf("attempt %d fault=%+v", attempt, fault)
		}
		if attempt < maxAttempts {
			// Still retriable: hand the claim back so a later attempt (or a
			// reconciled producer gate) can pick it up.
			if !fault.Released || fault.Terminal ||
				repository.status != "dispatching" || repository.failures != 0 {
				t.Fatalf(
					"attempt %d fault=%+v status=%q failures=%d",
					attempt, fault, repository.status, repository.failures,
				)
			}
			continue
		}
		// Terminal attempt: durable, distinguishable, alertable — and never
		// the silent route_disabled drop the TRD forbids.
		if !fault.Terminal || fault.Released {
			t.Fatalf("terminal fault=%+v", fault)
		}
		if repository.status != "failed" || repository.failures != 1 {
			t.Fatalf(
				"terminal status=%q failures=%d", repository.status, repository.failures,
			)
		}
		if repository.lastFailCategory != RouteReconciliationCategory {
			t.Fatalf("terminal category=%q", repository.lastFailCategory)
		}
		if repository.lastFailCategory == "route_disabled" {
			t.Fatal("unit terminalized as route_disabled")
		}
	}
	if len(faults) != maxAttempts {
		t.Fatalf("faults=%d want %d", len(faults), maxAttempts)
	}
}

// TestUnroutableScopeStaysRetryableWhenReleaseFails proves the fail-safe holds
// even when the claim cannot be handed back: the unit is left leased and
// recoverable through lease expiry, never terminalized.
func TestUnroutableScopeStaysRetryableWhenReleaseFails(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	unit := providerUnit()
	unit.Provider, unit.Dataset = "pagerduty", "services"
	unit.CostClass = providersync.CostLight
	unit.DatasetOptions = map[string]any{}
	repository := newMemoryUnitRepository(unit)
	repository.releaseErr = errors.New("release unavailable")
	var faults []RouteFault
	handler := &Handler{
		Repository:    repository,
		LeaseDuration: time.Minute,
		Heartbeat:     10 * time.Second,
		Now:           func() time.Time { return now },
		BuildExecutor: func(
			*providersync.LeaseSession,
		) (providersync.CompleteRouteExecutor, error) {
			t.Fatal("an unroutable scope must never build an executor")
			return providersync.CompleteRouteExecutor{}, nil
		},
		OnRouteFault: func(fault RouteFault) { faults = append(faults, fault) },
	}

	err := handler.Work(context.Background(), providerExecution(unit, now, 1))

	if err == nil || err.Error() != retryableCategory {
		t.Fatalf("category=%v want %q", err, retryableCategory)
	}
	if repository.failures != 0 || repository.status == "failed" {
		t.Fatalf("unit terminalized: status=%q failures=%d", repository.status, repository.failures)
	}
	if len(faults) != 1 || faults[0].Released || faults[0].Terminal {
		t.Fatalf("faults=%v", faults)
	}
}

// TestDeterministicIdentityFaultTerminalizesOnFirstAttempt covers the
// repository-identity fault. It cannot clear on retry, so burning the
// remaining attempts would only delay the outcome and then hide the cause
// behind the generic provider_unit_exhausted category.
func TestDeterministicIdentityFaultTerminalizesOnFirstAttempt(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	repository := newMemoryUnitRepository(providerUnit())
	handler := &Handler{
		Repository: repository,
		Switches: providersync.CompleteRouteSwitches{
			LaunchDarklyFeatureFlags: true,
		},
		LeaseDuration: time.Minute,
		Heartbeat:     10 * time.Second,
		Now:           func() time.Time { return now },
		BuildExecutor: func(
			*providersync.LeaseSession,
		) (providersync.CompleteRouteExecutor, error) {
			return providersync.CompleteRouteExecutor{},
				providersync.ErrRepositoryIdentityAmbiguous
		},
	}
	execution := providerExecution(repository.unit, now, 1)
	execution.Definition.MaxAttempts = 5

	err := handler.Work(context.Background(), execution)

	if !errors.Is(err, providersync.ErrRepositoryIdentityAmbiguous) {
		t.Fatalf("error=%v", err)
	}
	if err.Error() == retryableCategory {
		t.Fatalf("deterministic fault was marked retryable: %v", err)
	}
	if repository.status != "failed" || repository.failures != 1 {
		t.Fatalf("status=%q failures=%d", repository.status, repository.failures)
	}
	if repository.lastFailCategory != RepositoryIdentityCategory {
		t.Fatalf("category=%q", repository.lastFailCategory)
	}
}
