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
				fault.RouteEnabled || !fault.Released {
				t.Fatalf("fault=%+v", fault)
			}
		})
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
	if len(faults) != 1 || faults[0].Released {
		t.Fatalf("faults=%v", faults)
	}
}
