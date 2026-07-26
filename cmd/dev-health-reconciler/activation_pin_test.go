package main

import (
	"context"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/joboutbox"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchcontract"
	"github.com/full-chaos/dev-health-ops/internal/syncreconciler"
	"github.com/jackc/pgx/v5/pgxpool"
)

// checkedInReconcilerActivationPin is the reviewed expectation for every field of
// reconcilerActivation.
//
// syncMutation is the seam between observing the sync dispatch plane and owning
// it. The mutation pipeline below it is fully wired and reviewed -- that is
// deliberate, so flipping the flag cannot ship a half-built composition -- which
// means the flip itself is a one-character change with production consequences
// and nothing previously noticed it. dependencies.go states the intent directly:
// "changing from observation to mutation must retain concrete River delivery
// capabilities in the same reviewed source change."
//
// These tests enforce the FIRST half of that sentence and not the second. They
// make the flip visible and deliberate -- it cannot happen without editing a pin
// in the same commit. They cannot verify that concrete delivery capability is
// retained: set the flag and the pin together and everything here passes, 42501
// and all. A reviewer supplies that evidence; see CHAOS-3146. Reviewed twice now
// because the original wording claimed the whole sentence, and an overstated
// comment is how a green test becomes read as permission to flip.
//
// Flipping syncMutation today also ships a known 42501: the Materializer runs on
// the coordinator pool and inserts into public.sync_dispatch_outbox, which
// coordinatorPosture() declares without INSERT (CHAOS-3146). So the pin's current
// value is not merely "not yet" -- it is load-bearing.
//
// What this file does NOT enforce, corrected after adversarial review pointed out
// the original comment claimed otherwise: it cannot verify that flipping the seam
// RETAINS concrete River delivery capability. Set the flag and the pin together
// and both tests pass, 42501 and all. These tests prove the seam is dormant and
// that changing it is deliberate; the capability evidence is a human's job at
// review time, and CHAOS-3146 is the specific blocker. Do not let a green test
// here be read as permission to flip.
var checkedInReconcilerActivationPin = map[string]bool{
	"syncMutation": false,
}

// TestCheckedInReconcilerActivationMatchesItsPin fails on a flipped value, an
// added field, and a removed field.
//
// The structural halves matter more than the value. A test comparing the struct
// against its zero value would pass when a new dormant seam was added, because a
// new bool defaults to false -- so an unreviewed activation switch could enter
// the tree already invisible to the gate. Requiring the field SET to match makes
// the pin bidirectional.
func TestCheckedInReconcilerActivationMatchesItsPin(t *testing.T) {
	actual := reconcilerActivationFlags(
		t, checkedInReconcilerActivation, "reconcilerActivation",
	)
	assertPinnedActivationFlags(
		t, "reconcilerActivation", checkedInReconcilerActivationPin, actual,
	)
}

// TestProductionReconcilerSelectsTheShadowStepper asserts the state the process
// reaches, not the value of a variable.
//
// This is the pin that matters, and the structural one above is secondary. It
// enters through configureReconcilerDependenciesWithSourcesAndLogger -- the
// wrapper that supplies the PRODUCTION activation -- with fake sources whose
// mutation and shadow builders each record being called. If anything activates
// the mutation path, by any route (a second activation literal, an init-time
// assignment, a parallel activation type), the mutation builder runs and this
// fails. Adversarial review showed the structural pin alone is silently defeated
// by a value constructed at the call site; this is what closes that.
func TestProductionReconcilerSelectsTheShadowStepper(t *testing.T) {
	// The contract roots are repo-relative, so reach the stepper branch the same
	// way the neighbouring dependency tests do. Without this the configuration
	// bails out early and the test cannot see which stepper was chosen -- which it
	// detects and reports below rather than passing vacuously.
	t.Chdir(filepath.Join("..", ".."))

	var shadowBuilt, mutationBuilt bool

	sources := reconcilerSourcesForTest(t, &fakeReconcilerDatabase{})
	sources.buildRelay = func(
		*pgxpool.Pool, *pgxpool.Pool, *pgxpool.Pool, string, *jobruntime.Registry,
	) (joboutbox.RelayStepper, error) {
		return reconcilerStepFunc(func(
			context.Context, time.Time, int,
		) (joboutbox.StepResult, error) {
			return joboutbox.StepResult{}, nil
		}), nil
	}
	sources.buildSyncShadow = func(
		*pgxpool.Pool, *syncdispatchcontract.Registry,
	) (syncreconciler.Stepper, error) {
		shadowBuilt = true
		return syncStepFunc(func(
			context.Context, time.Time, int,
		) (syncreconciler.Observation, error) {
			return syncreconciler.Observation{}, nil
		}), nil
	}
	sources.buildSyncMutation = func(
		*pgxpool.Pool, *pgxpool.Pool, *pgxpool.Pool, string,
		*syncdispatchcontract.Registry,
	) (syncreconciler.Stepper, error) {
		mutationBuilt = true
		return syncStepFunc(func(
			context.Context, time.Time, int,
		) (syncreconciler.Observation, error) {
			return syncreconciler.Observation{}, nil
		}), nil
	}

	if _, err := configureReconcilerDependenciesWithSourcesAndLogger(
		context.Background(),
		config.Config{RiverDatabaseSchema: "river"},
		health.NewRegistry(100*time.Millisecond),
		reconcilerTestLogger(),
		sources,
	); err != nil {
		t.Fatalf("configuring the reconciler with fake sources failed: %v", err)
	}

	if mutationBuilt {
		t.Fatal(
			"the PRODUCTION reconciler configuration built the sync MUTATION " +
				"stepper. This process must observe, not mutate, until the seam is " +
				"deliberately flipped -- and flipping it today ships a 42501 " +
				"(CHAOS-3146). If activation is intended, that is a reviewed source " +
				"change: update the pin, record the capability evidence, and expect " +
				"this test to need rewriting.",
		)
	}
	if !shadowBuilt {
		t.Fatal(
			"the production reconciler configuration built NEITHER stepper, so this " +
				"test can no longer tell observation from mutation. Fix the test " +
				"before trusting it: a pin that cannot reach the branch it guards is " +
				"worse than no pin, because it reports success.",
		)
	}
}

// TestReconcilerSpecUsesTheConfigurationThisFilePins closes the chain from `main`
// to the function the behavioural pin exercises.
//
// The behavioural pin calls configureReconcilerDependenciesWithSourcesAndLogger
// so it can inject fakes. `shell.Main` does not call that directly -- it calls
// whatever reconcilerSpec names, which is configureReconcilerDependenciesWithLogger,
// which delegates to the sources-taking function with production sources. This
// test pins both ends of that chain: the spec field points where we think, and the
// delegate is the function under test.
//
// Precisely what IS and IS NOT covered, because the distinction is the whole point
// of this file: the spec field is asserted here; the ACTIVATION VALUE is asserted
// by the behavioural pin, because configureReconcilerDependenciesWithSourcesAndLogger
// is the sole supplier of checkedInReconcilerActivation. What is not asserted is
// the body of configureReconcilerDependenciesWithLogger beyond its identity -- if
// someone changed it to pass a different activation, the behavioural pin would not
// see it, because it enters below that point. That is the residual gap, and the
// mitigation is that the function is four lines of pure delegation. Do not let it
// grow logic.
func TestReconcilerSpecUsesTheConfigurationThisFilePins(t *testing.T) {
	if reconcilerSpec.ConfigureDependencies != nil {
		t.Fatal(
			"reconcilerSpec now sets ConfigureDependencies. shell.Main may call that " +
				"instead of ConfigureDependenciesWithLogger, so the pins here no longer " +
				"cover the production path. Retarget them.",
		)
	}
	if reconcilerSpec.ConfigureDependenciesWithLogger == nil {
		t.Fatal(
			"reconcilerSpec.ConfigureDependenciesWithLogger is nil, so these pins " +
				"cover nothing that production runs",
		)
	}

	// Function values are not comparable in Go; compare code pointers.
	pinned := reflect.ValueOf(configureReconcilerDependenciesWithLogger).Pointer()
	wired := reflect.ValueOf(reconcilerSpec.ConfigureDependenciesWithLogger).Pointer()
	if pinned != wired {
		t.Fatal(
			"reconcilerSpec.ConfigureDependenciesWithLogger is NOT " +
				"configureReconcilerDependenciesWithLogger. The pins therefore describe " +
				"a function the binary does not call, and activation could ship green. " +
				"Restore the wiring or retarget the pins -- do not delete this test.",
		)
	}
}

// reconcilerActivationFlags reads every direct field of the activation struct.
//
// Blank fields are rejected rather than collected: two `_ bool` fields collapse
// into one map key, so a second could be added without failing the exact-set
// check. A blank field cannot be a usable seam, so refusing them keeps the
// exact-set claim literally true.
func reconcilerActivationFlags(t *testing.T, value any, name string) map[string]bool {
	t.Helper()

	reflected := reflect.ValueOf(value)
	structType := reflected.Type()
	flags := make(map[string]bool, structType.NumField())
	for index := 0; index < structType.NumField(); index++ {
		field := structType.Field(index)
		if field.Name == "_" {
			t.Fatalf(
				"%s has a blank field at index %d. Blank fields collapse into one "+
					"map key, which would let a second be added without failing the "+
					"exact-set check -- and a blank field cannot be a seam.",
				name, index,
			)
		}
		if field.Type.Kind() != reflect.Bool {
			// reflect.Value.Bool is the accessor that works on an unexported
			// field; Interface() panics. A non-bool seam must force a deliberate
			// extension of this test rather than be silently skipped.
			t.Fatalf(
				"%s.%s is %s, not bool: extend this pin to cover the new kind "+
					"before adding it, or the seam ships unpinned",
				name, field.Name, field.Type.Kind(),
			)
		}
		flags[field.Name] = reflected.Field(index).Bool()
	}
	return flags
}

// TestAssertPinnedActivationFlagsFailsOnEveryDisagreement exercises the helper's
// failure branches directly.
//
// This helper is duplicated from the scheduler binary because the two are
// separate `package main`s. Without negative tests, removing either traversal
// during maintenance leaves this suite green while THIS binary silently loses its
// guard and the other keeps it -- an asymmetry that is easy to miss precisely
// because the two files look identical.
func TestAssertPinnedActivationFlagsFailsOnEveryDisagreement(t *testing.T) {
	cases := map[string]struct {
		pinned map[string]bool
		actual map[string]bool
	}{
		"flipped value":     {map[string]bool{"a": false}, map[string]bool{"a": true}},
		"field not pinned":  {map[string]bool{}, map[string]bool{"a": false}},
		"pin without field": {map[string]bool{"a": false}, map[string]bool{}},
	}
	for name, testCase := range cases {
		t.Run(name, func(t *testing.T) {
			probe := &testing.T{}
			assertPinnedActivationFlags(probe, "probe", testCase.pinned, testCase.actual)
			if !probe.Failed() {
				t.Fatalf(
					"assertPinnedActivationFlags accepted %q. One of its two "+
						"traversals is missing, so a real %s would pass unnoticed.",
					name, name,
				)
			}
		})
	}

	probe := &testing.T{}
	assertPinnedActivationFlags(
		probe, "probe", map[string]bool{"a": false}, map[string]bool{"a": false},
	)
	if probe.Failed() {
		t.Fatal("assertPinnedActivationFlags rejected a matching pin: too strict")
	}
}

// assertPinnedActivationFlags reports every disagreement rather than the first,
// so one run shows the whole delta.
func assertPinnedActivationFlags(t *testing.T, name string, pinned, actual map[string]bool) {
	t.Helper()

	for _, field := range sortedActivationKeys(actual) {
		expected, declared := pinned[field]
		if !declared {
			t.Errorf(
				"%s.%s exists in the struct but is not pinned. Add it with its "+
					"reviewed value; a new activation seam must not enter the tree "+
					"unpinned.",
				name, field,
			)
			continue
		}
		if actual[field] != expected {
			t.Errorf(
				"%s.%s is %t, pinned as %t. If this flip is intended, change the "+
					"pin in THIS commit and record the evidence for it; if it is "+
					"not, revert it -- this seam changes what the process owns.",
				name, field, actual[field], expected,
			)
		}
	}

	for _, field := range sortedActivationKeys(pinned) {
		if _, present := actual[field]; !present {
			t.Errorf(
				"%s.%s is pinned but no longer exists. Remove it from the pin in "+
					"the same commit that removed the field, so the pin cannot "+
					"accumulate expectations about seams that are gone.",
				name, field,
			)
		}
	}
}

func sortedActivationKeys(source map[string]bool) []string {
	keys := make([]string, 0, len(source))
	for key := range source {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// TestReconcilerActivationPinIsNotVacuous guards the guard: a pin comparing an
// empty set against an empty set passes forever while proving nothing, and that
// is invisible in a green run.
func TestReconcilerActivationPinIsNotVacuous(t *testing.T) {
	if len(checkedInReconcilerActivationPin) == 0 {
		t.Fatal("the activation pin is empty, so it cannot fail: it proves nothing")
	}

	structType := reflect.TypeOf(checkedInReconcilerActivation)
	for _, field := range sortedActivationKeys(checkedInReconcilerActivationPin) {
		if _, found := structType.FieldByName(field); !found {
			t.Errorf(
				"pinned key %q is not a field of reconcilerActivation: the pin is "+
					"asserting something about a name that does not exist",
				field,
			)
		}
	}
}
