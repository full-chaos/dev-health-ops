package main

import (
	"context"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
)

// This file pins the scheduler's activation seam two ways, and the order matters
// because only the first one is about the binary.
//
// The BEHAVIOURAL pin below calls the PRODUCTION entry point --
// configureSchedulerDependencies, the function shell.Main reaches -- and asserts
// what the process actually does. That is the property worth having: it holds no
// matter where the activation value comes from, so constructing a second
// activation literal, assigning one at init time, or introducing a parallel
// activation type cannot slip past it.
//
// The STRUCTURAL pin is secondary. It compares the checked-in activation value's
// fields against a reviewed map, which catches a flipped flag and an added seam
// early and with a precise message -- but it only describes that one variable. An
// earlier version of this file had ONLY the structural pin and claimed it made
// the seam's intent "enforceable". That was the same mistake this epic keeps
// producing: it verified the property its author was thinking about (the
// variable) rather than the one that mattered (what the binary activates).
// Adversarial review showed a production value constructed elsewhere passes it
// silently. Keep both, and do not let the structural one grow claims about the
// binary.
//
// What NEITHER pin can do, stated so nobody infers otherwise: they cannot prove
// that flipping a seam retains the delivery capability the seam exists to guard.
// Pinning goOwnsMarkers false says the process is dormant; it says nothing about
// whether a materializer exists once it is true. That evidence is a human's job
// at review time, and CHAOS-3145 is where the requirement lives.

// checkedInSchedulerActivationPin is the reviewed expectation for every field of
// schedulerActivation. Update it in the SAME commit that flips a flag, with the
// evidence for the flip in the commit message.
var checkedInSchedulerActivationPin = map[string]bool{
	"goOwnsMarkers": false,
}

// schedulerReadinessNamesClosedWhenDormant are the readiness names the dormant
// path must register as unavailable. Hard-coded rather than read from the
// production call, because a test that asks the code under test what it should
// have done proves nothing.
var schedulerReadinessNamesClosedWhenDormant = []string{
	"domain_postgres",
	"queue_postgres",
	"coordinator_postgres",
	"river_schema",
	"scheduler_loop",
}

// TestProductionSchedulerConfigurationIsDormant asserts the state the process
// reaches, not the value of a variable.
//
// This is the pin that survives a second activation literal, an init-time
// assignment, a parallel activation type, and a test-only reset of the global:
// it goes through the production wrapper and checks the outcome. If someone
// activates the scheduler by any route, this test fails.
func TestProductionSchedulerConfigurationIsDormant(t *testing.T) {
	registry := health.NewRegistry(time.Second)

	components, err := configureSchedulerDependencies(
		context.Background(), config.Config{}, registry,
	)

	if err != nil {
		t.Fatalf(
			"the production scheduler configuration returned an error (%v). The "+
				"dormant path is expected to register closed readiness and return "+
				"nil error; an error here means this test can no longer tell "+
				"dormant from activated, so fix the test before trusting it.",
			err,
		)
	}
	if len(components) != 0 {
		t.Fatalf(
			"the production scheduler configuration built %d lifecycle "+
				"component(s). A dormant scheduler must build none and must not "+
				"open a PostgreSQL pool. If activation is intended, that is a "+
				"reviewed source change: update the pin in this file, record the "+
				"evidence, and expect this test to need rewriting.",
			len(components),
		)
	}

	// SetReady is what a started process does; do it here so Readiness reports
	// the per-name checks rather than short-circuiting on "runtime".
	registry.SetReady(true)
	readiness := registry.Readiness(context.Background())
	if readiness.Ready {
		t.Fatal(
			"the dormant production configuration reports READY. Every externally " +
				"visible name must stay closed while the scheduler owns nothing, or " +
				"an operator sees a healthy scheduler that schedules nothing.",
		)
	}
	failed := make(map[string]bool, len(readiness.Failed))
	for _, name := range readiness.Failed {
		failed[name] = true
	}
	for _, name := range schedulerReadinessNamesClosedWhenDormant {
		if !failed[name] {
			t.Errorf(
				"readiness name %q is not among the failing checks %v. The dormant "+
					"path must register every one of these as unavailable; a name that "+
					"is simply absent is worse than one that fails, because /readyz "+
					"cannot report on a check nobody registered.",
				name, readiness.Failed,
			)
		}
	}
}

// TestSchedulerSpecUsesTheConfigurationThisFilePins pins the spec-to-function
// link. It does not close the chain from `main`, and an earlier version of this
// comment claimed it did -- see the coverage statement below, which review
// required after the reconciler file made the same overstatement.
//
// The behavioural pin calls configureSchedulerDependencies directly. That proves
// what THAT function does; it does not prove `shell.Main` reaches it. Point
// schedulerSpec at a different configure function and the pin keeps passing while
// the binary runs something else entirely -- the same defeat as the original
// version of this file, one level further out. Comparing the spec's function
// pointer to the function under test makes the chain complete:
// main -> shell.Main(schedulerSpec) -> this field -> the tested function.
//
// Function values are not comparable in Go, so compare code pointers. A nil field
// is also a failure: it would mean the binary has no configuration at all, or
// that the logger-taking variant is now in use and this pin no longer covers the
// path production takes.
//
// PINNED: the spec field IS configureSchedulerDependencies, and the logger-taking
// field is nil so shell.Main cannot take the other one. Combined with the
// behavioural pin above -- which calls that exact function -- the link from the
// spec to the observed dormant behaviour is covered.
//
// NOT PINNED: `main()` calling shell.Main(schedulerSpec). A test cannot observe
// main(), so a binary rewired to a different spec would not fail here. And, as
// stated above, neither pin proves that flipping the seam retains the capability
// the seam guards.
func TestSchedulerSpecUsesTheConfigurationThisFilePins(t *testing.T) {
	if schedulerSpec.ConfigureDependenciesWithLogger != nil {
		t.Fatal(
			"schedulerSpec now sets ConfigureDependenciesWithLogger. shell.Main may " +
				"call that instead of ConfigureDependencies, so the behavioural pin " +
				"below no longer proves anything about the production path. Retarget " +
				"the pin at whichever field shell.Main actually invokes.",
		)
	}
	if schedulerSpec.ConfigureDependencies == nil {
		t.Fatal(
			"schedulerSpec.ConfigureDependencies is nil, so this pin covers nothing " +
				"that production runs",
		)
	}

	pinned := reflect.ValueOf(configureSchedulerDependencies).Pointer()
	wired := reflect.ValueOf(schedulerSpec.ConfigureDependencies).Pointer()
	if pinned != wired {
		t.Fatal(
			"schedulerSpec.ConfigureDependencies is NOT " +
				"configureSchedulerDependencies. The behavioural pin therefore tests a " +
				"function the binary does not call, and activation could ship green. " +
				"Either restore the wiring or retarget the pin at the function " +
				"shell.Main really invokes -- do not delete this test.",
		)
	}
}

// TestCheckedInSchedulerActivationMatchesItsPin is the structural pin: a flipped
// value, an added field, and a removed field each fail.
//
// The two structural halves matter as much as the value. A test comparing the
// struct against its zero value would PASS when a new seam was added, because a
// new bool defaults to false -- so an unreviewed switch could enter the tree
// already invisible. Requiring the field SET to match is what makes this
// bidirectional for this variable.
func TestCheckedInSchedulerActivationMatchesItsPin(t *testing.T) {
	actual := activationFlagsOf(t, checkedInSchedulerActivation, "schedulerActivation")
	assertPinnedFlags(t, "schedulerActivation", checkedInSchedulerActivationPin, actual)
}

// activationFlagsOf reads every direct field of an activation struct by name.
//
// Blank fields are rejected rather than collected: two `_ bool` fields collapse
// into one map key, so the second would be added without failing the exact-set
// check. A blank field cannot be a usable activation seam anyway, so refusing
// them keeps the exact-set claim literally true instead of nearly true.
func activationFlagsOf(t *testing.T, value any, name string) map[string]bool {
	t.Helper()

	reflected := reflect.ValueOf(value)
	structType := reflected.Type()
	flags := make(map[string]bool, structType.NumField())
	for index := 0; index < structType.NumField(); index++ {
		field := structType.Field(index)
		if field.Name == "_" {
			t.Fatalf(
				"%s has a blank field at index %d. Blank fields collapse into one "+
					"map key, which would let a second one be added without failing "+
					"the exact-set check -- and a blank field cannot be a seam.",
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

// assertPinnedFlags reports every disagreement rather than the first, so one run
// shows the whole delta.
func assertPinnedFlags(t *testing.T, name string, pinned, actual map[string]bool) {
	t.Helper()

	for _, field := range sortedKeys(actual) {
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
					"pin in THIS commit and record the evidence; if it is not, "+
					"revert it -- this seam changes what the process owns.",
				name, field, actual[field], expected,
			)
		}
	}

	for _, field := range sortedKeys(pinned) {
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

func sortedKeys(source map[string]bool) []string {
	keys := make([]string, 0, len(source))
	for key := range source {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// TestAssertPinnedFlagsFailsOnEveryDisagreement exercises the helper's failure
// branches directly.
//
// Without this, the helper is only ever called with a matching singleton, so
// deleting either of its two loops during maintenance leaves the suite green
// while one binary silently loses its guard -- the duplicate copy in
// dev-health-reconciler makes that asymmetry easy to miss. Each case below fails
// if and only if the corresponding loop exists.
func TestAssertPinnedFlagsFailsOnEveryDisagreement(t *testing.T) {
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
			assertPinnedFlags(probe, "probe", testCase.pinned, testCase.actual)
			if !probe.Failed() {
				t.Fatalf(
					"assertPinnedFlags accepted %q. One of its two traversals is "+
						"missing, so a real %s would pass unnoticed.",
					name, name,
				)
			}
		})
	}

	probe := &testing.T{}
	assertPinnedFlags(
		probe, "probe", map[string]bool{"a": false}, map[string]bool{"a": false},
	)
	if probe.Failed() {
		t.Fatal("assertPinnedFlags rejected a matching pin: it is too strict to use")
	}
}

// TestSchedulerActivationPinIsNotVacuous guards the guard: a pin comparing an
// empty set against an empty set passes forever while proving nothing, and that
// is invisible in a green run.
func TestSchedulerActivationPinIsNotVacuous(t *testing.T) {
	if len(checkedInSchedulerActivationPin) == 0 {
		t.Fatal("the activation pin is empty, so it cannot fail: it proves nothing")
	}

	structType := reflect.TypeOf(checkedInSchedulerActivation)
	for _, field := range sortedKeys(checkedInSchedulerActivationPin) {
		if _, found := structType.FieldByName(field); !found {
			t.Errorf(
				"pinned key %q is not a field of schedulerActivation: the pin is "+
					"asserting something about a name that does not exist",
				field,
			)
		}
	}
}
