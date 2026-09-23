package schedulerservice

import (
	"context"
	"log/slog"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/platform/lifecycle"
)

// This file pins the scheduler's activation seam two ways, and the order matters
// because only the first one is about the binary.
//
// The BEHAVIOURAL pin below calls the PRODUCTION entry point --
// configureSchedulerDependencies, the function shell.Execute reaches -- and asserts
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
// The source-level construction and readiness proof lives in
// TestSchedulerProductionFactoryBuildsReviewedRuntime; deployment remains a
// separate, live-environment acceptance step.

// checkedInSchedulerActivationPin is the reviewed expectation for every field of
// schedulerActivation. Update it in the SAME commit that flips a flag, with the
// evidence for the flip in the commit message.
var checkedInSchedulerActivationPin = map[string]bool{
	"goOwnsMarkers": true,
}

// TestProductionSchedulerConfigurationBuildsTheReviewedLoop asserts the state
// the process reaches, not the value of a variable.
//
// This is the pin that survives a second activation literal, an init-time
// assignment, a parallel activation type, and a test-only reset of the global:
// it goes through the production wrapper and checks the outcome. If someone
// disables the scheduler by any route, this test fails.
//
// The cfg passed in is config.Load's own real output for schedulerSpec.Service
// with no environment set, not a hand-built config.Config{} literal.
// Adversarial review pointed out twice: first that an empty config leaves room
// for a future configureSchedulerDependencies to branch on cfg.Service and
// activate only for realistic input; then, after that was fixed by adding just
// Service, that a hand-picked subset still isn't what production computes --
// every other field a future refactor might branch on (for instance
// cfg.CoordinatorDatabaseRole, which config.Load defaults to a non-empty
// value even with no environment set) was still left zero, unlike production.
// Calling config.Load itself removes the gap between "what this test passes"
// and "what production would actually compute for this input", for every
// field config.Load can populate without a real secret being set.
//
// The production factory is replaced only to avoid opening real PostgreSQL
// pools. TestSchedulerProductionFactoryBuildsReviewedRuntime separately proves
// that the real factory builds and starts both the product and fixed loops with
// their readiness checks.
func TestProductionSchedulerConfigurationBuildsTheReviewedLoop(t *testing.T) {
	original := productionSchedulerDependencySources
	t.Cleanup(func() { productionSchedulerDependencySources = original })

	built := false
	productionSchedulerDependencySources = schedulerDependencySources{
		buildLoop: func(_ context.Context, cfg config.Config, registry *health.Registry, _ ...*slog.Logger) (lifecycle.Component, error) {
			built = true
			if cfg.Service != schedulerSpec.Service || registry == nil {
				t.Fatalf("production config/service=%q registry=%v", cfg.Service, registry)
			}
			return schedulerTestComponent{}, nil
		},
	}
	registry := health.NewRegistry(time.Second)

	cfg, err := config.Load(config.Spec{
		Service:   schedulerSpec.Service,
		LookupEnv: func(string) (string, bool) { return "", false },
	})
	if err != nil {
		t.Fatalf("loading a production-defaulted config for %q: %v", schedulerSpec.Service, err)
	}

	components, err := configureSchedulerDependencies(context.Background(), cfg, registry, nil)

	if err != nil || !built || len(components) != 1 || components[0].Name() != "scheduler-test-loop" {
		t.Fatalf("production activation built=%t components=%v err=%v", built, components, err)
	}

}

// TestSchedulerSpecUsesTheConfigurationThisFilePins pins the spec-to-function
// link. It does not close the chain from `main`, and an earlier version of this
// comment claimed it did -- see the coverage statement below, which review
// required after the reconciler file made the same overstatement.
//
// The behavioural pin calls configureSchedulerDependencies directly. That proves
// what THAT function does; it does not prove `shell.Execute` reaches it. Point
// schedulerSpec at a different configure function and the pin keeps passing while
// the binary runs something else entirely -- the same defeat as the original
// version of this file, one level further out. Comparing the spec's function
// pointer to the function under test makes the chain complete:
// dho scheduler -> shell.Execute(schedulerSpec) -> this field -> the tested function.
//
// Function values are not comparable in Go, so compare code pointers. A nil field
// is also a failure: it would mean the binary has no configuration at all, or
// that the logger-taking variant is now in use and this pin no longer covers the
// path production takes.
//
// PINNED: the spec field's code pointer equals configureSchedulerDependencies's
// code pointer, and the logger-taking field is nil so shell.Execute cannot take the
// other one. Combined with the behavioural pin above -- which calls that exact
// function -- the link from the spec to the observed dormant behaviour is
// covered, PROVIDED both sides stay what they are today: direct references to
// the same plain top-level function declaration.
//
// NOT PINNED, and this is a real gap rather than pedantry: that neither side is
// ever refactored into a closure or a bound method value. Adversarial review
// pointed out that Go documents a function's code pointer as not necessarily
// unique -- two closures produced by a shared factory, or two bound method
// values with different receivers, can compare equal by reflect.Value.Pointer()
// while invoking different code. That is not what schedulerSpec.
// ConfigureDependencies or configureSchedulerDependencies are today -- both are
// direct references to one package-level function declaration, for which this
// code-pointer comparison is conclusive -- but nothing here would notice the
// day either one stops being that. A stronger runtime check (comparing
// runtime.FuncForPC names, for instance) would not close this: if the
// compiler/linker ever did fold two distinct closures onto one code address,
// there would be exactly one symbol table entry for it, so name and pointer
// comparisons would agree and still miss the same case. This is a limit of
// what reflection can prove, not a gap this file chose to leave open.
//
// NOT PINNED (closed at the dispatch level, not the runtime level): `dho
// scheduler` running shell.Execute(schedulerSpec). A running test cannot
// observe process startup. TestSchedulerCommandRunsThePinnedSpec
// (service_test.go) covers the half a dispatch can: `dho scheduler --help`,
// run through the dho command tree, prints the usage line of
// schedulerSpec's service identity. It does NOT cover a second, still-open
// half: a future activation route added inside shell.Execute itself -- reading an
// environment variable directly, or dispatching on something other than the
// spec's configure fields -- would bypass every pin in this file the same way
// a rewired Command() would, and no test here would notice, because none of
// them exercise shell.Execute's own internals.
//
// NOT PINNED: a future configureSchedulerDependencies that branches on a
// config.Config field only populated when a real secret environment variable
// is set (cfg.CoordinatorDatabaseURI.Configured(), for instance), rather than
// on checkedInSchedulerActivation. TestProductionSchedulerConfigurationBuildsTheReviewedLoop
// passes config.Load's own defaulted output, which closes the version of this
// where the branch condition is merely non-empty (config.Load already
// defaults most fields); it does not close a branch on whether a SECRET was
// actually configured, because supplying one would turn this into an
// integration test.
//
// And, as stated above, neither pin proves that flipping the seam retains the
// capability the seam guards.
func TestSchedulerSpecUsesTheConfigurationThisFilePins(t *testing.T) {
	// Retargeted (CHAOS-3903): the scheduler moved to the logger-aware hook so
	// the fixed maintenance loop can name the schedules that fail a window.
	// The guard this replaces did its job -- it refused to keep pinning a field
	// shell.Execute had stopped invoking -- so the mirror-image check is kept: if
	// the wiring ever moves BACK, this pin must be retargeted again rather than
	// silently covering nothing.
	if schedulerSpec.ConfigureDependencies != nil {
		t.Fatal(
			"schedulerSpec now sets ConfigureDependencies. shell.Execute refuses a spec " +
				"with both hooks and would call the other one, so the behavioural pin " +
				"below no longer proves anything about the production path. Retarget " +
				"the pin at whichever field shell.Execute actually invokes.",
		)
	}
	if schedulerSpec.ConfigureDependenciesWithLogger == nil {
		t.Fatal(
			"schedulerSpec.ConfigureDependenciesWithLogger is nil, so this pin covers " +
				"nothing that production runs",
		)
	}

	pinned := reflect.ValueOf(configureSchedulerDependencies).Pointer()
	wired := reflect.ValueOf(schedulerSpec.ConfigureDependenciesWithLogger).Pointer()
	if pinned != wired {
		t.Fatal(
			"schedulerSpec.ConfigureDependenciesWithLogger is NOT " +
				"configureSchedulerDependencies. The behavioural pin therefore tests a " +
				"function the binary does not call, and activation could ship green. " +
				"Either restore the wiring or retarget the pin at the function " +
				"shell.Execute really invokes -- do not delete this test.",
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
