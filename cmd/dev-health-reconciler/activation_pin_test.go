package main

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
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
//
// The cfg passed in names the production Service (reconcilerSpec.Service), not
// only RiverDatabaseSchema. Adversarial review pointed out that a config this
// unlike config.Load's real output leaves room for a future rewrite to branch
// on cfg.Service and activate only when the input looks production-shaped --
// this test would then never see it. This does not close every such route
// (cfg still has other zero fields), but it removes the cheapest version of it.
func TestProductionReconcilerSelectsTheShadowStepper(t *testing.T) {
	// The contract roots are repo-relative, so reach the stepper branch the same
	// way the neighbouring dependency tests do. Without this the configuration
	// bails out early and the test cannot see which stepper was chosen -- which it
	// detects and reports below rather than passing vacuously.
	t.Chdir(filepath.Join("..", ".."))

	var shadowBuilt, mutationBuilt bool

	sources := reconcilerSourcesForTest(t, &fakeReconcilerDatabase{})
	// This test observes WHICH BRANCH the production activation selects. It cannot
	// observe what production's own buildSyncShadow returns, and that limit is
	// measured rather than assumed: wrapping the production builder and asserting
	// on its result was tried and fails on the clean tree, because
	// syncreconciler.NewShadow needs a real pool and this test runs against a fake
	// database. Using a real database would make this an integration test, which
	// changes when and whether it runs -- and an activation pin that CI can skip is
	// not a pin.
	//
	// RESIDUAL RISK, stated because a reader will otherwise assume otherwise:
	// repoint productionReconcilerDependencySources.buildSyncShadow at an adapter
	// that returns a MUTATING stepper and this test still passes. The branch is
	// right and its contents are not checked here. What guards that is
	// syncreconciler.NewShadow's own tests plus review of that field, not this
	// file.
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
		config.Config{Service: reconcilerSpec.Service, RiverDatabaseSchema: "river"},
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

// TestReconcilerSpecInvokesTheReviewedActivation closes the gap named in the
// test above's residual-risk statement, and in this file's other comments:
// that the behavioural pin enters through
// configureReconcilerDependenciesWithSourcesAndLogger, one level below the
// production wrapper, so keeping the configureReconcilerDependenciesWithLogger
// symbol but rewriting its body to supply reconcilerActivation{syncMutation:
// true}, or to delegate somewhere else, would leave every other pin in this
// file green.
//
// This closes that specific route: it swaps fakes into the PACKAGE VARIABLE
// productionReconcilerDependencySources -- what
// configureReconcilerDependenciesWithLogger actually reads -- restores it via
// defer, and calls reconcilerSpec.ConfigureDependenciesWithLogger, the spec
// field itself, which TestReconcilerSpecUsesTheConfigurationThisFilePins below
// pins to be that exact function. Between the two tests, a body rewrite that
// hardcodes mutation, or that stops reading productionReconcilerDependencySources
// at all, has nowhere left to hide: this test calls through the real symbol,
// reached through the real spec field, reading the real (now-faked) global.
func TestReconcilerSpecInvokesTheReviewedActivation(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))

	original := productionReconcilerDependencySources
	defer func() { productionReconcilerDependencySources = original }()

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
	productionReconcilerDependencySources = sources

	if reconcilerSpec.ConfigureDependenciesWithLogger == nil {
		t.Fatal("reconcilerSpec.ConfigureDependenciesWithLogger is nil; this test has nothing to call through")
	}
	if _, err := reconcilerSpec.ConfigureDependenciesWithLogger(
		context.Background(),
		config.Config{Service: reconcilerSpec.Service, RiverDatabaseSchema: "river"},
		health.NewRegistry(100*time.Millisecond),
		reconcilerTestLogger(),
	); err != nil {
		t.Fatalf("configuring the reconciler through the spec field failed: %v", err)
	}

	if mutationBuilt {
		t.Fatal(
			"calling reconcilerSpec.ConfigureDependenciesWithLogger built the sync " +
				"MUTATION stepper. configureReconcilerDependenciesWithLogger's body " +
				"must still delegate to checkedInReconcilerActivation through " +
				"productionReconcilerDependencySources -- a hardcoded activation, or a " +
				"delegate that bypasses that global, would reach here.",
		)
	}
	if !shadowBuilt {
		t.Fatal(
			"calling reconcilerSpec.ConfigureDependenciesWithLogger built NEITHER " +
				"stepper, so this test can no longer tell observation from mutation.",
		)
	}
}

// TestProductionSyncShadowBuilderReturnsTheShadowStepper closes the specific
// gap named in TestProductionReconcilerSelectsTheShadowStepper's comment above:
// that test's fakes replace
// productionReconcilerDependencySources.buildSyncShadow entirely, so it cannot
// see what that field's PRODUCTION value actually returns. Adversarial review
// pointed out that repointing buildSyncShadow at an adapter returning a
// mutating stepper would still pass, because nothing calls the real field.
//
// This calls the real field directly. syncreconciler.NewObserver only rejects a
// nil pool and performs no I/O during construction -- confirmed by reading
// NewObserver/NewShadow, not assumed -- so an inert, unconnected *pgxpool.Pool
// is enough to exercise it without becoming an integration test. The registry
// is loaded from the real committed contract artifact, the same one production
// loads, because a nil or fake registry would let construction take a path
// production never takes.
func TestProductionSyncShadowBuilderReturnsTheShadowStepper(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))

	if productionReconcilerDependencySources.buildSyncShadow == nil {
		t.Fatal("productionReconcilerDependencySources.buildSyncShadow is nil; nothing to pin")
	}

	registry, err := syncdispatchcontract.Load(defaultSyncDispatchContractRoot)
	if err != nil {
		t.Fatalf("loading the real sync-dispatch contract: %v", err)
	}

	stepper, err := productionReconcilerDependencySources.buildSyncShadow(&pgxpool.Pool{}, registry)
	if err != nil {
		t.Fatalf("the production shadow builder failed against an inert pool: %v", err)
	}
	if _, ok := stepper.(*syncreconciler.Shadow); !ok {
		t.Fatalf(
			"productionReconcilerDependencySources.buildSyncShadow returned %T, not "+
				"*syncreconciler.Shadow. This field is what the dormant path actually "+
				"runs; if it now returns something else, the reviewed shadow-vs-mutation "+
				"distinction this file pins no longer describes production.",
			stepper,
		)
	}
}

// TestReconcilerSpecUsesTheConfigurationThisFilePins pins ONE LINK of the chain
// from `main` to the behaviour asserted below. A fourth adversarial round found
// that the round-3 fix corrected the CLAIM here without closing three of the
// four gaps it named, and found two more (an untested config.Config{} shape,
// and this PINNED line's own overclaim about what code-pointer equality
// proves). The honest, now-verified statement of coverage follows.
//
// The behavioural pin calls configureReconcilerDependenciesWithSourcesAndLogger
// so it can inject fakes. `shell.Main` does not call that directly -- it calls
// whatever reconcilerSpec names, which is configureReconcilerDependenciesWithLogger,
// which delegates to the sources-taking function with production sources. This
// test pins both ends of that chain: the spec field points where we think, and the
// delegate is the function under test.
//
// PINNED: `reconcilerSpec.ConfigureDependenciesWithLogger`'s code pointer equals
// configureReconcilerDependenciesWithLogger's, and ConfigureDependencies is nil
// so shell.Main cannot take the other field. This is conclusive PROVIDED both
// sides stay what they are today -- direct references to the same package-level
// function declaration, not a closure or bound method value. Go documents a
// function's code pointer as not necessarily unique for those; a stronger
// runtime check (comparing runtime.FuncForPC names) would not close that
// residual case either, because two code paths truly folded onto one address
// would share one symbol-table entry and so agree under a name comparison too.
// This is a limit of what reflection can prove, disclosed here rather than
// silently left off the list the way it was after round 3.
//
// PINNED: given the checked-in activation, the sources-taking function selects the
// SHADOW stepper and never the mutation stepper -- asserted by the behavioural test
// below, which is the sole supplier of checkedInReconcilerActivation.
//
// PINNED: TestReconcilerSpecInvokesTheReviewedActivation, above, calls through
// reconcilerSpec.ConfigureDependenciesWithLogger itself (not a direct call to
// the sources-taking function), with fakes swapped into the package variable
// that function actually reads. Keeping the
// configureReconcilerDependenciesWithLogger symbol but rewriting its body to
// hardcode mutation, or to delegate elsewhere, fails there. Round 3 found this
// gap; it is closed, not disclosed.
//
// PINNED: TestProductionSyncShadowBuilderReturnsTheShadowStepper, above, calls
// productionReconcilerDependencySources.buildSyncShadow directly (not a fake
// substitute) against an inert pool and the real committed contract, and
// requires its return type to be *syncreconciler.Shadow. Round 3 found that the
// behavioural test's fakes conceal this field's real behaviour; this closes it
// for what construction can prove -- the concrete stepper type actually wired,
// not the outcome of a fully connected Step() call.
//
// NOT PINNED, and these are real gaps rather than pedantry:
//
//   - `main()` calling shell.Main(reconcilerSpec) at runtime. A running test
//     cannot observe process startup.
//     TestReconcilerMainInvokesShellMainWithThePinnedSpec, below, closes the
//     practical version of this by parsing main.go's committed source instead.
//   - That flipping the seam retains concrete River delivery capability. Set the
//     flag and the pin together and everything here passes, 42501 and all
//     (CHAOS-3146).
//
// A green run here therefore means "the seam is dormant and changing it is
// deliberate", never "it is safe to flip".
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

// TestReconcilerMainInvokesShellMainWithThePinnedSpec closes the one gap this
// file states it cannot: whether `main()` actually calls
// shell.Main(reconcilerSpec). A running test cannot observe process startup,
// but it can read the committed source that becomes it. This parses main.go
// directly and requires func main()'s body to be exactly one statement,
// calling shell.Main with the identifier every other pin in this file already
// covers end to end.
//
// This is deliberately strict about shape: rewiring main() to call something
// else, to pass a copy or a second spec, or to do anything at all beyond this
// one call, fails the test. A future legitimate change to main() must update
// this test in the same commit, which is the point -- that change becomes
// visible instead of silently falling outside every other pin's reach.
func TestReconcilerMainInvokesShellMainWithThePinnedSpec(t *testing.T) {
	fileSet := token.NewFileSet()
	file, err := parser.ParseFile(fileSet, "main.go", nil, 0)
	if err != nil {
		t.Fatalf("parsing main.go: %v", err)
	}

	var mainDecl *ast.FuncDecl
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Recv != nil || function.Name.Name != "main" {
			continue
		}
		mainDecl = function
	}
	if mainDecl == nil || mainDecl.Body == nil {
		t.Fatal(
			"main.go declares no func main() with a body; the pins in this file " +
				"cover a function the binary never runs",
		)
	}
	if len(mainDecl.Body.List) != 1 {
		t.Fatalf(
			"func main() has %d statements, want exactly 1 (the shell.Main call "+
				"this test pins). If this is a reviewed change, confirm the new "+
				"statements cannot skip or alter the shell.Main(reconcilerSpec) call "+
				"before updating this test.",
			len(mainDecl.Body.List),
		)
	}

	expressionStatement, ok := mainDecl.Body.List[0].(*ast.ExprStmt)
	if !ok {
		t.Fatalf("func main()'s only statement is not a call expression: %#v", mainDecl.Body.List[0])
	}
	call, ok := expressionStatement.X.(*ast.CallExpr)
	if !ok {
		t.Fatalf("func main()'s statement is not a function call: %#v", expressionStatement.X)
	}
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		t.Fatalf("func main() does not call a package-qualified function: %#v", call.Fun)
	}
	packageIdent, ok := selector.X.(*ast.Ident)
	if !ok || packageIdent.Name != "shell" || selector.Sel.Name != "Main" {
		t.Fatalf("func main() calls %#v, not shell.Main", call.Fun)
	}
	if len(call.Args) != 1 {
		t.Fatalf("shell.Main call has %d arguments, want exactly 1", len(call.Args))
	}
	argument, ok := call.Args[0].(*ast.Ident)
	if !ok || argument.Name != "reconcilerSpec" {
		t.Fatalf(
			"func main() passes %#v to shell.Main, not reconcilerSpec -- the pins "+
				"in this file cover a spec the binary does not use",
			call.Args[0],
		)
	}
}
