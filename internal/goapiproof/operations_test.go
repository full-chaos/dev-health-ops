package goapiproof

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

// The operations the running query-api registers, written out rather
// than loaded from the same place the code reads: this test's job is to
// fail when two artefacts DISAGREE, and a test that reads the code's own
// source of truth can only ever agree with itself.
//
// Fifteen at the 2026-09-07 measurement (enablement artifact
// 10-status-canary.json / go_api_operations.json); seventeen since
// CHAOS-4991 registered `pr` and CHAOS-5523 registered
// `featureFlagEvents` on 2026-09-09.
//
// Keeping this list correct by hand is exactly what failed: both
// operations were registered without an entry in operationSpecs, and
// nothing caught it until go-api-prove refused a live JOB 5 run.
// TestOperationSpecsCoverExactlyWhatQueryAPIRegisters (registered_set_test.go)
// is the guard that does not depend on anyone remembering; this list
// stays because the refusal tests below need a known-good base to
// perturb, and because a second, independent statement of the set is
// what makes a drift legible rather than merely detected.
var registeredOperations = []string{
	"experiments",
	"acrRepositoryScopes", "catalogValues", "busFactor",
	"aiOpportunities", "improveOpportunities",
	"aiAttributedPrs", "aiAttributionOverview", "aiComparison", "aiImpactSummary", "aiReviewLoad", "aiRiskBreakdown",
	"productTelemetryDashboard", "productTelemetryPlatformDashboard",
	"aiGovernanceSummary", "aiWorkflowDrilldown",
	"connectorsDataHealth", "dataHealthIdentity", "mappingCoverageHealth", "metricLineage",
	"capacityForecast", "capacityForecasts", "cognitiveLoad", "compoundingRisk", "complexityTimeseries",
	"featureFlagEvents", "featureFlags", "flowMatrix", "hotspots",
	"releaseImpact",
	"investmentBreakdown", "investmentFull", "operatingReview", "pr",
	"reportRuns", "reviewEdges", "savedReport", "savedReports", "securityAlerts", "securityOverview", "testopsRisk", "throughputForecast", "testOpsCoverage", "testOpsPipeline", "testOpsTest", "featureFlagTimeseries",
	"workGraphArtifacts", "workGraphEdges", "workGraphFlow",
}

func TestAssertCoverageAcceptsTheRegisteredSet(t *testing.T) {
	if err := AssertCoverage(registeredOperations); err != nil {
		t.Fatalf("the committed table must cover every registered operation: %v", err)
	}
}

// D15/R4: an operation the running binary registers but this table cannot
// build a request for must FAIL the run, never be quietly skipped.
func TestAssertCoverageRefusesAnUncoveredOperation(t *testing.T) {
	err := AssertCoverage(append(append([]string(nil), registeredOperations...), "somethingBrandNew"))
	if err == nil {
		t.Fatal("an uncovered registered operation must fail, not be skipped")
	}
	if !strings.Contains(err.Error(), "somethingBrandNew") {
		t.Fatalf("the failure must NAME the uncovered operation, got %v", err)
	}
}

// The opposite direction: a stale entry here reads as coverage while
// proving nothing about a binary that no longer registers it.
func TestAssertCoverageRefusesAStaleEntry(t *testing.T) {
	err := AssertCoverage(registeredOperations[:len(registeredOperations)-1])
	if err == nil {
		t.Fatal("an operation covered here but not registered must fail")
	}
	if !strings.Contains(err.Error(), "workGraphFlow") {
		t.Fatalf("the failure must NAME the stale entry, got %v", err)
	}
}

func TestSpecForRefusesAnUnknownOperation(t *testing.T) {
	if _, err := SpecFor("noSuchOperation"); err == nil {
		t.Fatal("an unknown operation must be a refusal, never an empty request")
	}
}

// Every spec must actually build variables -- a nil Variables func would
// panic at request time, deep inside a run, instead of failing here.
func TestEverySpecBuildsVariables(t *testing.T) {
	window := DefaultWindow()
	for _, operation := range KnownOperations() {
		spec, err := SpecFor(operation)
		if err != nil {
			t.Fatalf("SpecFor(%q): %v", operation, err)
		}
		if spec.Variables == nil {
			t.Fatalf("%s has no Variables builder", operation)
		}
		variables := spec.Variables("70d529e0", window)
		if len(variables) == 0 {
			t.Fatalf("%s built an empty variables object", operation)
		}
		if _, err := json.Marshal(variables); err != nil {
			t.Fatalf("%s built variables that do not encode: %v", operation, err)
		}
	}
}

// Every windowed operation must actually USE the window it is handed --
// a spec that ignores it would silently query a different range than the
// operator asked for, and two runs with different --since flags would
// produce identical, indistinguishable receipts.
func TestWindowedSpecsUseTheWindow(t *testing.T) {
	base := DefaultWindow()
	moved := Window{
		SinceUTC:  "2025-01-01T00:00:00Z",
		UntilUTC:  "2025-02-01T00:00:00Z",
		SinceDate: "2025-01-01",
		UntilDate: "2025-02-01",
		WeekStart: "2025-01-06",
	}

	// The operations whose SDL input carries no date range at all. Named
	// explicitly so adding a windowed operation without wiring its window
	// fails this test rather than joining a silent allowlist.
	// featureFlagEvents and pr are windowless for the same reason
	// featureFlags is: they select current state or one stored row, not a
	// range. `pr` is additionally refused at run time (InstanceVariable),
	// but its Variables func is still checked here -- a spec that quietly
	// ignored the window would be wrong whether or not the run sends it.
	windowless := map[string]bool{
		"acrRepositoryScopes": true, "catalogValues": true,
		"connectorsDataHealth": true, "dataHealthIdentity": true, "mappingCoverageHealth": true, "metricLineage": true,
		"capacityForecast": true, "capacityForecasts": true,
		"featureFlagEvents": true, "featureFlags": true, "pr": true,
		"experiments": true, "busFactor": true, "compoundingRisk": true, "reportRuns": true, "savedReport": true, "savedReports": true, "securityAlerts": true, "securityOverview": true,
		"releaseImpact": true, "throughputForecast": true, "workGraphArtifacts": true, "workGraphEdges": true,
		"aiOpportunities": true, "improveOpportunities": true,
		"aiWorkflowDrilldown": true,
		"workGraphFlow":       true,
	}

	for _, operation := range KnownOperations() {
		spec, err := SpecFor(operation)
		if err != nil {
			t.Fatalf("SpecFor(%q): %v", operation, err)
		}
		encode := func(w Window) string {
			encoded, err := json.Marshal(spec.Variables("70d529e0", w))
			if err != nil {
				t.Fatalf("%s: %v", operation, err)
			}
			return string(encoded)
		}
		same := encode(base) == encode(moved)
		if windowless[operation] != same {
			if same {
				t.Fatalf("%s ignores the request window but is not declared windowless", operation)
			}
			t.Fatalf("%s is declared windowless but its request changed with the window", operation)
		}
	}
}

// Every declared volatile-field path must be rooted at `data.<operation>`
// -- an exclusion aimed at another operation's subtree could never match,
// and would sit here reading as coverage.
func TestVolatileExclusionsAreRootedAtTheirOperation(t *testing.T) {
	for _, operation := range KnownOperations() {
		spec, err := SpecFor(operation)
		if err != nil {
			t.Fatalf("SpecFor(%q): %v", operation, err)
		}
		for path, reason := range spec.Parity.VolatileFields {
			prefix := "data." + spec.ResponseRoot + "."
			if !strings.HasPrefix(path, prefix) {
				t.Fatalf("%s declares exclusion %q outside the subtree its document selects (want prefix %q)", operation, path, prefix)
			}
			if len(strings.TrimSpace(reason)) < 40 {
				t.Fatalf("%s exclusion %q has no written reason (got %q)", operation, path, reason)
			}
		}
	}
}

func TestWindowValidateRejectsAnIncompleteWindow(t *testing.T) {
	if err := DefaultWindow().Validate(); err != nil {
		t.Fatalf("the default window must be valid: %v", err)
	}
	incomplete := DefaultWindow()
	incomplete.WeekStart = ""
	if err := incomplete.Validate(); err == nil {
		t.Fatal("an empty window field must be refused, not sent as an empty string")
	}
}

// Every declared Tier-B entry must carry a written reason and sit inside
// the subtree its operation's REGISTERED DOCUMENT selects -- which is not
// always the operation's name. Checking against the name is what an
// earlier version of this test did, and it rejected every correct
// investment/flowMatrix path (those documents select `analytics`).
func TestTierBDeclarationsAreReasonedAndRooted(t *testing.T) {
	for _, operation := range KnownOperations() {
		spec, err := SpecFor(operation)
		if err != nil {
			t.Fatalf("SpecFor(%q): %v", operation, err)
		}
		for path, reason := range spec.Parity.FloatTierB {
			prefix := "data." + spec.ResponseRoot + "."
			if !strings.HasPrefix(path, prefix) {
				t.Errorf("%s declares Tier-B path %q outside the subtree its document selects (want prefix %q)", operation, path, prefix)
			}
			if len(strings.TrimSpace(reason)) < 20 {
				t.Errorf("%s Tier-B path %q has no written reason (got %q)", operation, path, reason)
			}
		}
	}
}

// Same, for baseline defects: every entry must name a ticket and at least
// one path, or it declares nothing and can only ever read as coverage.
func TestBaselineDefectDeclarationsNameATicketAndPaths(t *testing.T) {
	checkDefects := func(t *testing.T, label string, responseRoot string, defects []BaselineDefect) {
		t.Helper()
		if err := validateBaselineDefects(defects); err != nil {
			t.Errorf("%s: %v", label, err)
		}
		for _, defect := range defects {
			if !strings.HasPrefix(defect.Ticket, "CHAOS-") {
				t.Errorf("%s declares a baseline defect with ticket %q -- a defect with no ticket is an opinion", label, defect.Ticket)
			}
			if len(defect.Paths) == 0 {
				t.Errorf("%s baseline defect %s cites no field paths, so it excuses nothing", label, defect.Ticket)
			}
			for _, path := range defect.Paths {
				prefix := "data." + responseRoot
				if path != prefix && !strings.HasPrefix(path, prefix+".") {
					t.Errorf("%s baseline defect %s cites %q outside the subtree its document selects (want prefix %q)", label, defect.Ticket, path, prefix)
				}
			}
		}
	}
	for _, operation := range KnownOperations() {
		spec, err := SpecFor(operation)
		if err != nil {
			t.Fatalf("SpecFor(%q): %v", operation, err)
		}
		checkDefects(t, operation, spec.ResponseRoot, spec.Parity.BaselineDefects)
		// A Variant's ResponseRoot is the operation's own -- see
		// OperationSpec.Variants: a variant shares its operation's
		// registered document, only Variables/Parity differ.
		for _, variant := range spec.Variants {
			checkDefects(t, operation+":"+variant.Name, spec.ResponseRoot, variant.Parity.BaselineDefects)
		}
	}
}

// TestEveryVariantIsNamed pins OperationSpec.Variants' own contract: an
// empty Name is indistinguishable from the base request on the Outcome
// and in every report line, which defeats the entire reason Variants
// exist -- to tell more than one request under the same operation apart.
func TestEveryVariantIsNamed(t *testing.T) {
	for _, operation := range KnownOperations() {
		spec, err := SpecFor(operation)
		if err != nil {
			t.Fatalf("SpecFor(%q): %v", operation, err)
		}
		for i, variant := range spec.Variants {
			if variant.Name == "" {
				t.Errorf("%s: Variants[%d] has an empty Name", operation, i)
			}
		}
	}
}

// CHAOS-5426: flowMatrix's base
// request sends dimension=WORK_TYPE, the one dimension #2374 left
// untouched, so it can never prove that fix -- see flowMatrixInvestmentVariant's
// doc comment. This pins that the TEAM and REPO Variants actually exist,
// actually send useInvestment=true (the shape web's useChordFlow.ts
// always sends, and the one #2374 conditions its fix on), and actually
// cite CHAOS-5426 rather than silently reusing CHAOS-5448's WORK_TYPE-only
// FINAL-semantics declaration (which does not apply to the investment
// source these variants read).
func TestFlowMatrixVariantsExerciseTeamAndRepoWithInvestment(t *testing.T) {
	spec, err := SpecFor("flowMatrix")
	if err != nil {
		t.Fatalf("SpecFor(flowMatrix): %v", err)
	}
	want := map[string]bool{"TEAM": false, "REPO": false}
	if len(spec.Variants) != len(want) {
		t.Fatalf("flowMatrix declares %d Variants, want %d (TEAM, REPO): %#v", len(spec.Variants), len(want), spec.Variants)
	}
	for _, variant := range spec.Variants {
		if _, known := want[variant.Name]; !known {
			t.Fatalf("flowMatrix declares an unexpected Variant %q, want only TEAM/REPO", variant.Name)
		}
		want[variant.Name] = true

		vars := variant.Variables("org-under-test", DefaultWindow())
		batch, _ := vars["batch"].(map[string]any)
		fm, _ := batch["flowMatrix"].(map[string]any)
		if fm == nil {
			t.Fatalf("variant %q: Variables()[\"batch\"][\"flowMatrix\"] is not a map: %#v", variant.Name, vars)
		}
		if dim, _ := fm["dimension"].(string); dim != variant.Name {
			t.Errorf("variant %q: dimension = %v, want %q", variant.Name, fm["dimension"], variant.Name)
		}
		if useInvestment, _ := fm["useInvestment"].(bool); !useInvestment {
			t.Errorf("variant %q: useInvestment = %v, want true -- this is the exact shape web's useChordFlow.ts always sends, and the one CHAOS-5426's fix conditions on", variant.Name, fm["useInvestment"])
		}

		if len(variant.Parity.BaselineDefects) != 1 || variant.Parity.BaselineDefects[0].Ticket != "CHAOS-5426" {
			t.Errorf("variant %q declares BaselineDefects %#v, want exactly one citing CHAOS-5426", variant.Name, variant.Parity.BaselineDefects)
		}
	}
	for name, seen := range want {
		if !seen {
			t.Errorf("flowMatrix is missing the %s Variant", name)
		}
	}

	// The base request's own CHAOS-5448 declaration must survive
	// untouched -- Variants are ADDITIVE, never a repoint of the base
	// request that would drop WORK_TYPE's FINAL-semantics regression
	// coverage.
	if len(spec.Parity.BaselineDefects) != 1 || spec.Parity.BaselineDefects[0].Ticket != "CHAOS-5448" {
		t.Errorf("flowMatrix's base request declares %#v, want exactly one citing CHAOS-5448 (WORK_TYPE unaffected by CHAOS-5426)", spec.Parity.BaselineDefects)
	}
	baseVars := spec.Variables("org-under-test", DefaultWindow())
	baseBatch, _ := baseVars["batch"].(map[string]any)
	baseFM, _ := baseBatch["flowMatrix"].(map[string]any)
	if dim, _ := baseFM["dimension"].(string); dim != "WORK_TYPE" {
		t.Errorf("flowMatrix's base request dimension = %v, want WORK_TYPE unchanged", baseFM["dimension"])
	}
}

// ResponseRoot is load-bearing for every declaration check above, so it
// must be present and must not be quietly assumed equal to the operation
// name -- three of the fifteen operations select a differently-named root.
func TestEverySpecDeclaresItsResponseRoot(t *testing.T) {
	sharedRoots := 0
	for _, operation := range KnownOperations() {
		spec, err := SpecFor(operation)
		if err != nil {
			t.Fatalf("SpecFor(%q): %v", operation, err)
		}
		if spec.ResponseRoot == "" {
			t.Errorf("%s declares no ResponseRoot, so its parity paths cannot be checked", operation)
		}
		if spec.ResponseRoot != operation {
			sharedRoots++
		}
	}
	// flowMatrix, investmentBreakdown and investmentFull all select
	// `analytics`; catalogValues and acrRepositoryScopes both select
	// `catalog`; releaseImpact selects `workGraphEdges`. If this ever reads 0, either the documents changed or
	// someone "tidied" ResponseRoot into a copy of the operation name --
	// and the parity-path checks above would silently start passing for
	// paths that can never match.
	if sharedRoots != 14 {
		t.Fatalf("expected 14 operations whose response root differs from their name, got %d", sharedRoots)
	}
}

// RootNullable is a hand-kept mirror of the SDL, so it can drift from it.
// This derives the truth from contracts/graphql/v1/schema.graphql itself --
// a root declared without a trailing `!` is nullable -- and fails if the
// table disagrees.
//
// It exists because round 2's F4 was exactly this class in the other
// direction: the code assumed a fact about the schema (no root is nullable)
// that the schema did not support, and two of fifteen operations became
// unprovable as a result. A guard that reads the schema cannot make that
// assumption.
func TestResponseRootNullabilityMatchesTheSDL(t *testing.T) {
	sdl, err := os.ReadFile(filepath.Join(repoRootFromTest(t), "contracts", "graphql", "v1", "schema.graphql"))
	if err != nil {
		t.Fatalf("read SDL: %v", err)
	}

	checked := 0
	for _, operation := range KnownOperations() {
		spec, err := SpecFor(operation)
		if err != nil {
			t.Fatalf("SpecFor(%q): %v", operation, err)
		}
		pattern := regexp.MustCompile(`(?m)^\s*` + regexp.QuoteMeta(spec.ResponseRoot) + `\([^)]*\)\s*:\s*([^\n]+)$`)
		match := pattern.FindSubmatch(sdl)
		if match == nil {
			t.Errorf("%s: root field %q not found in the SDL", operation, spec.ResponseRoot)
			continue
		}
		checked++
		declaration := strings.TrimSpace(string(match[1]))
		nullable := !strings.HasSuffix(declaration, "!")
		if spec.RootNullable != nullable {
			t.Errorf("%s: RootNullable=%v but the SDL declares %q (nullable=%v)",
				operation, spec.RootNullable, declaration, nullable)
		}
	}

	// Non-vacuity: every operation must have been resolved against the SDL.
	// A regex that stopped matching would otherwise leave this test silently
	// checking nothing.
	if checked != len(KnownOperations()) {
		t.Fatalf("only %d of %d roots were found in the SDL -- the match has stopped working",
			checked, len(KnownOperations()))
	}
	// And both answers must actually occur, or the comparison is trivial.
	nullableCount := 0
	for _, operation := range KnownOperations() {
		spec, _ := SpecFor(operation)
		if spec.RootNullable {
			nullableCount++
		}
	}
	if nullableCount == 0 || nullableCount == len(KnownOperations()) {
		t.Fatalf("all %d roots share one nullability: this test cannot discriminate", len(KnownOperations()))
	}
}

// repoRootFromTest walks up to the module root.
func repoRootFromTest(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("could not find the repository root")
		}
		dir = parent
	}
}

// r1 P3: TestEverySpecBuildsVariables only requires a non-empty,
// JSON-encodable object, so mutating featureFlagEvents' limit from 1000 to
// 100 passed every test. The value is not arbitrary -- the registered
// document requires the variable, and 1000 is the SDL's own default for
// the argument (`limit: Int! = 1000`), which is what a client omitting it
// gets. A different value silently measures a different request than the
// one real traffic makes, and parity evidence gathered over a shorter page
// is incomplete evidence that reads as complete.
func TestFeatureFlagEventsAsksForTheSDLDefaultPage(t *testing.T) {
	spec, err := SpecFor("featureFlagEvents")
	if err != nil {
		t.Fatalf("SpecFor: %v", err)
	}
	variables := spec.Variables("70d529e0", DefaultWindow())

	if got := variables["limit"]; got != 1000 {
		t.Fatalf("featureFlagEvents limit = %v, want 1000 -- the SDL declares `limit: Int! = 1000`, so any other value measures a request no client makes", got)
	}
	// The document declares $orgId and $limit as required and $flagKey /
	// $environment as nullable; all four are sent, so the request shape
	// stays readable beside the document.
	for _, name := range []string{"orgId", "flagKey", "environment", "limit"} {
		if _, ok := variables[name]; !ok {
			t.Fatalf("featureFlagEvents omits $%s: the registered document declares it", name)
		}
	}
	if got := variables["orgId"]; got != "70d529e0" {
		t.Fatalf("featureFlagEvents orgId = %v, want the run's org", got)
	}
}

// The SDL is the authority for that default, so it is read rather than
// trusted: if someone changes `limit: Int! = 1000` in the schema, the
// spec above is measuring the wrong page and this says so.
func TestTheFeatureFlagEventsLimitMatchesTheSDLDefault(t *testing.T) {
	source, err := os.ReadFile(filepath.Join("..", "..", "contracts", "graphql", "v1", "schema.graphql"))
	if err != nil {
		t.Fatalf("read the SDL: %v", err)
	}
	declaration := regexp.MustCompile(`featureFlagEvents\([^)]*limit:\s*Int!\s*=\s*(\d+)`)
	match := declaration.FindSubmatch(source)
	if match == nil {
		t.Fatal("could not find featureFlagEvents' limit default in the SDL: the declaration changed shape and this guard stopped guarding")
	}
	if string(match[1]) != "1000" {
		t.Fatalf("the SDL now defaults limit to %s, but the spec sends 1000: go-api-prove is measuring a different page than a client that omits the argument", match[1])
	}
}

// The release impact requests are the ones the feature flag pages send: the
// RELEASE source type, an empty node id for every release, and the page
// limit. A change to any of them proves a request no page sends.
func TestReleaseImpactRequestsMatchThePage(t *testing.T) {
	spec, err := SpecFor("releaseImpact")
	if err != nil {
		t.Fatal(err)
	}
	if spec.ResponseRoot != "workGraphEdges" {
		t.Fatalf("response root %q, want workGraphEdges", spec.ResponseRoot)
	}
	encode := func(vars map[string]any) string {
		encoded, err := json.Marshal(vars)
		if err != nil {
			t.Fatal(err)
		}
		return string(encoded)
	}
	if got, want := encode(spec.Variables("org-1", DefaultWindow())), `{"filters":{"limit":200,"nodeId":"","sourceType":"RELEASE"},"orgId":"org-1"}`; got != want {
		t.Fatalf("base request %s, want %s", got, want)
	}
	byName := map[string]Variant{}
	for _, v := range spec.Variants {
		byName[v.Name] = v
	}
	if len(byName) != 3 {
		t.Fatalf("%d variants, want LIMIT_ONE, NODE_ID_VALID and SOURCE_TYPE_POPULATED", len(byName))
	}
	limitOne := byName["LIMIT_ONE"]
	if got, want := encode(limitOne.Variables("org-1", DefaultWindow())), `{"filters":{"limit":1,"nodeId":"","sourceType":"RELEASE"},"orgId":"org-1"}`; got != want {
		t.Fatalf("LIMIT_ONE request %s, want %s", got, want)
	}
	node := byName["NODE_ID_VALID"]
	if node.Instance == nil {
		t.Fatal("NODE_ID_VALID names no run-supplied identifier")
	}
	vars := node.Variables("org-1", DefaultWindow())
	node.Instance.Bind(vars, "node-1")
	if got, want := encode(vars), `{"filters":{"limit":200,"nodeId":"node-1"},"orgId":"org-1"}`; got != want {
		t.Fatalf("NODE_ID_VALID request %s, want %s", got, want)
	}
	echo := node.Instance.Echo("node-1")
	if len(echo) != 1 || echo[0].List != "data.workGraphEdges.edges" || echo[0].Value != "node-1" {
		t.Fatalf("NODE_ID_VALID echo %+v", echo)
	}
	if len(node.Parity.RequireNonEmpty) != 1 || node.Parity.RequireNonEmpty[0] != "data.workGraphEdges.edges" {
		t.Fatalf("NODE_ID_VALID must require a non-empty edge list, got %v", node.Parity.RequireNonEmpty)
	}
	sourceType := byName["SOURCE_TYPE_POPULATED"]
	if sourceType.Instance == nil {
		t.Fatal("SOURCE_TYPE_POPULATED names no run-supplied source type")
	}
	vars = sourceType.Variables("org-1", DefaultWindow())
	if got, want := encode(vars), `{"filters":{"limit":50,"nodeId":"","sourceType":"PR"},"orgId":"org-1"}`; got != want {
		t.Fatalf("SOURCE_TYPE_POPULATED request %s, want %s", got, want)
	}
	sourceType.Instance.Bind(vars, "FEATURE_FLAG")
	if got, want := encode(vars), `{"filters":{"limit":50,"nodeId":"","sourceType":"FEATURE_FLAG"},"orgId":"org-1"}`; got != want {
		t.Fatalf("SOURCE_TYPE_POPULATED bound request %s, want %s", got, want)
	}
	if echo := sourceType.Instance.Echo("FEATURE_FLAG"); len(echo) != 1 || echo[0].Fields[0] != "sourceType" || echo[0].Value != "FEATURE_FLAG" {
		t.Fatalf("SOURCE_TYPE_POPULATED echo %+v", echo)
	}
	if len(sourceType.Parity.RequireNonEmpty) != 1 || sourceType.Parity.RequireNonEmpty[0] != "data.workGraphEdges.edges" {
		t.Fatalf("SOURCE_TYPE_POPULATED must require a non-empty edge list, got %v", sourceType.Parity.RequireNonEmpty)
	}
}

// A team id is stored on the newest rollup days only, so every team-scoped AI
// variant must end its window at the last complete UTC day, and a variant that
// selects nothing by construction must carry no declaration that could match
// nothing.
func TestAITeamVariantsReachTheLastCompleteDayAndEmptyVariantsDeclareNothing(t *testing.T) {
	restore := aiClock
	defer func() { aiClock = restore }()
	aiClock = func() time.Time { return time.Date(2026, 9, 20, 23, 59, 59, 0, time.UTC) }
	const lastComplete = "2026-09-19"
	for _, operation := range []string{"aiImpactSummary", "aiComparison", "aiReviewLoad", "aiRiskBreakdown", "aiAttributedPrs", "aiAttributionOverview"} {
		spec, err := SpecFor(operation)
		if err != nil {
			t.Fatal(err)
		}
		seen := map[string]bool{}
		for _, v := range spec.Variants {
			seen[v.Name] = true
			vars := v.Variables("org", DefaultWindow())
			end := vars["dateRange"].(map[string]any)["endDate"].(string)
			switch v.Name {
			case "TEAM_VALID":
				if end != lastComplete {
					t.Errorf("%s/TEAM_VALID ends at %s, want the last complete day %s", operation, end, lastComplete)
				}
			case "REPO_UNKNOWN", "REPO_NAME_UNKNOWN", "TEAM_UNKNOWN", "REPO_AND_TEAM_UNKNOWN":
				if len(v.Parity.FloatTierB) != 0 || len(v.Parity.BaselineDefects) != 0 || len(v.Parity.OrderInsensitiveLists) != 0 {
					t.Errorf("%s/%s selects nothing by construction and must declare nothing", operation, v.Name)
				}
			default:
				if end != DefaultWindow().UntilDate {
					t.Errorf("%s/%s must keep the base window, ends at %s", operation, v.Name, end)
				}
			}
		}
		if !seen["TEAM_VALID"] {
			t.Errorf("%s has no TEAM_VALID variant", operation)
		}
	}
	// a run window that already ends later keeps its own end
	if got := aiWindowEnd(Window{UntilDate: "2026-12-31"}); got != "2026-12-31" {
		t.Errorf("a later until-date is kept, got %s", got)
	}
	// the run day itself is never included
	aiClock = func() time.Time { return time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC) }
	if got := aiWindowEnd(Window{UntilDate: "2026-09-01"}); got != "2026-09-19" {
		t.Errorf("at the start of the run day the window ends the day before, got %s", got)
	}
}

// Every base request and every scope or page breakout of an AI list operation
// that can select rows requires a non-empty list, so two empty answers cannot
// pass as a measurement; only variants that select nothing by construction, and
// an offset page, do not.
func TestAIListOperationsRequireNonEmptyLists(t *testing.T) {
	lists := map[string]string{
		"aiImpactSummary": "data.aiImpactSummary.daily", "aiReviewLoad": "data.aiReviewLoad.byBucket",
		"aiRiskBreakdown": "data.aiRiskBreakdown.byBucket", "aiAttributedPrs": "data.aiAttributedPrs.rows",
		"aiAttributionOverview": "data.aiAttributionOverview.rows",
	}
	for operation, list := range lists {
		spec, err := SpecFor(operation)
		if err != nil {
			t.Fatal(err)
		}
		has := func(o Options) bool {
			for _, p := range o.RequireNonEmpty {
				if p == list {
					return true
				}
			}
			return false
		}
		if !has(spec.Parity) {
			t.Errorf("%s: the base request does not require %s", operation, list)
		}
		for _, v := range spec.Variants {
			unknown := strings.HasSuffix(v.Name, "_UNKNOWN") || v.Name == "REPO_AND_TEAM_UNKNOWN"
			switch {
			case unknown || v.Name == "PAGE_OFFSET":
				if has(v.Parity) {
					t.Errorf("%s/%s may hold nothing and must not require %s", operation, v.Name, list)
				}
			case v.Instance == nil && !has(v.Parity):
				t.Errorf("%s/%s does not require %s", operation, v.Name, list)
			}
		}
	}
}
