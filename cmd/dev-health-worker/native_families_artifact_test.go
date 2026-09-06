package main

// This file statically parses daily.go's own registration wiring -- the
// dailyNativeFamilyRegistrations function body and the metrics.remaining.*
// switch on spec.Kind -- and regenerates the checked-in contract artifact
// contracts/native-families/v1/native-families.json from what it finds.
// TestNativeFamiliesArtifactUpToDate fails when the checked-in file disagrees
// with a fresh parse.
//
// WHY STATIC PARSING, NOT A LIVE RUN: dailyNativeFamilyRegistrations and the
// remaining-family switch both call real NewXExecutor(conn) constructors that
// verify their own ClickHouse/Postgres schema at construction time (e.g.
// verifyRecommendationsSchema, verifyMembershipSchema) -- there is no nil-safe
// way to run this wiring and observe which map key or switch arm gets
// populated without a live database connection. Reading the SOURCE instead
// (same technique internal/providersync/route_wiring_comment_drift_test.go
// already uses for a different wiring-drift class) needs no database and can
// never silently diverge from what actually executes, because it reads the
// exact statements that execute.
//
// docs/go-migration-matrix.md's generator (scripts/gen_go_migration_matrix_docs.py)
// reads this artifact for §2's native/post_bridge family set and §3's
// native-vs-compat split -- team-lead's explicit ruling, 2026-09-04: no
// curated Python dict may be the source of truth for those two sections
// anymore; only a Go-emitted, drift-tested artifact may.
//
// Regenerate after any change to daily.go's registration wiring:
//
//	UPDATE_NATIVE_FAMILIES_ARTIFACT=1 go test ./cmd/dev-health-worker/... -run TestNativeFamiliesArtifactUpToDate

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily"
)

type nativeFamiliesArtifact struct {
	SchemaVersion int               `json:"schema_version"`
	GeneratedFrom string            `json:"generated_from"`
	Daily         map[string]string `json:"daily"`
	// Finalize is RUN-scoped, kept separate from Daily rather than folded in
	// (CHAOS-4290). Daily's families run per PARTITION; a finalize family runs
	// once per run after every partition has landed. One map would make the
	// artifact -- a contract other tooling reads -- state that they are the
	// same kind of thing, and would also silently change Daily's cardinality.
	Finalize  map[string]string `json:"finalize"`
	Remaining map[string]string `json:"remaining"`
	// Workgraph covers the two live workgraph/investment River kinds
	// (workgraph.build, investment.materialize) -- investment.dispatch/
	// chunk/finalize were deleted under CHAOS-4438 (dead Go shells, zero
	// producers). Added by CHAOS-4441's cutover: before it, every one of
	// these kinds took the same HTTP bridge executor, so there was nothing
	// to record and the artifact had no key for them at all -- which is
	// exactly why the artifact could not have detected that the cutover had
	// never happened. It can now.
	Workgraph map[string]string `json:"workgraph"`
}

// knownFamilyNameConstants resolves a registration indexed by a CONSTANT rather
// than a string literal, e.g. `finalize[daily.ICFinalizeFamilyName] = ...`.
//
// The value is taken from the real package, not restated, so this cannot drift
// from the constant it names. A selector the extractor does not know is an
// ERROR, never a skip -- see extractDailyFamilies.
var knownFamilyNameConstants = map[string]string{
	"ICFinalizeFamilyName":        daily.ICFinalizeFamilyName,
	"TeamCognitiveLoadFamilyName": daily.TeamCognitiveLoadFamilyName,
	"TeamComplexityFamilyName":    daily.TeamComplexityFamilyName,
	"BenchmarkingFamilyName":      daily.BenchmarkingFamilyName,
}

const nativeFamiliesGeneratedFrom = "cmd/dev-health-worker/daily.go + workgraph.go (static AST parse, cmd/dev-health-worker/native_families_artifact_test.go)"

func repoRootFromCmdDailyWorker(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("resolving repo root: %v", err)
	}
	return root
}

// parseDailyGoFile parses cmd/dev-health-worker/daily.go once per call.
func parseDailyGoFile(t *testing.T) (*token.FileSet, *ast.File) {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "daily.go", nil, 0)
	if err != nil {
		t.Fatalf("parsing daily.go: %v", err)
	}
	return fset, file
}

// extractDailyFamilies walks dailyNativeFamilyRegistrations' body for
// `native["x"] = ...`, `postBridge["x"] = ...` and `finalize[Const] = ...`
// assignments, returning the partition-scoped families and the run-scoped ones
// separately.
//
// IT FAILS ON ANYTHING IT DOES NOT UNDERSTAND, which is the whole point
// (CHAOS-4290). The previous version switched on the map identifier with two
// cases and no default, and resolved only string-literal indices. #2243's
// registration is `finalize[daily.ICFinalizeFamilyName]` -- a THIRD map, indexed
// by a CONSTANT -- so it missed on both counts and was silently dropped. The
// exact-cardinality assertion downstream then passed while certifying a split it
// could not see, which is the same defect codex r1 caught here pointing the
// other way.
//
// A guard blind to a scope passes BY CONSTRUCTION. So an unknown map identifier
// and an unresolvable index are both errors now, and a fourth scope cannot be
// added silently.
func extractDailyFamilies(file *ast.File) (partition, finalize map[string]string, err error) {
	partition = map[string]string{}
	finalize = map[string]string{}
	var target *ast.FuncDecl
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if ok && fn.Name.Name == "dailyNativeFamilyRegistrations" {
			target = fn
			break
		}
	}
	if target == nil {
		return nil, nil, fmt.Errorf("dailyNativeFamilyRegistrations not found -- wiring was renamed or removed")
	}

	var walkErr error
	ast.Inspect(target.Body, func(n ast.Node) bool {
		if walkErr != nil {
			return false
		}
		assign, ok := n.(*ast.AssignStmt)
		if !ok || len(assign.Lhs) != 1 {
			return true
		}
		index, ok := assign.Lhs[0].(*ast.IndexExpr)
		if !ok {
			return true
		}
		mapIdent, ok := index.X.(*ast.Ident)
		if !ok {
			return true
		}
		// Only assignments into one of the registration maps are ours; an index
		// into any other local map is legitimately none of this test's business.
		scope, isScope := map[string]string{
			"native": "native", "postBridge": "post_bridge", "finalize": "finalize",
		}[mapIdent.Name]
		if !isScope {
			return true
		}
		name, resolveErr := resolveFamilyNameIndex(index.Index)
		if resolveErr != nil {
			walkErr = fmt.Errorf("registration into %q: %w", mapIdent.Name, resolveErr)
			return false
		}
		if scope == "finalize" {
			finalize[name] = scope
		} else {
			partition[name] = scope
		}
		return true
	})
	if walkErr != nil {
		return nil, nil, walkErr
	}
	if len(partition) == 0 {
		return nil, nil, fmt.Errorf("found zero native/postBridge assignments -- parsing broke or the wiring shape changed")
	}
	return partition, finalize, nil
}

// resolveFamilyNameIndex turns a map index into a family name. A string literal
// is used directly; a selector is resolved through knownFamilyNameConstants,
// whose values come from the real package. Anything else is an error rather
// than a skip -- a dropped registration is invisible, and invisible is exactly
// how the finalize scope went unnoticed.
func resolveFamilyNameIndex(expr ast.Expr) (string, error) {
	switch index := expr.(type) {
	case *ast.BasicLit:
		if index.Kind != token.STRING {
			return "", fmt.Errorf("index is a non-string literal %s", index.Value)
		}
		name, err := strconv.Unquote(index.Value)
		if err != nil {
			return "", fmt.Errorf("unquoting %q: %w", index.Value, err)
		}
		return name, nil
	case *ast.SelectorExpr:
		name, known := knownFamilyNameConstants[index.Sel.Name]
		if !known {
			return "", fmt.Errorf(
				"index is the constant %s, which this test cannot resolve -- add it to "+
					"knownFamilyNameConstants (taking the value from the package, not restating it)",
				index.Sel.Name)
		}
		return name, nil
	case *ast.Ident:
		return "", fmt.Errorf("index is the identifier %s, which this test cannot resolve", index.Name)
	default:
		return "", fmt.Errorf("index is a %T, which this test cannot resolve", expr)
	}
}

// remainingKindConstantRe matches one `KindRemainingXxx = "metrics.remaining.xxx"`
// const spec in internal/jobcontract/types.go.
var remainingKindConstantRe = regexp.MustCompile(`(KindRemaining\w+)\s*=\s*"([^"]+)"`)

// extractRemainingKindConstants reads internal/jobcontract/types.go and
// returns Kind constant identifier (e.g. "KindRemainingCapacity") -> its
// string value (e.g. "metrics.remaining.capacity").
func extractRemainingKindConstants(t *testing.T, repoRoot string) map[string]string {
	t.Helper()
	path := filepath.Join(repoRoot, "internal", "jobcontract", "types.go")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	result := map[string]string{}
	for _, match := range remainingKindConstantRe.FindAllStringSubmatch(string(raw), -1) {
		result[match[1]] = match[2]
	}
	if len(result) == 0 {
		t.Fatalf("found zero KindRemaining* constants in %s -- parsing broke, or the constants moved", path)
	}
	return result
}

// extractRemainingExecutorAssignment finds the switch on spec.Kind whose case
// labels are jobcontract.KindRemaining* (there is a SECOND, syntactically
// identical `switch spec.Kind` for daily-metrics kinds earlier in the same
// file -- disambiguated by requiring at least one case label to contain
// "Remaining"). For each case, it finds the addRemainingWorker[...](...) call
// in the case body and reads its 5th positional argument's identifier: the
// literal name `compatibility` means COMPAT, anything else means NATIVE.
// Returns Kind constant identifier -> "native" | "compat".
func extractRemainingExecutorAssignment(t *testing.T, file *ast.File) map[string]string {
	t.Helper()
	result := map[string]string{}
	var target *ast.SwitchStmt

	ast.Inspect(file, func(n ast.Node) bool {
		if target != nil {
			return false
		}
		sw, ok := n.(*ast.SwitchStmt)
		if !ok {
			return true
		}
		tagSel, ok := sw.Tag.(*ast.SelectorExpr)
		if !ok || tagSel.Sel.Name != "Kind" {
			return true
		}
		if !switchHasRemainingCase(sw) {
			return true
		}
		target = sw
		return false
	})
	if target == nil {
		t.Fatal("the metrics.remaining.* switch on spec.Kind was not found in daily.go -- wiring shape changed; update this test")
	}

	for _, stmt := range target.Body.List {
		clause, ok := stmt.(*ast.CaseClause)
		if !ok || len(clause.List) != 1 {
			continue
		}
		kindSel, ok := clause.List[0].(*ast.SelectorExpr)
		if !ok {
			continue
		}
		kindName := kindSel.Sel.Name
		if len(kindName) < len("KindRemaining") || kindName[:len("KindRemaining")] != "KindRemaining" {
			continue
		}
		argIdent := findAddRemainingWorkerFifthArg(clause.Body)
		if argIdent == "" {
			continue // e.g. `default:` clause, no addRemainingWorker call
		}
		if argIdent == "compatibility" {
			result[kindName] = "compat"
		} else {
			result[kindName] = "native"
		}
	}
	if len(result) == 0 {
		t.Fatal("found zero KindRemaining* case executor assignments -- parsing broke, or the switch shape changed")
	}
	return result
}

func switchHasRemainingCase(sw *ast.SwitchStmt) bool {
	for _, stmt := range sw.Body.List {
		clause, ok := stmt.(*ast.CaseClause)
		if !ok || len(clause.List) != 1 {
			continue
		}
		sel, ok := clause.List[0].(*ast.SelectorExpr)
		if ok && len(sel.Sel.Name) >= len("KindRemaining") && sel.Sel.Name[:len("KindRemaining")] == "KindRemaining" {
			return true
		}
	}
	return false
}

// findAddRemainingWorkerFifthArg walks a case-clause body for a call whose
// callee is the generic function addRemainingWorker[...] and returns the
// identifier name of its 5th positional argument (0-indexed 4), or "" if no
// such call is present.
func findAddRemainingWorkerFifthArg(body []ast.Stmt) string {
	var found string
	for _, stmt := range body {
		ast.Inspect(stmt, func(n ast.Node) bool {
			if found != "" {
				return false
			}
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			var funcIdent *ast.Ident
			switch fn := call.Fun.(type) {
			case *ast.IndexExpr: // addRemainingWorker[T](...)
				funcIdent, _ = fn.X.(*ast.Ident)
			case *ast.IndexListExpr: // generic with multiple type params, unused here but safe to handle
				funcIdent, _ = fn.X.(*ast.Ident)
			}
			if funcIdent == nil || funcIdent.Name != "addRemainingWorker" {
				return true
			}
			if len(call.Args) < 5 {
				return true
			}
			ident, ok := call.Args[4].(*ast.Ident)
			if !ok {
				return true
			}
			found = ident.Name
			return false
		})
		if found != "" {
			break
		}
	}
	return found
}

// buildRemainingArtifact maps Kind-constant-keyed executor assignments to
// family-name-keyed ("capacity", "dora", ...) results via the Kind constants'
// string values, which are byte-identical to remaining/families.json's
// `route_key` field (e.g. "metrics.remaining.capacity").
func buildRemainingArtifact(t *testing.T, repoRoot string, file *ast.File) map[string]string {
	t.Helper()
	kindConstants := extractRemainingKindConstants(t, repoRoot)
	executorByKindName := extractRemainingExecutorAssignment(t, file)

	valueToExecutor := map[string]string{}
	for kindName, executor := range executorByKindName {
		value, ok := kindConstants[kindName]
		if !ok {
			t.Fatalf("switch case %s has no matching KindRemaining* constant in internal/jobcontract/types.go", kindName)
		}
		valueToExecutor[value] = executor
	}

	familiesPath := filepath.Join(repoRoot, "internal", "jobs", "metrics", "remaining", "families.json")
	raw, err := os.ReadFile(familiesPath)
	if err != nil {
		t.Fatalf("reading %s: %v", familiesPath, err)
	}
	var inventory struct {
		Families []struct {
			Name     string `json:"name"`
			RouteKey string `json:"route_key"`
		} `json:"families"`
	}
	if err := json.Unmarshal(raw, &inventory); err != nil {
		t.Fatalf("unmarshalling %s: %v", familiesPath, err)
	}

	result := map[string]string{}
	for _, family := range inventory.Families {
		executor, ok := valueToExecutor[family.RouteKey]
		if !ok {
			t.Fatalf(
				"remaining family %q (route_key %q) has no matching case in daily.go's metrics.remaining.* switch",
				family.Name, family.RouteKey,
			)
		}
		result[family.Name] = executor
	}
	return result
}

func buildNativeFamiliesArtifact(t *testing.T) nativeFamiliesArtifact {
	t.Helper()
	repoRoot := repoRootFromCmdDailyWorker(t)
	_, file := parseDailyGoFile(t)
	partition, finalize, err := extractDailyFamilies(file)
	if err != nil {
		t.Fatalf("extracting daily family registrations: %v", err)
	}
	return nativeFamiliesArtifact{
		SchemaVersion: 1,
		GeneratedFrom: nativeFamiliesGeneratedFrom,
		Daily:         partition,
		Finalize:      finalize,
		Remaining:     buildRemainingArtifact(t, repoRoot, file),
		Workgraph:     extractWorkgraphExecutors(t, repoRoot),
	}
}

func artifactPath(repoRoot string) string {
	return filepath.Join(repoRoot, "contracts", "native-families", "v1", "native-families.json")
}

func marshalArtifact(t *testing.T, artifact nativeFamiliesArtifact) []byte {
	t.Helper()
	raw, err := json.MarshalIndent(artifact, "", "  ")
	if err != nil {
		t.Fatalf("marshalling native-families artifact: %v", err)
	}
	return append(raw, '\n')
}

func TestNativeFamiliesArtifactUpToDate(t *testing.T) {
	repoRoot := repoRootFromCmdDailyWorker(t)
	artifact := buildNativeFamiliesArtifact(t)
	expected := marshalArtifact(t, artifact)

	path := artifactPath(repoRoot)
	if os.Getenv("UPDATE_NATIVE_FAMILIES_ARTIFACT") == "1" {
		if err := os.WriteFile(path, expected, 0o644); err != nil {
			t.Fatalf("writing %s: %v", path, err)
		}
		return
	}

	actual, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf(
			"reading %s: %v -- run UPDATE_NATIVE_FAMILIES_ARTIFACT=1 go test ./cmd/dev-health-worker/... -run TestNativeFamiliesArtifactUpToDate to create it",
			path, err,
		)
	}
	if string(actual) != string(expected) {
		t.Fatalf(
			"contracts/native-families/v1/native-families.json is stale relative to cmd/dev-health-worker/daily.go's "+
				"actual registration wiring. Run:\n\n"+
				"  UPDATE_NATIVE_FAMILIES_ARTIFACT=1 go test ./cmd/dev-health-worker/... -run TestNativeFamiliesArtifactUpToDate\n\n"+
				"and commit the result.\n\nwant:\n%s\n\ngot:\n%s", expected, actual,
		)
	}
}

// TestNativeFamiliesArtifactMatchesKnownSplit is a falsification/regression
// control: it pins the exact remaining and daily splits, so an accidental
// wiring change is caught even if someone forgot to regenerate the artifact
// (that case is ALSO caught by the drift test above, but this one names the
// expected shape explicitly for a reviewer).
//
// The expected shape lives in the four want* slices below and NOWHERE ELSE.
// This comment used to restate it as "9 native + 1 post_bridge", and that
// prose went stale in silence: CHAOS-4283 added two post_bridge families and
// left the sentence saying one, and CHAOS-4285 then added a native family
// which made the "9" accidentally true again for the wrong reason. A count
// written in a comment cannot be asserted, so it is not written here -- the
// cardinality check below derives every total with len(), and the slices are
// the single source of truth a reviewer should read.
func TestNativeFamiliesArtifactMatchesKnownSplit(t *testing.T) {
	artifact := buildNativeFamiliesArtifact(t)

	wantRemainingNative := []string{
		"capacity", "dora", "membership_backfill", "recommendations",
		"release_impact", "work_item_attribution",
	}
	wantRemainingCompat := []string{"complexity"}
	assertExecutorSet(t, artifact.Remaining, wantRemainingNative, "native")
	assertExecutorSet(t, artifact.Remaining, wantRemainingCompat, "compat")
	if len(artifact.Remaining) != len(wantRemainingNative)+len(wantRemainingCompat) {
		t.Fatalf("expected exactly %d remaining families, got %d: %v",
			len(wantRemainingNative)+len(wantRemainingCompat), len(artifact.Remaining), artifact.Remaining)
	}

	wantDailyNative := []string{
		"team_wellbeing", "repo_user_commit", "incident", "deploy", "cicd",
		"file_hotspots", "file_risk_hotspots", "testops_risk",
		// CHAOS-5078: work_item_attribution is native, and its three readers
		// return to the pre_bridge (native) phase with it. They read
		// work_item_team_attributions, which it WRITES; families.json's
		// `after` edges order the writer ahead of its readers within the
		// phase, which is what made post_bridge unnecessary.
		"work_item_attribution", "work_item_state", "work_item", "work_item_estimate",
		// CHAOS-4284: the three TestOps families this PR ports. They were added
		// to the artifact but NOT to this list, which was a one-way SUBSET check
		// with no cardinality assertion on this branch -- so it certified a split
		// it had never seen, exactly the defect CHAOS-4283's r1 P3 added the
		// cardinality check below to stop. That check caught this on the merge.
		"testops_pipeline", "testops_test", "testops_coverage",
		// CHAOS-4285. This entry was MISSING on this branch until the
		// merge-forward of main exposed it. The branch registered
		// ai_governance as a native daily executor and updated the generated
		// artifact, but never updated this expected split -- and the test
		// still passed, because at that point both halves were one-way SUBSET
		// checks. Main's own codex r1 (P3) added the cardinality check below,
		// and that check is what turned this silent gap into a failure.
		"ai_governance",
		// CHAOS-4279: review_edges is pre_bridge, not post_bridge -- both its
		// inputs are RAW SYNC tables, not another daily family's output, so
		// nothing in this partition has to run before it.
		"review_edges",
		// CHAOS-4280, same shape as ai_governance above.
		"ai_impact",
		// CHAOS-4286: work_graph_edges is pre_bridge for the same reason --
		// every input is a raw sync table plus the shared incident
		// projection, so nothing else in the partition has to precede it.
		"work_graph_edges",
		// CHAOS-4286 part B: ai_workflow is pre_bridge for the same reason --
		// its inputs (git_pull_requests, work_graph_issue_pr) are raw sync
		// tables, not another daily family's own output.
		"ai_workflow",
	}
	// CHAOS-5078 retired the CHAOS-4283 three (work_item_state, work_item,
	// work_item_estimate) from post_bridge -- they moved to native/pre_bridge
	// above, alongside work_item_attribution which now writes
	// work_item_team_attributions natively (families.json's `after` edges
	// order the writer ahead of them within pre_bridge, retiring the reason
	// post_bridge existed for them). compounding_risk is the one remaining
	// post_bridge family, for a DIFFERENT reason CHAOS-5078 does not touch:
	// not a stale attribution snapshot, but execution order -- its input
	// repo_metrics_daily is written by repo_user_commit in the SAME
	// partition, and computeNativeFamilies walks families in SORTED order,
	// where "compounding_risk" precedes "repo_user_commit". CHAOS-5078
	// already retired work_item_state/work_item/work_item_estimate from this
	// list (moved to native/pre_bridge, per the comment above -- landed on
	// the base branch between this PR's fork point and its squash-merge, so
	// they never appear here at all in this PR's own diff).
	// benchmarking (CHAOS-4288) is NO LONGER in this list either -- CHAOS-5194
	// relocated it to finalize scope (see wantDailyFinalize below and
	// BenchmarkingFinalizeExecutor's own doc comment).
	wantDailyPostBridge := []string{"compounding_risk"}
	assertExecutorSet(t, artifact.Daily, wantDailyNative, "native")
	assertExecutorSet(t, artifact.Daily, wantDailyPostBridge, "post_bridge")
	// The cardinality check the Remaining half above already had, and this half
	// did not. Without it these two assertions are one-way SUBSET checks: they
	// prove every EXPECTED family has the expected verdict, and say nothing
	// about families that are present and unexpected. Codex r1 (P3, EXECUTED)
	// demonstrated the consequence -- this branch added work_item and
	// work_item_estimate as post_bridge and the stale test still passed, so a
	// test calling itself an "exact split" certified a split it had never seen.
	if len(artifact.Daily) != len(wantDailyNative)+len(wantDailyPostBridge) {
		t.Fatalf("expected exactly %d daily families, got %d: %v",
			len(wantDailyNative)+len(wantDailyPostBridge), len(artifact.Daily), artifact.Daily)
	}

	// CHAOS-4290: ic_finalize is the first RUN-scoped native family. It gets its
	// OWN exact-cardinality check rather than joining the count above, because
	// the two scopes answer different questions and folding them would let a
	// finalize family appear while a partition family silently disappeared.
	wantDailyFinalize := []string{"ic_finalize", "team_cognitive_load", "team_complexity", "benchmarking"}
	assertExecutorSet(t, artifact.Finalize, wantDailyFinalize, "finalize")
	if len(artifact.Finalize) != len(wantDailyFinalize) {
		t.Fatalf("expected exactly %d finalize families, got %d: %v",
			len(wantDailyFinalize), len(artifact.Finalize), artifact.Finalize)
	}
}

// The NEGATIVE control for the extractor, and the reason the whole scope went
// unnoticed: a guard blind to a scope passes by construction, so the guard must
// be shown to FAIL on a scope it does not know.
//
// Both arms matter. An unknown MAP identifier is a fourth registration scope
// added without telling this test. An unresolvable INDEX is the shape #2243
// actually used -- `finalize[daily.ICFinalizeFamilyName]` -- which the previous
// literal-only extractor dropped silently even for a map it did recognise.
func TestExtractorRefusesRegistrationsItCannotClassify(t *testing.T) {
	for _, testCase := range []struct{ name, body, wantSubstring string }{
		{
			name:          "unknown scope map",
			body:          `preBridgeLate["some_family"] = x`,
			wantSubstring: "",
		},
		{
			name:          "unresolvable constant index",
			body:          `finalize[daily.NotAKnownConstant] = x`,
			wantSubstring: "NotAKnownConstant",
		},
		{
			name:          "unresolvable identifier index",
			body:          `finalize[someLocalName] = x`,
			wantSubstring: "someLocalName",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			source := "package main\nfunc dailyNativeFamilyRegistrations() {\n" +
				"\tnative := map[string]any{}\n\tfinalize := map[string]any{}\n" +
				"\tpreBridgeLate := map[string]any{}\n\tvar x, someLocalName any\n" +
				"\tnative[\"team_wellbeing\"] = x\n\t_ = someLocalName\n\t_ = preBridgeLate\n\t" +
				testCase.body + "\n}\n"
			fset := token.NewFileSet()
			file, err := parser.ParseFile(fset, "synthetic.go", source, 0)
			if err != nil {
				t.Fatalf("parsing the synthetic source: %v", err)
			}
			_, _, extractErr := extractDailyFamilies(file)
			if testCase.wantSubstring == "" {
				// An unknown map is NOT an error -- it is legitimately not one of
				// the registration maps. What must hold is that it is not silently
				// classified INTO one of them.
				partition, finalize, err := extractDailyFamilies(file)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if _, leaked := partition["some_family"]; leaked {
					t.Fatal("a registration into an unknown map was classified as a partition family")
				}
				if _, leaked := finalize["some_family"]; leaked {
					t.Fatal("a registration into an unknown map was classified as a finalize family")
				}
				return
			}
			if extractErr == nil {
				t.Fatalf("extractor accepted %q -- an index it cannot resolve is a "+
					"SILENTLY DROPPED registration, which is exactly how the finalize "+
					"scope went unnoticed", testCase.body)
			}
			if !strings.Contains(extractErr.Error(), testCase.wantSubstring) {
				t.Fatalf("error %q does not name %q, so it cannot tell a maintainer what to fix",
					extractErr, testCase.wantSubstring)
			}
		})
	}
}

func assertExecutorSet(t *testing.T, got map[string]string, names []string, want string) {
	t.Helper()
	for _, name := range names {
		if got[name] != want {
			t.Errorf("expected %s executor for %q, got %q", want, name, got[name])
		}
	}
}

// --- workgraph/investment section (CHAOS-4441) ---

// workgraphExecutorByIdent maps addWorkgraphWorker's executor VARIABLES to the
// executor label the artifact records. The identifiers are the ones that
// function actually passes to each New*Handler call, so a rename that is not
// reflected here fails loudly in extractWorkgraphExecutors rather than
// silently recording the wrong label.
// The names are addWorkgraphWorker's own PARAMETERS, not the caller's
// variables: buildWorkgraphWorker calls them `compatibility`/`nativeInvestment`
// but the function under parse receives them as `executor`/`nativeInvestment`,
// and this map reads the callee. Both spellings of the bridge parameter are
// listed so a future rename in either direction is a deliberate edit here
// rather than a silent mislabel.
var workgraphExecutorByIdent = map[string]string{
	"executor":         "compat",
	"compatibility":    "compat",
	"nativeInvestment": "native",
}

// extractWorkgraphExecutors statically parses cmd/dev-health-worker/workgraph.go's
// addWorkgraphWorker switch and records, per River kind, WHICH executor that
// kind's handler is constructed with.
//
// This is the machine-checked half of the cutover. The prose claim "investment
// .materialize is native now" is worth nothing on its own -- CHAOS-4441 was
// marked Done for a day while every kind still went to the bridge, and no
// artifact in the tree could contradict it. This function makes the dispatch
// switch itself the source of truth.
func extractWorkgraphExecutors(t *testing.T, repoRoot string) map[string]string {
	t.Helper()

	kindValues := parseJobContractKindValues(t, repoRoot)

	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "workgraph.go", nil, 0)
	if err != nil {
		t.Fatalf("parsing workgraph.go: %v", err)
	}

	var dispatch *ast.FuncDecl
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if ok && function.Name.Name == "addWorkgraphWorker" {
			dispatch = function
			break
		}
	}
	if dispatch == nil {
		t.Fatal("addWorkgraphWorker not found in workgraph.go -- the dispatch switch this artifact reads has moved")
	}

	result := map[string]string{}
	ast.Inspect(dispatch, func(node ast.Node) bool {
		clause, ok := node.(*ast.CaseClause)
		if !ok || len(clause.List) != 1 {
			return true
		}
		selector, ok := clause.List[0].(*ast.SelectorExpr)
		if !ok {
			return true
		}
		kind, known := kindValues[selector.Sel.Name]
		if !known {
			t.Fatalf("addWorkgraphWorker switches on jobcontract.%s, which is not a kind constant", selector.Sel.Name)
		}

		// Find the New*Handler call inside this case and read its SECOND
		// argument -- the executor. Every handler constructor in this switch
		// takes (store, executor, ...), so the position is uniform.
		executor := ""
		ast.Inspect(clause, func(inner ast.Node) bool {
			call, ok := inner.(*ast.CallExpr)
			if !ok || len(call.Args) < 2 {
				return true
			}
			function, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || !handlerConstructorPattern.MatchString(function.Sel.Name) {
				return true
			}
			ident, ok := call.Args[1].(*ast.Ident)
			if !ok {
				t.Fatalf("kind %q passes a non-identifier executor to %s -- this parse cannot label it", kind, function.Sel.Name)
			}
			label, known := workgraphExecutorByIdent[ident.Name]
			if !known {
				t.Fatalf(
					"kind %q is constructed with executor variable %q, which has no label in "+
						"workgraphExecutorByIdent. Add it there (and say whether it is native or compat) "+
						"in the same change that introduced it.",
					kind, ident.Name,
				)
			}
			executor = label
			return false
		})
		if executor == "" {
			t.Fatalf("no handler constructor found for kind %q in addWorkgraphWorker", kind)
		}
		result[kind] = executor
		return true
	})

	if len(result) == 0 {
		t.Fatal("addWorkgraphWorker's switch yielded no kinds -- the parse is broken, not the wiring")
	}
	return result
}

var handlerConstructorPattern = regexp.MustCompile(`^New[A-Za-z]+Handler$`)

// parseJobContractKindValues reads internal/jobcontract/types.go so the kind
// STRINGS in the artifact are the real constant values rather than a
// hand-transcribed table that could drift from them.
func parseJobContractKindValues(t *testing.T, repoRoot string) map[string]string {
	t.Helper()
	path := filepath.Join(repoRoot, "internal", "jobcontract", "types.go")
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, nil, 0)
	if err != nil {
		t.Fatalf("parsing %s: %v", path, err)
	}
	values := map[string]string{}
	ast.Inspect(file, func(node ast.Node) bool {
		spec, ok := node.(*ast.ValueSpec)
		if !ok || len(spec.Names) != 1 || len(spec.Values) != 1 {
			return true
		}
		literal, ok := spec.Values[0].(*ast.BasicLit)
		if !ok || literal.Kind != token.STRING {
			return true
		}
		unquoted, err := strconv.Unquote(literal.Value)
		if err != nil {
			return true
		}
		values[spec.Names[0].Name] = unquoted
		return true
	})
	return values
}

// TestWorkgraphArtifactRecordsTheInvestmentCutover is the falsification control
// for the flip: it names the expected executor per kind explicitly, so a
// revert of the cutover fails HERE with a readable message even if someone
// regenerated the artifact to match the reverted wiring.
//
// investment.dispatch/chunk/finalize were deleted entirely under CHAOS-4438
// (dead Go shells, zero producers ever created a request row for them) --
// they no longer appear in the artifact at all. workgraph.build stays compat
// until CHAOS-4924's six remaining sub-builders land.
func TestWorkgraphArtifactRecordsTheInvestmentCutover(t *testing.T) {
	artifact := buildNativeFamiliesArtifact(t)

	want := map[string]string{
		"workgraph.build":        "compat",
		"investment.materialize": "native",
	}
	if len(artifact.Workgraph) != len(want) {
		t.Fatalf("expected %d workgraph kinds, got %d: %v", len(want), len(artifact.Workgraph), artifact.Workgraph)
	}
	for kind, expected := range want {
		if artifact.Workgraph[kind] != expected {
			t.Errorf("expected %s executor for %q, got %q", expected, kind, artifact.Workgraph[kind])
		}
	}
}
