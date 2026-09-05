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
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"testing"
)

type nativeFamiliesArtifact struct {
	SchemaVersion int               `json:"schema_version"`
	GeneratedFrom string            `json:"generated_from"`
	Daily         map[string]string `json:"daily"`
	Remaining     map[string]string `json:"remaining"`
}

const nativeFamiliesGeneratedFrom = "cmd/dev-health-worker/daily.go (static AST parse, cmd/dev-health-worker/native_families_artifact_test.go)"

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

// extractDailyNativeFamilies walks dailyNativeFamilyRegistrations' body for
// `native["name"] = ...` / `postBridge["name"] = ...` assignments and returns
// family name -> "native" | "post_bridge".
func extractDailyNativeFamilies(t *testing.T, file *ast.File) map[string]string {
	t.Helper()
	result := map[string]string{}
	var target *ast.FuncDecl
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if ok && fn.Name.Name == "dailyNativeFamilyRegistrations" {
			target = fn
			break
		}
	}
	if target == nil {
		t.Fatal("dailyNativeFamilyRegistrations function not found in daily.go -- wiring was renamed or removed; update this test")
	}
	ast.Inspect(target.Body, func(n ast.Node) bool {
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
		lit, ok := index.Index.(*ast.BasicLit)
		if !ok || lit.Kind != token.STRING {
			return true
		}
		name, err := strconv.Unquote(lit.Value)
		if err != nil {
			t.Fatalf("unquoting family name literal %q: %v", lit.Value, err)
		}
		switch mapIdent.Name {
		case "native":
			result[name] = "native"
		case "postBridge":
			result[name] = "post_bridge"
		}
		return true
	})
	if len(result) == 0 {
		t.Fatal("found zero native/postBridge family assignments in dailyNativeFamilyRegistrations -- parsing broke, or the wiring shape changed; update this test")
	}
	return result
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
	return nativeFamiliesArtifact{
		SchemaVersion: 1,
		GeneratedFrom: nativeFamiliesGeneratedFrom,
		Daily:         extractDailyNativeFamilies(t, file),
		Remaining:     buildRemainingArtifact(t, repoRoot, file),
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

	wantRemainingNative := []string{"capacity", "dora", "membership_backfill", "recommendations", "work_item_attribution"}
	wantRemainingCompat := []string{"complexity", "release_impact"}
	assertExecutorSet(t, artifact.Remaining, wantRemainingNative, "native")
	assertExecutorSet(t, artifact.Remaining, wantRemainingCompat, "compat")
	if len(artifact.Remaining) != len(wantRemainingNative)+len(wantRemainingCompat) {
		t.Fatalf("expected exactly %d remaining families, got %d: %v",
			len(wantRemainingNative)+len(wantRemainingCompat), len(artifact.Remaining), artifact.Remaining)
	}

	wantDailyNative := []string{
		"team_wellbeing", "repo_user_commit", "incident", "deploy", "cicd",
		"file_hotspots", "file_risk_hotspots", "testops_risk",
		// CHAOS-4285. This entry was MISSING on this branch until the
		// merge-forward of main exposed it. The branch registered
		// ai_governance as a native daily executor and updated the generated
		// artifact, but never updated this expected split -- and the test
		// still passed, because at that point both halves were one-way SUBSET
		// checks. Main's own codex r1 (P3) added the cardinality check below,
		// and that check is what turned this silent gap into a failure.
		"ai_governance",
		// CHAOS-4280, same shape as ai_governance directly above: this branch
		// registered ai_impact as a native daily executor but this expected
		// split predates the merge-forward that brought ai_governance's fix
		// (and this cardinality check) in -- add it now rather than let the
		// one-way-subset gap reopen for a second family.
		"ai_impact",
	}
	// CHAOS-4283: work_item and work_item_estimate join work_item_state in
	// post_bridge -- all three read work_item_team_attributions, which the
	// still-Python work_item_attribution family writes in the same partition.
	wantDailyPostBridge := []string{"work_item_state", "work_item", "work_item_estimate"}
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
}

func assertExecutorSet(t *testing.T, got map[string]string, names []string, want string) {
	t.Helper()
	for _, name := range names {
		if got[name] != want {
			t.Errorf("expected %s executor for %q, got %q", want, name, got[name])
		}
	}
}
