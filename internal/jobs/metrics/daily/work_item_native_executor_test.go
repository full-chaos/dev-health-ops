package daily

import (
	"context"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

// workItemScopeGolden decodes only the item half of
// tests/fixtures/daily_work_item_python_golden.json -- the same frozen file the
// workitemmetrics parity tests read, produced from real production Python.
type workItemScopeGolden struct {
	Items []struct {
		WorkItemID    string  `json:"work_item_id"`
		Provider      string  `json:"provider"`
		WorkScopeID   string  `json:"work_scope_id"`
		ProjectID     *string `json:"project_id"`
		ProjectKey    *string `json:"project_key"`
		ProjectName   *string `json:"project_name"`
		NativeTeamKey *string `json:"native_team_key"`
	} `json:"items"`
}

func deref(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

// TestWorkItemMetricsRowWorkScopeIDMatchesPythonGolden pins THIS package's
// work_scope_id derivation against Python's WorkItem.work_scope_id property.
//
// The workitemmetrics parity tests cannot cover this: the shared compute takes
// work_scope_id as a caller-supplied input (Attribution.WorkScopeID), so it is
// believed there, not verified. The derivation itself lives here, on
// workItemStateWorkItem.workScopeID (embedded by workItemMetricsRow) -- and it
// encodes five ordered fallbacks, including the jira-prefers-project_key arm
// and the linear team-only native_team_key arm, either of which could regress
// without any other test in this repo noticing.
func TestWorkItemMetricsRowWorkScopeIDMatchesPythonGolden(t *testing.T) {
	data, err := os.ReadFile(filepath.Join(
		"..", "..", "..", "..", "tests", "fixtures", "daily_work_item_python_golden.json",
	))
	if err != nil {
		t.Fatal(err)
	}
	var golden workItemScopeGolden
	if err := json.Unmarshal(data, &golden); err != nil {
		t.Fatal(err)
	}
	if len(golden.Items) == 0 {
		t.Fatal("golden fixture has no items")
	}

	// The corpus must contain shapes that DISCRIMINATE each arm -- not merely
	// shapes that reach it.
	//
	// This distinction is not pedantic: it was measured. An earlier version of
	// this test asserted only "a jira item with a project_key exists", and
	// deleting the jira arm from workScopeID entirely left it GREEN (mutation
	// M6 survived) -- because with project_id/project_name/native_team_key all
	// empty, the FINAL project_key fallback returns the identical answer. A
	// test whose expected value equals the fallback proves nothing about which
	// path ran. Each flag below therefore requires the shape where removing
	// that arm CHANGES the result.
	var discriminatesJira, discriminatesProjectName, discriminatesNativeTeamKey, sawEmpty bool
	for _, item := range golden.Items {
		row := workItemMetricsRow{workItemStateWorkItem: workItemStateWorkItem{
			WorkItemID:    item.WorkItemID,
			Provider:      item.Provider,
			ProjectKey:    deref(item.ProjectKey),
			ProjectID:     deref(item.ProjectID),
			ProjectName:   deref(item.ProjectName),
			NativeTeamKey: deref(item.NativeTeamKey),
		}}
		if got := row.workScopeID(); got != item.WorkScopeID {
			t.Errorf("%s work_scope_id: got %q, python produced %q",
				item.WorkItemID, got, item.WorkScopeID)
		}
		// jira arm: only discriminating when a LOWER-precedence field would
		// otherwise win, i.e. project_id/project_name/native_team_key is also
		// set to a different value.
		if item.Provider == "jira" && deref(item.ProjectKey) != "" &&
			(deref(item.ProjectID) != "" || deref(item.ProjectName) != "" ||
				deref(item.NativeTeamKey) != "") {
			discriminatesJira = true
		}
		// project_name arm: discriminating when project_id is empty (so the
		// arm is reached) and native_team_key is set to something else (so
		// dropping the arm changes the answer rather than falling to "").
		if deref(item.ProjectID) == "" && deref(item.ProjectName) != "" &&
			deref(item.NativeTeamKey) != "" &&
			deref(item.ProjectName) != deref(item.NativeTeamKey) {
			discriminatesProjectName = true
		}
		// native_team_key arm: discriminating when it is the ONLY source, so
		// dropping it yields "" instead of the key.
		if deref(item.ProjectID) == "" && deref(item.ProjectName) == "" &&
			deref(item.NativeTeamKey) != "" && deref(item.ProjectKey) == "" {
			discriminatesNativeTeamKey = true
		}
		if item.WorkScopeID == "" {
			sawEmpty = true
		}
	}
	if !discriminatesJira {
		t.Error(
			"corpus has no jira item with project_key AND a lower-precedence " +
				"scope field set differently; without it, deleting the jira arm " +
				"of workScopeID leaves this test GREEN (measured: mutation M6 " +
				"survived exactly this gap)",
		)
	}
	if !discriminatesProjectName {
		t.Error(
			"corpus has no item where project_name is reached AND differs from " +
				"native_team_key; the project_name arm is undiscriminated",
		)
	}
	if !discriminatesNativeTeamKey {
		t.Error(
			"corpus has no item whose ONLY scope source is native_team_key; " +
				"the native_team_key fallback arm is undiscriminated",
		)
	}
	if !sawEmpty {
		t.Error("corpus never produces an EMPTY work_scope_id; this test is vacuous for that arm")
	}
}

func TestNewWorkItemExecutorsRefuseANilConnection(t *testing.T) {
	if _, err := NewWorkItemExecutor(nil); err == nil {
		t.Fatal("NewWorkItemExecutor accepted a nil connection; every native family executor must fail closed")
	}
	if _, err := NewWorkItemEstimateExecutor(nil); err == nil {
		t.Fatal("NewWorkItemEstimateExecutor accepted a nil connection; every native family executor must fail closed")
	}
}

// TestWorkItemExecutorsRefuseAPartitionWithNoOrganizationOrDay covers the
// precondition both executors share via newWorkItemPartitionScope. A partition
// that reached here without an org or a target day is a dispatcher bug, and
// computing an empty day-window against the whole org would write plausible
// WRONG rows rather than failing.
func TestWorkItemExecutorsRefuseAPartitionWithNoOrganizationOrDay(t *testing.T) {
	day := time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)
	repo := uuid.New().String()

	for _, testCase := range []struct {
		name string
		run  Run
	}{
		{"no organization", Run{TargetDay: day}},
		{"no target day", Run{OrganizationID: "org-a"}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			partition := Partition{ID: "partition-a", RepoIDs: []RepositoryID{RepositoryID(repo)}}
			if _, err := (&WorkItemExecutor{conn: stubDriverConn{}}).ComputeFamily(
				context.Background(), testCase.run, partition,
			); err == nil {
				t.Error("work_item accepted a partition with no organization or target day")
			}
			if _, err := (&WorkItemEstimateExecutor{conn: stubDriverConn{}}).ComputeFamily(
				context.Background(), testCase.run, partition,
			); err == nil {
				t.Error("work_item_estimate accepted a partition with no organization or target day")
			}
		})
	}
}

// TestWorkItemExecutorsRefuseUnparseableRepoIDs guards the other half of
// newWorkItemPartitionScope: a malformed repo id must refuse, not silently
// compute over the remaining repos and report a partial row count as success.
func TestWorkItemExecutorsRefuseUnparseableRepoIDs(t *testing.T) {
	run := Run{OrganizationID: "org-a", TargetDay: time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)}
	partition := Partition{ID: "partition-a", RepoIDs: []RepositoryID{RepositoryID("not-a-uuid")}}

	if _, err := (&WorkItemExecutor{conn: stubDriverConn{}}).ComputeFamily(
		context.Background(), run, partition,
	); err == nil {
		t.Error("work_item accepted an unparseable repo id")
	}
	if _, err := (&WorkItemEstimateExecutor{conn: stubDriverConn{}}).ComputeFamily(
		context.Background(), run, partition,
	); err == nil {
		t.Error("work_item_estimate accepted an unparseable repo id")
	}
}

// TestNilWorkItemExecutorsRefuse covers the nil-receiver arm every native
// family executor carries.
func TestNilWorkItemExecutorsRefuse(t *testing.T) {
	var workItem *WorkItemExecutor
	if _, err := workItem.ComputeFamily(context.Background(), Run{}, Partition{}); err == nil {
		t.Error("a nil WorkItemExecutor did not refuse")
	}
	var estimate *WorkItemEstimateExecutor
	if _, err := estimate.ComputeFamily(context.Background(), Run{}, Partition{}); err == nil {
		t.Error("a nil WorkItemEstimateExecutor did not refuse")
	}
}

// TestWorkItemLoadersShareTheLoadWorkItemsPredicate asserts that
// LoadWorkItemMetricsWorkItems and LoadWorkItemStateWorkItems apply the SAME
// row-selection predicate -- the one ported from
// ClickHouseMetricsLoader.load_work_items (loaders/clickhouse.py:454).
//
// The two loaders exist separately only because they select different COLUMN
// sets. If their WHERE clauses ever drift, one family silently computes over a
// different population than the other from the same partition, and no other
// test in this repo would notice: the parity tests take items as given, and the
// integration test drives only one loader per family.
//
// This parses the two query strings out of their own source file rather than
// restating the predicate as a literal here. A restated literal is a THIRD copy
// that can drift from both, and would let a matching pair of wrong clauses pass.
func TestWorkItemLoadersShareTheLoadWorkItemsPredicate(t *testing.T) {
	metricsWhere := whereClauseOf(t, "work_item_native_clickhouse.go", "LoadWorkItemMetricsWorkItems")
	stateWhere := whereClauseOf(t, "work_item_state_native_clickhouse.go", "LoadWorkItemStateWorkItems")

	if metricsWhere != stateWhere {
		t.Errorf(
			"the two work-item loaders no longer share load_work_items' predicate.\n"+
				"  LoadWorkItemMetricsWorkItems: %s\n"+
				"  LoadWorkItemStateWorkItems:   %s\n"+
				"They must select the SAME rows and differ only in columns; if this "+
				"divergence is deliberate, the Python loader they both port "+
				"(loaders/clickhouse.py:454) changed and BOTH need updating.",
			metricsWhere, stateWhere,
		)
	}
	// Vacuity guard: an extraction bug that returned "" for both would make the
	// comparison above trivially true.
	for _, clause := range []string{metricsWhere, stateWhere} {
		for _, required := range []string{"created_at < ?", "status != 'done'", "completed_at >= ?"} {
			if !strings.Contains(clause, required) {
				t.Fatalf("extracted predicate %q is missing %q -- the extractor, not the "+
					"loaders, is broken, and the equality assertion above is vacuous",
					clause, required)
			}
		}
	}
}

// whereClauseOf extracts the WHERE clause of the SQL actually passed to the
// query call inside the named function, normalised to one space-separated line.
//
// It parses the Go AST and binds extraction to the ARGUMENT of the Query call,
// rather than scanning the function's source for a raw literal. Codex r1 (P2)
// showed why the scanning version was not a guard at all: it took the first
// backtick after the signature, so adding a harmless, used raw-string constant
// containing the CURRENT correct WHERE clause before the real SQL let the real
// query regress freely -- the guard would extract the decoy, match, and pass its
// own fragment checks. A check that can be satisfied by a string that is not the
// executed query is indistinguishable from no check.
//
// Concatenation is handled by folding a BinaryExpr of string literals, since
// these queries are assembled as `"SELECT " + sharedColumns + " FROM ... WHERE"`.
// Anything that is not a foldable literal expression fails loudly rather than
// silently yielding a partial string.
func whereClauseOf(t *testing.T, fileName, funcName string) string {
	t.Helper()
	fileSet := token.NewFileSet()
	parsed, err := parser.ParseFile(fileSet, fileName, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", fileName, err)
	}

	var target *ast.FuncDecl
	for _, decl := range parsed.Decls {
		function, ok := decl.(*ast.FuncDecl)
		if ok && function.Name.Name == funcName {
			target = function
			break
		}
	}
	if target == nil {
		t.Fatalf("%s not found in %s", funcName, fileName)
	}

	var queries []string
	ast.Inspect(target, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || len(call.Args) < 2 {
			return true
		}
		// Both the direct `conn.Query(ctx, sql, ...)` shape and the
		// `remaining.QueryWorkItemDerivationSubjects(ctx, conn, sql, ...)`
		// helper shape put the SQL in a fixed position; take whichever
		// argument folds to a string containing WHERE.
		if selector.Sel.Name != "Query" && !strings.HasPrefix(selector.Sel.Name, "Query") {
			return true
		}
		for _, arg := range call.Args {
			folded, ok := foldStringExpr(arg)
			if ok && strings.Contains(folded, "WHERE") {
				queries = append(queries, folded)
			}
		}
		return true
	})

	if len(queries) == 0 {
		t.Fatalf("%s contains no query call whose SQL argument has a WHERE clause", funcName)
	}
	if len(queries) > 1 {
		t.Fatalf("%s issues %d WHERE-bearing queries; this guard assumes exactly one", funcName, len(queries))
	}
	wherePos := strings.Index(queries[0], "WHERE")
	return strings.Join(strings.Fields(queries[0][wherePos:]), " ")
}

// foldStringExpr resolves a string literal, or a `+` chain of them, to its
// value. A package-qualified constant (remaining.WorkItemDerivationSubjectColumns)
// cannot be resolved from this file's AST alone; it contributes no WHERE clause,
// so it folds to an empty string rather than failing the whole extraction.
func foldStringExpr(expr ast.Expr) (string, bool) {
	switch node := expr.(type) {
	case *ast.BasicLit:
		if node.Kind != token.STRING {
			return "", false
		}
		value, err := strconv.Unquote(node.Value)
		if err != nil {
			return "", false
		}
		return value, true
	case *ast.BinaryExpr:
		if node.Op != token.ADD {
			return "", false
		}
		left, leftOK := foldStringExpr(node.X)
		right, rightOK := foldStringExpr(node.Y)
		if !leftOK && !rightOK {
			return "", false
		}
		return left + right, true
	case *ast.SelectorExpr, *ast.Ident:
		return "", true
	default:
		return "", false
	}
}
