package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// CHAOS-4035. The unreclaimable sweep was added to buildSyncMutationPipeline
// on the domain pool while its first statement read a coordinator-exclusive
// table. Nothing noticed. The pool-composition comment above that function
// claimed the pipeline "is wired so activation does not ship a 42501" and
// simply did not mention the sweep, so the comment stayed literally true of
// what it listed while being wrong about the function it described.
//
// A comment cannot check itself, and neither can the reviewer who wrote it.
// This test reads the actual source: every call inside buildSyncMutationPipeline
// that is handed one of the three runtime pools must be pinned here with the
// exact pool set it receives, and the doc comment must name every pinned
// component. Adding a component therefore forces three edits that a reviewer
// sees together -- the call, the pin, and the comment -- instead of one that
// nobody sees at all.
var checkedInPoolComposition = map[string][]string{
	// The defect: this one needs ALL THREE. worker_job_routes is
	// coordinator-exclusive; sync_run_units is SELECT-only for the
	// coordinator, so the write cannot follow the read; and river_job lives
	// under the queue role's schema USAGE alone (CHAOS-4097), so the
	// delivery-liveness read cannot join either of the other two.
	"buildUnreclaimableSweep": {"coordinatorPool", "domainPool", "queuePool"},
	// sync_run_reference_discoveries and sync_run_post_dispatches are
	// coordinator-exclusive.
	"syncreconciler.NewMaterializer": {"coordinatorPool"},
	// CHAOS-4583: joins sync_run_reference_discoveries, which the queue role
	// has no grant on at all -- same pool as Materializer, same reasoning.
	"syncreconciler.NewTerminalOutboxClose": {"coordinatorPool"},
	// River job tables live under the queue role.
	"syncreconciler.NewTerminalDeliveryRepair": {"queuePool"},
	"riverpgxv5.New": {"queuePool"},
	// Domain-granted tables only.
	"syncreconciler.NewLeaseRepair": {"domainPool"},
	"syncreconciler.NewObserver":    {"domainPool"},
	// Observe/claim on the domain role, River delivery on the queue role.
	"syncreconciler.NewKernel": {"domainPool", "queuePool"},
	// The publish closure's reference read.
	"syncDispatchReference": {"domainPool"},
}

// componentsTheCommentMustName maps a pinned callee to the word the
// pool-composition comment has to use for it. A pin without a matching word
// in the comment is exactly the CHAOS-4035 shape: wired, and undocumented.
var componentsTheCommentMustName = map[string]string{
	"buildUnreclaimableSweep":                  "UnreclaimableSweep",
	"syncreconciler.NewMaterializer":           "Materializer",
	"syncreconciler.NewTerminalOutboxClose":    "TerminalOutboxClose",
	"syncreconciler.NewTerminalDeliveryRepair": "TerminalDeliveryRepair",
	"syncreconciler.NewLeaseRepair":            "LeaseRepair",
	"syncreconciler.NewObserver":               "Observer",
	"syncreconciler.NewKernel":                 "Kernel",
	"riverpgxv5.New":                           "River client",
	"syncDispatchReference":                    "publish closure",
}

const poolCompositionFunc = "buildSyncMutationPipeline"

func runtimePoolNames() map[string]bool {
	return map[string]bool{"coordinatorPool": true, "domainPool": true, "queuePool": true}
}

func TestPoolCompositionMatchesItsPin(t *testing.T) {
	observed, _ := parsePoolComposition(t)

	for callee, wantPools := range checkedInPoolComposition {
		gotPools, wired := observed[callee]
		if !wired {
			t.Errorf("%s is pinned to pools %v but is no longer handed a pool in %s; "+
				"delete the pin deliberately rather than leaving it to rot",
				callee, wantPools, poolCompositionFunc)
			continue
		}
		if !equalStringSets(gotPools, wantPools) {
			t.Errorf("%s is constructed with %v, pinned as %v.\n"+
				"  If the change is intended, update the pin AND the "+
				"pool-composition comment above %s in the same edit.\n"+
				"  If it is not, this is CHAOS-4035 again: a component reading "+
				"one role's exclusive tables through another role's pool ships "+
				"green and answers 42501 in production.",
				callee, gotPools, wantPools, poolCompositionFunc)
		}
	}
	for callee, gotPools := range observed {
		if _, pinned := checkedInPoolComposition[callee]; !pinned {
			t.Errorf("%s is handed pools %v inside %s but is not pinned. "+
				"Add it to checkedInPoolComposition and to the pool-composition "+
				"comment, naming the role-exclusive tables that decide its pool.",
				callee, gotPools, poolCompositionFunc)
		}
	}
}

// The comment is the artefact the next author reads. AC5 of CHAOS-4035 asks
// for it to be accurate; this is what keeps it that way after the ticket is
// closed.
func TestPoolCompositionCommentNamesEveryPinnedComponent(t *testing.T) {
	_, doc := parsePoolComposition(t)
	if doc == "" {
		t.Fatalf("%s has no doc comment; the pool composition is undocumented", poolCompositionFunc)
	}
	for callee := range checkedInPoolComposition {
		word, described := componentsTheCommentMustName[callee]
		if !described {
			t.Errorf("%s is pinned but has no expected comment wording; add one", callee)
			continue
		}
		if !strings.Contains(doc, word) {
			t.Errorf("the pool-composition comment above %s never mentions %q (%s), "+
				"so the next component added to this pipeline reads an incomplete list",
				poolCompositionFunc, word, callee)
		}
	}
	// Non-vacuity: the comment must actually discuss pools, not merely happen
	// to contain the component names somewhere in prose.
	for pool := range runtimePoolNames() {
		role := strings.TrimSuffix(pool, "Pool")
		if !strings.Contains(strings.ToLower(doc), role) {
			t.Errorf("the pool-composition comment never names the %s pool", role)
		}
	}
}

// parsePoolComposition returns, for each callee inside buildSyncMutationPipeline,
// the sorted set of runtime pool identifiers passed to it directly, plus that
// function's doc comment.
func parsePoolComposition(t *testing.T) (map[string][]string, string) {
	t.Helper()
	fileSet := token.NewFileSet()
	parsed, err := parser.ParseFile(
		fileSet, filepath.Join(".", "dependencies.go"), nil, parser.ParseComments,
	)
	if err != nil {
		t.Fatalf("parse dependencies.go: %v", err)
	}
	pools := runtimePoolNames()
	observed := map[string]map[string]bool{}
	doc := ""
	found := false
	for _, decl := range parsed.Decls {
		function, isFunc := decl.(*ast.FuncDecl)
		if !isFunc || function.Name.Name != poolCompositionFunc {
			continue
		}
		found = true
		if function.Doc != nil {
			doc = function.Doc.Text()
		}
		ast.Inspect(function.Body, func(node ast.Node) bool {
			call, isCall := node.(*ast.CallExpr)
			if !isCall {
				return true
			}
			name := calleeName(call.Fun)
			if name == "" {
				return true
			}
			for _, arg := range call.Args {
				identifier, isIdent := arg.(*ast.Ident)
				if !isIdent || !pools[identifier.Name] {
					continue
				}
				if observed[name] == nil {
					observed[name] = map[string]bool{}
				}
				observed[name][identifier.Name] = true
			}
			return true
		})
	}
	if !found {
		t.Fatalf("%s not found in dependencies.go", poolCompositionFunc)
	}
	flattened := make(map[string][]string, len(observed))
	for name, set := range observed {
		names := make([]string, 0, len(set))
		for pool := range set {
			names = append(names, pool)
		}
		sort.Strings(names)
		flattened[name] = names
	}
	return flattened, doc
}

func calleeName(expr ast.Expr) string {
	switch fun := expr.(type) {
	case *ast.Ident:
		return fun.Name
	case *ast.SelectorExpr:
		pkg, isIdent := fun.X.(*ast.Ident)
		if !isIdent {
			return ""
		}
		return pkg.Name + "." + fun.Sel.Name
	default:
		return ""
	}
}

func equalStringSets(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	sortedWant := append([]string(nil), want...)
	sort.Strings(sortedWant)
	for index := range got {
		if got[index] != sortedWant[index] {
			return false
		}
	}
	return true
}
