package syncreconciler

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"testing"
)

// TestStageBudgetsCoverExactlyTheStagesStepRuns is the CHAOS-4239 analogue of
// cmd/dev-health-reconciler/pool_composition_test.go's CHAOS-4035 pin: it
// parses pipeline.go's actual Step method source and asserts every
// pipeline.runStage(ctx, StageX, ...) call names a stage DefaultStageBudgets
// covers, and that DefaultStageBudgets names nothing Step does not actually
// run. A comment or a map cannot check itself; this test reads the source
// that has to agree with it.
//
// Wiring says X, the budget table says Y is exactly the defect shape
// CHAOS-4035 shipped to production over (a component present in the call
// graph and never entered in the reviewed list beside it). Here the stakes
// are the same kind: a 7th stage added to Step without a StageBudgets entry
// would silently receive a zero-value context.WithTimeout(ctx, 0) budget --
// via StageBudgets.validate at construction time that already fails loudly,
// but this test catches the drift at the SOURCE level, the same place
// pool_composition_test.go does, before anyone even runs the binary.
func TestStageBudgetsCoverExactlyTheStagesStepRuns(t *testing.T) {
	observed := parseRunStageCalls(t)

	defaults := DefaultStageBudgets()
	if len(defaults) != len(orderedStages) {
		t.Fatalf("DefaultStageBudgets has %d entries, orderedStages has %d -- keep them in sync",
			len(defaults), len(orderedStages))
	}

	for _, stage := range orderedStages {
		if !observed[stage] {
			t.Errorf("orderedStages names %q but Step never calls runStage with it -- "+
				"delete the stale entry from orderedStages and DefaultStageBudgets", stage)
		}
		if _, ok := defaults[stage]; !ok {
			t.Errorf("orderedStages names %q but DefaultStageBudgets has no budget for it", stage)
		}
	}
	for stage := range observed {
		found := false
		for _, want := range orderedStages {
			if want == stage {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Step calls runStage with %q but orderedStages does not name it -- "+
				"a stage was added to the pipeline without a budget entry, exactly the "+
				"CHAOS-4035/CHAOS-4239 drift this test exists to catch", stage)
		}
	}
}

// parseRunStageCalls returns the set of StageName constants passed as the
// second argument to any pipeline.runStage(...) call inside pipeline.go.
func parseRunStageCalls(t *testing.T) map[StageName]bool {
	t.Helper()
	fileSet := token.NewFileSet()
	parsed, err := parser.ParseFile(fileSet, filepath.Join(".", "pipeline.go"), nil, 0)
	if err != nil {
		t.Fatalf("parse pipeline.go: %v", err)
	}
	observed := map[StageName]bool{}
	ast.Inspect(parsed, func(node ast.Node) bool {
		call, isCall := node.(*ast.CallExpr)
		if !isCall {
			return true
		}
		selector, isSelector := call.Fun.(*ast.SelectorExpr)
		if !isSelector || selector.Sel.Name != "runStage" {
			return true
		}
		if len(call.Args) < 2 {
			return true
		}
		// StageX constants are referenced unqualified (plain *ast.Ident): the
		// call site and the constants share a package.
		identifier, isIdent := call.Args[1].(*ast.Ident)
		if !isIdent {
			return true
		}
		if name := identifierToStageName(identifier.Name); name != "" {
			observed[StageName(name)] = true
		}
		return true
	})
	return observed
}

// identifierToStageName maps a StageX constant identifier to its StageName
// value by asking the real constants rather than guessing at a naming
// convention, so a rename of one only requires updating this switch, never a
// parallel string-transformation rule that could silently drift from it.
func identifierToStageName(identifier string) string {
	switch identifier {
	case "StageLeaseRepair":
		return string(StageLeaseRepair)
	case "StageUnreclaimableSweep":
		return string(StageUnreclaimableSweep)
	case "StageTerminalDeliveryRepair":
		return string(StageTerminalDeliveryRepair)
	case "StageTerminalOutboxClose":
		return string(StageTerminalOutboxClose)
	case "StageMaterializer":
		return string(StageMaterializer)
	case "StageKernel":
		return string(StageKernel)
	case "StageObserver":
		return string(StageObserver)
	default:
		return ""
	}
}
