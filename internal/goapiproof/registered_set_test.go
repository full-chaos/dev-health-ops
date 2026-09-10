package goapiproof

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"sort"
	"testing"
)

// queryRouteSource is cmd/query-api's route table -- the artefact that
// decides which operations the running binary actually registers.
const queryRouteSource = "../../cmd/query-api/query_route.go"

// digestByOperationVar is the map literal inside mountQueryRoutes whose
// KEYS are the registered operation names. query_route.go's own
// TestMountedRouteLogMessage_ListsExactlyRegisteredOperations depends on
// this same assignment keeping its shape, and says so in a comment there.
const digestByOperationVar = "digestByOperation"

// TestOperationSpecsCoverExactlyWhatQueryAPIRegisters is the guard that
// does not depend on anyone remembering.
//
// CHAOS-4991 registered `pr` and CHAOS-5523 registered
// `featureFlagEvents` without adding entries to operationSpecs. Nothing
// failed: this package's tests checked operationSpecs against a
// hand-written list in its own test file, so the two agreed with each
// other while both disagreed with the binary. It surfaced when
// go-api-prove refused a live JOB 5 run -- correctly, via AssertCoverage,
// but hours after the merge and at the worst possible moment.
//
// Trap #122 tracked three artefacts that must move together when an
// operation is registered. This is the fourth: the prover's own request
// table. The test derives BOTH sides from source -- the registered set by
// parsing cmd/query-api's route table, the covered set from
// operationSpecs itself -- so neither can be updated to match the other
// by hand.
func TestOperationSpecsCoverExactlyWhatQueryAPIRegisters(t *testing.T) {
	registered := registeredOperationsFromRouteTable(t)

	// A parse that finds nothing must FAIL, not pass vacuously. If
	// query_route.go is refactored so the literal no longer has this
	// shape, this test must say so rather than quietly assert that the
	// empty set is covered -- which every table trivially satisfies.
	if len(registered) == 0 {
		t.Fatalf("parsed %s and found no operations in the %s literal: the route table changed shape and this guard stopped guarding",
			queryRouteSource, digestByOperationVar)
	}

	covered := map[string]bool{}
	for _, name := range KnownOperations() {
		covered[name] = true
	}
	registeredSet := map[string]bool{}
	for _, name := range registered {
		registeredSet[name] = true
	}

	var uncovered, stale []string
	for _, name := range registered {
		if !covered[name] {
			uncovered = append(uncovered, name)
		}
	}
	for _, name := range KnownOperations() {
		if !registeredSet[name] {
			stale = append(stale, name)
		}
	}
	sort.Strings(uncovered)
	sort.Strings(stale)

	if len(uncovered) > 0 {
		t.Errorf("cmd/query-api registers %v but operationSpecs has no entry for them: go-api-prove will refuse EVERY run against a deployment carrying this build (AssertCoverage), which is what happened to JOB 5 on 2026-09-10. Add an entry per operation -- and if its request cannot be built from the org and the window, set InstanceVariable rather than inventing a value",
			uncovered)
	}
	if len(stale) > 0 {
		t.Errorf("operationSpecs covers %v but cmd/query-api does not register them: a stale entry reads as coverage while proving nothing",
			stale)
	}
}

// TestTheHandWrittenRegisteredListMatchesTheRouteTable keeps the list in
// operations_test.go honest.
//
// That list exists so the refusal tests have a known-good base to
// perturb, and a second independent statement of the set makes a drift
// legible. Both of those depend on it being RIGHT, and it silently was
// not for two operations -- so it is checked against source too rather
// than trusted.
func TestTheHandWrittenRegisteredListMatchesTheRouteTable(t *testing.T) {
	fromSource := registeredOperationsFromRouteTable(t)
	if len(fromSource) == 0 {
		t.Fatalf("parsed %s and found no operations: see the sibling test", queryRouteSource)
	}

	byHand := append([]string(nil), registeredOperations...)
	sort.Strings(byHand)

	if len(byHand) != len(fromSource) {
		t.Fatalf("registeredOperations lists %d operations, cmd/query-api registers %d\n  by hand: %v\n  source:  %v",
			len(byHand), len(fromSource), byHand, fromSource)
	}
	for i := range byHand {
		if byHand[i] != fromSource[i] {
			t.Fatalf("registeredOperations disagrees with cmd/query-api at index %d: %q vs %q\n  by hand: %v\n  source:  %v",
				i, byHand[i], fromSource[i], byHand, fromSource)
		}
	}
}

// registeredOperationsFromRouteTable parses cmd/query-api's route table
// and returns the operation names it registers, sorted.
//
// Parsed with go/ast rather than matched with a regex: a regex that stops
// matching returns the empty set, and an empty set passes a coverage
// check trivially. The AST walk either finds the assignment or the
// caller's length check fails the test.
func registeredOperationsFromRouteTable(t *testing.T) []string {
	t.Helper()

	path, err := filepath.Abs(queryRouteSource)
	if err != nil {
		t.Fatalf("resolve %s: %v", queryRouteSource, err)
	}
	file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}

	var names []string
	ast.Inspect(file, func(n ast.Node) bool {
		assign, ok := n.(*ast.AssignStmt)
		if !ok || len(assign.Lhs) != 1 || len(assign.Rhs) != 1 {
			return true
		}
		ident, ok := assign.Lhs[0].(*ast.Ident)
		if !ok || ident.Name != digestByOperationVar {
			return true
		}
		literal, ok := assign.Rhs[0].(*ast.CompositeLit)
		if !ok {
			return true
		}
		for _, element := range literal.Elts {
			pair, ok := element.(*ast.KeyValueExpr)
			if !ok {
				continue
			}
			key, ok := pair.Key.(*ast.BasicLit)
			if !ok || key.Kind != token.STRING {
				continue
			}
			// The key is a quoted string literal; strip the quotes
			// without unquoting escapes, because an operation name that
			// needed escaping would not be a valid GraphQL operation name
			// in the first place.
			if len(key.Value) >= 2 {
				names = append(names, key.Value[1:len(key.Value)-1])
			}
		}
		return false
	})

	sort.Strings(names)
	return names
}

// `pr` is the one registered operation whose request cannot be built from
// the org and the window: its document requires $id, an identifier for one
// stored pull request.
//
// The table must still COVER it -- an operation missing from
// operationSpecs refuses the whole run via AssertCoverage, which is the
// failure this change exists to fix -- so the entry exists and declares
// what it cannot supply. This pins that declaration, because the failure
// mode if it is dropped is silent: `ID!` accepts any string, so an
// invented id returns null on BOTH planes and the comparison reports a
// match having measured nothing.
func TestPrDeclaresTheIdentifierItCannotSupply(t *testing.T) {
	spec, err := SpecFor("pr")
	if err != nil {
		t.Fatalf("SpecFor(\"pr\"): %v -- it must be COVERED, or every run is refused", err)
	}
	if spec.InstanceVariable != "id" {
		t.Fatalf("pr.InstanceVariable = %q, want \"id\": without it the run sends an invented identifier and compares null to null", spec.InstanceVariable)
	}

	// And it is the ONLY one, so this does not become a quiet way to
	// exclude an operation that could in fact be measured.
	for _, name := range KnownOperations() {
		other, err := SpecFor(name)
		if err != nil {
			t.Fatalf("SpecFor(%q): %v", name, err)
		}
		if name != "pr" && other.InstanceVariable != "" {
			t.Fatalf("%s declares InstanceVariable=%q: if an operation CAN be built from the org and the window, it must be, not skipped", name, other.InstanceVariable)
		}
	}
}

// r1 P3: the guard above enumerates the map LITERAL, so an operation
// added at runtime -- `digestByOperation["futureOperation"] = ...` after
// the literal -- is invisible to it. The reviewer demonstrated exactly
// that, and query-api's own mounted-route test caught it there (18 logged
// vs 17 in the catalog), but this package would have gone on believing
// its coverage was complete.
//
// Static analysis cannot follow a runtime insertion. What it CAN do is
// refuse to let one exist unnoticed: the literal is the only place the
// map is allowed to gain entries, so any later index assignment to it
// fails here with a pointer at this test. That is the honest boundary --
// the guard does not claim to enumerate runtime state, it claims the
// route table has no runtime state to enumerate, and checks it.
func TestTheRouteTableGainsOperationsOnlyInItsLiteral(t *testing.T) {
	path, err := filepath.Abs(queryRouteSource)
	if err != nil {
		t.Fatalf("resolve %s: %v", queryRouteSource, err)
	}
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}

	var offenders []string
	ast.Inspect(file, func(n ast.Node) bool {
		assign, ok := n.(*ast.AssignStmt)
		if !ok {
			return true
		}
		for _, lhs := range assign.Lhs {
			index, ok := lhs.(*ast.IndexExpr)
			if !ok {
				continue
			}
			ident, ok := index.X.(*ast.Ident)
			if !ok || ident.Name != digestByOperationVar {
				continue
			}
			offenders = append(offenders, fset.Position(assign.Pos()).String())
		}
		return true
	})

	if len(offenders) > 0 {
		t.Fatalf("%s is assigned by index at %v: TestOperationSpecsCoverExactlyWhatQueryAPIRegisters reads the map LITERAL, so an operation registered this way is invisible to it and go-api-prove would refuse the run in production instead. Put the operation in the literal, or teach that test to follow this",
			digestByOperationVar, offenders)
	}
}
