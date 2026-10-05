package workersctl

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

// CHAOS-8710 F8: finalize-redrive opens ClickHouse (and reads its credential)
// only on the path that can write a marker.
func TestFinalizeRedriveOpensClickHouseOnlyWhenItCanWriteAMarker(t *testing.T) {
	for _, test := range []struct {
		dryRun, includeSucceeded, want bool
	}{
		{false, true, true},
		{false, false, false},
		{true, true, false},
		{true, false, false},
	} {
		if got := finalizeRedriveWritesMarker(test.dryRun, test.includeSucceeded); got != test.want {
			t.Fatalf("finalizeRedriveWritesMarker(dryRun=%v, includeSucceeded=%v) = %v, want %v",
				test.dryRun, test.includeSucceeded, got, test.want)
		}
	}
}

// CHAOS-8710 F11: the helper above only matters if the verb's ClickHouse open
// is guarded by it. Parse the verb and require that attachRunMarker is called
// only inside an `if` whose condition calls finalizeRedriveWritesMarker.
func TestFinalizeRedriveVerbGuardsItsClickHouseOpenWithTheHelper(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "main.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var verb *ast.FuncDecl
	for _, declaration := range file.Decls {
		if function, ok := declaration.(*ast.FuncDecl); ok && function.Name.Name == "dispatchMetricsFinalizeRedrive" {
			verb = function
		}
	}
	if verb == nil {
		t.Fatal("dispatchMetricsFinalizeRedrive not found: the guard cannot be checked")
	}
	callsNamed := func(node ast.Node, name string) bool {
		found := false
		ast.Inspect(node, func(child ast.Node) bool {
			if call, ok := child.(*ast.CallExpr); ok {
				if ident, ok := call.Fun.(*ast.Ident); ok && ident.Name == name {
					found = true
				}
			}
			return !found
		})
		return found
	}
	guarded, unguarded := 0, 0
	var walk func(node ast.Node, inGuard bool)
	walk = func(node ast.Node, inGuard bool) {
		ast.Inspect(node, func(child ast.Node) bool {
			switch typed := child.(type) {
			case *ast.IfStmt:
				if callsNamed(typed.Cond, "finalizeRedriveWritesMarker") {
					walk(typed.Body, true)
					return false
				}
			case *ast.CallExpr:
				if ident, ok := typed.Fun.(*ast.Ident); ok && ident.Name == "attachRunMarker" {
					if inGuard {
						guarded++
					} else {
						unguarded++
					}
				}
			}
			return true
		})
	}
	walk(verb.Body, false)
	if guarded != 1 || unguarded != 0 {
		t.Fatalf("attachRunMarker calls in the finalize-redrive verb: guarded by finalizeRedriveWritesMarker=%d, unguarded=%d, want 1 and 0", guarded, unguarded)
	}
}
