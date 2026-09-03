package pythonparity

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestEveryRotGuardUsesTheRegistry is what makes the single-source registry
// actually single.
//
// Extracting the field lists into payloadGuard removes today's duplication. It
// does not stop tomorrow's: a new rot guard can call
// `comparePayload(frozen, rendered, "cases")` with literals and be perfectly
// correct on the day it is written, while silently re-opening the drift the
// registry closed. Nothing else in the package would notice, because a guard
// comparing the right fields by literal passes every test there is.
//
// So this asserts the PROPERTY rather than the instance: inside
// *_rot_guard_test.go, comparePayload is never passed a string literal. The
// fields must come from a registry entry, which is the only way the guard and
// TestShippedFixturesExposeThePayloadFieldsTheGuardsCompare are forced to agree.
//
// It reads the AST rather than grepping, because a grep for `comparePayload(.*"`
// matches a literal inside a comment or an error message on the same line, and
// misses a call whose arguments wrap onto the next one. Both mistakes were made
// in this package before -- a comment-stripped proof was demanded once precisely
// because a claimed "comments only" change was a string literal.
func TestEveryRotGuardUsesTheRegistry(t *testing.T) {
	guards, err := filepath.Glob("*_rot_guard_test.go")
	if err != nil {
		t.Fatal(err)
	}
	// A glob that matches nothing would make every assertion below vacuous.
	if len(guards) == 0 {
		t.Fatal("no *_rot_guard_test.go files found; this test would pass by " +
			"examining nothing")
	}

	callSites := 0
	for _, path := range guards {
		fileSet := token.NewFileSet()
		parsed, err := parser.ParseFile(fileSet, path, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		ast.Inspect(parsed, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			name, ok := call.Fun.(*ast.Ident)
			if !ok || name.Name != "comparePayload" {
				return true
			}
			callSites++
			for index, argument := range call.Args {
				literal, ok := argument.(*ast.BasicLit)
				if !ok || literal.Kind != token.STRING {
					continue
				}
				t.Errorf("%s:%d: comparePayload is passed the string literal %s as "+
					"argument %d. Field names must come from a payloadGuard entry, or "+
					"this guard and TestShippedFixturesExposeThePayloadFieldsTheGuards"+
					"Compare can disagree about which fields are compared -- the exact "+
					"drift the registry exists to make impossible.",
					path, fileSet.Position(literal.Pos()).Line, literal.Value, index)
			}
			return true
		})
	}

	// The detector must have had something to detect. Without this, deleting
	// every guard would turn this test green.
	if callSites == 0 {
		t.Error("no comparePayload call sites found in any *_rot_guard_test.go; " +
			"either the guards stopped using it or this detector stopped working, " +
			"and a detector that finds nothing reports exactly what a clean tree does")
	}
}

// TestTheRegistryDetectorActuallyDetects is the positive control for the test
// above.
//
// Without it, TestEveryRotGuardUsesTheRegistry passing means either "no guard
// uses literals" or "the AST walk is broken and finds nothing" -- and those look
// identical from the outside. This feeds the detector source it MUST reject.
func TestTheRegistryDetectorActuallyDetects(t *testing.T) {
	const offending = `package pythonparity

func guard() {
	comparePayload(nil, nil, "cases", "distinct_input_values")
}
`
	fileSet := token.NewFileSet()
	parsed, err := parser.ParseFile(fileSet, "synthetic_rot_guard_test.go",
		strings.NewReader(offending), 0)
	if err != nil {
		t.Fatal(err)
	}

	found := 0
	ast.Inspect(parsed, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		if name, ok := call.Fun.(*ast.Ident); !ok || name.Name != "comparePayload" {
			return true
		}
		for _, argument := range call.Args {
			if literal, ok := argument.(*ast.BasicLit); ok && literal.Kind == token.STRING {
				found++
			}
		}
		return true
	})

	if found != 2 {
		t.Errorf("the literal detector found %d string arguments in source that "+
			"contains exactly 2; the check in TestEveryRotGuardUsesTheRegistry "+
			"cannot be trusted, and its green would mean nothing", found)
	}
}

// TestRegistryFixturesExistOnDisk stops the registry naming a file that is not
// shipped.
//
// comparePayload fails closed on a missing field, but a guard pointed at a
// fixture that does not exist fails at os.ReadFile with a path error -- which
// reads as an environment problem rather than as a registry that has rotted.
func TestRegistryFixturesExistOnDisk(t *testing.T) {
	if len(allPayloadGuards) == 0 {
		t.Fatal("empty registry; nothing asserted")
	}
	for _, guard := range allPayloadGuards {
		path := filepath.Join("../../tests/fixtures", guard.fixture)
		if _, err := os.Stat(path); err != nil {
			t.Errorf("registry names %q but it is not on disk (%v); the guard "+
				"referencing it would fail with a path error, which reads as a broken "+
				"checkout rather than a stale registry", guard.fixture, err)
		}
		if len(guard.fields) == 0 {
			t.Errorf("registry entry %q names no fields; comparePayload would compare "+
				"nothing and the guard would pass unconditionally", guard.fixture)
		}
	}
}
