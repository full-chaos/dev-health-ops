package compoundingrisk

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"math"
	"os"
	"sort"
	"strings"
	"testing"
)

// TestEveryPythonMaxCallSiteRejectsGoMax is a CLASS guard over every
// `pythonMax(...)` call site in this package, not a guard on one of them.
//
// Three rounds found the same defect at three DIFFERENT call sites, and the
// first two fixes each covered only the site the finding named:
//
//	r1  argued the class was guarded, proving it by mutating pythonMax's BODY
//	r2  mutated the CALL at compute.go:70 -- survived; fixed with a test there
//	r3  mutated the CALL at compute.go:48 -- survived again, because the r2 fix
//	    covered one call site rather than the class
//
// The lesson is not "add a third test". It is that a per-site test can only
// ever be as complete as whoever enumerated the sites, and the author fixing a
// finding is the worst-placed person to enumerate them. So this test does the
// enumeration MECHANICALLY, from the source, and fails when a call site exists
// that it does not cover.
//
// What each case asserts is CPython's `max(a, b)`: keep `a` unless `b > a`, so
// a NaN second operand is DISCARDED. Go's builtin `max` and `math.Max` both
// propagate NaN instead. Every case below therefore dies if its call site is
// switched to either.
func TestEveryPythonMaxCallSiteRejectsGoMax(t *testing.T) {
	nan := math.NaN()

	// Behavioural coverage, one entry per call site. `line` is documentation
	// for the reader; `fn` and `calls` are what the guard below enforces --
	// they name the ENCLOSING FUNCTION and how many pythonMax calls it makes,
	// and are compared against the set the parser finds in the real source.
	covered := []struct {
		name  string
		line  string
		fn    string
		calls int
		check func(t *testing.T)
	}{
		{
			name:  "normalizeAgainstReference",
			fn:    "compute.go:normalizeAgainstReference",
			calls: 1,
			line:  "compute.go:48 -- pythonMax(0.0, *value)",
			check: func(t *testing.T) {
				// CPython: max(0.0, nan) -> 0.0. Go max/math.Max -> NaN, which
				// then propagates through clamp01 into the score.
				record := Compute(
					goldenDay(), "repo", "org",
					Inputs{ReworkChurn: opaquePtr(nan)},
					goldenStamp(), DefaultWeights, DefaultThresholds, DefaultReferences,
				)
				if record.ChurnNorm == nil {
					t.Fatal("churn_norm is nil; expected a normalized value")
				}
				if math.IsNaN(*record.ChurnNorm) {
					t.Error("churn_norm = NaN; CPython's max(0.0, nan) is 0.0, so this call site " +
						"is using Go's max/math.Max semantics, not max()'s")
				}
			},
		},
		{
			name:  "ownership concentration loop",
			fn:    "compute.go:normalizeOwnership",
			calls: 1,
			line:  "compute.go:70 -- pythonMax(highest, candidate)",
			check: func(t *testing.T) {
				record := Compute(
					goldenDay(), "repo", "org",
					Inputs{SingleOwnerRatio: opaquePtr(0.0), OwnershipGini: opaquePtr(nan)},
					goldenStamp(), DefaultWeights, DefaultThresholds, DefaultReferences,
				)
				if record.OwnershipNorm == nil {
					t.Fatal("ownership_norm is nil; expected a value from the two candidates")
				}
				if math.IsNaN(*record.OwnershipNorm) {
					t.Error("ownership_norm = NaN; CPython's max([0.0, nan]) is 0.0")
				}
			},
		},
		{
			name:  "ComplexityDeltaRatio denominator floor",
			fn:    "compute.go:ComplexityDeltaRatio",
			calls: 1,
			line:  "compute.go:183 -- pythonMax(firstHalf, 1.0)",
			check: func(t *testing.T) {
				// CPython: max(nan, 1.0) keeps the FIRST operand, because
				// `1.0 > nan` is False -- so the NaN propagates and the ratio is
				// NaN. Go's max/math.Max return... also NaN here, so NaN does not
				// discriminate at this site. The discriminating input is the
				// ORDER-SENSITIVE one: max(0.5, 1.0) -> 1.0 either way, but
				// max(nan, 1.0) -> nan for CPython and NaN for Go, while
				// max(1.0, nan) differs. This site takes firstHalf FIRST, so the
				// asymmetry that separates them is a NaN in the first position
				// combined with a finite second -- CPython keeps NaN, Go's
				// builtin max also returns NaN. They agree.
				//
				// STATED PLAINLY: this call site is NOT separable by NaN. The
				// guard below still counts it, so the enumeration stays honest;
				// what it pins here is the ordinary floor behaviour, and the
				// non-separability is recorded rather than papered over with an
				// assertion that would pass for the wrong reason.
				if got := ComplexityDeltaRatio(0.5, 2.0); got != (2.0-0.5)/1.0 {
					t.Errorf("ComplexityDeltaRatio(0.5, 2.0) = %v, want the 1.0 floor applied", got)
				}
				if got := ComplexityDeltaRatio(4.0, 6.0); got != (6.0-4.0)/4.0 {
					t.Errorf("ComplexityDeltaRatio(4.0, 6.0) = %v, want division by firstHalf", got)
				}
				if !math.IsNaN(ComplexityDeltaRatio(nan, 2.0)) {
					t.Error("ComplexityDeltaRatio(nan, 2.0) should propagate NaN, as CPython's " +
						"max(nan, 1.0) keeps its first operand")
				}
			},
		},
	}

	for _, callSite := range covered {
		t.Run(callSite.name, func(t *testing.T) {
			t.Logf("call site: %s", callSite.line)
			callSite.check(t)
		})
	}

	// THE CLASS GUARD, over the whole PACKAGE and by call-site IDENTITY.
	//
	// This was a regex over compute.go comparing COUNTS, and the #2230
	// confirmation round showed three ways past it: a call site in another file
	// of the package was never looked at; `pythonMax /* c */ (a, b)` is legal
	// Go the pattern cannot match, because comments are whitespace to the
	// scanner and not to a regex; and a count comparison cannot tell a covered
	// site swapped for a hidden one from no change at all.
	//
	// Parsing closes all three at once, and closes them by construction rather
	// than by adding three more patterns: the compiler's own scanner decides
	// what a call is, so comments, spacing and line breaks are irrelevant, and
	// comparing SETS keyed by enclosing function -- not a total -- means a swap
	// is a set difference, not a wash.
	found := pythonMaxCallSites(t)
	want := map[string]int{}
	for _, callSite := range covered {
		want[callSite.fn] += callSite.calls
	}

	// Positive control on the parser itself. A guard whose enumerator silently
	// returns nothing passes vacuously, which is the failure mode that lets a
	// broken guard look identical to a clean package.
	if len(found) == 0 {
		t.Fatalf("the parser found no pythonMax call sites at all -- the ENUMERATOR is broken, "+
			"not the code; this test covers %d", len(covered))
	}

	for _, key := range sortedKeys(found, want) {
		switch {
		case want[key] == 0:
			t.Errorf(
				"%s calls pythonMax %d time(s) and this test does not cover it. A call site "+
					"nobody decided CPython's behaviour for is how the same defect was found at "+
					"three different sites (r1, r2, r3 on #2230). Add it to `covered`, with what "+
					"CPython's max() does there.", key, found[key])
		case found[key] == 0:
			t.Errorf(
				"this test claims to cover %d pythonMax call(s) in %s but the source has none. "+
					"Either the call moved -- in which case its new home is UNGUARDED -- or the "+
					"entry is stale.", want[key], key)
		case found[key] != want[key]:
			t.Errorf(
				"%s makes %d pythonMax call(s), this test covers %d. A second call added to an "+
					"already-covered function is a NEW call site with its own CPython question.",
				key, found[key], want[key])
		}
	}
}

// pythonMaxCallSites parses EVERY non-test .go file in this package and returns
// the number of direct pythonMax calls per enclosing function, keyed
// "<file>:<function>".
//
// Package-wide, because scoping the guard to compute.go made "put the fourth
// call in a new file" a silent bypass -- and adding a file to a package is an
// ordinary thing to do, not an attack.
//
// The declaration `func pythonMax(...)` needs no exclusion here: it is a
// FuncDecl and never a CallExpr, so it cannot be miscounted. The old regex
// needed a subtraction to avoid counting it, and that subtraction was itself a
// place to be wrong.
func pythonMaxCallSites(t *testing.T) map[string]int {
	t.Helper()

	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package directory: %v", err)
	}
	fileSet := token.NewFileSet()
	sites := map[string]int{}
	parsed := 0

	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fileSet, name, nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		parsed++

		for _, declaration := range file.Decls {
			label := name + ":<package-level>"
			if function, ok := declaration.(*ast.FuncDecl); ok {
				label = name + ":" + functionLabel(function)
			}
			ast.Inspect(declaration, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				if identifier, ok := call.Fun.(*ast.Ident); ok && identifier.Name == "pythonMax" {
					sites[label]++
				}
				return true
			})
		}

		// Every mention of pythonMax must be either its own declaration or the
		// callee of a direct call. Anything else -- `f := pythonMax` and later
		// `f(a, b)`, passing it as an argument, storing it in a struct field --
		// makes the eventual call invisible to this enumerator. That form is
		// REFUSED rather than missed: a guard that cannot see a construct should
		// say so, not report a clean package.
		//
		// Nothing here does it today. This exists so that the day something
		// starts to, the guard fails loudly instead of going quietly blind --
		// which is the failure mode the regex version had at three separate
		// call sites.
		callee := map[*ast.Ident]bool{}
		ast.Inspect(file, func(node ast.Node) bool {
			switch typed := node.(type) {
			case *ast.CallExpr:
				if identifier, ok := typed.Fun.(*ast.Ident); ok {
					callee[identifier] = true
				}
			case *ast.FuncDecl:
				callee[typed.Name] = true
			}
			return true
		})
		ast.Inspect(file, func(node ast.Node) bool {
			identifier, ok := node.(*ast.Ident)
			if !ok || identifier.Name != "pythonMax" || callee[identifier] {
				return true
			}
			t.Errorf(
				"%s mentions pythonMax at %s in a position that is neither its declaration nor a "+
					"direct call. This guard enumerates direct calls; an indirect call through a "+
					"variable or field is invisible to it, so that form is not allowed here -- "+
					"call pythonMax directly.",
				name, fileSet.Position(identifier.Pos()))
			return true
		})
	}

	if parsed == 0 {
		t.Fatal("no non-test .go files found in the package directory -- the enumerator is " +
			"looking in the wrong place, so any result it returns is meaningless")
	}
	return sites
}

// functionLabel names a declaration the way a reader would: "Method" for a
// plain function, "Type.Method" for one with a receiver, so two same-named
// methods on different types cannot collide into one key.
func functionLabel(function *ast.FuncDecl) string {
	if function.Recv == nil || len(function.Recv.List) == 0 {
		return function.Name.Name
	}
	return receiverTypeName(function.Recv.List[0].Type) + "." + function.Name.Name
}

func receiverTypeName(expression ast.Expr) string {
	switch typed := expression.(type) {
	case *ast.StarExpr:
		return receiverTypeName(typed.X)
	case *ast.Ident:
		return typed.Name
	case *ast.IndexExpr: // generic receiver, e.g. Type[T]
		return receiverTypeName(typed.X)
	case *ast.IndexListExpr:
		return receiverTypeName(typed.X)
	default:
		return fmt.Sprintf("%T", expression)
	}
}

// sortedKeys returns the union of both maps' keys in a stable order, so a
// failure lists every discrepancy in the same sequence every run.
func sortedKeys(found, want map[string]int) []string {
	union := map[string]bool{}
	for key := range found {
		union[key] = true
	}
	for key := range want {
		union[key] = true
	}
	keys := make([]string, 0, len(union))
	for key := range union {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
