package pythonparity

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// comparePayloadOffences reports every comparePayload call in *parsed* whose
// registry argument is not a plain identifier.
//
// # ONE IMPLEMENTATION, DELIBERATELY
//
// This function exists as a named thing because the previous version of this
// file had TWO copies of the walk: the real check, and a "positive control"
// with its own duplicated copy. codex round 1 on CHAOS-4914 proved what that
// costs -- it changed the REAL detector to accept only token.CHAR, added a
// direct "cases" literal to a guard, and all four tests still passed. The
// control certified its own copy while the thing it controlled was broken.
//
// That is the exact defect this PR was opened to fix, committed inside the test
// written to prevent it. The registry stops a guard and its test disagreeing
// about fields; this stops a detector and its control disagreeing about what
// counts as an offence. Same fix, one level up.
//
// # WHY "MUST BE AN IDENTIFIER" RATHER THAN "MUST NOT BE A LITERAL"
//
// The first version rejected *ast.BasicLit arguments. That is a blacklist of
// one shape, and both codex and lane-ci-flakes walked around it in a line:
// assign the literal to a variable and the call site is an *ast.Ident. codex
// went further and showed it was not theoretical -- it mutated a guard to
// compare an indirect "cases", DROPPING distinct_input_values, and the check
// stayed green.
//
// So this whitelists the one permitted shape instead. A registry entry is
// referenced by name; anything else -- a literal, a composite literal built
// inline, a function call, a field selector -- is an offence. Every evasion
// fails by construction rather than by having been anticipated, and the rule
// gets STRICTER as people invent new shapes rather than looser.
//
// comparePayload's signature does most of the work now (it takes a payloadGuard,
// so a call site cannot pass a field name at all). This remains as the secondary
// guard that stops an inline `payloadGuard{...}` literal rebuilding the
// duplication the registry removed.
func comparePayloadOffences(fileSet *token.FileSet, parsed *ast.File) []string {
	var offences []string
	ast.Inspect(parsed, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		name, ok := call.Fun.(*ast.Ident)
		if !ok {
			return true
		}
		// The unvalidated core is for the synthetic cases in
		// goldenpayload_behaviour_test.go only. A rot guard calling it would
		// bypass isRegisteredGuard entirely, which is the runtime check that
		// makes the syntactic rule below redundant rather than load-bearing.
		if name.Name == "comparePayloadFields" {
			offences = append(offences, describeOffence(fileSet, call,
				"calls comparePayloadFields, which skips registry validation"))
			return true
		}
		if name.Name != "comparePayload" {
			return true
		}
		// comparePayload(frozen, rendered, <registry entry>)
		if len(call.Args) != 3 {
			offences = append(offences, describeOffence(fileSet, call,
				"takes an unexpected number of arguments"))
			return true
		}
		if _, isIdent := call.Args[2].(*ast.Ident); !isIdent {
			offences = append(offences, describeOffence(fileSet, call,
				"passes something other than a bare registry identifier"))
		}
		return true
	})
	return offences
}

func describeOffence(fileSet *token.FileSet, call *ast.CallExpr, what string) string {
	position := fileSet.Position(call.Pos())
	return filepath.Base(position.Filename) + ":" +
		strings.TrimSpace(strings.Join([]string{itoa(position.Line), what}, " "))
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var digits []byte
	for n > 0 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
		n /= 10
	}
	return string(digits)
}

// TestEveryRotGuardUsesTheRegistry is what keeps the single-source registry
// single.
//
// comparePayload taking a payloadGuard already stops a guard naming fields
// directly. This stops the remaining route: constructing an inline
// `payloadGuard{fixture: "...", fields: []string{...}}` at the call site, which
// would rebuild the duplication with the type system's blessing.
func TestEveryRotGuardUsesTheRegistry(t *testing.T) {
	guards, err := filepath.Glob("*_rot_guard_test.go")
	if err != nil {
		t.Fatal(err)
	}
	// A glob matching nothing would make every assertion below vacuous.
	if len(guards) == 0 {
		t.Fatal("no *_rot_guard_test.go files found; this test would pass by " +
			"examining nothing")
	}

	callSites := 0
	var offences []string
	for _, path := range guards {
		fileSet := token.NewFileSet()
		parsed, err := parser.ParseFile(fileSet, path, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		callSites += countComparePayloadCalls(parsed)
		offences = append(offences, comparePayloadOffences(fileSet, parsed)...)
	}

	for _, offence := range offences {
		t.Errorf("%s. A guard must name a registry entry and nothing else, or it "+
			"and TestShippedFixturesExposeThePayloadFieldsTheGuardsCompare can "+
			"disagree about which fields are compared -- the drift the registry "+
			"exists to make impossible.", offence)
	}

	// The detector must have had something to detect. Without this, deleting
	// every guard would turn this test green.
	if callSites == 0 {
		t.Error("no comparePayload call sites found in any *_rot_guard_test.go; " +
			"either the guards stopped using it or this detector stopped working, " +
			"and a detector that finds nothing reports exactly what a clean tree does")
	}
}

func countComparePayloadCalls(parsed *ast.File) int {
	count := 0
	ast.Inspect(parsed, func(node ast.Node) bool {
		if call, ok := node.(*ast.CallExpr); ok {
			if name, ok := call.Fun.(*ast.Ident); ok && name.Name == "comparePayload" {
				count++
			}
		}
		return true
	})
	return count
}

// TestTheRegistryDetectorActuallyDetects is the positive control, and it now
// exercises THE PRODUCTION FUNCTION rather than a copy of it.
//
// The previous version had its own inline AST walk. codex round 1 broke the real
// detector (accept only token.CHAR), planted a direct literal in a guard, and
// every test here still passed -- because this control was only ever checking
// its own duplicate. A control that cannot fail when the thing it controls
// breaks is worse than no control, since it reports the same green either way.
func TestTheRegistryDetectorActuallyDetects(t *testing.T) {
	for _, testCase := range []struct {
		name        string
		source      string
		wantOffence bool
	}{
		{
			name:        "a bare registry identifier is permitted",
			source:      "package p\nfunc g() { comparePayload(nil, nil, reprBandGuard) }\n",
			wantOffence: false,
		},
		{
			name:        "a string literal is an offence",
			source:      "package p\nfunc g() { comparePayload(nil, nil, \"cases\") }\n",
			wantOffence: true,
		},
		{
			// The evasion that defeated the previous detector.
			name:        "an inline composite literal is an offence",
			source:      "package p\nfunc g() { comparePayload(nil, nil, payloadGuard{fixture: \"x\"}) }\n",
			wantOffence: true,
		},
		{
			name:        "a field selector is an offence",
			source:      "package p\nfunc g() { comparePayload(nil, nil, other.guard) }\n",
			wantOffence: true,
		},
		{
			name:        "a function call is an offence",
			source:      "package p\nfunc g() { comparePayload(nil, nil, syntheticGuard(\"cases\")) }\n",
			wantOffence: true,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			fileSet := token.NewFileSet()
			parsed, err := parser.ParseFile(fileSet, "synthetic_rot_guard_test.go",
				strings.NewReader(testCase.source), 0)
			if err != nil {
				t.Fatal(err)
			}
			offences := comparePayloadOffences(fileSet, parsed)
			if got := len(offences) > 0; got != testCase.wantOffence {
				t.Errorf("comparePayloadOffences returned %d offence(s), want offence=%v. "+
					"This control calls the PRODUCTION detector, so a mismatch here means "+
					"TestEveryRotGuardUsesTheRegistry cannot be trusted either.",
					len(offences), testCase.wantOffence)
			}
		})
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

// TestTheDetectorInspectsRealGuardFiles closes the half of round 1's P1b that
// round 2 showed was still open.
//
// TestTheRegistryDetectorActuallyDetects couples the control to the production
// function, but only ever feeds it SYNTHETIC source. codex round 2 forked the
// detector to return no offences except for `synthetic_rot_guard_test.go`,
// planted an inline narrowed guard in a REAL file, and all four focused tests
// stayed green. A detector keyed on filename passes a control that only shows it
// synthetic filenames.
//
// So this runs the detector over a real *_rot_guard_test.go -- unmodified, then
// with an offending line spliced in -- and requires it to distinguish them. A
// fork that special-cases file names cannot satisfy both halves.
func TestTheDetectorInspectsRealGuardFiles(t *testing.T) {
	guards, err := filepath.Glob("*_rot_guard_test.go")
	if err != nil || len(guards) == 0 {
		t.Fatalf("no real guard files to inspect (err=%v)", err)
	}
	real := guards[0]

	clean, err := os.ReadFile(real)
	if err != nil {
		t.Fatal(err)
	}

	fileSet := token.NewFileSet()
	parsed, err := parser.ParseFile(fileSet, real, clean, 0)
	if err != nil {
		t.Fatal(err)
	}
	if offences := comparePayloadOffences(fileSet, parsed); len(offences) != 0 {
		t.Fatalf("the real guard %s is already offending: %v", real, offences)
	}

	// Splice an offending call into the REAL file's source, keeping its real
	// name so a filename-keyed detector cannot tell the two apart.
	// Appended, not prepended: a declaration inserted above the import block
	// fails to parse, which would fail this test for the wrong reason.
	offending := string(clean) +
		"\nfunc splicedOffender() { comparePayload(nil, nil, payloadGuard{fixture: \"x\"}) }\n"
	if offending == string(clean) {
		t.Fatal("splice did not apply; this test would pass vacuously")
	}
	parsedOffending, err := parser.ParseFile(fileSet, real, offending, 0)
	if err != nil {
		t.Fatal(err)
	}
	if offences := comparePayloadOffences(fileSet, parsedOffending); len(offences) == 0 {
		t.Errorf("the detector found no offence in %s after an offending call was "+
			"spliced in. It responds to synthetic source but not to real guard "+
			"files, so TestEveryRotGuardUsesTheRegistry proves nothing about them.",
			real)
	}
}

// callersOfTheUnvalidatedCore returns the files in this package that call
// comparePayloadFields, which skips registry validation.
var comparePayloadFieldsAllowed = map[string]bool{
	// Defines it, and calls it from comparePayload after validating.
	"goldenpayload_test.go": true,
	// Exercises the comparison logic itself against hand-written documents,
	// which is the whole reason an unvalidated core exists.
	"goldenpayload_behaviour_test.go": true,
}

// TestTheUnvalidatedCoreIsCalledOnlyWhereItIsTested closes codex round 3's
// helper bypass, and it is the third place I have had to move the boundary.
//
// TestEveryRotGuardUsesTheRegistry inspects only files matching
// *_rot_guard_test.go, and only DIRECT comparePayloadFields calls. So the most
// ordinary refactor in the world walks around it -- extract a helper into any
// other file in the package:
//
//	func compareOnlyCases(a, b []byte) error {
//	    return comparePayloadFields(a, b, []string{"cases"})
//	}
//
// and have the guard call that. Reproduced: every registry control passed while
// the guard silently compared only "cases". That is drift by a well-meaning
// editor, not a hostile author -- "extract the duplicated call into a helper" is
// what a careful person does on a Tuesday.
//
// Chasing call PATHS through the AST would be the direct answer and the wrong
// one: it is unbounded, and each new indirection is another shape to enumerate,
// which is the mistake this file has now made three times. This asks a bounded
// question instead -- WHICH FILES may reach the unvalidated core at all -- and
// answers it over the whole package rather than one glob. A helper anywhere else
// is itself a call site in a file that is not allowed, wherever it hides.
func TestTheUnvalidatedCoreIsCalledOnlyWhereItIsTested(t *testing.T) {
	sources, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	if len(sources) == 0 {
		t.Fatal("no .go files found; this test would pass by examining nothing")
	}

	inspected, callSites := 0, 0
	for _, path := range sources {
		fileSet := token.NewFileSet()
		parsed, err := parser.ParseFile(fileSet, path, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		inspected++
		ast.Inspect(parsed, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			name, ok := call.Fun.(*ast.Ident)
			if !ok || name.Name != "comparePayloadFields" {
				return true
			}
			callSites++
			if comparePayloadFieldsAllowed[filepath.Base(path)] {
				return true
			}
			t.Errorf("%s:%d calls comparePayloadFields, which skips registry "+
				"validation. Only %v may. A helper here lets a rot guard compare "+
				"an arbitrary field set while every registry control still passes "+
				"-- silent drift, reachable by an ordinary extract-a-helper "+
				"refactor. Call comparePayload with a registry entry instead.",
				path, fileSet.Position(call.Pos()).Line,
				sortedAllowedFiles())
			return true
		})
	}

	if inspected < 2 || callSites == 0 {
		t.Errorf("inspected %d file(s) and found %d comparePayloadFields call "+
			"site(s); a scan that finds nothing reports exactly what a clean "+
			"package reports", inspected, callSites)
	}
}

func sortedAllowedFiles() []string {
	names := make([]string, 0, len(comparePayloadFieldsAllowed))
	for name := range comparePayloadFieldsAllowed {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
