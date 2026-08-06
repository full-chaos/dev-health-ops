package providersync

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestNormalizeWatermarkWriteClampsToBothCeilings(t *testing.T) {
	now := time.Date(2026, 6, 17, 12, 0, 0, 0, time.UTC)
	windowEnd := now.Add(-6 * time.Hour)
	for _, test := range []struct {
		name     string
		incoming time.Time
		bound    *time.Time
		want     time.Time
	}{
		{
			name:     "a value inside both ceilings is untouched",
			incoming: windowEnd.Add(-time.Minute),
			bound:    &windowEnd,
			want:     windowEnd.Add(-time.Minute),
		},
		{
			// The window_end ceiling. This value is in the PAST, so the `now`
			// ceiling alone would let it through -- and the next run would then
			// start after data the unit never fetched. Python measured real
			// ~5h gaps masked by a 60s overlap exactly here.
			name:     "a value past the window end clamps to the window end",
			incoming: now.Add(-time.Hour),
			bound:    &windowEnd,
			want:     windowEnd,
		},
		{
			name:     "a future value clamps to now when there is no window end",
			incoming: now.Add(72 * time.Hour),
			bound:    nil,
			want:     now,
		},
		{
			// Both ceilings apply; the tighter one wins.
			name:     "a future value clamps to the window end when that is tighter",
			incoming: now.Add(72 * time.Hour),
			bound:    &windowEnd,
			want:     windowEnd,
		},
		{
			name:     "a window end in the future does not raise the now ceiling",
			incoming: now.Add(48 * time.Hour),
			bound:    ptrTime(now.Add(96 * time.Hour)),
			want:     now,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := normalizeWatermarkWrite(test.incoming, test.bound, now, "org", "src", "commits")
			if !got.Equal(test.want) {
				t.Fatalf("normalizeWatermarkWrite = %s, want %s", got, test.want)
			}
		})
	}
}

func ptrTime(value time.Time) *time.Time { return &value }

// TestNormalizeWatermarkWriteReturnsUTC pins the REPRESENTATION half of the
// boundary, which the table above cannot see: time.Time comparisons are
// instant-based, so dropping the `.UTC()` normalization changes no verdict and
// no stored timestamptz, and every case there would still pass.
//
// What it does change is what the value PRINTS as. The clamp warning formats
// both the requested and the clamped value with RFC3339Nano, and the
// normalizer is the one place a provider-supplied timestamp -- which arrives
// in whatever zone the provider's API used -- crosses into this repository's
// watermark path. A boundary that emitted mixed offsets would make the one
// diagnostic that explains a silent coverage gap unreadable, and it would
// diverge from Python's `_normalize_watermark_write`, which returns UTC.
func TestNormalizeWatermarkWriteReturnsUTC(t *testing.T) {
	now := time.Date(2026, 6, 17, 12, 0, 0, 0, time.UTC)
	elsewhere := time.FixedZone("UTC+5:30", 5*3600+1800)
	windowEnd := now.Add(-6 * time.Hour).In(elsewhere)
	for _, test := range []struct {
		name     string
		incoming time.Time
		bound    *time.Time
	}{
		{
			name:     "an unclamped value is returned in UTC",
			incoming: now.Add(-time.Hour).In(elsewhere),
			bound:    nil,
		},
		{
			name:     "a value clamped to now is returned in UTC",
			incoming: now.Add(72 * time.Hour).In(elsewhere),
			bound:    nil,
		},
		{
			name:     "a value clamped to a non-UTC window end is returned in UTC",
			incoming: now.Add(-time.Hour).In(elsewhere),
			bound:    &windowEnd,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := normalizeWatermarkWrite(test.incoming, test.bound, now, "org", "src", "commits")
			if got.Location() != time.UTC {
				t.Fatalf("normalizeWatermarkWrite returned %s (location %s), want UTC",
					got.Format(time.RFC3339Nano), got.Location())
			}
		})
	}
}

const (
	watermarkSQLName        = "upsertWatermarkSQL"
	watermarkNormalizerName = "normalizeWatermarkWrite"
)

// callsTheNormalizer reports whether an expression's subtree contains a direct
// call to normalizeWatermarkWrite.
func callsTheNormalizer(node ast.Node) bool {
	found := false
	ast.Inspect(node, func(inner ast.Node) bool {
		call, ok := inner.(*ast.CallExpr)
		if !ok {
			return true
		}
		if identifier, ok := call.Fun.(*ast.Ident); ok &&
			identifier.Name == watermarkNormalizerName {
			found = true
		}
		return true
	})
	return found
}

// normalizerDerivedNames returns the local identifiers in one function body
// whose value came out of normalizeWatermarkWrite. This is the dataflow half:
// the production writer passes `normalized`, not the call itself, so a
// call-site check that only looked for a literal call expression would report
// the compliant writer as a bypass.
func normalizerDerivedNames(body *ast.BlockStmt) map[string]bool {
	derived := map[string]bool{}
	ast.Inspect(body, func(node ast.Node) bool {
		switch statement := node.(type) {
		case *ast.AssignStmt:
			for index, target := range statement.Lhs {
				identifier, ok := target.(*ast.Ident)
				if !ok {
					continue
				}
				var value ast.Expr
				switch {
				case len(statement.Rhs) == len(statement.Lhs):
					value = statement.Rhs[index]
				case len(statement.Rhs) == 1:
					// `a, err := f(...)` -- any result of a normalizer call is
					// treated as derived, which is the conservative direction.
					value = statement.Rhs[0]
				default:
					continue
				}
				if callsTheNormalizer(value) {
					derived[identifier.Name] = true
				}
			}
		case *ast.ValueSpec:
			for index, name := range statement.Names {
				if index < len(statement.Values) && callsTheNormalizer(statement.Values[index]) {
					derived[name.Name] = true
				}
			}
		}
		return true
	})
	return derived
}

// carriesANormalizedValue reports whether an argument expression is, or is
// built from, a normalizeWatermarkWrite result.
func carriesANormalizedValue(argument ast.Expr, derived map[string]bool) bool {
	if callsTheNormalizer(argument) {
		return true
	}
	found := false
	ast.Inspect(argument, func(node ast.Node) bool {
		if identifier, ok := node.(*ast.Ident); ok && derived[identifier.Name] {
			found = true
		}
		return true
	})
	return found
}

// TestEveryWatermarkWriteRoutesThroughTheNormalizer is clause C10(c)'s
// coverage obligation, and it is DERIVED rather than hand-listed on purpose.
//
// Python's lane closed this class twice and got it wrong the first time
// exactly by hand-enumerating writers: a second writer sat outside the
// boundary while the class was reported closed. So this test parses this
// package's own source, finds every CALL SITE that executes
// upsertWatermarkSQL, and requires THAT SITE to be passed a value produced by
// normalizeWatermarkWrite -- with a vacuity guard, because a derivation that
// finds nothing must FAIL rather than read as "all writers are compliant".
//
// PER CALL SITE is the whole point, and it is what this guard originally got
// wrong. Its first form asked only whether both names appeared SOMEWHERE in
// the same function, so a function containing one compliant writer and one
// bypassing writer -- the exact shape Python shipped -- passed it. That
// weakened form was observed passing a planted second writer before this
// version was written.
//
// The AST walk is scoped to non-test files: a test may legitimately write a
// deliberately-corrupt row directly to seed the state the boundary exists to
// correct (see the CHAOS-3412 note about tests that go vacuous once the
// boundary works and must therefore bypass the public API).
func TestEveryWatermarkWriteRoutesThroughTheNormalizer(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	fileSet := token.NewFileSet()
	callSites := 0
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fileSet, filepath.Join(".", name), nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Body == nil {
				continue
			}
			derived := normalizerDerivedNames(function.Body)
			measured := map[*ast.Ident]bool{}
			ast.Inspect(function.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				statement := -1
				for index, argument := range call.Args {
					identifier, ok := argument.(*ast.Ident)
					if ok && identifier.Name == watermarkSQLName {
						statement, measured[identifier] = index, true
						break
					}
				}
				if statement < 0 {
					return true
				}
				callSites++
				for index, argument := range call.Args {
					if index != statement && carriesANormalizedValue(argument, derived) {
						return true
					}
				}
				t.Errorf("%s:%d: %s executes %s with no argument produced by %s. "+
					"A bypassing writer poisons every dataset that reads through "+
					"a shared or fallback watermark row -- and a sibling compliant "+
					"writer in the same function does not cover it.",
					name, fileSet.Position(call.Pos()).Line, function.Name.Name,
					watermarkSQLName, watermarkNormalizerName)
				return true
			})
			// MEASUREMENT GUARD: a reference to the SQL constant that is not an
			// argument of a call is a shape this derivation cannot judge (it is
			// stored in a variable, wrapped by a helper, or interpolated). That
			// is an unmeasured writer, and it must fail rather than be skipped.
			ast.Inspect(function.Body, func(node ast.Node) bool {
				identifier, ok := node.(*ast.Ident)
				if !ok || identifier.Name != watermarkSQLName || measured[identifier] {
					return true
				}
				t.Errorf("%s:%d: %s references %s outside a call's argument list; "+
					"this derivation cannot tell what value that writer passes, so "+
					"the write boundary is UNMEASURED here rather than clean.",
					name, fileSet.Position(identifier.Pos()).Line,
					function.Name.Name, watermarkSQLName)
				return true
			})
		}
	}
	// VACUITY GUARD: the derivation above is only evidence if it found the
	// writer it was supposed to find. Zero call sites means the SQL constant
	// was renamed, or this walk stopped matching -- either way the test proves
	// nothing and must say so.
	if callSites == 0 {
		t.Fatal("derivation found no watermark write call sites at all; this " +
			"guard is vacuous and cannot have measured the write boundary")
	}
}
