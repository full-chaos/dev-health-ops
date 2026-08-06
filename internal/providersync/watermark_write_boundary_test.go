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

// TestEveryWatermarkWriteRoutesThroughTheNormalizer is clause C10(c)'s
// coverage obligation, and it is DERIVED rather than hand-listed on purpose.
//
// Python's lane closed this class twice and got it wrong the first time
// exactly by hand-enumerating writers: a second writer sat outside the
// boundary while the class was reported closed. So this test parses this
// package's own source, finds every call that executes upsertWatermarkSQL,
// and requires each one to be preceded by a normalizeWatermarkWrite call in
// the same function -- with a vacuity guard, because a derivation that finds
// nothing must FAIL rather than read as "all writers are compliant".
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
	writers := 0
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
			executes, normalizes := false, false
			ast.Inspect(function.Body, func(node ast.Node) bool {
				identifier, ok := node.(*ast.Ident)
				if !ok {
					return true
				}
				switch identifier.Name {
				case "upsertWatermarkSQL":
					executes = true
				case "normalizeWatermarkWrite":
					normalizes = true
				}
				return true
			})
			if !executes {
				continue
			}
			writers++
			if !normalizes {
				t.Errorf("%s: %s executes upsertWatermarkSQL without routing the "+
					"value through normalizeWatermarkWrite. A bypassing writer "+
					"poisons every dataset that reads through a shared or "+
					"fallback watermark row.", name, function.Name.Name)
			}
		}
	}
	// VACUITY GUARD: the derivation above is only evidence if it found the
	// writer it was supposed to find. Zero writers means the SQL constant was
	// renamed, or this walk stopped matching -- either way the test proves
	// nothing and must say so.
	if writers == 0 {
		t.Fatal("derivation found no watermark writers at all; this guard is " +
			"vacuous and cannot have measured the write boundary")
	}
}
