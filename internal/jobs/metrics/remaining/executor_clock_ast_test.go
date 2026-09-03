package remaining

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"
)

// TestNoRawClockCallSitesOutsideTheAccessor stops the defect recurring by
// ADDITION rather than by regression.
//
// CHAOS-4954 converted three raw `executor.nowUTC()` call sites. None of them
// was wrong on purpose -- all three predate the accessor. Converting them does
// not make the fourth harder to write: the raw field is still in the struct,
// and the next person adding a DORA or capacity method has no signal. A
// measured recurrence of three, with nothing preventing a fourth, is what
// earns this guard a place; it is regression prevention, not symmetry with
// some other package.
//
// # AST, NOT TEXT
//
// A grep for `.nowUTC()` matches comments, strings and doc examples, and
// misses a call reached through a differently-named receiver. This walks the
// parsed package and looks for a CALL whose function is a selector `.nowUTC`,
// which is exactly the construct being banned and nothing else.
//
// # THE POSITIVE CONTROL IS NOT OPTIONAL
//
// A guard whose all-clear is "found nothing" cannot distinguish "no
// violations" from "the walker is broken", "the file list was empty", or "the
// parse failed" -- every one of which reports success. So this asserts it
// FINDS the two legitimate `nowUTC` references inside executor_clock.go
// first. If that positive control ever fails, the zero below means nothing.
func TestNoRawClockCallSitesOutsideTheAccessor(t *testing.T) {
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, ".", nil, 0)
	if err != nil {
		t.Fatalf("parse package: %v", err)
	}
	if len(pkgs) == 0 {
		t.Fatal("parsed no packages -- the guard would vacuously pass")
	}

	var violations []string
	legitimate := 0
	files := 0

	for _, pkg := range pkgs {
		for path, file := range pkg.Files {
			files++
			// filepath.Base, because ParseDir keys files by the name it was
			// given -- here bare "executor_clock.go", which matched neither of
			// this check's first two forms. The positive control below caught
			// that on the first run, which is the whole reason it exists.
			inAccessor := filepath.Base(path) == "executor_clock.go"
			ast.Inspect(file, func(node ast.Node) bool {
				selector, ok := node.(*ast.SelectorExpr)
				if !ok || selector.Sel.Name != "nowUTC" {
					return true
				}
				if inAccessor {
					legitimate++
					return true
				}
				violations = append(violations,
					fset.Position(selector.Pos()).String())
				return true
			})
		}
	}

	// POSITIVE CONTROL, asserted BEFORE the verdict below is allowed to mean
	// anything: the two nowOrRefuse methods each pass executor.nowUTC to
	// clockOrRefuse, so the walker must see exactly those.
	if files == 0 {
		t.Fatal("walked no files -- the guard would vacuously pass")
	}
	if legitimate < 2 {
		t.Fatalf("the walker found %d nowUTC reference(s) inside "+
			"executor_clock.go, want at least 2 (one per nowOrRefuse method). "+
			"The walker is broken or the file was not parsed, so the "+
			"zero-violations result below proves nothing", legitimate)
	}

	if len(violations) > 0 {
		t.Errorf("nowUTC is referenced outside executor_clock.go at %s -- use "+
			"the kind's nowOrRefuse() accessor. Reading the field directly is "+
			"the defect CHAOS-4954 fixed at three sites; the field is still "+
			"there, so nothing but this test stops a fourth",
			strings.Join(violations, ", "))
	}
}
