package remaining

import (
	"go/ast"
	"go/parser"
	"go/token"
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
// parsed package instead.
//
// IT BANS ANY SELECTOR REFERENCE TO nowUTC, NOT ONLY A CALL. An earlier
// version of this comment said "a CALL ... and nothing else", which described
// something NARROWER than the code does -- a bare `f := executor.nowUTC`,
// never invoked here, is caught too (peer review, 3092). The broader behaviour
// is the correct one: a reference stored now and called later bypasses the
// accessor exactly as a direct call does, so there is no reason to exempt it.
//
// A composite-literal key -- `&DORAExecutor{nowUTC: f}` -- is a KeyValueExpr,
// not a selector, and is deliberately NOT caught. That is not an oversight in
// the walk: construction is the constructor's business, and the tests in this
// package build literals on purpose to reach the refusal path. The line this
// guard draws is reaching through a RECEIVER to read the field, which is the
// thing the accessor exists to replace.
//
// # A RENAME CANNOT SILENCE THIS
//
// The obvious hole in matching by name rather than by type is that renaming
// the field would leave the walk looking for something that no longer exists,
// silent while the defect is fully reintroduced. It does not, and the reason
// is the positive control below rather than anything deliberate here: the
// control asserts the walk still finds nowUTC INSIDE executor_clock.go, and a
// rename removes those occurrences too. Guard and control fail together, and
// the control's message names the real cause. A control that proves the
// instrument works also proves it is still pointed at the right thing.
//
// What remains is false POSITIVES -- another type in this package gaining a
// nowUTC field. Those are loud, arrive with the change that causes them, and
// fail closed, which is the acceptable direction.
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
		files += len(pkg.Files)
		fileViolations, fileLegitimate := findNowUTCReferences(fset, pkg.Files)
		violations = append(violations, fileViolations...)
		legitimate += fileLegitimate
	}

	// POSITIVE CONTROL, asserted BEFORE the verdict below is allowed to mean
	// anything: the two nowOrRefuse methods each pass executor.nowUTC to
	// clockOrRefuse, so the walker must see exactly those.
	if files == 0 {
		t.Fatal("walked no files -- the guard would vacuously pass")
	}
	if legitimate < 2 {
		t.Fatalf("the walker found %d nowUTC reference(s) inside "+
			"the two nowOrRefuse() accessor declarations, want at least 2 (one "+
			"per nowOrRefuse method). The walker is broken or the file was not "+
			"parsed, so the zero-violations result below proves nothing", legitimate)
	}

	if len(violations) > 0 {
		t.Errorf("nowUTC is referenced outside a nowOrRefuse() accessor at %s "+
			"-- use the kind's nowOrRefuse() accessor. Reading the field "+
			"directly is the defect CHAOS-4954 fixed at three sites; the field "+
			"is still there, so nothing but this test stops a fourth",
			strings.Join(violations, ", "))
	}
}

// isNowOrRefuseAccessor reports whether decl IS one of the two nowOrRefuse
// methods this guard exists to exempt.
//
// DECLARATION-scoped, not FILE-scoped -- codex round 2's P2 on this guard:
// the original check exempted every selector in executor_clock.go by
// filename alone (`filepath.Base(path) == "executor_clock.go"`), so a THIRD
// function added to that same file, reading executor.nowUTC() directly,
// would bypass nowOrRefuse() while the guard stayed green. Narrowing to the
// specific method declarations below closes that hole, and it also closes the
// round's second finding for free: there is no longer a file-level boolean
// to flip into "classify every file as the accessor" -- that mutation has
// nothing left to mutate.
//
// WorkItemAttributionExecutor (CHAOS-3092 PR-B) added a THIRD exempt type,
// living in its own file rather than executor_clock.go -- the exemption is
// keyed on the method's receiver type and name, not on which file declares
// it, so this needed no change beyond the type list below.
//
// TestClockGuardCatchesAHelperAddedToTheAccessorFile is the negative fixture
// proving this.
func isNowOrRefuseAccessor(decl ast.Decl) bool {
	funcDecl, ok := decl.(*ast.FuncDecl)
	if !ok || funcDecl.Name.Name != "nowOrRefuse" {
		return false
	}
	if funcDecl.Recv == nil || len(funcDecl.Recv.List) != 1 {
		return false
	}
	star, ok := funcDecl.Recv.List[0].Type.(*ast.StarExpr)
	if !ok {
		return false
	}
	ident, ok := star.X.(*ast.Ident)
	if !ok {
		return false
	}
	return ident.Name == "DORAExecutor" || ident.Name == "CapacityExecutor" ||
		ident.Name == "WorkItemAttributionExecutor" || ident.Name == "RecommendationsExecutor" ||
		ident.Name == "MembershipExecutor" || ident.Name == "ReleaseImpactExecutor" ||
		ident.Name == "ComplexityExecutor"
}

// findNowUTCReferences walks every top-level declaration in every file,
// classifying each `nowUTC` selector reference by DECLARATION
// (isNowOrRefuseAccessor), not by file. Shared by the live package guard
// above and the isolated fixture tests below, so all three exercise the
// identical classification logic rather than implementations that could
// drift apart.
//
// A nested `*ast.FuncLit` (closure) is walked SEPARATELY, with the exemption
// forced off, regardless of what encloses it (codex round 3's P2): the
// original walk applied `inAccessor` uniformly to a decl's WHOLE subtree, so
// a closure defined INSIDE a nowOrRefuse accessor -- reading
// executor.nowUTC() directly instead of through the parameter the accessor's
// own design routes it through -- inherited the exemption meant only for the
// accessor's own two selectors. TestClockGuardCatchesAClosureInsideTheAccessor
// is the negative fixture proving this.
func findNowUTCReferences(fset *token.FileSet, files map[string]*ast.File) (violations []string, legitimate int) {
	var walk func(root ast.Node, inAccessor bool)
	walk = func(root ast.Node, inAccessor bool) {
		ast.Inspect(root, func(node ast.Node) bool {
			// The root of THIS walk call is never reclassified as a nested
			// closure, even when root itself is a *ast.FuncLit (the recursive
			// call below) -- ast.Inspect's first callback is always for root.
			// Without this check, that first callback would match the type
			// assertion below and recurse on itself forever.
			if node == root {
				return true
			}
			if funcLit, ok := node.(*ast.FuncLit); ok {
				walk(funcLit, false)
				return false // the recursive call above already covers it.
			}
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
	for _, file := range files {
		for _, decl := range file.Decls {
			walk(decl, isNowOrRefuseAccessor(decl))
		}
	}
	return violations, legitimate
}

// TestClockGuardCatchesAHelperAddedToTheAccessorFile is codex round 2's P2 on
// this guard, closed: reproduces the EXACT shape the finding warned about --
// a new helper function landing in executor_clock.go (same file as the two
// legitimate accessors) that reads executor.nowUTC() directly -- and proves
// the declaration-scoped classification (isNowOrRefuseAccessor) still catches
// it, where the pre-fix file-scoped check would not have.
//
// Parses an ISOLATED synthetic source (not the real package directory) on
// purpose: this test's job is to prove the WALKER's classification logic is
// declaration-scoped, independent of whatever the real file currently
// contains. Using the real file would only prove today's file has no rogue
// helper in it -- exactly what the round-2 finding says is NOT the guarantee
// this guard makes.
func TestClockGuardCatchesAHelperAddedToTheAccessorFile(t *testing.T) {
	const source = `package remaining

import "time"

func (executor *DORAExecutor) nowOrRefuse() (time.Time, error) {
	return clockOrRefuse("DORAExecutor", executor.nowUTC)
}

func (executor *CapacityExecutor) nowOrRefuse() (time.Time, error) {
	return clockOrRefuse("CapacityExecutor", executor.nowUTC)
}

// rogueHelper is the shape round 2's P2 warned about: a NEW function in this
// same file, reading nowUTC directly instead of going through nowOrRefuse().
func rogueHelper(executor *DORAExecutor) time.Time {
	return executor.nowUTC()
}
`
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "executor_clock.go", source, 0)
	if err != nil {
		t.Fatalf("parse fixture: %v", err)
	}

	violations, legitimate := findNowUTCReferences(fset, map[string]*ast.File{"executor_clock.go": file})

	if legitimate != 2 {
		t.Fatalf("legitimate = %d, want 2 (the two real nowOrRefuse accessors) -- "+
			"the fixture no longer matches the production shape it is modelling", legitimate)
	}
	if len(violations) != 1 {
		t.Fatalf("violations = %v, want exactly 1 (rogueHelper) -- a new helper "+
			"added to the accessor's own file was not caught", violations)
	}
}

// TestClockGuardCatchesAClosureInsideTheAccessor is codex round 3's P2 on
// this guard, closed: reproduces the shape the finding warned about -- a
// closure DEFINED INSIDE a nowOrRefuse accessor that reads
// executor.nowUTC() directly, bypassing the clockOrRefuse parameter the
// accessor's own design routes it through -- and proves findNowUTCReferences
// no longer lets it inherit its enclosing accessor's exemption.
//
// Isolated synthetic source, same reasoning as the sibling fixture above:
// this test's job is to prove the WALKER's classification logic, not
// today's file.
func TestClockGuardCatchesAClosureInsideTheAccessor(t *testing.T) {
	const source = `package remaining

import "time"

func (executor *DORAExecutor) nowOrRefuse() (time.Time, error) {
	// rogueClosure is the shape round 3's P2 warned about: a closure defined
	// INSIDE this accessor, reading nowUTC directly instead of through the
	// clockOrRefuse parameter below.
	rogueClosure := func() time.Time {
		return executor.nowUTC()
	}
	_ = rogueClosure
	return clockOrRefuse("DORAExecutor", executor.nowUTC)
}

func (executor *CapacityExecutor) nowOrRefuse() (time.Time, error) {
	return clockOrRefuse("CapacityExecutor", executor.nowUTC)
}
`
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "executor_clock.go", source, 0)
	if err != nil {
		t.Fatalf("parse fixture: %v", err)
	}

	violations, legitimate := findNowUTCReferences(fset, map[string]*ast.File{"executor_clock.go": file})

	if legitimate != 2 {
		t.Fatalf("legitimate = %d, want 2 (the two real nowOrRefuse selectors, NOT the "+
			"closure's) -- the fixture no longer matches the production shape it is modelling", legitimate)
	}
	if len(violations) != 1 {
		t.Fatalf("violations = %v, want exactly 1 (the closure's nowUTC reference) -- "+
			"a closure inside the accessor was not caught", violations)
	}
}
