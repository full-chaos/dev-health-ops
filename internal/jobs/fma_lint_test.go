// Package jobs hosts a static-analysis regression guard for CHAOS-4818
// (Go FMA fusion on arm64 diverging from CPython's double rounding). See
// TestNoUnguardedFloatFMAInJobsPackages's doc comment for the full rationale
// and the shape it detects.
package jobs

import (
	"fmt"
	"go/ast"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"golang.org/x/tools/go/packages"
)

func writeFile(path, content string) error {
	return os.WriteFile(path, []byte(content), 0o600)
}

// fmaFinding is one unguarded `x*y +/- z` site: a multiplication of two
// floating-point operands that is the DIRECT, unconverted operand of an
// addition or subtraction. This is exactly the shape CHAOS-4818's
// ReleaseImpactConfidence and its seven sibling fixes closed: the Go spec
// permits fusing such an expression into a single fused-multiply-add on
// arm64 (one rounding) where CPython always rounds the multiply and the
// add/subtract separately.
type fmaFinding struct {
	position string
	snippet  string
}

// TestNoUnguardedFloatFMAInJobsPackages is the CHAOS-4818 CI-arch-independence
// guard: a `go vet`-style lint over every non-test .go file under
// internal/jobs/... (the brief's sweep scope), using real type information
// (go/packages + go/types, not a regex) so it flags only genuine float64/
// float32 multiplications and never a superficially similar integer or
// time.Duration expression (e.g. `13*time.Minute + 30*time.Second`,
// daily.go:414 -- confirmed NOT flagged, see TestFMALintDetectsTheKnownShape
// for the same discrimination on a minimal fixture).
//
// # What this catches, and what it deliberately does not
//
// It flags `x*y + z` / `x*y - z` where the multiplication is an
// UNPARENTHESIZED, UNCONVERTED direct operand of the +/-, e.g.
// `0.35*a + 0.35*b`. It does NOT flag the fixed idiom `float64(0.35*a) +
// float64(0.35*b)`, because the multiplication's immediate (paren-stripped)
// parent is then a conversion call to float64/float32, which the Go spec
// documents as forcing rounding and preventing fusion.
//
// It CANNOT catch the second, less obvious CHAOS-4818 exposure this sweep
// found (deployPercentile, deploy.go, and its four siblings): a value that
// gets fused with a LATER STATEMENT's subtraction after the compiler
// rematerializes it across an intervening function call (fusion "across
// statements", which the Go spec explicitly permits and which measurably
// happened here even with the multiply visually nowhere near a `+`/`-` in
// source). That class has no purely syntactic signature -- it depends on
// register-allocation pressure around a specific call site -- and is
// covered instead by the bit-pattern regression tests (fma_golden_test.go
// and its per-package copies), not by this lint. RISK-NOTES in the PR body
// says this explicitly rather than implying the lint is a complete guard.
func TestNoUnguardedFloatFMAInJobsPackages(t *testing.T) {
	cfg := &packages.Config{
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedSyntax |
			packages.NeedTypes | packages.NeedTypesInfo | packages.NeedImports | packages.NeedDeps,
		Dir: "../..",
	}
	pkgs, err := packages.Load(cfg, "./internal/jobs/...")
	if err != nil {
		t.Fatalf("load internal/jobs/...: %v", err)
	}
	if packages.PrintErrors(pkgs) > 0 {
		t.Fatalf("packages.Load reported errors loading internal/jobs/... (see above)")
	}
	if len(pkgs) == 0 {
		t.Fatal("no packages loaded under internal/jobs/... -- the scope is wrong")
	}

	var findings []fmaFinding
	for _, pkg := range pkgs {
		for _, file := range pkg.Syntax {
			// Use the file's own recorded position rather than indexing
			// pkg.CompiledGoFiles in parallel with pkg.Syntax: the two
			// slices are not guaranteed to stay aligned (build-tag-excluded
			// or cgo-processed files can desync them), which panics on an
			// out-of-range index for some packages instead of just
			// mis-filtering.
			filename := pkg.Fset.Position(file.Pos()).Filename
			if strings.HasSuffix(filename, "_test.go") {
				continue
			}
			findings = append(findings, findUnguardedFloatFMASites(pkg.Fset, pkg.TypesInfo, file)...)
		}
	}

	sort.Slice(findings, func(i, j int) bool { return findings[i].position < findings[j].position })
	if len(findings) > 0 {
		var b strings.Builder
		b.WriteString("unguarded float x*y +/- z site(s) found -- Go may fuse these into one FMA on arm64, rounding differently than CPython (CHAOS-4818). Wrap each product: float64(a*b) + float64(c*d).\n")
		for _, f := range findings {
			fmt.Fprintf(&b, "  %s: %s\n", f.position, f.snippet)
		}
		t.Error(b.String())
	}
}

// TestFMALintDetectsTheKnownShape proves the checker itself works, using an
// embedded fixture package rather than mutating real source: one unguarded
// case (must be flagged), one float64()-wrapped case (must NOT be flagged),
// and one integer case that looks the same syntactically (must NOT be
// flagged -- integer multiply-add is exact, never FMA-exposed).
func TestFMALintDetectsTheKnownShape(t *testing.T) {
	const src = `package fixture

func Unguarded(a, b, c, d float64) float64 {
	return a*b + c*d
}

func Guarded(a, b, c, d float64) float64 {
	return float64(a*b) + float64(c*d)
}

func IntegerMath(a, b, c, d int) int {
	return a*b + c*d
}
`
	// Both a*b and c*d in `Unguarded` are individually flagged: each is a
	// direct multiply operand of the same `+`, so EITHER could be the one
	// Go fuses (fusing the right operand into an add is the common case,
	// but the compiler is free to choose either side) -- both need the
	// float64() wrap, matching every real fix in this PR.
	findings := parseAndCheckFixture(t, src)
	if len(findings) != 2 {
		t.Fatalf("got %d findings, want exactly 2 (a*b and c*d, both unguarded operands of + in Unguarded): %+v", len(findings), findings)
	}
}

// findUnguardedFloatFMASites walks file's AST looking for a *ast.BinaryExpr
// with Op MUL, both of whose operand types are float32/float64 (per info),
// that is itself the direct (paren-stripped) operand of a +/- BinaryExpr,
// with no intervening float64()/float32() conversion call.
func findUnguardedFloatFMASites(fset *token.FileSet, info *types.Info, file *ast.File) []fmaFinding {
	v := &fmaVisitor{fset: fset, info: info}
	ast.Walk(v, file)
	return v.findings
}

// fmaVisitor implements ast.Visitor with an explicit paren-stripped
// ancestor stack. Go's ast.Walk calls Visit(nil) when leaving a node
// (immediately after all its children have been visited), which is how the
// stack pops symmetrically with the pushes on entry.
type fmaVisitor struct {
	fset     *token.FileSet
	info     *types.Info
	stack    []ast.Node
	findings []fmaFinding
}

func (v *fmaVisitor) Visit(n ast.Node) ast.Visitor {
	if n == nil {
		if len(v.stack) > 0 {
			v.stack = v.stack[:len(v.stack)-1]
		}
		return nil
	}
	if _, isParen := n.(*ast.ParenExpr); !isParen {
		v.stack = append(v.stack, n)
	}
	if mul, ok := n.(*ast.BinaryExpr); ok && mul.Op == token.MUL && isFloatBinaryExpr(v.info, mul) {
		if directOperandOfUnguardedAddSub(v.stack) {
			v.findings = append(v.findings, fmaFinding{
				position: v.fset.Position(mul.Pos()).String(),
				snippet:  exprString(v.fset, mul),
			})
		}
	}
	return v
}

// directOperandOfUnguardedAddSub inspects the ancestor stack (paren-stripped,
// innermost node -- the MUL under test -- last) for whether the MUL's
// immediate parent is a +/- BinaryExpr. If the MUL were instead wrapped in
// float64(...)/float32(...), that CallExpr (not the MUL) would be the
// direct operand of the +/-, so the MUL's immediate parent would be the
// CallExpr, not the BinaryExpr, and this returns false.
func directOperandOfUnguardedAddSub(stack []ast.Node) bool {
	if len(stack) < 2 {
		return false
	}
	parent := stack[len(stack)-2]
	parentBinary, ok := parent.(*ast.BinaryExpr)
	if !ok {
		return false
	}
	return parentBinary.Op == token.ADD || parentBinary.Op == token.SUB
}

// isFloatBinaryExpr reports whether the given expression's static type is
// float32 or float64 (never true for int/time.Duration/etc, so ordinary
// integer or duration arithmetic that looks syntactically identical is
// never flagged).
func isFloatBinaryExpr(info *types.Info, expr ast.Expr) bool {
	t := info.TypeOf(expr)
	if t == nil {
		return false
	}
	basic, ok := t.Underlying().(*types.Basic)
	if !ok {
		return false
	}
	return basic.Kind() == types.Float32 || basic.Kind() == types.Float64
}

func exprString(fset *token.FileSet, expr ast.Expr) string {
	return fmt.Sprintf("<expr at %s>", fset.Position(expr.Pos()))
}

// parseAndCheckFixture is the self-test helper: type-checks the given
// single-file source (a complete, importable package with no external
// imports) in a scratch module and runs the same site-finding logic the
// real sweep uses.
func parseAndCheckFixture(t *testing.T, src string) []fmaFinding {
	t.Helper()
	dir := t.TempDir()
	if err := writeFile(filepath.Join(dir, "fixture.go"), src); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	if err := writeFile(filepath.Join(dir, "go.mod"), "module fixture\n\ngo 1.27\n"); err != nil {
		t.Fatalf("write fixture go.mod: %v", err)
	}
	cfg := &packages.Config{
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedSyntax |
			packages.NeedTypes | packages.NeedTypesInfo | packages.NeedDeps | packages.NeedImports,
		Dir: dir,
	}
	pkgs, err := packages.Load(cfg, "./...")
	if err != nil {
		t.Fatalf("load fixture package: %v", err)
	}
	if packages.PrintErrors(pkgs) > 0 {
		t.Fatalf("fixture package failed to type-check")
	}
	var findings []fmaFinding
	for _, pkg := range pkgs {
		for _, file := range pkg.Syntax {
			findings = append(findings, findUnguardedFloatFMASites(pkg.Fset, pkg.TypesInfo, file)...)
		}
	}
	return findings
}
