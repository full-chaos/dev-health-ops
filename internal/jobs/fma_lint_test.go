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
// It also flags one level of indirection through a non-conversion wrapper
// call -- `identity(a*b) + c` -- because codex round 1 constructed exactly
// that (EXECUTED: arm64 assembly showed `FMADDD` for it) and the original
// version of this check, which looked only at the MUL's immediate parent,
// missed it (the immediate parent there is the CallExpr, not the +/-).
//
// This lint is NOT a soundness guarantee against arbitrary indirection --
// no purely syntactic check can be, once inlining is in play. Two classes
// it structurally cannot catch, both real, both found by this sweep:
//
//  1. Deeper indirection than one call (the MUL stored in a variable that a
//     LATER, unrelated statement adds to something, a value threaded through
//     two function boundaries, a struct field).
//  2. The second, less obvious CHAOS-4818 exposure this sweep found
//     (deployPercentile, deploy.go, and its four siblings): a value that
//     gets fused with a LATER STATEMENT's subtraction after the compiler
//     rematerializes it across an intervening function call (fusion "across
//     statements", which the Go spec explicitly permits and which
//     measurably happened here even with the multiply visually nowhere near
//     a `+`/`-` in source).
//
// Both classes have no purely syntactic signature and are covered instead
// by the bit-pattern regression tests (fma_golden_test.go and its
// per-package copies), not by this lint. RISK-NOTES in the PR body says
// this explicitly rather than implying the lint is a complete guard.
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
// one integer case that looks the same syntactically (must NOT be flagged
// -- integer multiply-add is exact, never FMA-exposed), and codex round 1's
// construction -- a non-conversion wrapper call around the multiply, which
// the round-1 version of this checker missed (must be flagged).
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

func identity(v float64) float64 { return v }

func WrappedByNonConversionCall(a, b, c float64) float64 {
	return identity(a*b) + c
}

func ParenSibling(a, b, c float64) float64 {
	return (c) + a*b
}

func ShadowedFloat64Conversion(a, b, c float64) float64 {
	float64 := func(v float64) float64 { return v }
	return float64(a*b) + c
}
`
	// Both a*b and c*d in `Unguarded` are individually flagged: each is a
	// direct multiply operand of the same `+`, so EITHER could be the one
	// Go fuses (fusing the right operand into an add is the common case,
	// but the compiler is free to choose either side) -- both need the
	// float64() wrap, matching every real fix in this PR. WrappedByNon
	// ConversionCall's a*b is the codex round-1 P1 repro: identity() is not
	// a float64()/float32() conversion, so it gives no rounding guarantee,
	// and if inlined the compiler can still fuse straight-line a*b + c.
	// ParenSibling and ShadowedFloat64Conversion are codex round-2's two P1
	// repros: a parenthesized SIBLING operand corrupting the round-1
	// hand-rolled ancestor stack (fixed by the parent-map rewrite), and a
	// purely-syntactic "is this a float64() call" check accepting a locally
	// shadowed identifier of that name (fixed by resolving through
	// go/types' info.Uses instead of comparing ident.Name).
	findings := parseAndCheckFixture(t, src)
	if len(findings) != 5 {
		t.Fatalf("got %d findings, want exactly 5 (a*b and c*d in Unguarded, a*b in WrappedByNonConversionCall, a*b in ParenSibling, a*b in ShadowedFloat64Conversion): %+v", len(findings), findings)
	}
}

// findUnguardedFloatFMASites walks file's AST looking for a *ast.BinaryExpr
// with Op MUL, both of whose operand types are float32/float64 (per info),
// that is itself the direct (paren-stripped) operand of a +/- BinaryExpr,
// with no intervening float64()/float32() conversion call.
//
// Codex round 2 (P1, EXECUTED) found the ROUND-1 implementation of this
// walk -- a hand-rolled ast.Visitor with an explicit push/pop stack --
// itself buggy: it pushed every non-ParenExpr node on entry but popped
// UNCONDITIONALLY on every Visit(nil), including a ParenExpr's own exit
// (which never pushed). For `(c) + a*b`, that extra pop after leaving the
// `(c)` ParenExpr silently dropped the enclosing `+` from the stack before
// the walk ever reached `a*b`, so the MUL's parent lookup came up empty and
// the site went unflagged -- codex's repro showed a real fused-vs-separated
// bit difference for exactly that construction. Building a NORMAL parent
// map first (one pass, ast.Inspect's own paired nil-call is symmetric by
// construction because it pairs with EVERY node, not just the ones this
// code chose to track) and then walking up through it, skipping ParenExpr
// explicitly at lookup time rather than at collection time, removes the
// asymmetry class entirely.
func findUnguardedFloatFMASites(fset *token.FileSet, info *types.Info, file *ast.File) []fmaFinding {
	parent := buildParentMap(file)
	var findings []fmaFinding
	ast.Inspect(file, func(n ast.Node) bool {
		mul, ok := n.(*ast.BinaryExpr)
		if !ok || mul.Op != token.MUL || !isFloatBinaryExpr(info, mul) {
			return true
		}
		if directOperandOfUnguardedAddSub(info, parent, mul) {
			findings = append(findings, fmaFinding{
				position: fset.Position(mul.Pos()).String(),
				snippet:  exprString(fset, mul),
			})
		}
		return true
	})
	return findings
}

// buildParentMap does ONE full walk and records, for every node, the node
// literally enclosing it in the AST (ParenExpr included -- callers skip
// past those explicitly, rather than this map silently doing it for them,
// which is exactly the kind of "collection-time" skip that caused the
// round-1 bug).
func buildParentMap(file *ast.File) map[ast.Node]ast.Node {
	parent := make(map[ast.Node]ast.Node)
	var stack []ast.Node
	ast.Inspect(file, func(n ast.Node) bool {
		if n == nil {
			// ast.Inspect calls f(nil) once for EVERY node whose f(node)
			// call returned true, immediately after that node's children
			// are done -- paired 1:1 with every push below, unlike the
			// round-1 visitor which only pushed non-ParenExpr nodes but
			// popped on every exit.
			if len(stack) > 0 {
				stack = stack[:len(stack)-1]
			}
			return true
		}
		if len(stack) > 0 {
			parent[n] = stack[len(stack)-1]
		}
		stack = append(stack, n)
		return true
	})
	return parent
}

// nonParenParent walks up the parent map from n, skipping any ParenExpr
// ancestors, and returns the first non-ParenExpr ancestor (or nil at the
// file root).
func nonParenParent(parent map[ast.Node]ast.Node, n ast.Node) ast.Node {
	for {
		p, ok := parent[n]
		if !ok {
			return nil
		}
		if _, isParen := p.(*ast.ParenExpr); isParen {
			n = p
			continue
		}
		return p
	}
}

// directOperandOfUnguardedAddSub reports whether mul feeds a +/- BinaryExpr
// either DIRECTLY, or through exactly one wrapping call that is NOT a
// float64()/float32() conversion.
//
// The second case exists because of a codex round-1 finding (P1, EXECUTED):
// `identity(a*b) + c`, where `identity` is a small Go function that returns
// its argument unchanged, is NOT protected the way `float64(a*b) + c` is --
// only an explicit conversion is documented by the Go spec as forcing
// rounding, so a generic wrapper call gives no such guarantee, and if it
// gets inlined the compiler can still see straight-line `a*b + c` and fuse
// it (codex's repro: `FMADDD` in the arm64 assembly for exactly this shape).
// Syntactically, the MUL's immediate parent there is the CallExpr, not the
// +/- -- so the original (round-1) version of this check, which only looked
// one level up, missed it. This flags one level further: MUL -> non-
// float64/float32 CallExpr -> +/-.
//
// This does NOT make the lint sound against arbitrarily deep indirection
// (the MUL stored in a variable, returned from a helper, passed through two
// function boundaries, etc.) -- no purely syntactic check can be, once
// inlining is in play. That residual class is exactly why fma_golden_test.go
// exists: a bit-pattern regression test catches what static analysis
// structurally cannot. See this function's package doc comment.
func directOperandOfUnguardedAddSub(info *types.Info, parent map[ast.Node]ast.Node, mul ast.Node) bool {
	p := nonParenParent(parent, mul)
	if isAddSubBinary(p) {
		return true
	}
	call, ok := p.(*ast.CallExpr)
	if !ok || isFloatConversionCall(info, call) {
		return false
	}
	return isAddSubBinary(nonParenParent(parent, call))
}

func isAddSubBinary(n ast.Node) bool {
	binary, ok := n.(*ast.BinaryExpr)
	return ok && (binary.Op == token.ADD || binary.Op == token.SUB)
}

// isFloatConversionCall reports whether call is genuinely the builtin
// float64()/float32() conversion -- the only wrapping this codebase's
// established CHAOS-4818 fix idiom uses, and the only one the Go spec
// documents as forcing rounding and preventing fusion.
//
// Codex round 2 (P1, EXECUTED) found the round-1 version of this check was
// purely syntactic (`ident.Name == "float64"`), so a LOCAL shadowing
// identifier of that exact name -- `float64 := func(v float64) float64 {
// return v }` -- passed it, and the lint exempted `float64(a*b) + c` even
// though that "conversion" is just an ordinary function call the compiler
// can inline and fuse through, same repro shape as identity() above. Using
// `info.Uses[ident]` (populated by go/types) resolves what the identifier
// ACTUALLY refers to at this position, not what it is spelled -- a
// shadowing local resolves to that local's own object, never to the
// universe-scope `float64`/`float32` type, so this correctly rejects it.
func isFloatConversionCall(info *types.Info, call *ast.CallExpr) bool {
	ident, ok := call.Fun.(*ast.Ident)
	if !ok {
		return false
	}
	typeName, ok := info.Uses[ident].(*types.TypeName)
	if !ok {
		return false
	}
	basic, ok := typeName.Type().(*types.Basic)
	if !ok {
		return false
	}
	return basic.Kind() == types.Float64 || basic.Kind() == types.Float32
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
