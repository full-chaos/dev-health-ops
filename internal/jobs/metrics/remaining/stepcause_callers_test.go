package remaining

import (
	"go/ast"
	"go/types"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/tools/go/packages"
)

const jobruntimePath = "github.com/full-chaos/dev-health-ops/internal/jobruntime"

// inAttributionPartition reports whether a file belongs to the
// work_item_attribution partition: handler.go, every work_item_attribution*.go,
// or any file importing stepcause. The other families of this package (dora,
// capacity, recommendations, membership, release impact) attach their own causes
// and are out of scope.
func inAttributionPartition(file *ast.File, filename string) bool {
	base := filepath.Base(filename)
	if base == "handler.go" || strings.HasPrefix(base, "work_item_attribution") {
		return true
	}
	for _, spec := range file.Imports {
		if strings.HasSuffix(spec.Path.Value, `/remaining/stepcause"`) {
			return true
		}
	}
	return false
}

// TestOnlyStepcauseAttachesASafeCause pins the third bypass of the step-label
// guard. In the files of the work_item_attribution partition (tests included):
//
//   - jobruntime.WithSafeCauseText is never called: it attaches ANY text as the
//     log cause, around stepcause.Failure (the only code allowed to build one,
//     from a closed step and two validated codes);
//   - jobruntime.WithSafeCause is called only inside ComputePartition (the
//     permanent ErrInvalidState refusals that pre-date the step label and sit on
//     a Permanent path, never a retryable one) or in a _test.go file.
//
// It reads go/types' resolved uses, not source text, so an import alias, a dot
// import or a function value (`f := jobruntime.WithSafeCauseText`) is caught
// the same way as a plain call.
func TestOnlyStepcauseAttachesASafeCause(t *testing.T) {
	loaded, err := packages.Load(&packages.Config{
		Mode:  packages.NeedName | packages.NeedFiles | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo | packages.NeedImports | packages.NeedDeps,
		Dir:   "../../../..",
		Tests: true,
	}, "./internal/jobs/metrics/remaining/...")
	if err != nil {
		t.Fatal(err)
	}
	if packages.PrintErrors(loaded) > 0 {
		t.Fatal("loading the remaining packages failed")
	}
	filesChecked := 0
	for _, pkg := range loaded {
		if strings.Contains(pkg.PkgPath, "/remaining/stepcause") {
			continue
		}
		for _, file := range pkg.Syntax {
			filename := pkg.Fset.Position(file.Pos()).Filename
			if !inAttributionPartition(file, filename) {
				continue
			}
			filesChecked++
			isTest := strings.HasSuffix(filename, "_test.go")
			// Every declaration, package-level vars and consts included: a
			// `var f = jobruntime.WithSafeCauseText` has no function body.
			for _, declaration := range file.Decls {
				enclosing := "" // package level
				if function, ok := declaration.(*ast.FuncDecl); ok {
					enclosing = function.Name.Name
				}
				ast.Inspect(declaration, func(node ast.Node) bool {
					ident, ok := node.(*ast.Ident)
					if !ok {
						return true
					}
					callee, ok := pkg.TypesInfo.Uses[ident].(*types.Func)
					if !ok || callee.Pkg() == nil || callee.Pkg().Path() != jobruntimePath {
						return true
					}
					switch {
					case callee.Name() == "WithSafeCauseText":
						t.Errorf("%s: jobruntime.WithSafeCauseText is used outside stepcause; use stepcause.Failure",
							pkg.Fset.Position(ident.Pos()))
					case callee.Name() == "WithSafeCause" && !isTest && enclosing != "ComputePartition":
						t.Errorf("%s: jobruntime.WithSafeCause is used in %q; only ComputePartition's permanent refusals may, a step failure uses stepcause.Failure",
							pkg.Fset.Position(ident.Pos()), enclosing)
					}
					return true
				})
			}
		}
	}
	if filesChecked < 3 {
		t.Fatalf("only %d attribution-partition files were checked: the load or the file selection found too little", filesChecked)
	}
}
