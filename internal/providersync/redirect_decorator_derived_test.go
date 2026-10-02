package providersync

import (
	"fmt"
	"go/ast"
	"go/types"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"golang.org/x/tools/go/packages"
)

// The decorators of this package are DERIVED by type, not listed: a decorator is a named struct type of this package with a Do
// method and a field of type providerfoundation.HTTPDoer. Every composite literal of one must take that field from the
// Doer of a constructed provider client (`<provider client>.Doer`), which providerfoundation.NewHTTPClient guarded once at
// construction, below every decorator: so a decorator can never hide a following client from the origin guard. The two
// literals inside CountRequests and RequestCountingDoer.Rewrap rebuild a Wrapper that httpguard looks through. A literal
// that takes its delegate from anywhere else FAILS here; an empty derived set FAILS (the derivation would be vacuous).
func TestEveryDecoratorWrapsTheGuardedDoerOfAConstructedClient(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	problems, decorators := decoratorProblems(t, root, "./internal/providersync")
	for _, problem := range problems {
		t.Error(problem)
	}
	if decorators < 40 {
		t.Errorf("only %d decorator types derived: the derivation is vacuous or broken", decorators)
	}
	t.Logf("%d decorator types derived", decorators)
}

func decoratorProblems(t *testing.T, root, pattern string) ([]string, int) {
	t.Helper()
	loaded, err := packages.Load(&packages.Config{Dir: root, Mode: packages.NeedName | packages.NeedFiles | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo | packages.NeedImports}, pattern)
	if err != nil {
		t.Fatal(err)
	}
	var problems []string
	derived := map[string]bool{}
	for _, pkg := range loaded {
		for _, loadErr := range pkg.Errors {
			problems = append(problems, fmt.Sprintf("LOAD error: %v", loadErr))
		}
		doerField := map[*types.Named]string{}
		for _, name := range pkg.Types.Scope().Names() {
			named, ok := pkg.Types.Scope().Lookup(name).Type().(*types.Named)
			if !ok {
				continue
			}
			structType, ok := named.Underlying().(*types.Struct)
			if !ok || !hasDoMethod(named) {
				continue
			}
			for i := 0; i < structType.NumFields(); i++ {
				if isHTTPDoer(structType.Field(i).Type()) {
					doerField[named] = structType.Field(i).Name()
					derived[name] = true
					break
				}
			}
		}
		for _, file := range pkg.Syntax {
			if strings.HasSuffix(pkg.Fset.Position(file.Pos()).Filename, "_test.go") {
				continue
			}
			for _, declaration := range file.Decls {
				symbol := ""
				if fn, ok := declaration.(*ast.FuncDecl); ok {
					symbol = fn.Name.Name
				}
				ast.Inspect(declaration, func(node ast.Node) bool {
					lit, ok := node.(*ast.CompositeLit)
					if !ok {
						return true
					}
					named, ok := types.Unalias(pkg.TypesInfo.TypeOf(lit)).(*types.Named)
					if !ok {
						return true
					}
					field, isDecorator := doerField[named]
					if !isDecorator {
						return true
					}
					position := pkg.Fset.Position(lit.Pos())
					label := fmt.Sprintf("%s:%d %s", filepath.Base(position.Filename), position.Line, named.Obj().Name())
					if named.Obj().Name() == "RequestCountingDoer" && (symbol == "CountRequests" || symbol == "Rewrap") {
						return true
					}
					value := literalField(lit, field)
					if value == nil || !fromConstructedClient(pkg.TypesInfo, value) {
						problems = append(problems, "DECORATOR wraps a doer that is not the Doer of a constructed provider client: "+label)
					}
					return true
				})
			}
		}
	}
	sort.Strings(problems)
	return problems, len(derived)
}

func hasDoMethod(named *types.Named) bool {
	for i := 0; i < named.NumMethods(); i++ {
		if named.Method(i).Name() == "Do" {
			return true
		}
	}
	return false
}

func isHTTPDoer(t types.Type) bool {
	named, ok := types.Unalias(t).(*types.Named)
	return ok && named.Obj().Name() == "HTTPDoer" && named.Obj().Pkg() != nil && strings.HasSuffix(named.Obj().Pkg().Path(), "/internal/providerfoundation")
}

func literalField(lit *ast.CompositeLit, field string) ast.Expr {
	for _, element := range lit.Elts {
		if pair, ok := element.(*ast.KeyValueExpr); ok {
			if key, ok := pair.Key.(*ast.Ident); ok && key.Name == field {
				return pair.Value
			}
		}
	}
	return nil
}

// fromConstructedClient: the expression is `<x>.Doer` where x is a (pointer to a) providerfoundation.HTTPClient.
func fromConstructedClient(info *types.Info, expression ast.Expr) bool {
	selector, ok := expression.(*ast.SelectorExpr)
	if !ok || selector.Sel.Name != "Doer" {
		return false
	}
	t := info.TypeOf(selector.X)
	if pointer, ok := types.Unalias(t).(*types.Pointer); ok {
		t = pointer.Elem()
	}
	named, ok := types.Unalias(t).(*types.Named)
	return ok && named.Obj().Name() == "HTTPClient" && named.Obj().Pkg() != nil && strings.HasSuffix(named.Obj().Pkg().Path(), "/internal/providerfoundation")
}
