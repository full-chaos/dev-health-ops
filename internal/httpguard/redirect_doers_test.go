package httpguard

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

// Every CONCRETE type that enters providerfoundation.HTTPDoer anywhere in production code (an argument, an assignment, a
// field of a literal, a return value, a var initialiser) is derived by type, and must be one of:
//
//   - *net/http.Client: the provider constructors guard it once (providerfoundation.NewHTTPClient);
//   - a type that implements httpguard.Wrapper: the guard is rebuilt around the doer it wraps;
//   - a route decorator (a struct of the same package with a Do method and an HTTPDoer field) assigned to the Doer field of a
//     constructed provider client: it wraps client.Doer, below which the guard already sits.
//
// So a decorator that is not a Wrapper can never be SUPPLIED into a provider client constructor: it cannot become an HTTPDoer
// at all, except by being assigned onto a constructed client. A new production type that implements Do and is used as an
// HTTPDoer fails here until it is made a Wrapper or built from client.Doer. An empty derived set FAILS.
func TestEveryConcreteTypeThatBecomesAnHTTPDoerIsGuardedOrWrapsTheGuardedDoer(t *testing.T) {
	root, err := filepath.Abs(moduleRootRel)
	if err != nil {
		t.Fatal(err)
	}
	problems, entering := doerProblems(t, root, []string{"./..."})
	for _, problem := range problems {
		t.Error(problem)
	}
	if entering < 5 {
		t.Errorf("only %d concrete types derived as entering HTTPDoer: the derivation is vacuous or broken", entering)
	}
	t.Logf("%d concrete types enter HTTPDoer", entering)
}

func doerProblems(t *testing.T, root string, patterns []string) ([]string, int) {
	t.Helper()
	loaded, err := packages.Load(&packages.Config{Dir: root, Mode: packages.NeedName | packages.NeedFiles | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo | packages.NeedImports | packages.NeedDeps}, patterns...)
	if err != nil {
		t.Fatal(err)
	}
	wrapper := wrapperInterface(loaded)
	var problems []string
	seen := map[string]bool{}
	for _, pkg := range loaded {
		if strings.Contains(pkg.PkgPath, "/internal/testsupport") || strings.Contains(pkg.PkgPath, "/testdata/") {
			continue
		}
		for _, loadErr := range pkg.Errors {
			problems = append(problems, fmt.Sprintf("LOAD error in %s: %v", pkg.PkgPath, loadErr))
		}
		if pkg.TypesInfo == nil {
			continue
		}
		check := func(expression ast.Expr, target types.Type, assignedToClientDoer bool) {
			if !isProviderHTTPDoer(target) {
				return
			}
			actual := pkg.TypesInfo.TypeOf(expression)
			if actual == nil || types.IsInterface(actual) || pkg.TypesInfo.Types[expression].IsNil() {
				return
			}
			key := types.TypeString(actual, nil)
			if isNetHTTPClientPointer(actual) || (wrapper != nil && types.Implements(actual, wrapper)) {
				seen[key] = true
				return
			}
			if named := namedOf(actual); named != nil && isRouteDecorator(named) && assignedToClientDoer {
				seen[key] = true
				return
			}
			position := pkg.Fset.Position(expression.Pos())
			if strings.HasSuffix(position.Filename, "_test.go") {
				return
			}
			problems = append(problems, fmt.Sprintf("DOER %s becomes a providerfoundation.HTTPDoer at %s:%d without being a *http.Client, a Wrapper, or a decorator assigned onto a constructed client's Doer", key, filepath.Base(position.Filename), position.Line))
		}
		for _, file := range pkg.Syntax {
			if strings.HasSuffix(pkg.Fset.Position(file.Pos()).Filename, "_test.go") {
				continue
			}
			ast.Inspect(file, func(node ast.Node) bool {
				switch n := node.(type) {
				case *ast.CallExpr:
					if signature, ok := pkg.TypesInfo.TypeOf(n.Fun).(*types.Signature); ok {
						for i, argument := range n.Args {
							if signature.Params().Len() == 0 {
								break
							}
							index := i
							if index >= signature.Params().Len() {
								index = signature.Params().Len() - 1
							}
							check(argument, signature.Params().At(index).Type(), false)
						}
					}
				case *ast.AssignStmt:
					for i, left := range n.Lhs {
						if i < len(n.Rhs) && len(n.Lhs) == len(n.Rhs) {
							check(n.Rhs[i], pkg.TypesInfo.TypeOf(left), isClientDoerSelector(pkg.TypesInfo, left))
						}
					}
				case *ast.KeyValueExpr:
					if lit, ok := pkg.TypesInfo.TypeOf(n.Key).(types.Type); ok && lit != nil {
						check(n.Value, lit, false)
					}
				case *ast.ReturnStmt:
					// results are checked against the function's declared results below
				case *ast.ValueSpec:
					for i, value := range n.Values {
						if i < len(n.Names) {
							check(value, pkg.TypesInfo.TypeOf(n.Names[i]), false)
						}
					}
				}
				return true
			})
			for _, declaration := range file.Decls {
				fn, ok := declaration.(*ast.FuncDecl)
				if !ok || fn.Body == nil || fn.Type.Results == nil {
					continue
				}
				results := pkg.TypesInfo.Defs[fn.Name].(*types.Func).Type().(*types.Signature).Results()
				ast.Inspect(fn.Body, func(node ast.Node) bool {
					if _, nested := node.(*ast.FuncLit); nested {
						return false
					}
					if ret, ok := node.(*ast.ReturnStmt); ok && len(ret.Results) == results.Len() {
						for i, value := range ret.Results {
							check(value, results.At(i).Type(), false)
						}
					}
					return true
				})
			}
		}
	}
	sort.Strings(problems)
	return uniqueStrings(problems), len(seen)
}

func namedOf(t types.Type) *types.Named {
	if pointer, ok := types.Unalias(t).(*types.Pointer); ok {
		t = pointer.Elem()
	}
	named, _ := types.Unalias(t).(*types.Named)
	return named
}

func isNetHTTPClientPointer(t types.Type) bool {
	pointer, ok := types.Unalias(t).(*types.Pointer)
	return ok && isClient(pointer.Elem())
}

func isProviderHTTPDoer(t types.Type) bool {
	named, ok := types.Unalias(t).(*types.Named)
	return ok && named.Obj().Name() == "HTTPDoer" && named.Obj().Pkg() != nil && strings.HasSuffix(named.Obj().Pkg().Path(), "/internal/providerfoundation")
}

// isRouteDecorator: a named struct with a Do method and a field of type HTTPDoer.
func isRouteDecorator(named *types.Named) bool {
	structType, ok := named.Underlying().(*types.Struct)
	if !ok {
		return false
	}
	hasDo := false
	for i := 0; i < named.NumMethods(); i++ {
		if named.Method(i).Name() == "Do" {
			hasDo = true
		}
	}
	if !hasDo {
		return false
	}
	for i := 0; i < structType.NumFields(); i++ {
		if isProviderHTTPDoer(structType.Field(i).Type()) {
			return true
		}
	}
	return false
}

// isClientDoerSelector: the expression is `<x>.Doer` where x is a (pointer to a) providerfoundation.HTTPClient.
func isClientDoerSelector(info *types.Info, expression ast.Expr) bool {
	selector, ok := expression.(*ast.SelectorExpr)
	if !ok || selector.Sel.Name != "Doer" {
		return false
	}
	named := namedOf(info.TypeOf(selector.X))
	return named != nil && named.Obj().Name() == "HTTPClient" && named.Obj().Pkg() != nil && strings.HasSuffix(named.Obj().Pkg().Path(), "/internal/providerfoundation")
}

// wrapperInterface is the httpguard.Wrapper interface as the loaded packages see it.
func wrapperInterface(loaded []*packages.Package) *types.Interface {
	var find func(pkg *packages.Package, seen map[string]bool) *types.Interface
	find = func(pkg *packages.Package, seen map[string]bool) *types.Interface {
		if seen[pkg.PkgPath] {
			return nil
		}
		seen[pkg.PkgPath] = true
		if pkg.PkgPath == modulePath+"/internal/httpguard" && pkg.Types != nil {
			if obj := pkg.Types.Scope().Lookup("Wrapper"); obj != nil {
				iface, _ := obj.Type().Underlying().(*types.Interface)
				return iface
			}
		}
		for _, imported := range pkg.Imports {
			if iface := find(imported, seen); iface != nil {
				return iface
			}
		}
		return nil
	}
	for _, pkg := range loaded {
		if iface := find(pkg, map[string]bool{}); iface != nil {
			return iface
		}
	}
	return nil
}
