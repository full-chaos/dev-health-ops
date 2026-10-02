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
		var current *ast.FuncDecl
		check := func(expression ast.Expr, target types.Type, assignedToClientDoer bool) {
			if !isProviderHTTPDoer(target) {
				return
			}
			actual := pkg.TypesInfo.TypeOf(expression)
			if actual == nil || pkg.TypesInfo.Types[expression].IsNil() {
				return
			}
			if types.IsInterface(actual) {
				// An interface-typed value hides its concrete source: only providerfoundation.HTTPDoer itself (whose
				// concrete sources are checked where they enter it) may flow into an HTTPDoer.
				if !isProviderHTTPDoer(actual) && !isWrapperPlumbing(pkg.TypesInfo, expression, current, wrapper) {
					position := pkg.Fset.Position(expression.Pos())
					if !strings.HasSuffix(position.Filename, "_test.go") {
						problems = append(problems, fmt.Sprintf("DOER a value of the interface type %s becomes a providerfoundation.HTTPDoer at %s:%d: its concrete source is hidden (type it as providerfoundation.HTTPDoer)", types.TypeString(actual, nil), filepath.Base(position.Filename), position.Line))
					}
				}
				return
			}
			key := types.TypeString(actual, nil)
			if isNetHTTPClientPointer(actual) || (wrapper != nil && types.Implements(actual, wrapper)) {
				seen[key] = true
				return
			}
			if named := namedOf(actual); named != nil && named.Obj().Name() == "refusedDoer" && named.Obj().Pkg() != nil && strings.HasSuffix(named.Obj().Pkg().Path(), "/internal/providerfoundation") {
				seen[key] = true // the one explicit exception: the doer a PagerDuty entry point uses in place of a refused one; it sends nothing
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
				if declaration, ok := node.(*ast.FuncDecl); ok {
					current = declaration
				}
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

// isWrapperPlumbing: the two derived places where a doer legitimately travels as the anonymous Do-only interface of
// httpguard.Wrapper: the result of a call of Wrapper.Unwrap (the inner doer of a decorator the guard is rebuilding), and the
// parameter of a method named Rewrap (the inner doer the guard hands back).
func isWrapperPlumbing(info *types.Info, expression ast.Expr, current *ast.FuncDecl, wrapper *types.Interface) bool {
	switch e := expression.(type) {
	case *ast.CallExpr:
		if selector, ok := e.Fun.(*ast.SelectorExpr); ok && selector.Sel.Name == "Unwrap" {
			if _, ok := info.Uses[selector.Sel].(*types.Func); ok && wrapper != nil {
				receiver := info.TypeOf(selector.X)
				return receiver != nil && (types.Implements(receiver, wrapper) || types.Identical(receiver.Underlying(), wrapper.Underlying()) || types.Implements(types.NewPointer(receiver), wrapper))
			}
		}
	case *ast.Ident:
		if current == nil || current.Recv == nil || current.Name.Name != "Rewrap" || wrapper == nil {
			return false
		}
		if receiver := info.TypeOf(current.Recv.List[0].Type); receiver == nil || !types.Implements(receiver, wrapper) {
			return false
		}
		object := info.Uses[e]
		for _, param := range current.Type.Params.List {
			for _, name := range param.Names {
				if info.Defs[name] == object {
					return true
				}
			}
		}
	}
	return false
}
