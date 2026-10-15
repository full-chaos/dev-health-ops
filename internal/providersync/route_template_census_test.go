package providersync

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// CHAOS-9776 / CHAOS-9801 census: every provider page-walk options literal,
// every object fetch and every client request in this package names a static
// route template, so a status or transport failure says which route failed
// without printing a path that holds an owner, repository, project, member or
// issue name. A new file or a new un-templated call fails here.
//
// The template must be static: a string literal, a constant or variable named
// like a route (route, *Route), or a concatenation of those. A value built
// from anything else (a path, a root, a request value) fails the census.
func TestEveryPageWalkAndObjectFetchNamesARouteTemplate(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	var missing []string
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		bad := 0
		ast.Inspect(file, func(node ast.Node) bool {
			switch value := node.(type) {
			case *ast.CompositeLit:
				selector, ok := value.Type.(*ast.SelectorExpr)
				if !ok || (selector.Sel.Name != "GitHubPageOptions" && selector.Sel.Name != "GitLabPageOptions") {
					return true
				}
				if len(value.Elts) == 0 {
					return true
				}
				found := false
				for _, element := range value.Elts {
					pair, ok := element.(*ast.KeyValueExpr)
					if !ok {
						continue
					}
					if key, ok := pair.Key.(*ast.Ident); ok && key.Name == "Route" {
						found = true
						if !staticRouteExpr(pair.Value) {
							bad++
						}
					}
				}
				if !found {
					bad++
				}
			case *ast.CallExpr:
				switch fun := value.Fun.(type) {
				case *ast.Ident:
					if fun.Name == "fetchObject" && name != "native_rest.go" {
						bad++
					}
					if fun.Name == "fetchObjectRoute" && (len(value.Args) < 4 || !staticRouteExpr(value.Args[2])) {
						bad++
					}
				case *ast.SelectorExpr:
					receiver := receiverName(fun.X)
					if (receiver == "client" || receiver == "Client") && len(value.Args) >= 3 {
						switch fun.Sel.Name {
						case "Do":
							bad++
						case "DoRoute":
							if !staticRouteExpr(value.Args[2]) {
								bad++
							}
						}
					}
				}
			}
			return true
		})
		if bad > 0 {
			missing = append(missing, name)
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Fatalf("page-walk options, object fetches or client requests without a static route template in %v: use Route / fetchObjectRoute / DoRoute with a constant from route_templates.go (or a literal)", missing)
	}
}

func receiverName(expr ast.Expr) string {
	switch value := expr.(type) {
	case *ast.Ident:
		return value.Name
	case *ast.SelectorExpr:
		return value.Sel.Name
	}
	return ""
}

// staticRouteExpr reports whether expr can only be a compile-time or
// code-chosen route template: a string literal, a route-named identifier or
// field, an index into a route-named slice, or a concatenation of those.
func staticRouteExpr(expr ast.Expr) bool {
	switch value := expr.(type) {
	case *ast.BasicLit:
		return value.Kind == token.STRING
	case *ast.Ident:
		return routeName(value.Name)
	case *ast.SelectorExpr:
		return routeName(value.Sel.Name)
	case *ast.IndexExpr:
		return staticRouteExpr(value.X)
	case *ast.ParenExpr:
		return staticRouteExpr(value.X)
	case *ast.BinaryExpr:
		return value.Op == token.ADD && staticRouteExpr(value.X) && staticRouteExpr(value.Y)
	}
	return false
}

func routeName(name string) bool {
	lower := strings.ToLower(name)
	return lower == "route" || lower == "routes" || lower == "routebase" || lower == "suffix" || strings.HasSuffix(name, "Route")
}

func TestStaticRouteExprRejectsRequestValues(t *testing.T) {
	for source, want := range map[string]bool{
		`"/users/{login}"`:                true,
		`githubRepoRoute`:                 true,
		`githubRepoRoute + "/pulls"`:      true,
		`target.route`:                    true,
		`routes[index]`:                   true,
		`githubRepoRoute + "/x" + suffix`: true,
		`root`:                            false,
		`root + "/pulls"`:                 false,
		`path`:                            false,
		`"/repos/" + owner`:               false,
		`fmt.Sprint(x)`:                   false,
	} {
		expr, err := parser.ParseExpr(source)
		if err != nil {
			t.Fatal(err)
		}
		if got := staticRouteExpr(expr); got != want {
			t.Errorf("staticRouteExpr(%s) = %v, want %v", source, got, want)
		}
	}
}
