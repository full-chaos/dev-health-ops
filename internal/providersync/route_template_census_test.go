package providersync

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// routeCensusDirs are the packages whose provider requests the census reads,
// relative to this package: providersync itself and the scheduler's source
// discovery.
var routeCensusDirs = []string{".", "../scheduler/sync"}

// CHAOS-9776 / CHAOS-9801 census: every provider page-walk options literal,
// every object fetch and every client request in routeCensusDirs names a
// static route template, so a status or transport failure says which route
// failed without printing a path that holds an owner, repository, project,
// member or issue name. A new file or a new un-templated call fails here.
//
// A page-walk options literal is any providerfoundation type with a Route
// field (read from that package's source). A client request is any
// four-argument .Do(ctx, method, path, body) call, whatever its receiver is
// named. The template must be static: a string literal, a constant or variable
// named like a route (route, *Route), or a concatenation of those. A value
// built from anything else (a path, a root, a request value) fails the census.
func TestEveryPageWalkAndObjectFetchNamesARouteTemplate(t *testing.T) {
	optionTypes := routeOptionTypes(t)
	if len(optionTypes) < 4 {
		t.Fatalf("read %d providerfoundation option types with a Route field; the census must see them", len(optionTypes))
	}
	var missing []string
	files := 0
	for _, dir := range routeCensusDirs {
		names, err := filepath.Glob(filepath.Join(dir, "*.go"))
		if err != nil {
			t.Fatal(err)
		}
		for _, name := range names {
			if strings.HasSuffix(name, "_test.go") {
				continue
			}
			source, err := os.ReadFile(name)
			if err != nil {
				t.Fatal(err)
			}
			files++
			if bad := routeCensusFile(t, name, source, optionTypes); bad > 0 {
				missing = append(missing, filepath.ToSlash(name))
			}
		}
	}
	if files < 100 {
		t.Fatalf("read %d files; the census must see providersync and scheduler/sync", files)
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Fatalf("page-walk options, object fetches or client requests without a static route template in %v: use Route / fetchObjectRoute / DoRoute with a constant from route_templates.go (or a literal)", missing)
	}
}

// routeOptionTypes is every providerfoundation struct type with a Route field.
func routeOptionTypes(t *testing.T) map[string]bool {
	t.Helper()
	names, err := filepath.Glob("../providerfoundation/*.go")
	if err != nil {
		t.Fatal(err)
	}
	types := map[string]bool{}
	for _, name := range names {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(file, func(node ast.Node) bool {
			spec, ok := node.(*ast.TypeSpec)
			if !ok {
				return true
			}
			if structType, ok := spec.Type.(*ast.StructType); ok {
				for _, field := range structType.Fields.List {
					for _, fieldName := range field.Names {
						if fieldName.Name == "Route" {
							types[spec.Name.Name] = true
						}
					}
				}
			}
			return true
		})
	}
	return types
}

// routeCensusFile counts the un-templated provider requests of one source file.
func routeCensusFile(t *testing.T, name string, source []byte, optionTypes map[string]bool) int {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), name, source, 0)
	if err != nil {
		t.Fatal(err)
	}
	bad := 0
	ast.Inspect(file, func(node ast.Node) bool {
		switch value := node.(type) {
		case *ast.CompositeLit:
			selector, ok := value.Type.(*ast.SelectorExpr)
			if !ok || !optionTypes[selector.Sel.Name] {
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
				if fun.Name == "fetchObject" {
					bad++
				}
				if fun.Name == "fetchObjectRoute" && (len(value.Args) < 4 || !staticRouteExpr(value.Args[2])) {
					bad++
				}
			case *ast.SelectorExpr:
				switch {
				case fun.Sel.Name == "Do" && len(value.Args) == 4:
					bad++
				case fun.Sel.Name == "DoUnauthenticated" && len(value.Args) == 3:
					bad++
				case fun.Sel.Name == "DoRoute" && (len(value.Args) < 3 || !staticRouteExpr(value.Args[2])):
					bad++
				}
			}
		}
		return true
	})
	return bad
}

func TestRouteCensusFlagsEveryUnTemplatedRequest(t *testing.T) {
	optionTypes := routeOptionTypes(t)
	for source, want := range map[string]int{
		`client.DoRoute(ctx, "GET", githubRepoRoute, root, nil)`:                               0,
		`providerfoundation.JiraPageOptions{Route: "/rest/api/3/project/search", MaxPages: 1}`: 0,
		`client2.Do(ctx, "GET", "/orgs/"+owner+"/repos", nil)`:                                 1,
		`prover.http.Do(ctx, http.MethodGet, path, nil)`:                                       1,
		`client.DoRoute(ctx, "GET", path, path, nil)`:                                          1,
		`providerfoundation.JiraPageOptions{MaxPages: 1}`:                                      1,
		`providerfoundation.LaunchDarklyOffsetOptions{Route: root}`:                            1,
		`providerfoundation.PagerDutyOffsetOptions{Path: "/users"}`:                            1,
		`client.DoUnauthenticated(ctx, http.MethodGet, location)`:                              1,
		`client.DoUnauthenticatedRoute(ctx, http.MethodGet, githubArtifactZipRoute, location)`: 0,
		`fetchObject(ctx, client, root, &out)`:                                                 1,
		`fetchObjectRoute(ctx, client, root, root, &out)`:                                      1,
	} {
		file := "package p\n\nfunc f() {\n\t_ = " + source + "\n}\n"
		if got := routeCensusFile(t, "fixture.go", []byte(file), optionTypes); got != want {
			t.Errorf("%s: census counts %d, want %d", source, got, want)
		}
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
