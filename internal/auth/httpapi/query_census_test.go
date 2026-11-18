package httpapi

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

// queryReadAllowed are the route reads that keep a net/url query parse, each
// with its reason. Every other route reads its query through Query.
var queryReadAllowed = map[string]string{
	"internal/apiservice/acr/access.go:principalAccessHandler:ParseQuery(RawQuery)": "Go-native acr route: an undecodable query or a ';' is refused 400 by design (access_test.go pins it)",
	"internal/auth/authapi/oauth.go:authorize:ParseQuery(RawQuery)":                 "Go-native OAuth authorize: a query it cannot parse is refused invalid_request by design",
	"internal/auth/identity/saml/handlers.go:decodeACS:ParseForm":                   "ACS binding body parse on a clone whose RawQuery is cleared: the URL query is never read",
}

// Every non-test file of the route trees is scanned: a r.URL.Query(), a
// url.ParseQuery(r.URL.RawQuery), or a form parse (which reads the URL query
// too) outside queryReadAllowed fails, since net/url drops a pair holding an
// unescaped ';' or a bad escape that Starlette keeps.
func TestRouteQueryReadsGoThroughQuery(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	var unnamed []string
	files := 0
	for _, dir := range []string{"internal/api", "internal/apiservice", "internal/auth", "internal/queryapi"} {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, entry fs.DirEntry, err error) error {
			if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return err
			}
			rel, _ := filepath.Rel(root, path)
			rel = filepath.ToSlash(rel)
			file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.SkipObjectResolution)
			if err != nil {
				return err
			}
			files++
			for _, decl := range file.Decls {
				name := ""
				if fn, ok := decl.(*ast.FuncDecl); ok {
					name = fn.Name.Name
				}
				ast.Inspect(decl, func(node ast.Node) bool {
					call, ok := node.(*ast.CallExpr)
					if !ok {
						return true
					}
					if kind := queryReadKind(call); kind != "" {
						site := rel + ":" + name + ":" + kind
						seen[site] = true
						if _, ok := queryReadAllowed[site]; !ok {
							unnamed = append(unnamed, site)
						}
					}
					return true
				})
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if files < 500 {
		t.Fatalf("scanned %d files: the walk did not reach the route trees", files)
	}
	sort.Strings(unnamed)
	for _, site := range unnamed {
		t.Errorf("%s reads the query with net/url; read it with httpapi.Query (or name it in queryReadAllowed with a reason)", site)
	}
	for site := range queryReadAllowed {
		if !seen[site] {
			t.Errorf("queryReadAllowed names %s, which no longer exists", site)
		}
	}
}

func queryReadKind(call *ast.CallExpr) string {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return ""
	}
	switch sel.Sel.Name {
	case "Query":
		if inner, ok := sel.X.(*ast.SelectorExpr); ok && inner.Sel.Name == "URL" && len(call.Args) == 0 {
			return "URL.Query()"
		}
	case "ParseQuery":
		if len(call.Args) == 1 {
			if arg, ok := call.Args[0].(*ast.SelectorExpr); ok && arg.Sel.Name == "RawQuery" {
				return "ParseQuery(RawQuery)"
			}
		}
	case "ParseForm", "ParseMultipartForm", "FormValue", "PostFormValue":
		return sel.Sel.Name
	}
	return ""
}
