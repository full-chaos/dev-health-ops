package teamsidentity

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// A 2xx body read outside providerfoundation's response helpers has no route
// template and no size cap. A non-test file of this package may touch a
// response body only to Close it; reads go through readDiscoveryBody.
var responseNames = map[string]bool{"response": true, "resp": true, "res": true}

func responseBodyTouches(t *testing.T, name string, source []byte) []string {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, name, source, 0)
	if err != nil {
		t.Fatal(err)
	}
	isBody := func(expr ast.Expr) (*ast.SelectorExpr, bool) {
		selector, ok := expr.(*ast.SelectorExpr)
		if !ok || selector.Sel.Name != "Body" {
			return nil, false
		}
		ident, ok := selector.X.(*ast.Ident)
		return selector, ok && responseNames[ident.Name]
	}
	closed := map[*ast.SelectorExpr]bool{}
	ast.Inspect(file, func(node ast.Node) bool {
		if selector, ok := node.(*ast.SelectorExpr); ok && selector.Sel.Name == "Close" {
			if body, ok := isBody(selector.X); ok {
				closed[body] = true
			}
		}
		return true
	})
	var sites []string
	ast.Inspect(file, func(node ast.Node) bool {
		if expr, ok := node.(ast.Expr); ok {
			if body, ok := isBody(expr); ok && !closed[body] {
				sites = append(sites, name+"@"+strconv.Itoa(fset.Position(body.Pos()).Line))
			}
		}
		return true
	})
	return sites
}

func TestNoTeamsIdentityResponseBodyReadBypassesTheRouteHelpers(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	var offenders []string
	files := 0
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		source, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		files++
		offenders = append(offenders, responseBodyTouches(t, name, source)...)
	}
	if files < 20 {
		t.Fatalf("scanned %d files; the census must see the whole package", files)
	}
	sort.Strings(offenders)
	if len(offenders) > 0 {
		t.Fatalf("response body touched outside Close:\n%s", strings.Join(offenders, "\n"))
	}
}

func TestResponseBodyTouchDetectorFlagsRawReads(t *testing.T) {
	for source, want := range map[string]int{
		"io.ReadAll(response.Body)":                        1,
		"io.ReadAll(io.LimitReader(resp.Body, 9))":         1,
		"json.NewDecoder(response.Body).Decode(&v)":        1,
		"io.Copy(&buffer, res.Body)":                       1,
		"reader := response.Body":                          1,
		"response.Body.Close()":                            0,
		"defer response.Body.Close()":                      0,
		"io.ReadAll(reader)":                               0,
		"json.NewDecoder(bytes.NewReader(raw)).Decode(&v)": 0,
		"providerfoundation.ReadResponseBody(response, 9)": 0,
	} {
		file := "package p\n\nfunc f() {\n\t_ = " + source + "\n}\n"
		if strings.HasPrefix(source, "reader :=") || strings.HasPrefix(source, "defer ") || strings.HasSuffix(source, ".Close()") {
			file = "package p\n\nfunc f() {\n\t" + source + "\n}\n"
		}
		if got := len(responseBodyTouches(t, "fixture.go", []byte(file))); got != want {
			t.Errorf("%s: detector counts %d, want %d", source, got, want)
		}
	}
}
