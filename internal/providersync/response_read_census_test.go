package providersync

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// responseReadAllowedFiles are the providerfoundation files that read a response body
// on purpose: the helpers themselves, the page decoders, the HTTP client's own error
// classification, and the credential and OAuth exchanges that carry no route template.
var responseReadAllowedFiles = map[string]bool{
	"response_read.go":                   true,
	"pagination.go":                      true,
	"http.go":                            true,
	"clients.go":                         true,
	"pagerduty_oauth.go":                 true,
	"pagerduty_oauth_callback.go":        true,
	"pagerduty_credential_validation.go": true,
}

func isIODiscard(expr ast.Expr) bool {
	selector, ok := expr.(*ast.SelectorExpr)
	if !ok || selector.Sel.Name != "Discard" {
		return false
	}
	pkg, ok := selector.X.(*ast.Ident)
	return ok && pkg.Name == "io"
}

func responseBodyReadSites(t *testing.T, name string, source []byte) []string {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, name, source, 0)
	if err != nil {
		t.Fatal(err)
	}
	var sites []string
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		pkg, ok := selector.X.(*ast.Ident)
		if !ok || !((pkg.Name == "io" && selector.Sel.Name == "ReadAll") || (pkg.Name == "json" && selector.Sel.Name == "NewDecoder") || (pkg.Name == "io" && selector.Sel.Name == "Copy")) {
			return true
		}
		if pkg.Name == "io" && selector.Sel.Name == "Copy" && len(call.Args) > 0 && isIODiscard(call.Args[0]) {
			return true // a drain into io.Discard reads no payload
		}
		reads := false
		ast.Inspect(call, func(inner ast.Node) bool {
			if field, ok := inner.(*ast.SelectorExpr); ok && field.Sel.Name == "Body" {
				reads = true
			}
			return true
		})
		if reads {
			sites = append(sites, fmt.Sprintf("%s.%s@%d", pkg.Name, selector.Sel.Name, fset.Position(call.Pos()).Line))
		}
		return true
	})
	return sites
}

// A 2xx body read or decode outside the response helpers has no route template:
// its failure prints no route=. Every read goes through providerfoundation.ReadResponseBody
// (or a decodePage walker) so a failure is a ProviderError that names the route.
func TestNoProviderResponseBodyReadBypassesTheRouteHelpers(t *testing.T) {
	var offenders []string
	files := 0
	for _, dir := range []string{".", "../scheduler/sync", "../providerfoundation"} {
		names, err := filepath.Glob(filepath.Join(dir, "*.go"))
		if err != nil {
			t.Fatal(err)
		}
		for _, name := range names {
			if strings.HasSuffix(name, "_test.go") || (dir == "../providerfoundation" && responseReadAllowedFiles[filepath.Base(name)]) {
				continue
			}
			source, err := os.ReadFile(name)
			if err != nil {
				t.Fatal(err)
			}
			files++
			for _, site := range responseBodyReadSites(t, name, source) {
				offenders = append(offenders, name+": "+site)
			}
		}
	}
	if files < 50 {
		t.Fatalf("scanned %d files; the census must see providersync, scheduler/sync and providerfoundation", files)
	}
	sort.Strings(offenders)
	if len(offenders) > 0 {
		t.Fatalf("response body read outside the route helpers:\n%s", strings.Join(offenders, "\n"))
	}
}

func TestResponseBodyReadDetectorFlagsRawReads(t *testing.T) {
	for source, want := range map[string]int{
		"io.ReadAll(io.LimitReader(response.Body, 10))":     1,
		"json.NewDecoder(response.Body).Decode(&v)":         1,
		"json.NewDecoder(io.LimitReader(resp.Body, 9))":     1,
		"io.Copy(io.Discard, response.Body)":                0,
		"io.Copy(&buffer, response.Body)":                   1,
		"io.Copy(file, resp.Body)":                          1,
		"io.Copy(io.Discard, reader)":                       0,
		"io.ReadAll(reader)":                                0,
		"json.NewDecoder(bytes.NewReader(raw)).Decode(&v)":  0,
		"providerfoundation.ReadResponseBody(response, 10)": 0,
	} {
		file := "package p\n\nfunc f() {\n\t_, _ = " + source + "\n}\n"
		if got := len(responseBodyReadSites(t, "fixture.go", []byte(file))); got != want {
			t.Errorf("%s: detector counts %d, want %d", source, got, want)
		}
	}
}

// graphQLErrorsAllowedFiles hold GraphQL `errors` reads that return a verdict
// or a bool, never an error, or that define the helpers.
var graphQLErrorsAllowedFiles = map[string]string{
	"response_read.go":    "defines the response helpers",
	"response_graphql.go": "defines the GraphQL helpers",
	"linear_absence.go":   "classifiers return a SnapshotAbsence verdict, not an error",
}

var graphQLHelperNames = map[string]bool{
	"ResponseGraphQL": true, "GraphQLShapeFailure": true,
	"ResponseFailure": true, "ResponseShape": true, "ResponseInvalid": true,
}

func isGraphQLSentinel(expr ast.Expr) bool {
	switch value := expr.(type) {
	case *ast.Ident:
		return value.Name == "ErrGraphQLResponse" || value.Name == "ErrGraphQLComplexity"
	case *ast.SelectorExpr:
		return value.Sel.Name == "ErrGraphQLResponse" || value.Sel.Name == "ErrGraphQLComplexity"
	}
	return false
}

func calleeName(call *ast.CallExpr) string {
	switch fun := call.Fun.(type) {
	case *ast.Ident:
		return fun.Name
	case *ast.SelectorExpr:
		return fun.Sel.Name
	}
	return ""
}

func mentionsGraphQLErrors(expr ast.Expr) bool {
	found := false
	ast.Inspect(expr, func(node ast.Node) bool {
		switch value := node.(type) {
		case *ast.SelectorExpr:
			found = found || value.Sel.Name == "Errors"
		case *ast.CallExpr:
			found = found || calleeName(value) == "graphQLErrors"
		case *ast.BasicLit:
			found = found || value.Value == `"errors"`
		}
		return true
	})
	return found
}

func callsGraphQLHelper(node ast.Node) bool {
	found := false
	ast.Inspect(node, func(inner ast.Node) bool {
		if call, ok := inner.(*ast.CallExpr); ok {
			name := calleeName(call)
			found = found || name == "ResponseGraphQL" || name == "GraphQLShapeFailure"
		}
		return true
	})
	return found
}

func returnsNonLiteralError(block *ast.BlockStmt) bool {
	found := false
	ast.Inspect(block, func(node ast.Node) bool {
		if ret, ok := node.(*ast.ReturnStmt); ok && len(ret.Results) > 0 {
			switch last := ret.Results[len(ret.Results)-1].(type) {
			case *ast.Ident:
				found = found || (last.Name != "nil" && last.Name != "true" && last.Name != "false")
			case *ast.SelectorExpr, *ast.CallExpr:
				found = true
			}
		}
		return true
	})
	return found
}

func graphQLErrorReturnSites(t *testing.T, name string, source []byte) []string {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, name, source, 0)
	if err != nil {
		t.Fatal(err)
	}
	var sites []string
	var walk func(node ast.Node, allowed bool)
	walk = func(node ast.Node, allowed bool) {
		ast.Inspect(node, func(inner ast.Node) bool {
			if inner == node {
				return true
			}
			switch value := inner.(type) {
			case *ast.GenDecl:
				return false // a var declaration of the sentinel itself
			case *ast.FuncLit:
				// a cause func (func(error) error) returns the sentinel as a cause
				if value.Type.Results != nil && len(value.Type.Params.List) == 1 && len(value.Type.Results.List) == 1 {
					walk(value.Body, true)
					return false
				}
			case *ast.CallExpr:
				if name := calleeName(value); graphQLHelperNames[name] || name == "Is" { // errors.Is reads a sentinel, it does not return one
					return false
				}
			case *ast.IfStmt:
				if mentionsGraphQLErrors(value.Cond) && returnsNonLiteralError(value.Body) && !callsGraphQLHelper(value.Body) {
					sites = append(sites, fmt.Sprintf("errors-branch@%d", fset.Position(value.Pos()).Line))
				}
			case *ast.Ident, *ast.SelectorExpr:
				if !allowed && isGraphQLSentinel(value.(ast.Expr)) {
					sites = append(sites, fmt.Sprintf("bare-sentinel@%d", fset.Position(value.Pos()).Line))
					return false
				}
			}
			return true
		})
	}
	walk(file, false)
	return sites
}

// A GraphQL `errors` array in a 2xx fails with a route. Every such return goes
// through providerfoundation.ResponseGraphQL (response in scope) or
// GraphQLShapeFailure (no response), so the error names route= and keeps its sentinel.
func TestNoGraphQLErrorsReturnBypassesTheRouteHelpers(t *testing.T) {
	var offenders []string
	files := 0
	for _, dir := range []string{".", "../scheduler/sync", "../providerfoundation"} {
		names, err := filepath.Glob(filepath.Join(dir, "*.go"))
		if err != nil {
			t.Fatal(err)
		}
		for _, name := range names {
			if strings.HasSuffix(name, "_test.go") || graphQLErrorsAllowedFiles[filepath.Base(name)] != "" {
				continue
			}
			source, err := os.ReadFile(name)
			if err != nil {
				t.Fatal(err)
			}
			files++
			for _, site := range graphQLErrorReturnSites(t, name, source) {
				offenders = append(offenders, name+": "+site)
			}
		}
	}
	if files < 50 {
		t.Fatalf("scanned %d files; the census must see providersync, scheduler/sync and providerfoundation", files)
	}
	sort.Strings(offenders)
	if len(offenders) > 0 {
		t.Fatalf("GraphQL errors return outside the route helpers:\n%s", strings.Join(offenders, "\n"))
	}
}

func TestGraphQLErrorsReturnDetectorFlagsBareReturns(t *testing.T) {
	for body, want := range map[string]int{
		"return providerfoundation.ErrGraphQLResponse":                                               1,
		"return nil, ErrGraphQLComplexity":                                                           1,
		"return finish(result, providerfoundation.ErrGraphQLResponse)":                               1,
		"return providerfoundation.ResponseGraphQL(r, providerfoundation.ErrGraphQLResponse)":        0,
		"return providerfoundation.GraphQLShapeFailure(\"/graphql\", ErrGraphQLResponse)":            0,
		"return providerfoundation.ResponseShape(r, providerfoundation.ErrGraphQLResponse)":          0,
		"cause := func(error) error { return providerfoundation.ErrGraphQLResponse }; _ = cause":     0,
		"if len(envelope.Errors) > 0 { return nil, errBoom }":                                        1,
		"if len(envelope.Errors) > 0 { return nil, providerfoundation.ResponseGraphQL(r, errBoom) }": 0,
		"_ = errors.Is(err, providerfoundation.ErrGraphQLResponse)":                                  0,
		"if len(envelope.Errors) > 0 { return false }":                                               0,
	} {
		file := "package p\n\nfunc f() error {\n\t" + body + "\n\treturn nil\n}\n"
		if got := len(graphQLErrorReturnSites(t, "fixture.go", []byte(file))); got != want {
			t.Errorf("%s: detector counts %d, want %d", body, got, want)
		}
	}
}
