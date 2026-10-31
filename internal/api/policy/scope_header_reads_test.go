package policy

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"net/textproto"
	"slices"
	"strconv"
	"testing"
)

// scopeAllowedRequestHeaders are the request headers scope.go may read, each
// by a string-literal key.
var scopeAllowedRequestHeaders = []string{"X-Org-Id"}

func isRequestParam(field *ast.Field) bool {
	star, ok := field.Type.(*ast.StarExpr)
	if !ok {
		return false
	}
	sel, ok := star.X.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	pkg, ok := sel.X.(*ast.Ident)
	return ok && pkg.Name == "http" && sel.Sel.Name == "Request"
}

func requestNames(fields *ast.FieldList, into map[string]bool) {
	if fields == nil {
		return
	}
	for _, field := range fields.List {
		if !isRequestParam(field) {
			continue
		}
		for _, name := range field.Names {
			into[name.Name] = true
		}
	}
}

func cloneNames(from map[string]bool) map[string]bool {
	out := make(map[string]bool, len(from))
	for name := range from {
		out[name] = true
	}
	return out
}

func requestVar(expr ast.Expr, requests map[string]bool) bool {
	ident, ok := expr.(*ast.Ident)
	return ok && requests[ident.Name]
}

func allowedHeaderKey(arg ast.Expr) bool {
	lit, ok := arg.(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return false
	}
	key, err := strconv.Unquote(lit.Value)
	if err != nil {
		return false
	}
	return slices.Contains(scopeAllowedRequestHeaders, textproto.CanonicalMIMEHeaderKey(key))
}

// checkRequestHeaderReads returns one line per request-header access of file
// that is not r.Header.Get("<allowed>"), r.Header.Values("<allowed>") or
// r.Header["<allowed>"], with an allowed key given as a string literal. A
// request is a parameter of type *http.Request, or a name assigned from a
// call on one (r = r.WithContext(ctx)). Response-writer headers (w.Header())
// are not request reads.
func checkRequestHeaderReads(fset *token.FileSet, file *ast.File) []string {
	var problems []string
	report := func(pos token.Pos, format string, args ...any) {
		problems = append(problems, fmt.Sprintf("%s: %s", fset.Position(pos), fmt.Sprintf(format, args...)))
	}
	var walk func(node ast.Node, requests map[string]bool)
	walk = func(root ast.Node, requests map[string]bool) {
		ast.Inspect(root, func(node ast.Node) bool {
			switch typed := node.(type) {
			case *ast.FuncLit:
				inner := cloneNames(requests)
				requestNames(typed.Type.Params, inner)
				walk(typed.Body, inner)
				return false
			case *ast.AssignStmt:
				for index, right := range typed.Rhs {
					if requestVar(right, requests) {
						report(right.Pos(), "a request is copied to another name")
					}
					call, ok := right.(*ast.CallExpr)
					if !ok || index >= len(typed.Lhs) {
						continue
					}
					sel, ok := call.Fun.(*ast.SelectorExpr)
					if !ok || !requestVar(sel.X, requests) {
						continue
					}
					if left, ok := typed.Lhs[index].(*ast.Ident); ok {
						requests[left.Name] = true
					}
				}
			case *ast.ValueSpec:
				for _, value := range typed.Values {
					if requestVar(value, requests) {
						report(value.Pos(), "a request is copied to another name")
					}
				}
			case *ast.CallExpr:
				switch fun := typed.Fun.(type) {
				case *ast.SelectorExpr:
					if requestVar(fun.X, requests) && (fun.Sel.Name == "BasicAuth" || fun.Sel.Name == "Cookie" || fun.Sel.Name == "Cookies") {
						report(typed.Pos(), "%s reads a request credential", fun.Sel.Name)
					}
					if header, ok := fun.X.(*ast.SelectorExpr); ok && requestVar(header.X, requests) && header.Sel.Name == "Header" {
						if (fun.Sel.Name == "Get" || fun.Sel.Name == "Values") && len(typed.Args) == 1 && allowedHeaderKey(typed.Args[0]) {
							return false
						}
						report(typed.Pos(), "request header read %s is not Get or Values of an allowed literal key", fun.Sel.Name)
						return false
					}
				case *ast.Ident:
					if fun.Name == "BearerToken" {
						report(typed.Pos(), "BearerToken is called")
					}
				}
			case *ast.IndexExpr:
				if header, ok := typed.X.(*ast.SelectorExpr); ok && requestVar(header.X, requests) && header.Sel.Name == "Header" {
					if !allowedHeaderKey(typed.Index) {
						report(typed.Pos(), "request header index with a key that is not an allowed literal")
					}
					return false
				}
			case *ast.SelectorExpr:
				if requestVar(typed.X, requests) && typed.Sel.Name == "Header" {
					report(typed.Pos(), "request headers are used other than by Get, Values or index with an allowed literal key")
				}
			}
			return true
		})
	}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		requests := map[string]bool{}
		requestNames(fn.Type.Params, requests)
		walk(fn.Body, requests)
	}
	return problems
}

// checkTakesCallerFromPrincipal returns a line for each of OrgScope and
// Impersonation that does not call principalFrom.
func checkTakesCallerFromPrincipal(file *ast.File) []string {
	var problems []string
	for _, name := range []string{"OrgScope", "Impersonation"} {
		found, calls := false, false
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv == nil || fn.Name.Name != name || fn.Body == nil {
				continue
			}
			found = true
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				if call, ok := node.(*ast.CallExpr); ok {
					if ident, ok := call.Fun.(*ast.Ident); ok && ident.Name == "principalFrom" {
						calls = true
					}
				}
				return true
			})
		}
		switch {
		case !found:
			problems = append(problems, name+" is not declared in the file")
		case !calls:
			problems = append(problems, name+" does not call principalFrom")
		}
	}
	return problems
}

func parseSource(t *testing.T, source string) (*token.FileSet, *ast.File) {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "fixture.go", source, parser.SkipObjectResolution)
	if err != nil {
		t.Fatal(err)
	}
	return fset, file
}

// The scope middlewares take the caller from the Principal decision. In
// scope.go no request header is read except X-Org-Id by a literal key, and
// OrgScope and Impersonation call principalFrom.
func TestScopeTakesTheCallerFromPrincipalAndReadsOnlyXOrgID(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "scope.go", nil, parser.SkipObjectResolution)
	if err != nil {
		t.Fatal(err)
	}
	for _, problem := range checkRequestHeaderReads(fset, file) {
		t.Error(problem)
	}
	for _, problem := range checkTakesCallerFromPrincipal(file) {
		t.Error(problem)
	}
}

func TestRequestHeaderReadCheckerFixtures(t *testing.T) {
	const prelude = "package policy\nimport \"net/http\"\n"
	cases := []struct {
		name string
		body string
		want int
	}{
		{"allowed literal Values", `func f(w http.ResponseWriter, r *http.Request) { _ = r.Header.Values("X-Org-Id") }`, 0},
		{"allowed literal Get lowercase", `func f(w http.ResponseWriter, r *http.Request) { _ = r.Header.Get("x-org-id") }`, 0},
		{"allowed literal index", `func f(w http.ResponseWriter, r *http.Request) { _ = r.Header["X-Org-Id"] }`, 0},
		{"response writer headers", `func f(w http.ResponseWriter, r *http.Request) { h := w.Header(); h.Add("X-Impersonating", "true"); w.Header().Set("Content-Type", "x"); w.Header().Set("Authorization", "x") }`, 0},
		{"request kept by WithContext", `func f(w http.ResponseWriter, r *http.Request) { r = r.WithContext(r.Context()); _ = r.Header.Get("X-Org-Id") }`, 0},
		{"index with Authorization", `func f(w http.ResponseWriter, r *http.Request) { _ = r.Header["Authorization"] }`, 1},
		{"Get with Authorization", `func f(w http.ResponseWriter, r *http.Request) { _ = r.Header.Get("Authorization") }`, 1},
		{"Values with lowercase authorization", `func f(w http.ResponseWriter, r *http.Request) { _ = r.Header.Values("authorization") }`, 1},
		{"Get with a variable key", `func f(w http.ResponseWriter, r *http.Request) { k := "Authorization"; _ = r.Header.Get(k) }`, 1},
		{"header copied to a name", `func f(w http.ResponseWriter, r *http.Request) { h := r.Header; _ = h.Get("X-Org-Id") }`, 1},
		{"header ranged", `func f(w http.ResponseWriter, r *http.Request) { for range r.Header { } }`, 1},
		{"header cloned", `func f(w http.ResponseWriter, r *http.Request) { _ = r.Header.Clone() }`, 1},
		{"basic auth", `func f(w http.ResponseWriter, r *http.Request) { _, _, _ = r.BasicAuth() }`, 1},
		{"read in a function literal", `func f(next http.Handler) http.Handler { return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _ = r.Header.Get("Authorization") }) }`, 1},
		{"request copied", `func f(w http.ResponseWriter, r *http.Request) { q := r; _ = q }`, 1},
		{"bearer parse", `func f(w http.ResponseWriter, r *http.Request) { _, _ = BearerToken("x") }`, 1},
		{"basic auth method value", `func f(w http.ResponseWriter, r *http.Request) { f := r.BasicAuth; _, _, _ = f() }`, 1},
		{"cookie method value", `func f(w http.ResponseWriter, r *http.Request) { f := r.Cookie; _, _ = f("a") }`, 1},
		{"cookies method value", `func f(w http.ResponseWriter, r *http.Request) { f := r.Cookies; _ = f() }`, 1},
		{"bearer parse function value", `func f(w http.ResponseWriter, r *http.Request) { f := BearerToken; _, _ = f("x") }`, 1},
		{"derived request read", `func f(w http.ResponseWriter, r *http.Request) { r2 := r.WithContext(r.Context()); _ = r2.Header.Get("Authorization") }`, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fset, file := parseSource(t, prelude+tc.body)
			got := checkRequestHeaderReads(fset, file)
			if len(got) < tc.want || (tc.want == 0 && len(got) != 0) {
				t.Fatalf("problems %v, want %d", got, tc.want)
			}
		})
	}
}

func TestPrincipalCallCheckerFixtures(t *testing.T) {
	const prelude = "package policy\ntype Scope struct{}\n"
	both := `func (s *Scope) OrgScope() { principalFrom(nil) }
func (s *Scope) Impersonation() { principalFrom(nil) }`
	for name, tc := range map[string]struct {
		body string
		want int
	}{
		"both call":        {both, 0},
		"no OrgScope call": {"func (s *Scope) OrgScope() {}\nfunc (s *Scope) Impersonation() { principalFrom(nil) }", 1},
		"no Impersonation": {"func (s *Scope) OrgScope() { principalFrom(nil) }", 1},
	} {
		t.Run(name, func(t *testing.T) {
			_, file := parseSource(t, prelude+tc.body)
			if got := checkTakesCallerFromPrincipal(file); len(got) != tc.want {
				t.Fatalf("problems %v, want %d", got, tc.want)
			}
		})
	}
}
