package apiservice

import (
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// registration is one route registration found in the source: where it is
// written and which guard level its handler expression names.
type registration struct {
	File    string
	Line    int
	Method  string // "GET", ... ; "" when the method is not a literal
	Pattern string // fully resolved path; "" when it is not
	Tail    string // the literal tail of the path expression
	Head    string // the literal head of a path expression whose tail is a variable
	Level   string // Public, Optional, Authenticated, Admin, AdminOrg, Superuser, or "" for none named
}

var methodNames = map[string]string{
	"MethodGet": "GET", "MethodHead": "HEAD", "MethodPost": "POST", "MethodPut": "PUT",
	"MethodPatch": "PATCH", "MethodDelete": "DELETE", "MethodOptions": "OPTIONS",
}

var guardLevels = map[string]bool{"Public": true, "Optional": true, "Authenticated": true, "Admin": true, "AdminOrg": true, "Superuser": true}

// scanRegistrations parses the non-test Go files under dirs (repo-relative to
// internal/) and returns every httpapi.Route literal and every sso-style
// route(method, path, handler) call. Path expressions are resolved through the
// string constants and constant-initialized locals of the file's directory.
func scanRegistrations(t *testing.T, dirs ...string) []registration {
	t.Helper()
	var out []registration
	for _, root := range dirs {
		byDir := map[string][]string{}
		_ = filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
			if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			byDir[filepath.Dir(path)] = append(byDir[filepath.Dir(path)], path)
			return nil
		})
		for dir, files := range byDir {
			sort.Strings(files)
			fset := token.NewFileSet()
			parsed := map[string]*ast.File{}
			consts := map[string]string{}
			for _, path := range files {
				file, err := parser.ParseFile(fset, path, nil, 0)
				if err != nil {
					t.Fatal(err)
				}
				parsed[path] = file
				ast.Inspect(file, func(node ast.Node) bool {
					switch n := node.(type) {
					case *ast.ValueSpec:
						for i, ident := range n.Names {
							if i < len(n.Values) {
								if value, ok := literalString(n.Values[i], consts); ok {
									consts[ident.Name] = value
								}
							}
						}
					case *ast.AssignStmt:
						if n.Tok == token.DEFINE && len(n.Lhs) == len(n.Rhs) {
							for i, lhs := range n.Lhs {
								if ident, ok := lhs.(*ast.Ident); ok {
									if value, ok := literalString(n.Rhs[i], consts); ok {
										consts[ident.Name] = value
									}
								}
							}
						}
					}
					return true
				})
			}
			_ = dir
			closureLevel := map[string]string{}
			for _, path := range files {
				ast.Inspect(parsed[path], func(node ast.Node) bool {
					if decl, ok := node.(*ast.FuncDecl); ok && decl.Body != nil {
						if level := levelIn(decl.Body, nil); level != "" {
							closureLevel[decl.Name.Name] = level
						}
						return true
					}
					assign, ok := node.(*ast.AssignStmt)
					if !ok || assign.Tok != token.DEFINE || len(assign.Lhs) != 1 || len(assign.Rhs) != 1 {
						return true
					}
					lit, ok := assign.Rhs[0].(*ast.FuncLit)
					ident, isIdent := assign.Lhs[0].(*ast.Ident)
					if !ok || !isIdent {
						return true
					}
					if level := levelIn(lit.Body, nil); level != "" {
						closureLevel[ident.Name] = level
					}
					return true
				})
			}
			for _, path := range files {
				file := parsed[path]
				ast.Inspect(file, func(node ast.Node) bool {
					switch n := node.(type) {
					case *ast.CompositeLit:
						var methodExpr, patternExpr, handlerExpr ast.Expr
						for _, element := range n.Elts {
							kv, ok := element.(*ast.KeyValueExpr)
							if !ok {
								continue
							}
							key, _ := kv.Key.(*ast.Ident)
							if key == nil {
								continue
							}
							switch key.Name {
							case "Method":
								methodExpr = kv.Value
							case "Pattern":
								patternExpr = kv.Value
							case "Handler":
								handlerExpr = kv.Value
							}
						}
						if patternExpr != nil {
							out = append(out, buildRegistration(fset, path, methodExpr, patternExpr, handlerExpr, consts, closureLevel))
						}
					case *ast.CallExpr:
						ident, ok := n.Fun.(*ast.Ident)
						if ok && ident.Name == "route" && len(n.Args) == 3 {
							reg := buildRegistration(fset, path, n.Args[0], n.Args[1], n.Args[2], consts, closureLevel)
							if reg.Pattern != "" {
								reg.Pattern = consts["prefix"] + reg.Pattern
							}
							out = append(out, reg)
						}
					}
					return true
				})
			}
		}
	}
	return out
}

func literalString(expr ast.Expr, consts map[string]string) (string, bool) {
	switch typed := expr.(type) {
	case *ast.BasicLit:
		if typed.Kind == token.STRING {
			value, err := strconv.Unquote(typed.Value)
			return value, err == nil
		}
	case *ast.Ident:
		value, ok := consts[typed.Name]
		return value, ok
	case *ast.BinaryExpr:
		if typed.Op == token.ADD {
			left, lok := literalString(typed.X, consts)
			right, rok := literalString(typed.Y, consts)
			return left + right, lok && rok
		}
	case *ast.ParenExpr:
		return literalString(typed.X, consts)
	}
	return "", false
}

// tailString is the right-most string literal of a path expression.
func tailString(expr ast.Expr) string {
	switch typed := expr.(type) {
	case *ast.BasicLit:
		if typed.Kind == token.STRING {
			value, _ := strconv.Unquote(typed.Value)
			return value
		}
	case *ast.BinaryExpr:
		if typed.Op == token.ADD {
			return tailString(typed.Y)
		}
	}
	return ""
}

func buildRegistration(fset *token.FileSet, path string, methodExpr, patternExpr, handlerExpr ast.Expr, consts map[string]string, closures map[string]string) registration {
	reg := registration{File: filepath.ToSlash(path), Line: fset.Position(patternExpr.Pos()).Line}
	if selector, ok := methodExpr.(*ast.SelectorExpr); ok {
		reg.Method = methodNames[selector.Sel.Name]
	} else if methodExpr != nil {
		if value, ok := literalString(methodExpr, consts); ok {
			reg.Method = value
		}
	}
	if value, ok := literalString(patternExpr, consts); ok {
		reg.Pattern = value
	}
	reg.Tail = tailString(patternExpr)
	if binary, ok := patternExpr.(*ast.BinaryExpr); ok && reg.Pattern == "" && binary.Op == token.ADD {
		reg.Head, _ = literalString(binary.X, consts)
	}
	if handlerExpr != nil {
		reg.Level = levelIn(handlerExpr, closures)
	}
	return reg
}

// registrationFor returns the registration that mounts m: a resolved pattern
// equal to the route's, else an unresolved one whose literal head or tail the
// route carries; a registration with no method matches any method.
func registrationFor(regs []registration, m mountedRoute) (registration, bool) {
	probe := false
	for _, pattern := range httpapi.ProbePaths {
		probe = probe || pattern == m.Pattern
	}
	var fallback *registration
	for i := range regs {
		reg := regs[i]
		if reg.Method != "" && reg.Method != m.Method {
			continue
		}
		switch {
		case reg.Pattern != "" && reg.Pattern == m.Pattern:
			return reg, true
		case reg.Pattern == "" && reg.Head != "" && strings.HasPrefix(m.Pattern, reg.Head):
			fallback = &regs[i]
		case reg.Pattern == "" && reg.Tail != "" && strings.HasSuffix(m.Pattern, reg.Tail) && fallback == nil:
			fallback = &regs[i]
		case reg.Pattern == "" && reg.Head == "" && reg.Tail == "" && probe && strings.HasSuffix(reg.File, "health/health.go"):
			fallback = &regs[i]
		}
	}
	if fallback != nil {
		return *fallback, true
	}
	return registration{}, false
}

// levelIn is the first guard level a subtree names: a policy.<Level> selector,
// or a call of a local closure whose body names one.
func levelIn(root ast.Node, closures map[string]string) string {
	level := ""
	ast.Inspect(root, func(node ast.Node) bool {
		if level != "" {
			return false
		}
		switch n := node.(type) {
		case *ast.SelectorExpr:
			if pkg, ok := n.X.(*ast.Ident); ok && pkg.Name == "policy" && guardLevels[n.Sel.Name] {
				level = n.Sel.Name
			}
		case *ast.CallExpr:
			switch fun := n.Fun.(type) {
			case *ast.Ident:
				if closure, ok := closures[fun.Name]; ok {
					level = closure
				}
			case *ast.SelectorExpr:
				if closure, ok := closures[fun.Sel.Name]; ok {
					level = closure
				}
			}
		}
		return true
	})
	return level
}
