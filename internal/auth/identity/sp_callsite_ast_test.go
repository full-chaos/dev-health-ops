package identity

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"path/filepath"
	"sort"
	"strings"
)

// Guarantee: each call of IdentityBySubject (and of a named delegate) binds its principal to a fresh variable, and the
// same lexical function then reads that variable's ServiceProvider field, builds a ServiceProvider struct key from it,
// or passes it to signInTo. Refused, never followed: a result that is discarded, returned, forwarded, stored, copied,
// address-taken, reassigned, shadowed or captured by a func literal, and any method value or method expression of the
// lookup. spAllowlist is the only escape.

type spAllow struct{ file, fn, reason string }

var spAllowlist = []spAllow{
	{"identity/social/service.go", "lookup", "returns the IdentityBySubject result to its callers; each caller is held to the same rule"},
}

const spLookupName = "IdentityBySubject"

type spResult struct {
	gaps    []string
	files   map[string]bool
	allowed map[int]bool
}

func spEnclosingFunc(parent map[ast.Node]ast.Node, n ast.Node) ast.Node {
	for p := parent[n]; p != nil; p = parent[p] {
		switch p.(type) {
		case *ast.FuncDecl, *ast.FuncLit:
			return p
		}
	}
	return nil
}

func spIsBlank(e ast.Expr) bool {
	id, ok := e.(*ast.Ident)
	return ok && id.Name == "_"
}

func spWritten(parent map[ast.Node]ast.Node, n ast.Node) bool {
	switch p := parent[n].(type) {
	case *ast.AssignStmt:
		for _, l := range p.Lhs {
			if l == n {
				return true
			}
		}
	case *ast.IncDecStmt:
		return true
	}
	return false
}

func spUnderServiceProviderKey(parent map[ast.Node]ast.Node, n ast.Node) bool {
	for c := n; parent[c] != nil; c = parent[c] {
		if _, ok := parent[c].(ast.Stmt); ok {
			return false
		}
		if kv, ok := parent[c].(*ast.KeyValueExpr); ok && kv.Value == c {
			if k, ok := kv.Key.(*ast.Ident); ok && k.Name == "ServiceProvider" {
				return true
			}
		}
	}
	return false
}

// spUse classifies one use of a lookup's principal variable: a recognized check, a harmless read, or a refusal.
func spUse(parent map[ast.Node]ast.Node, info *types.Info, u *ast.Ident) (checks bool, refusal string) {
	switch p := parent[u].(type) {
	case *ast.AssignStmt:
		if spWritten(parent, u) {
			return false, "is assigned again"
		}
		return false, "is copied"
	case *ast.SelectorExpr:
		if p.X != u {
			return false, "escapes"
		}
		if spWritten(parent, p) {
			return false, "is written"
		}
		if p.Sel.Name == "ServiceProvider" || spUnderServiceProviderKey(parent, p) {
			return true, ""
		}
		return false, ""
	case *ast.CallExpr:
		if f, ok := p.Fun.(*ast.Ident); ok && len(p.Args) == 1 && p.Args[0] == u {
			if sf, ok := info.Uses[f].(*types.Func); ok && sf.Name() == "signInTo" {
				return true, ""
			}
		}
		return false, "is passed on"
	}
	return false, "escapes"
}

func spCheck(fset *token.FileSet, files []*ast.File, info *types.Info) spResult {
	res := spResult{files: map[string]bool{}, allowed: map[int]bool{}}
	parent := map[ast.Node]ast.Node{}
	for _, f := range files {
		var stack []ast.Node
		ast.Inspect(f, func(n ast.Node) bool {
			if n == nil {
				stack = stack[:len(stack)-1]
				return true
			}
			if len(stack) > 0 {
				parent[n] = stack[len(stack)-1]
			}
			stack = append(stack, n)
			return true
		})
	}
	exempt := map[ast.Node]bool{}
	tracked := map[types.Object]bool{}
	for _, f := range files {
		name := filepath.ToSlash(fset.Position(f.Pos()).Filename)
		for _, d := range f.Decls {
			fn, ok := d.(*ast.FuncDecl)
			if !ok {
				continue
			}
			for i, a := range spAllowlist {
				if strings.HasSuffix(name, a.file) && fn.Name.Name == a.fn {
					exempt[fn] = true
					tracked[info.Defs[fn.Name]] = true
					res.allowed[i] = true
				}
			}
		}
	}
	uses := map[types.Object][]*ast.Ident{}
	for id, obj := range info.Uses {
		uses[obj] = append(uses[obj], id)
	}
	gap := func(n ast.Node, format string, args ...any) {
		res.gaps = append(res.gaps, fmt.Sprintf("%s: %s", fset.Position(n.Pos()), fmt.Sprintf(format, args...)))
	}
	for id, obj := range info.Uses {
		if f, ok := obj.(*types.Func); !ok || (f.Name() != spLookupName && !tracked[obj]) {
			continue
		}
		res.files[filepath.ToSlash(fset.Position(id.Pos()).Filename)] = true
		var ref ast.Expr = id
		if sel, ok := parent[id].(*ast.SelectorExpr); ok && sel.Sel == id {
			ref = sel
			if s := info.Selections[sel]; s != nil && s.Kind() == types.MethodExpr {
				gap(sel, "%s is used as a method expression", id.Name)
				continue
			}
		}
		call, ok := parent[ref].(*ast.CallExpr)
		if !ok || call.Fun != ref {
			gap(ref, "%s is used as a value, not called: the guard cannot follow its result", id.Name)
			continue
		}
		fn := spEnclosingFunc(parent, call)
		if exempt[fn] {
			continue
		}
		var lhs ast.Expr
		switch p := parent[call].(type) {
		case *ast.AssignStmt:
			if len(p.Rhs) != 1 {
				gap(call, "%s call shares its assignment with other values", id.Name)
				continue
			}
			lhs = p.Lhs[0]
		case *ast.ValueSpec:
			if len(p.Values) != 1 {
				gap(call, "%s call shares its declaration with other values", id.Name)
				continue
			}
			lhs = p.Names[0]
		default:
			gap(call, "%s result is discarded, returned or forwarded: only a fresh variable is followed", id.Name)
			continue
		}
		if spIsBlank(lhs) {
			gap(call, "%s principal is discarded", id.Name)
			continue
		}
		def, ok := lhs.(*ast.Ident)
		if !ok {
			gap(call, "%s principal is stored, not bound to a fresh variable", id.Name)
			continue
		}
		v := info.Defs[def]
		if v == nil {
			gap(call, "%s principal is assigned to an existing variable", id.Name)
			continue
		}
		credited := false
		for _, u := range uses[v] {
			if spEnclosingFunc(parent, u) != fn {
				gap(u, "%s principal %s is captured by a func literal", id.Name, u.Name)
				continue
			}
			checks, refusal := spUse(parent, info, u)
			if refusal != "" {
				gap(u, "%s principal %s %s", id.Name, u.Name, refusal)
			}
			credited = credited || checks
		}
		if !credited {
			gap(call, "%s principal %s has no ServiceProvider check of its own", id.Name, def.Name)
		}
	}
	sort.Strings(res.gaps)
	return res
}

func callSiteGaps(path string, src string) []string {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, src, 0)
	if err != nil {
		return []string{"typecheck: " + err.Error()}
	}
	info := &types.Info{Defs: map[*ast.Ident]types.Object{}, Uses: map[*ast.Ident]types.Object{}, Selections: map[*ast.SelectorExpr]*types.Selection{}}
	var errs []string
	conf := types.Config{Error: func(err error) { errs = append(errs, err.Error()) }}
	_, _ = conf.Check("p", fset, []*ast.File{file}, info)
	if len(errs) > 0 {
		return []string{"typecheck: " + strings.Join(errs, "; ")}
	}
	return spCheck(fset, []*ast.File{file}, info).gaps
}
