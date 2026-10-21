package affected

import (
	"go/ast"
	"go/token"
	"sort"
	"strings"
)

// Table-driven reads: `for _, tc := range cases { os.ReadFile(tc.path) }` where cases is a composite literal of struct
// literals (inline, a local assigned once, or a package variable assigned once). The kind of tc.path is the join of the
// kinds of the field in every element; any use of the table or of tc that could change a field makes it unbounded.

// tableView is the list of composite literals a range value variable draws its elements from; a nil lit poisons it.
type tableView struct{ lits []*ast.CompositeLit }

func (sc *scope) view(name string) (*tableView, bool) {
	for s := sc; s != nil; s = s.parent {
		if v, ok := s.views[name]; ok {
			return v, true
		}
	}
	return nil, false
}

func (sc *scope) addView(name string, lit *ast.CompositeLit) {
	v, ok := sc.views[name]
	if !ok {
		v = &tableView{}
		if sc.views == nil {
			sc.views = map[string]*tableView{}
		}
		sc.views[name] = v
	}
	if lit == nil {
		v.lits = append(v.lits, nil)
		return
	}
	for _, l := range v.lits {
		if l == lit {
			return
		}
	}
	v.lits = append(v.lits, lit)
}

func (sc *scope) table(name string) (*ast.CompositeLit, bool) {
	for s := sc; s != nil; s = s.parent {
		if l, ok := s.tables[name]; ok {
			return l, true
		}
	}
	return nil, false
}

// structElems are the element literals of a table whose elements are all composite literals; ok is false otherwise.
func structElems(lit *ast.CompositeLit) (elems []*ast.CompositeLit, ok bool) {
	_, isMap := lit.Type.(*ast.MapType)
	_, isSlice := lit.Type.(*ast.ArrayType)
	if (!isMap && !isSlice) || len(lit.Elts) == 0 {
		return nil, false
	}
	for _, el := range lit.Elts {
		if kv, isKV := el.(*ast.KeyValueExpr); isKV {
			if !isMap {
				return nil, false
			}
			el = kv.Value
		}
		cl := compositeOf(el)
		if cl == nil {
			return nil, false
		}
		elems = append(elems, cl)
	}
	return elems, true
}

// structFields are the field names of the struct type of a table's elements, when the table spells it inline.
func structFields(lit *ast.CompositeLit) []string {
	var elem ast.Expr
	switch t := lit.Type.(type) {
	case *ast.ArrayType:
		elem = t.Elt
	case *ast.MapType:
		elem = t.Value
	}
	if p, ok := elem.(*ast.StarExpr); ok {
		elem = p.X
	}
	st, ok := elem.(*ast.StructType)
	if !ok {
		return nil
	}
	var names []string
	for _, f := range st.Fields.List {
		if len(f.Names) == 0 {
			names = append(names, "")
		}
		for _, n := range f.Names {
			names = append(names, n.Name)
		}
	}
	return names
}

// fieldKind is the join of the kinds of field f over every element of the tables of a range value variable.
func (c *fileCtx) fieldKind(v *tableView, f string, sc *scope) pathInfo {
	out := pathInfo{}
	for _, lit := range v.lits {
		if lit == nil {
			return dynInfo
		}
		elems, ok := structElems(lit)
		if !ok {
			return dynInfo
		}
		names := structFields(lit)
		for _, el := range elems {
			val, found, keyed := ast.Expr(nil), false, false
			for i, e := range el.Elts {
				if kv, isKV := e.(*ast.KeyValueExpr); isKV {
					keyed = true
					if id, isID := kv.Key.(*ast.Ident); isID && id.Name == f {
						val, found = kv.Value, true
					}
					continue
				}
				if keyed || i >= len(names) {
					return dynInfo
				}
				if names[i] == f && f != "" {
					val, found = e, true
				}
			}
			if !found {
				if len(el.Elts) > 0 && !keyed && len(el.Elts) != len(names) {
					return dynInfo
				}
				out = join(out, pathInfo{kind: kConst, value: ""})
				continue
			}
			out = join(out, c.pathKind(val, sc))
		}
	}
	if out.kind == kBottom {
		return dynInfo
	}
	return out
}

func rootIdent(e ast.Expr) string {
	for {
		switch x := e.(type) {
		case *ast.Ident:
			return x.Name
		case *ast.SelectorExpr:
			e = x.X
		case *ast.IndexExpr:
			e = x.X
		case *ast.StarExpr:
			e = x.X
		case *ast.ParenExpr:
			e = x.X
		default:
			return ""
		}
	}
}

// definesName reports whether n introduces name as a parameter, a result or a closure parameter.
func paramNamed(body ast.Node, name string) bool {
	found := false
	ast.Inspect(body, func(n ast.Node) bool {
		if ft, ok := n.(*ast.FuncType); ok {
			for _, fl := range []*ast.FieldList{ft.Params, ft.Results} {
				if fl == nil {
					continue
				}
				for _, f := range fl.List {
					for _, id := range f.Names {
						if id.Name == name {
							found = true
						}
					}
				}
			}
		}
		return !found
	})
	return found
}

// immutableTable: name is defined once in body (by the assignment of the literal) and used only as a range operand or
// as the argument of len.
func immutableTable(body ast.Node, name string, def *ast.AssignStmt) bool {
	if paramNamed(body, name) {
		return false
	}
	ok := true
	defs := 0
	allowed := map[*ast.Ident]bool{}
	ast.Inspect(body, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.AssignStmt:
			for _, l := range x.Lhs {
				if id, isID := l.(*ast.Ident); isID && id.Name == name {
					allowed[id] = true
					defs++
					if x != def {
						ok = false
					}
				}
			}
		case *ast.ValueSpec:
			for _, id := range x.Names {
				if id.Name == name {
					ok = false
				}
			}
		case *ast.RangeStmt:
			if id, isID := x.X.(*ast.Ident); isID && id.Name == name {
				allowed[id] = true
			}
			for _, k := range []ast.Expr{x.Key, x.Value} {
				if id, isID := k.(*ast.Ident); isID && id.Name == name {
					ok = false
				}
			}
		case *ast.CallExpr:
			if fn, isID := x.Fun.(*ast.Ident); isID && (fn.Name == "len" || fn.Name == "cap") && len(x.Args) == 1 {
				if id, isID := x.Args[0].(*ast.Ident); isID && id.Name == name {
					allowed[id] = true
				}
			}
		}
		return ok
	})
	if !ok || defs != 1 {
		return false
	}
	ast.Inspect(body, func(n ast.Node) bool {
		if id, isID := n.(*ast.Ident); isID && id.Name == name && !allowed[id] {
			ok = false
		}
		return ok
	})
	return ok
}

// immutableValue: the range value variable name is defined only by range statements (and `name := name`), and no field
// of it is assigned, address-taken, incremented or called.
func immutableValue(body ast.Node, name string) bool {
	if paramNamed(body, name) {
		return false
	}
	ok := true
	ast.Inspect(body, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.AssignStmt:
			self := len(x.Lhs) == len(x.Rhs) && x.Tok == token.DEFINE
			for i, l := range x.Lhs {
				if rootIdent(l) != name {
					continue
				}
				if r, isID := x.Rhs[min(i, len(x.Rhs)-1)].(*ast.Ident); self && isID && r.Name == name {
					if id, isLID := l.(*ast.Ident); isLID && id.Name == name {
						continue
					}
				}
				ok = false
			}
		case *ast.IncDecStmt:
			if rootIdent(x.X) == name {
				ok = false
			}
		case *ast.UnaryExpr:
			if x.Op == token.AND && rootIdent(x.X) == name {
				ok = false
			}
		case *ast.CallExpr:
			if sel, isSel := x.Fun.(*ast.SelectorExpr); isSel && rootIdent(sel.X) == name {
				ok = false
			}
		case *ast.ValueSpec:
			for _, id := range x.Names {
				if id.Name == name {
					ok = false
				}
			}
		case *ast.RangeStmt:
			if x.Tok == token.ASSIGN {
				for _, k := range []ast.Expr{x.Key, x.Value} {
					if k != nil && rootIdent(k) == name {
						ok = false
					}
				}
			}
			if k, isID := x.Key.(*ast.Ident); isID && k.Name == name {
				ok = false
			}
		}
		return ok
	})
	return ok
}

// boundChdirs clears chdirOpen when every directory change of the package goes to a temp directory: a relative read
// after it reads no repository file, and one before it reads below the package directory, where a read of the
// package's own files is already selected.
func (g *dirGroup) boundChdirs() {
	if !g.chdir {
		return
	}
	open := false
	for _, s := range g.files {
		c := &fileCtx{g: g, s: s, pkg: g.pkgOf[s]}
		c.eachBody(nil, func(body ast.Node, sc *scope) {
			ast.Inspect(body, func(n ast.Node) bool {
				if call, ok := n.(*ast.CallExpr); ok && isChdir(c.s.callName(call.Fun)) {
					if len(call.Args) != 1 || c.pathKind(call.Args[0], sc).kind != kTemp {
						open = true
					}
				}
				return true
			})
		})
	}
	g.chdirOpen = open
}

// A set joins constant, repository and temp paths: the values a variable takes from a table. Its value is the sorted
// elements, each a kind letter (c, r, t) and the value, separated by NUL; a set of one element is that element.
const maxSet = 128

func setElems(p pathInfo) ([]string, bool) {
	switch p.kind {
	case kConst:
		return []string{"c" + p.value}, true
	case kRepo:
		if p.depth != "" {
			return nil, false
		}
		return []string{"r" + p.value}, true
	case kTemp:
		return []string{"t"}, true
	case kSet:
		return strings.Split(p.value, "\x00"), true
	}
	return nil, false
}

func mkSet(elems []string) pathInfo {
	sort.Strings(elems)
	out := elems[:0]
	for i, e := range elems {
		if i == 0 || e != elems[i-1] {
			out = append(out, e)
		}
	}
	if len(out) > maxSet {
		return dynInfo
	}
	if len(out) == 1 {
		return fromElem(out[0])
	}
	return pathInfo{kind: kSet, value: strings.Join(out, "\x00")}
}

func fromElem(e string) pathInfo {
	switch e[0] {
	case 'c':
		return pathInfo{kind: kConst, value: e[1:]}
	case 'r':
		return pathInfo{kind: kRepo, value: e[1:]}
	}
	return pathInfo{kind: kTemp}
}

// members are the single paths of a value that setElems accepts.
func members(p pathInfo) []pathInfo {
	elems, ok := setElems(p)
	if !ok {
		return nil
	}
	out := make([]pathInfo, len(elems))
	for i, e := range elems {
		out[i] = fromElem(e)
	}
	return out
}
