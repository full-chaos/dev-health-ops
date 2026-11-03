package affected

import (
	"go/ast"
	"go/token"
)

// The scan parses without type information, so a name is bound by the lexical scopes of the file: a name declared by a
// parameter, a result, a receiver, a type parameter, a := or var, a range or type-switch variable, or a function-local
// type or constant is a local; any other use of a name in a file of the package is the package-level object or, when
// nothing in the package declares it, the universe.

// spreadSafePkgs are the packages whose variadic functions only read their final slice argument.
var spreadSafePkgs = map[string]bool{
	"os/exec": true, "path/filepath": true, "path": true, "fmt": true, "strings": true, "bytes": true, "os": true,
	"errors": true, "slices": true,
}

// localBindings is the set of identifiers of f that resolve to a function-local declaration.
func localBindings(f *ast.File) map[*ast.Ident]bool {
	b := &binder{local: map[*ast.Ident]bool{}}
	for _, d := range f.Decls {
		b.node(d)
	}
	return b.local
}

type binder struct {
	scopes []map[string]bool
	local  map[*ast.Ident]bool
}

func (b *binder) push() { b.scopes = append(b.scopes, map[string]bool{}) }
func (b *binder) pop()  { b.scopes = b.scopes[:len(b.scopes)-1] }

func (b *binder) declare(id *ast.Ident) {
	if id == nil || id.Name == "_" || len(b.scopes) == 0 {
		return
	}
	b.scopes[len(b.scopes)-1][id.Name] = true
	b.local[id] = true
}

func (b *binder) bound(name string) bool {
	for _, s := range b.scopes {
		if s[name] {
			return true
		}
	}
	return false
}

func (b *binder) fields(fl *ast.FieldList) {
	if fl == nil {
		return
	}
	for _, f := range fl.List {
		b.node(f.Type)
	}
	for _, f := range fl.List {
		for _, id := range f.Names {
			b.declare(id)
		}
	}
}

func (b *binder) stmts(list []ast.Stmt) {
	for _, st := range list {
		b.node(st)
	}
}

func (b *binder) funcType(recv *ast.FieldList, ft *ast.FuncType, body *ast.BlockStmt) {
	b.push()
	b.fields(recv)
	b.fields(ft.TypeParams)
	b.fields(ft.Params)
	b.fields(ft.Results)
	if body != nil {
		b.node(body)
	}
	b.pop()
}

func (b *binder) node(n ast.Node) {
	if n == nil {
		return
	}
	ast.Inspect(n, func(m ast.Node) bool {
		switch x := m.(type) {
		case nil:
			return false
		case *ast.Ident:
			if b.bound(x.Name) {
				b.local[x] = true
			}
		case *ast.FuncDecl:
			b.funcType(x.Recv, x.Type, x.Body)
			return false
		case *ast.FuncLit:
			b.funcType(nil, x.Type, x.Body)
			return false
		case *ast.BlockStmt:
			b.push()
			b.stmts(x.List)
			b.pop()
			return false
		case *ast.CaseClause:
			b.push()
			for _, e := range x.List {
				b.node(e)
			}
			b.stmts(x.Body)
			b.pop()
			return false
		case *ast.CommClause:
			b.push()
			b.node(x.Comm)
			b.stmts(x.Body)
			b.pop()
			return false
		case *ast.IfStmt:
			b.push()
			b.node(x.Init)
			b.node(x.Cond)
			b.node(x.Body)
			b.node(x.Else)
			b.pop()
			return false
		case *ast.ForStmt:
			b.push()
			b.node(x.Init)
			b.node(x.Cond)
			b.node(x.Post)
			b.node(x.Body)
			b.pop()
			return false
		case *ast.RangeStmt:
			b.push()
			b.node(x.X)
			if x.Tok == token.DEFINE {
				for _, e := range []ast.Expr{x.Key, x.Value} {
					if id, ok := e.(*ast.Ident); ok {
						b.declare(id)
					}
				}
			} else {
				b.node(x.Key)
				b.node(x.Value)
			}
			b.node(x.Body)
			b.pop()
			return false
		case *ast.SwitchStmt:
			b.push()
			b.node(x.Init)
			b.node(x.Tag)
			b.node(x.Body)
			b.pop()
			return false
		case *ast.TypeSwitchStmt:
			b.push()
			b.node(x.Init)
			var bound *ast.Ident
			switch a := x.Assign.(type) {
			case *ast.AssignStmt:
				for _, r := range a.Rhs {
					b.node(r)
				}
				if len(a.Lhs) == 1 {
					bound, _ = a.Lhs[0].(*ast.Ident)
				}
			default:
				b.node(a)
			}
			for _, c := range x.Body.List {
				cc := c.(*ast.CaseClause)
				b.push()
				for _, e := range cc.List {
					b.node(e)
				}
				b.declare(bound)
				b.stmts(cc.Body)
				b.pop()
			}
			b.pop()
			return false
		case *ast.AssignStmt:
			for _, r := range x.Rhs {
				b.node(r)
			}
			if x.Tok == token.DEFINE {
				for _, l := range x.Lhs {
					if id, ok := l.(*ast.Ident); ok {
						b.declare(id)
					} else {
						b.node(l)
					}
				}
			} else {
				for _, l := range x.Lhs {
					b.node(l)
				}
			}
			return false
		case *ast.ValueSpec:
			b.node(x.Type)
			for _, v := range x.Values {
				b.node(v)
			}
			for _, id := range x.Names {
				b.declare(id)
			}
			return false
		case *ast.TypeSpec:
			b.declare(x.Name)
			b.fields(x.TypeParams)
			b.node(x.Type)
			return false
		}
		return true
	})
}

// sliceTouched reports whether a file of package pkg uses the package variable name other than as its declaration, a
// range operand, the argument of len or cap, or the last argument of a spread into a function of spreadSafePkgs or
// into append. Each occurrence is bound first: a selector's field, a composite literal key, a field or parameter name,
// a function-local declaration and a bare name in another package (an external test package cannot name an unexported
// variable) are other objects and never count. len, cap, append and an import alias are trusted only when no scope of
// the file or the package declares the name. A binding the scan cannot establish counts as a use: the spread stays
// unresolved, which the md-bounded plan answers by not adding a Markdown row for it (pm D6610).
func (g *dirGroup) sliceTouched(pkg, name string) bool {
	key := pkg + "." + name
	if v, ok := g.touched[key]; ok {
		return v
	}
	pkgNames := map[string]bool{}
	for _, s := range g.files {
		if g.pkgOf[s] != pkg {
			continue
		}
		for _, d := range s.file.Decls {
			switch d := d.(type) {
			case *ast.FuncDecl:
				if d.Recv == nil {
					pkgNames[d.Name.Name] = true
				}
			case *ast.GenDecl:
				for _, sp := range d.Specs {
					switch sp := sp.(type) {
					case *ast.ValueSpec:
						for _, id := range sp.Names {
							pkgNames[id.Name] = true
						}
					case *ast.TypeSpec:
						pkgNames[sp.Name.Name] = true
					}
				}
			}
		}
	}
	touched := false
	for _, s := range g.files {
		if g.pkgOf[s] != pkg || touched {
			continue
		}
		local := localBindings(s.file)
		other := map[*ast.Ident]bool{} // identifiers that are not a use of the variable
		ast.Inspect(s.file, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.SelectorExpr:
				other[x.Sel] = true
			case *ast.KeyValueExpr:
				if id, ok := x.Key.(*ast.Ident); ok {
					other[id] = true
				}
			case *ast.Field:
				for _, id := range x.Names {
					other[id] = true
				}
			case *ast.ValueSpec:
				for _, id := range x.Names {
					other[id] = true
				}
			case *ast.FuncDecl:
				other[x.Name] = true
			case *ast.TypeSpec:
				other[x.Name] = true
			case *ast.LabeledStmt:
				other[x.Label] = true
			case *ast.BranchStmt:
				if x.Label != nil {
					other[x.Label] = true
				}
			case *ast.RangeStmt:
				if id, ok := x.X.(*ast.Ident); ok {
					other[id] = true
				}
			case *ast.CallExpr:
				if len(x.Args) == 0 {
					break
				}
				last, isID := x.Args[len(x.Args)-1].(*ast.Ident)
				if !isID {
					break
				}
				switch f := ast.Unparen(x.Fun).(type) {
				case *ast.Ident:
					if local[f] || pkgNames[f.Name] {
						break
					}
					if (f.Name == "len" || f.Name == "cap") && len(x.Args) == 1 {
						other[last] = true
					}
					if f.Name == "append" && x.Ellipsis.IsValid() {
						other[last] = true
					}
				case *ast.SelectorExpr:
					if q, ok := f.X.(*ast.Ident); ok && x.Ellipsis.IsValid() && !local[q] && !pkgNames[q.Name] {
						if path, imp := s.imports[q.Name]; imp && spreadSafePkgs[path] {
							other[last] = true
						}
					}
				}
			}
			return true
		})
		ast.Inspect(s.file, func(n ast.Node) bool {
			if id, ok := n.(*ast.Ident); ok && id.Name == name && !local[id] && !other[id] {
				touched = true
			}
			return !touched
		})
	}
	if g.touched == nil {
		g.touched = map[string]bool{}
	}
	g.touched[key] = touched
	return touched
}
