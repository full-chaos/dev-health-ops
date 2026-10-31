package affected

import (
	"go/ast"
	"go/token"
	"strings"
)

// The value flow of a bare root name (scan.go): a literal such as "README.md" in a table, a struct field or a call
// argument names a file only when its value can reach a call that reads one. flowGroup follows the value inside one
// directory of Go files (a package and its external tests): through assignments, variables and constants, range
// variables, struct fields by name, slices and maps, append and conversions, string concatenation, the parameters and
// results of the directory's own functions and methods, and the results of standard-library functions. Anything it
// cannot follow (a function of another package of the module or of a third party, a method with no declaration in the
// directory, a function value, a goroutine, a channel) counts as a read: the literal stays a read of the root file.

type flowFrame struct {
	decl *ast.FuncDecl
	lit  *ast.FuncLit // a closure called through the local variable it is assigned to
	call *ast.CallExpr
}

type flowGroup struct {
	// execInTemp reports whether an exec.Command call of a file runs in a temp directory (set by the scan)
	execInTemp func(*srcFile, *ast.CallExpr) bool
	// modulePure reports whether a function of a package of the module only computes (pureleaf.go)
	modulePure func(imp, name string) bool
	files      []*srcFile
	parent     map[ast.Node]ast.Node
	fileOf     map[*ast.File]*srcFile
	funcs      map[string][]*ast.FuncDecl // name -> functions and methods of that name
	callers    map[string][]*ast.CallExpr // function or method name -> the calls that name it
	types      map[string]*ast.TypeSpec   // the types the directory declares once, at package level, not as an alias
}

func newFlowGroup(files []*srcFile) *flowGroup {
	g := &flowGroup{files: files, parent: map[ast.Node]ast.Node{}, fileOf: map[*ast.File]*srcFile{},
		funcs: map[string][]*ast.FuncDecl{}, callers: map[string][]*ast.CallExpr{}, types: map[string]*ast.TypeSpec{}}
	typeDecls := map[string][]*ast.TypeSpec{}
	for _, s := range files {
		g.fileOf[s.file] = s
		var stack []ast.Node
		ast.Inspect(s.file, func(n ast.Node) bool {
			if n == nil {
				stack = stack[:len(stack)-1]
				return true
			}
			if len(stack) > 0 {
				g.parent[n] = stack[len(stack)-1]
			}
			stack = append(stack, n)
			switch x := n.(type) {
			case *ast.FuncDecl:
				g.funcs[x.Name.Name] = append(g.funcs[x.Name.Name], x)
			case *ast.TypeSpec:
				typeDecls[x.Name.Name] = append(typeDecls[x.Name.Name], x)
			case *ast.CallExpr:
				switch f := ast.Unparen(x.Fun).(type) {
				case *ast.Ident:
					g.callers[f.Name] = append(g.callers[f.Name], x)
				case *ast.SelectorExpr:
					g.callers[f.Sel.Name] = append(g.callers[f.Sel.Name], x)
				}
			}
			return true
		})
	}
	// A type name declared twice (the package and its external tests, or a function-local type) or as an alias does not
	// say which type a value has.
	for name, specs := range typeDecls {
		if len(specs) != 1 || specs[0].Assign.IsValid() {
			continue
		}
		if decl, ok := g.parent[specs[0]].(*ast.GenDecl); ok {
			if _, top := g.parent[decl].(*ast.File); top {
				g.types[name] = specs[0]
			}
		}
	}
	return g
}

// flowReaders: standard-library calls that read, walk, run or load what an argument names, or hand it to one that does.
var flowReaders = map[string]bool{"os/exec.Command": true, "os/exec.CommandContext": true, "os.Setenv": true, "os.Chdir": true,
	"os.StartProcess": true, "syscall.Exec": true, "syscall.ForkExec": true, "plugin.Open": true}

// flowSinks: standard-library calls that write, create or remove what an argument names, or write its bytes: they read
// no repository file, and their results (an error, a count, a file opened for writing) do not hold the value.
var flowSinks = map[string]bool{"os.WriteFile": true, "os.MkdirAll": true, "os.Mkdir": true, "os.Remove": true,
	"os.RemoveAll": true, "os.Rename": true, "os.Symlink": true, "os.Chmod": true, "os.Chtimes": true, "os.Create": true,
	"os.CreateTemp": true, "os.MkdirTemp": true,
	// an error's text is reported, never opened
	"fmt.Errorf": true, "errors.New": true}

// flowWriters: standard-library calls that write an argument's value into their first argument.
var flowWriters = map[string]bool{"fmt.Fprint": true, "fmt.Fprintf": true, "fmt.Fprintln": true, "io.WriteString": true,
	"io.Copy": true, "io.CopyN": true, "io.CopyBuffer": true, "encoding/binary.Write": true}

var flowReaderPrefixes = []string{"io/fs.", "io/ioutil.", "go/build.", "go/importer.", "debug/", "embed."}

// testingQuiet are the methods of a testing.T, B or F that only report or name a subtest: a value handed to them is read
// by no program.
var testingQuiet = map[string]bool{"Errorf": true, "Fatalf": true, "Logf": true, "Log": true, "Error": true, "Fatal": true,
	"Skip": true, "Skipf": true, "Run": true, "Helper": true}

var numericTypes = map[string]bool{"int": true, "int8": true, "int16": true, "int32": true, "int64": true, "uint": true, "uint8": true,
	"uint16": true, "uint32": true, "uint64": true, "uintptr": true, "float32": true, "float64": true, "bool": true}

var quietBuiltins = map[string]bool{"len": true, "cap": true, "delete": true, "print": true, "println": true, "panic": true,
	"new": true, "make": true, "clear": true, "complex": true, "real": true, "imag": true, "close": true}

const flowBudget = 20000

type flowRun struct {
	g *flowGroup
	// composed: the value went through string composition (+, Join, a library function), so it may name another directory
	composed bool
	steps    int
	seen     map[flowKey]bool
}

// flowKey is one visit: a node (or a variable's scope and name) with the field, map-key flag and calling frame it was
// reached under. Nodes are compared by identity: each file of a directory has its own token.FileSet.
type flowKey struct {
	n      ast.Node
	name   string
	field  string
	mapKey bool
	top    *ast.CallExpr
}

// shadowed reports whether a call's package qualifier (strings in strings.HasSuffix) is also a local or package-level
// name of the directory, so the call may not be the package's function.
func (g *flowGroup) shadowed(call *ast.CallExpr) bool {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	x, ok := sel.X.(*ast.Ident)
	if !ok {
		return true
	}
	r := &flowRun{g: g}
	return r.localDecls(x.Name, call) > 0 || r.pkgVar(x.Name)
}

// reaches reports whether the value of the literal can reach a read (true when the flow cannot be followed).
func (g *flowGroup) reaches(lit *ast.BasicLit) bool {
	r := &flowRun{g: g, seen: map[flowKey]bool{}}
	return r.expr(lit, "", false, nil, 0)
}

func (r *flowRun) file(n ast.Node) *srcFile {
	for ; n != nil; n = r.g.parent[n] {
		if f, ok := n.(*ast.File); ok {
			return r.g.fileOf[f]
		}
	}
	return nil
}

func (r *flowRun) key(n ast.Node, name, field string, mapKey bool, ret []flowFrame) flowKey {
	k := flowKey{n: n, name: name, field: field, mapKey: mapKey}
	if len(ret) > 0 {
		k.top = ret[len(ret)-1].call
	}
	return k
}

// expr follows the value of e: field is the struct field that holds the literal ("" = anywhere in the value), mapKey
// says the literal is a map key, ret is the stack of calls into the directory's own functions, depth counts the
// function boundaries crossed.
func (r *flowRun) expr(e ast.Node, field string, mapKey bool, ret []flowFrame, depth int) bool {
	r.steps++
	if r.steps > flowBudget || depth > 8 {
		return true
	}
	k := r.key(e, "", field, mapKey, ret)
	if r.seen[k] {
		return false
	}
	r.seen[k] = true
	switch p := r.g.parent[e].(type) {
	case *ast.ParenExpr, *ast.StarExpr, *ast.TypeAssertExpr:
		return r.expr(p, field, mapKey, ret, depth)
	case *ast.UnaryExpr:
		switch p.Op {
		case token.AND:
			return r.expr(p, field, mapKey, ret, depth)
		case token.ARROW:
			return true
		}
		return false
	case *ast.SelectorExpr:
		if call, ok := r.g.parent[p].(*ast.CallExpr); ok && call.Fun == p {
			return r.method(call, p, -1, field, mapKey, ret, depth)
		}
		if field != "" && p.Sel.Name != field {
			return false
		}
		return r.expr(p, "", mapKey, ret, depth)
	case *ast.IndexExpr:
		if p.X == e {
			return r.expr(p, field, mapKey, ret, depth)
		}
		return false
	case *ast.IndexListExpr:
		return true
	case *ast.SliceExpr:
		if p.X == e {
			return r.expr(p, field, mapKey, ret, depth)
		}
		return false
	case *ast.KeyValueExpr:
		comp, ok := r.g.parent[p].(*ast.CompositeLit)
		if !ok {
			return true
		}
		if p.Key == e {
			return r.expr(comp, "", true, ret, depth)
		}
		f := ""
		if id, ok := p.Key.(*ast.Ident); ok && field == "" {
			if _, isMap := comp.Type.(*ast.MapType); !isMap {
				f = id.Name
			}
		}
		return r.expr(comp, f, mapKey, ret, depth)
	case *ast.CompositeLit:
		switch p.Type.(type) {
		case *ast.ArrayType, *ast.MapType:
			return r.expr(p, field, mapKey, ret, depth)
		}
		return r.expr(p, "", mapKey, ret, depth)
	case *ast.BinaryExpr:
		if p.Op == token.ADD {
			r.composed = true
			return r.expr(p, "", false, ret, depth)
		}
		return false
	case *ast.CallExpr:
		if p.Fun == e {
			return true
		}
		for i, a := range p.Args {
			if a == e {
				return r.call(p, i, field, mapKey, ret, depth)
			}
		}
		return true
	case *ast.AssignStmt:
		for j, rh := range p.Rhs {
			if rh != e {
				continue
			}
			if len(p.Lhs) != len(p.Rhs) {
				// one call, several results: any of them may hold the value
				for _, l := range p.Lhs {
					if r.target(l, p, "", false, ret, depth) {
						return true
					}
				}
				return false
			}
			return r.target(p.Lhs[j], p, field, mapKey, ret, depth)
		}
		return false // the value is written to, not read
	case *ast.ValueSpec:
		for j, v := range p.Values {
			if v == e && j < len(p.Names) {
				return r.variable(p.Names[j].Name, p, field, mapKey, ret, depth)
			}
		}
		return true
	case *ast.RangeStmt:
		if p.X != e {
			return false
		}
		if id, ok := p.Value.(*ast.Ident); ok && !mapKey && id.Name != "_" {
			if r.variable(id.Name, p, field, false, ret, depth) {
				return true
			}
		}
		if id, ok := p.Key.(*ast.Ident); ok && mapKey && id.Name != "_" {
			return r.variable(id.Name, p, "", false, ret, depth)
		}
		return false
	case *ast.ReturnStmt:
		return r.returned(p, field, mapKey, ret, depth)
	case *ast.ExprStmt, *ast.IfStmt, *ast.ForStmt, *ast.SwitchStmt, *ast.CaseClause, *ast.IncDecStmt, *ast.Field:
		return false
	}
	return true
}

// call follows the value into argument i of a call.
func (r *flowRun) call(c *ast.CallExpr, i int, field string, mapKey bool, ret []flowFrame, depth int) bool {
	switch f := ast.Unparen(c.Fun).(type) {
	case *ast.ArrayType:
		return r.expr(c, field, mapKey, ret, depth) // a conversion
	case *ast.Ident:
		// A local name (a closure variable, a parameter) or a package variable shadows a builtin and a function of the
		// same name: only a closure the name is declared once to hold is followed.
		if nd := r.localDecls(f.Name, c); nd > 0 || r.pkgVar(f.Name) {
			if lit := r.closure(f); nd == 1 && lit != nil && len(ret) < 6 {
				return r.intoLit(lit, c, i, field, mapKey, ret, depth)
			}
			return true
		}
		switch {
		case f.Name == "append":
			return r.expr(c, field, mapKey, ret, depth)
		case f.Name == "string" || f.Name == "byte" || f.Name == "rune" || numericTypes[f.Name]:
			return r.expr(c, field, mapKey, ret, depth)
		case f.Name == "min" || f.Name == "max":
			return r.expr(c, field, mapKey, ret, depth) // the result is one of the arguments
		case f.Name == "copy":
			return true
		case quietBuiltins[f.Name]:
			return false
		}
		if s := r.file(c); s == nil || dotImports(s.file) {
			return true
		}
		var decls []*ast.FuncDecl
		for _, d := range r.g.funcs[f.Name] {
			if d.Recv == nil {
				decls = append(decls, d)
			}
		}
		if len(decls) == 0 {
			return true // a function value, a dot import or a type of another package
		}
		return r.into(decls, c, i, field, mapKey, ret, depth)
	case *ast.SelectorExpr:
		if x, ok := f.X.(*ast.Ident); ok {
			if s := r.file(c); s != nil {
				if imp, ok := s.imports[x.Name]; ok && r.localDecls(x.Name, c) == 0 && !r.pkgVar(x.Name) {
					return r.pkgCall(imp, f.Sel.Name, c, i, field, mapKey, ret, depth)
				}
			}
		}
		return r.method(c, f, i, field, mapKey, ret, depth)
	}
	return true
}

// pkgCall follows the value into a function of an imported package.
func (r *flowRun) pkgCall(imp, name string, c *ast.CallExpr, i int, field string, mapKey bool, ret []flowFrame, depth int) bool {
	full := imp + "." + name
	if strings.Contains(strings.SplitN(imp, "/", 2)[0], ".") {
		if r.g.modulePure != nil && r.g.modulePure(imp, name) {
			r.composed = true
			return r.expr(c, "", false, ret, depth) // the value lives on only in its results
		}
		return true // another package of the module, or a third party: not followed
	}
	// A writer call carries the value into its first argument, the writer: follow the writer variable too.
	if flowWriters[full] && i > 0 && len(c.Args) > 0 {
		w := ast.Expr(c.Args[0])
		for {
			switch x := w.(type) {
			case *ast.UnaryExpr:
				w = x.X
				continue
			case *ast.ParenExpr:
				w = x.X
				continue
			case *ast.SelectorExpr:
				w = x.X
				continue
			case *ast.IndexExpr:
				w = x.X
				continue
			case *ast.StarExpr:
				w = x.X
				continue
			}
			break
		}
		id, ok := w.(*ast.Ident)
		if !ok || r.variable(id.Name, c, "", false, ret, depth) {
			return true
		}
	}
	_, read := readArg[full]
	_, walk := walkArg[full]
	if (full == "os/exec.Command" || full == "os/exec.CommandContext") && !r.composed && r.g.execInTemp != nil {
		if s := r.file(c); s != nil && r.g.execInTemp(s, c) {
			return false // the arguments name files of a temp directory
		}
	}
	if read || walk || flowReaders[full] || strings.HasSuffix(name, "ParseFiles") || strings.HasSuffix(name, "ParseGlob") {
		return true
	}
	for _, p := range flowReaderPrefixes {
		if strings.HasPrefix(full, p) {
			return true
		}
	}
	switch full {
	case "path/filepath.Join", "path.Join":
		r.composed = true
		return r.expr(c, "", false, ret, depth)
	}
	if nameTests[full] || flowSinks[full] {
		return false
	}
	// any other standard-library function: the value lives on only in its results
	r.composed = true
	return r.expr(c, "", false, ret, depth)
}

// method follows the value into a method call: as argument i, or as the receiver (i < 0).
func (r *flowRun) method(c *ast.CallExpr, sel *ast.SelectorExpr, i int, field string, mapKey bool, ret []flowFrame, depth int) bool {
	// Only a receiver whose type is a type of this directory resolves to that type's method: a method of the same name
	// on a type of another package must not stand in for it.
	var decls []*ast.FuncDecl
	if typ := r.typeName(r.typeOf(sel.X, 0)); typ != "" {
		for _, d := range r.g.funcs[sel.Sel.Name] {
			if d.Recv != nil && len(d.Recv.List) == 1 && r.typeName(d.Recv.List[0].Type) == typ {
				decls = append(decls, d)
			}
		}
	}
	if len(decls) == 0 {
		recv := ""
		switch x := sel.X.(type) {
		case *ast.Ident:
			recv = x.Name
		case *ast.SelectorExpr:
			recv = x.Sel.Name
		}
		if i >= 0 && testingQuiet[sel.Sel.Name] && (recv == "t" || recv == "b" || recv == "tb" || recv == "f") {
			return false
		}
		return true
	}
	return r.into(decls, c, i, field, mapKey, ret, depth)
}

// into follows the value into the parameter for argument i (the receiver for i < 0) of each declaration.
func (r *flowRun) into(decls []*ast.FuncDecl, c *ast.CallExpr, i int, field string, mapKey bool, ret []flowFrame, depth int) bool {
	if len(ret) >= 6 {
		return true
	}
	for _, d := range decls {
		if d.Body == nil {
			return true
		}
		var name string
		if i < 0 {
			if d.Recv == nil || len(d.Recv.List) == 0 || len(d.Recv.List[0].Names) == 0 {
				continue // an unnamed receiver: the method never reads it
			}
			name = d.Recv.List[0].Names[0].Name
		} else {
			var names []string
			for _, f := range d.Type.Params.List {
				if len(f.Names) == 0 {
					names = append(names, "_") // unnamed parameters: the body cannot read them
				}
				for _, n := range f.Names {
					names = append(names, n.Name)
				}
			}
			if len(names) == 0 {
				continue
			}
			idx := min(i, len(names)-1) // a variadic parameter takes the trailing arguments
			name = names[idx]
		}
		if name == "_" {
			continue
		}
		if r.variable(name, d.Body, field, mapKey, append(append([]flowFrame{}, ret...), flowFrame{decl: d, call: c}), depth+1) {
			return true
		}
	}
	return false
}

// closure is the function literal a call through id runs: id is a local variable defined once, as `id := func...`, in
// the function that holds the call, and assigned nowhere else there. nil otherwise.
func (r *flowRun) closure(id *ast.Ident) *ast.FuncLit {
	var fn *ast.FuncDecl
	for n := r.g.parent[id]; n != nil; n = r.g.parent[n] {
		if d, ok := n.(*ast.FuncDecl); ok {
			fn = d
			break
		}
	}
	if fn == nil || fn.Body == nil || countAssigns(fn.Body, id.Name) != 1 {
		return nil
	}
	var lit *ast.FuncLit
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		if a, ok := n.(*ast.AssignStmt); ok && a.Tok == token.DEFINE && len(a.Lhs) == 1 && len(a.Rhs) == 1 {
			if l, ok := a.Lhs[0].(*ast.Ident); ok && l.Name == id.Name {
				lit, _ = a.Rhs[0].(*ast.FuncLit)
			}
		}
		return true
	})
	return lit
}

// intoLit follows the value into the parameter for argument i of a closure.
func (r *flowRun) intoLit(lit *ast.FuncLit, c *ast.CallExpr, i int, field string, mapKey bool, ret []flowFrame, depth int) bool {
	var names []string
	for _, f := range lit.Type.Params.List {
		if len(f.Names) == 0 {
			names = append(names, "_")
		}
		for _, n := range f.Names {
			names = append(names, n.Name)
		}
	}
	if len(names) == 0 {
		return false
	}
	name := names[min(i, len(names)-1)]
	if name == "_" {
		return false
	}
	return r.variable(name, lit.Body, field, mapKey, append(append([]flowFrame{}, ret...), flowFrame{lit: lit, call: c}), depth+1)
}

// returned follows a returned value to the call that receives it.
func (r *flowRun) returned(st *ast.ReturnStmt, field string, mapKey bool, ret []flowFrame, depth int) bool {
	var decl *ast.FuncDecl
	for n := r.g.parent[st]; n != nil; n = r.g.parent[n] {
		if lit, ok := n.(*ast.FuncLit); ok {
			if len(ret) > 0 && ret[len(ret)-1].lit == lit {
				return r.expr(ret[len(ret)-1].call, field, mapKey, ret[:len(ret)-1], depth+1)
			}
			return true
		}
		if d, ok := n.(*ast.FuncDecl); ok {
			decl = d
			break
		}
	}
	if decl == nil {
		return true
	}
	if len(ret) > 0 && ret[len(ret)-1].decl == decl {
		return r.expr(ret[len(ret)-1].call, field, mapKey, ret[:len(ret)-1], depth+1)
	}
	calls := r.g.callers[decl.Name.Name]
	if len(calls) == 0 {
		return ast.IsExported(decl.Name.Name) // a result only another package can receive
	}
	for _, c := range calls {
		if r.expr(c, field, mapKey, nil, depth+1) {
			return true
		}
	}
	return false
}

// target follows the value into the left side of an assignment.
func (r *flowRun) target(lhs ast.Expr, st *ast.AssignStmt, field string, mapKey bool, ret []flowFrame, depth int) bool {
	switch x := lhs.(type) {
	case *ast.Ident:
		if x.Name == "_" {
			return false
		}
		return r.variable(x.Name, st, field, mapKey, ret, depth)
	}
	base := lhs
	for {
		switch x := base.(type) {
		case *ast.SelectorExpr:
			base = x.X
			continue
		case *ast.IndexExpr:
			base = x.X
			continue
		case *ast.StarExpr:
			base = x.X
			continue
		case *ast.ParenExpr:
			base = x.X
			continue
		}
		break
	}
	if id, ok := base.(*ast.Ident); ok {
		return r.variable(id.Name, st, "", false, ret, depth)
	}
	return true
}

// variable follows every use of name in its scope: the function that holds at (a local, a parameter or a range
// variable), or the whole directory (a package-level variable or constant).
func (r *flowRun) variable(name string, at ast.Node, field string, mapKey bool, ret []flowFrame, depth int) bool {
	var scopes []ast.Node
	if b, ok := at.(*ast.BlockStmt); ok {
		scopes = []ast.Node{b} // a function body entered through a parameter
	} else {
		for n := at; n != nil; n = r.g.parent[n] {
			if d, ok := n.(*ast.FuncDecl); ok {
				scopes = []ast.Node{d}
				break
			}
		}
		if scopes == nil {
			for _, s := range r.g.files {
				scopes = append(scopes, s.file)
			}
		}
	}
	k := r.key(scopes[0], name, field, mapKey, ret)
	if r.seen[k] {
		return false
	}
	r.seen[k] = true
	found := false
	for _, sc := range scopes {
		ast.Inspect(sc, func(n ast.Node) bool {
			if found {
				return false
			}
			id, ok := n.(*ast.Ident)
			if !ok || id.Name != name || !r.use(id) {
				return true
			}
			if r.expr(id, field, mapKey, ret, depth) {
				found = true
			}
			return !found
		})
	}
	return found
}

// use reports whether an identifier reads a variable: not a field or method name, a struct key, a declaration or the
// left side of an assignment.
func (r *flowRun) use(id *ast.Ident) bool {
	switch p := r.g.parent[id].(type) {
	case *ast.SelectorExpr:
		return p.X == id
	case *ast.KeyValueExpr:
		return p.Value == id
	case *ast.AssignStmt:
		for _, l := range p.Lhs {
			if l == id {
				return false
			}
		}
	case *ast.ValueSpec:
		for _, n := range p.Names {
			if n == id {
				return false
			}
		}
	case *ast.RangeStmt:
		return p.X == id
	case *ast.Field, *ast.FuncDecl, *ast.TypeSpec, *ast.LabeledStmt, *ast.BranchStmt, *ast.ImportSpec:
		return false
	}
	return true
}

func dotImports(f *ast.File) bool {
	for _, im := range f.Imports {
		if im.Name != nil && im.Name.Name == "." {
			return true
		}
	}
	return false
}

// typeName is the directory's own type a type expression names (T or *T), "" for any other type or none.
func (r *flowRun) typeName(t ast.Expr) string {
	switch x := t.(type) {
	case *ast.StarExpr:
		return r.typeName(x.X)
	case *ast.ParenExpr:
		return r.typeName(x.X)
	case *ast.IndexExpr:
		return r.typeName(x.X)
	case *ast.Ident:
		if _, ok := r.g.types[x.Name]; ok {
			return x.Name
		}
	}
	return ""
}

// elem is the element type of a slice, array, map or variadic type (also through a type of the directory); nil when
// it is not known.
func (r *flowRun) elem(t ast.Expr) ast.Expr {
	switch x := t.(type) {
	case *ast.ArrayType:
		return x.Elt
	case *ast.MapType:
		return x.Value
	case *ast.Ellipsis:
		return x.Elt
	case *ast.Ident:
		if s, ok := r.g.types[x.Name]; ok {
			return r.elem(s.Type)
		}
	}
	return nil
}

// typeOf is the type of an expression as far as its syntax says: a composite literal, &, new, a conversion to a type of
// the directory, a call of a function of the directory with one result, an element of a typed slice or map, or a
// variable whose every declaration in its scope says the same type. nil when it is not known.
func (r *flowRun) typeOf(e ast.Expr, n int) ast.Expr {
	if n > 8 {
		return nil
	}
	switch x := e.(type) {
	case *ast.ParenExpr:
		return r.typeOf(x.X, n+1)
	case *ast.CompositeLit:
		return x.Type
	case *ast.UnaryExpr:
		if x.Op == token.AND {
			return r.typeOf(x.X, n+1)
		}
	case *ast.StarExpr:
		return r.typeOf(x.X, n+1)
	case *ast.IndexExpr:
		return r.elem(r.typeOf(x.X, n+1))
	case *ast.CallExpr:
		f, ok := ast.Unparen(x.Fun).(*ast.Ident)
		if !ok {
			return nil
		}
		if f.Name == "new" && len(x.Args) == 1 {
			return x.Args[0]
		}
		if _, ok := r.g.types[f.Name]; ok {
			return f
		}
		var result ast.Expr
		for _, d := range r.g.funcs[f.Name] {
			if d.Recv != nil || d.Type.Results == nil || len(d.Type.Results.List) != 1 || len(d.Type.Results.List[0].Names) > 1 {
				return nil
			}
			if result != nil && r.typeName(result) != r.typeName(d.Type.Results.List[0].Type) {
				return nil
			}
			result = d.Type.Results.List[0].Type
		}
		return result
	case *ast.Ident:
		return r.identType(x, n)
	}
	return nil
}

// resultType is the type of result j of `a, b := f(...)` where f is the directory's only function of that name; nil
// otherwise.
func (r *flowRun) resultType(rhs []ast.Expr, j int) ast.Expr {
	if len(rhs) != 1 {
		return nil
	}
	call, ok := rhs[0].(*ast.CallExpr)
	if !ok {
		return nil
	}
	f, ok := ast.Unparen(call.Fun).(*ast.Ident)
	if !ok || len(r.g.funcs[f.Name]) != 1 {
		return nil
	}
	d := r.g.funcs[f.Name][0]
	if d.Recv != nil || d.Type.Results == nil {
		return nil
	}
	var results []ast.Expr
	for _, fl := range d.Type.Results.List {
		for range max(1, len(fl.Names)) {
			results = append(results, fl.Type)
		}
	}
	if j >= len(results) {
		return nil
	}
	return results[j]
}

// identType is the type every declaration of the name in its scope gives it; nil when one is unknown or they differ.
func (r *flowRun) identType(id *ast.Ident, n int) ast.Expr {
	var scope []ast.Node
	for p := r.g.parent[id]; p != nil; p = r.g.parent[p] {
		if d, ok := p.(*ast.FuncDecl); ok {
			scope = []ast.Node{d}
			break
		}
	}
	local := scope != nil
	if !local {
		for _, s := range r.g.files {
			scope = append(scope, s.file)
		}
	}
	var types []ast.Expr
	unknown := false
	add := func(t ast.Expr) {
		if t == nil {
			unknown = true
		}
		types = append(types, t)
	}
	for _, sc := range scope {
		ast.Inspect(sc, func(m ast.Node) bool {
			switch x := m.(type) {
			case *ast.Field:
				for _, nm := range x.Names {
					if nm.Name == id.Name {
						add(x.Type)
					}
				}
			case *ast.AssignStmt:
				if x.Tok != token.DEFINE {
					return true
				}
				for j, l := range x.Lhs {
					if lid, ok := l.(*ast.Ident); ok && lid.Name == id.Name {
						if len(x.Lhs) == len(x.Rhs) {
							if rid, ok := x.Rhs[j].(*ast.Ident); ok && rid.Name == id.Name {
								continue // `row := row` keeps the type of the outer row
							}
						}
						if len(x.Lhs) != len(x.Rhs) {
							add(r.resultType(x.Rhs, j))
						} else {
							add(r.typeOf(x.Rhs[j], n+1))
						}
					}
				}
			case *ast.ValueSpec:
				for j, nm := range x.Names {
					if nm.Name != id.Name {
						continue
					}
					switch {
					case x.Type != nil:
						add(x.Type)
					case j < len(x.Values) && len(x.Values) == len(x.Names):
						add(r.typeOf(x.Values[j], n+1))
					default:
						add(nil)
					}
				}
			case *ast.RangeStmt:
				if v, ok := x.Value.(*ast.Ident); ok && v.Name == id.Name {
					add(r.elem(r.typeOf(x.X, n+1)))
				}
				if k, ok := x.Key.(*ast.Ident); ok && k.Name == id.Name {
					add(nil)
				}
			}
			return true
		})
	}
	// Exactly one declaration, and in a function no package-level name it could also mean: blocks are not modelled, so
	// two declarations (a sibling block, a parameter and a local) leave the type unknown.
	if unknown || len(types) != 1 || (local && r.pkgDeclared(id.Name)) {
		return nil
	}
	return types[0]
}

// pkgDeclared reports whether the directory declares name at package level: a variable, a constant or a function.
func (r *flowRun) pkgDeclared(name string) bool {
	for _, s := range r.g.files {
		for _, d := range s.file.Decls {
			switch x := d.(type) {
			case *ast.FuncDecl:
				if x.Recv == nil && x.Name.Name == name {
					return true
				}
			case *ast.GenDecl:
				for _, sp := range x.Specs {
					if vs, ok := sp.(*ast.ValueSpec); ok {
						for _, nm := range vs.Names {
							if nm.Name == name {
								return true
							}
						}
					}
				}
			}
		}
	}
	return false
}

// pkgVar reports whether the directory declares name as a package-level variable or constant.
func (r *flowRun) pkgVar(name string) bool {
	for _, s := range r.g.files {
		for _, d := range s.file.Decls {
			if x, ok := d.(*ast.GenDecl); ok {
				for _, sp := range x.Specs {
					if vs, ok := sp.(*ast.ValueSpec); ok {
						for _, nm := range vs.Names {
							if nm.Name == name {
								return true
							}
						}
					}
				}
			}
		}
	}
	return false
}

// localDecls is the number of declarations of name in the function that holds at (parameters, results, the receiver,
// := and var declarations, range variables, closure parameters); `x := x` copies x and is not counted.
func (r *flowRun) localDecls(name string, at ast.Node) int {
	var fn *ast.FuncDecl
	for p := at; p != nil; p = r.g.parent[p] {
		if d, ok := p.(*ast.FuncDecl); ok {
			fn = d
			break
		}
	}
	if fn == nil {
		return 0
	}
	n := 0
	ast.Inspect(fn, func(m ast.Node) bool {
		switch x := m.(type) {
		case *ast.Field:
			for _, nm := range x.Names {
				if nm.Name == name {
					n++
				}
			}
		case *ast.AssignStmt:
			if x.Tok != token.DEFINE {
				return true
			}
			for j, l := range x.Lhs {
				if id, ok := l.(*ast.Ident); ok && id.Name == name {
					if len(x.Lhs) == len(x.Rhs) {
						if rid, ok := x.Rhs[j].(*ast.Ident); ok && rid.Name == name {
							continue
						}
					}
					n++
				}
			}
		case *ast.ValueSpec:
			for _, nm := range x.Names {
				if nm.Name == name {
					n++
				}
			}
		case *ast.RangeStmt:
			if x.Tok == token.DEFINE {
				for _, e := range []ast.Expr{x.Key, x.Value} {
					if id, ok := e.(*ast.Ident); ok && id.Name == name {
						n++
					}
				}
			}
		}
		return true
	})
	return n
}
