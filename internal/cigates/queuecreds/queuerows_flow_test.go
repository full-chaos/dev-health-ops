package queuecreds_test

import (
	"crypto/sha256"
	"encoding/hex"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
	"testing"
)

var censusDirs = []string{"internal/joboutbox", "internal/syncreconciler", "internal/joboperator", "internal/jobruntime", "internal/workersctl"}

type srcFile struct {
	path string
	file *ast.File
	src  []byte
}

// formatCensus parses the Go files of the census directories under root and
// returns one finding per queue-row value formatted into an error or log
// output without a guard, and the classified outputs it met.
func realRootOf(t *testing.T) string { return repoRoot(t) }

func formatCensus(t *testing.T, root string, dirs []string) ([]string, map[string]bool) {
	t.Helper()
	fset := token.NewFileSet()
	var findings []string
	seen := map[string]bool{}
	for _, dir := range dirs {
		packages := map[string][]srcFile{}
		if _, err := os.Stat(filepath.Join(root, dir)); os.IsNotExist(err) && root != realRootOf(t) {
			continue
		}
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() && entry.Name() == "testdata" {
				return filepath.SkipDir
			}
			if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			source, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			file, err := parser.ParseFile(fset, path, source, 0)
			if err != nil {
				return err
			}
			rel, _ := filepath.Rel(root, path)
			rel = filepath.ToSlash(rel)
			pkgDir := filepath.ToSlash(filepath.Dir(rel))
			packages[pkgDir] = append(packages[pkgDir], srcFile{rel, file, source})
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		for _, pkg := range packages {
			findings = append(findings, unguardedOutputs(fset, pkg, seen)...)
		}
	}
	sort.Strings(findings)
	return findings, seen
}

// sinkCall names the calls that put a value into an error, a log line, a
// printed line, a span attribute or a metric label.
var sinkCall = regexp.MustCompile(`^(fmt\.(Errorf|Sprintf|Sprint|Sprintln|Fprintf|Fprint|Fprintln|Printf|Print|Println)|log\.(Print|Printf|Println|Fatal|Fatalf|Fatalln|Panic|Panicf|Panicln)|errors\.New|slog\.[A-Z][A-Za-z]*|slog\.Default\(\)\.[A-Za-z]+|.*(?:ogger|slog)(?:\(\))?\.(Debug|Info|Warn|Error|With|Log)[A-Za-z]*|.*\.SetAttributes|[A-Za-z]+\.WithAttributes|.*\.WithLabelValues|.*\.AddEvent|metric\.With[A-Za-z]+)$`)

var sinkLiteral = regexp.MustCompile(`^(prometheus\.Labels|attribute\.Set)$`)

// flow is the data-flow state of one package: the struct types that hold a
// queue-row field and the helpers that return one.
type flow struct {
	fset          *token.FileSet
	structs       map[string]bool
	helpers       map[string]bool
	loggerFields  map[string]bool
	loggerResults map[string]bool
	helperFields  map[string]map[string]bool
	// decls maps a function or method name to its declarations; params holds, by declaration, the parameter
	// positions that a caller passes an open queue-row value into; foreign holds the positions where that value does
	// not come from a variable of the parameter's own declared type, so the parameter's type is not its origin.
	decls   map[string][]*ast.FuncDecl
	params  map[*ast.FuncDecl]map[int]bool
	foreign map[*ast.FuncDecl]map[int]bool
	// interfaces holds the package's own interface type names; globals holds its package-level variable names.
	interfaces map[string]bool
	globals    map[string]bool
	// returning holds the functions whose return values returnsOrigin is checking, so recursion ends.
	returning map[*ast.FuncDecl]bool
}

// isLoggerType reports whether a type expression names a slog or log logger.
func isLoggerType(expr ast.Expr) bool {
	switch typed := expr.(type) {
	case *ast.StarExpr:
		return isLoggerType(typed.X)
	case *ast.SelectorExpr:
		base, ok := typed.X.(*ast.Ident)
		return ok && typed.Sel.Name == "Logger" && (base.Name == "slog" || base.Name == "log")
	}
	return false
}

// holdsStruct unwraps a pointer, slice, array or map type to the struct type name it names.
func holdsStruct(expr ast.Expr) string {
	switch typed := expr.(type) {
	case *ast.StarExpr:
		return holdsStruct(typed.X)
	case *ast.ArrayType:
		return holdsStruct(typed.Elt)
	case *ast.Ellipsis:
		return holdsStruct(typed.Elt)
	case *ast.MapType:
		return holdsStruct(typed.Value)
	case *ast.Ident:
		return typed.Name
	}
	return ""
}

func newFlow(fset *token.FileSet, pkg []srcFile) *flow {
	f := &flow{fset: fset, structs: map[string]bool{}, helpers: map[string]bool{}, loggerFields: map[string]bool{}, loggerResults: map[string]bool{}, helperFields: map[string]map[string]bool{}, decls: map[string][]*ast.FuncDecl{}, params: map[*ast.FuncDecl]map[int]bool{}, foreign: map[*ast.FuncDecl]map[int]bool{}, interfaces: map[string]bool{}, globals: map[string]bool{}}
	fieldTypes := map[string][]string{}
	for _, sf := range pkg {
		for _, decl := range sf.file.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok {
				f.decls[fn.Name.Name] = append(f.decls[fn.Name.Name], fn)
			}
			if fn, ok := decl.(*ast.FuncDecl); ok && fn.Type.Results != nil {
				for _, result := range fn.Type.Results.List {
					if isLoggerType(result.Type) {
						f.loggerResults[fn.Name.Name] = true
					}
				}
			}
			gen, ok := decl.(*ast.GenDecl)
			if !ok {
				continue
			}
			for _, spec := range gen.Specs {
				ts, ok := spec.(*ast.TypeSpec)
				if !ok {
					continue
				}
				if _, isInterface := ts.Type.(*ast.InterfaceType); isInterface {
					f.interfaces[ts.Name.Name] = true
				}
				st, ok := ts.Type.(*ast.StructType)
				if !ok {
					continue
				}
				for _, field := range st.Fields.List {
					if isLoggerType(field.Type) {
						for _, name := range field.Names {
							f.loggerFields[name.Name] = true
						}
					}
					if inner := holdsStruct(field.Type); inner != "" {
						fieldTypes[ts.Name.Name] = append(fieldTypes[ts.Name.Name], inner)
					}
				}
			}
		}
	}
	for _, sf := range pkg {
		for _, decl := range sf.file.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok {
				continue
			}
			for _, spec := range gen.Specs {
				ts, ok := spec.(*ast.TypeSpec)
				if !ok {
					continue
				}
				st, ok := ts.Type.(*ast.StructType)
				if !ok {
					continue
				}
				for _, field := range st.Fields.List {
					for _, name := range field.Names {
						if rowKeyName.MatchString(name.Name) {
							f.structs[ts.Name.Name] = true
						}
					}
				}
			}
		}
	}
	for changed := true; changed; {
		changed = false
		for outer, inners := range fieldTypes {
			for _, inner := range inners {
				if f.structs[inner] && !f.structs[outer] {
					f.structs[outer] = true
					changed = true
				}
			}
		}
	}
	f.collectGlobals(pkg)
	// Helper results and helper parameters feed each other: a parameter a caller fills makes the helper's result carry
	// the value, and a result that carries it taints the next call's argument. Both run to one joint fixpoint.
	for {
		changed := f.summarizeReturns(fset, pkg)
		if !f.propagateParams(fset, pkg) && !changed {
			break
		}
	}
	return f
}

// summarizeReturns marks the helpers whose result (or a field of a returned composite) carries an open queue-row value,
// to a fixpoint. It reports whether it marked anything new.
func (f *flow) summarizeReturns(fset *token.FileSet, pkg []srcFile) bool {
	grew := false
	for changed := true; changed; {
		changed = false
		for _, sf := range pkg {
			for _, decl := range sf.file.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || fn.Body == nil {
					continue
				}
				scope := f.scope(fn)
				parents := parentMap(fn.Body)
				var guards []*ast.CallExpr
				ast.Inspect(fn.Body, func(node ast.Node) bool {
					if call, ok := node.(*ast.CallExpr); ok && isGuardCall(fset, sf.src, call) {
						guards = append(guards, call)
					}
					return true
				})
				open := func(expr ast.Expr) bool {
					if !scope.valueTainted(expr) {
						return false
					}
					for _, leaf := range scope.leavesOf(fset, sf.src, expr) {
						if !scope.guarded(fset, sf.src, fn.Body, parents, guards, leaf) {
							return true
						}
					}
					return false
				}
				ast.Inspect(fn.Body, func(node ast.Node) bool {
					if _, lit := node.(*ast.FuncLit); lit {
						return false
					}
					ret, ok := node.(*ast.ReturnStmt)
					if !ok {
						return true
					}
					results := ret.Results
					if len(results) == 0 && fn.Type.Results != nil {
						// A bare return hands back the named results as they are at that point.
						for _, field := range fn.Type.Results.List {
							for _, name := range field.Names {
								named := &ast.Ident{NamePos: ret.Pos(), Name: name.Name}
								parents[named] = ret
								results = append(results, named)
							}
						}
					}
					for _, result := range results {
						if lit := compositeOf(result); lit != nil {
							for _, elt := range lit.Elts {
								field, value := "*", elt
								if kv, ok := elt.(*ast.KeyValueExpr); ok {
									if key, ok := kv.Key.(*ast.Ident); ok {
										field = key.Name
									}
									value = kv.Value
								}
								if scope.tainted(value) && open(value) {
									if f.helperFields[fn.Name.Name] == nil {
										f.helperFields[fn.Name.Name] = map[string]bool{}
									}
									if !f.helperFields[fn.Name.Name][field] {
										f.helperFields[fn.Name.Name][field] = true
										changed, grew = true, true
									}
								}
							}
						}
						if !f.helpers[fn.Name.Name] && open(result) {
							f.helpers[fn.Name.Name] = true
							changed, grew = true, true
						}
					}
					return true
				})
			}
		}
	}
	return grew
}

// paramNames lists the parameter names of a function in call order, one entry per name.
func paramNames(fn *ast.FuncDecl) []string {
	var out []string
	if fn.Type.Params == nil {
		return out
	}
	for _, field := range fn.Type.Params.List {
		if len(field.Names) == 0 {
			out = append(out, "_")
		}
		for _, name := range field.Names {
			out = append(out, name.Name)
		}
	}
	return out
}

// paramIndex maps a call argument position to the callee's parameter position: past the last parameter of a variadic
// function the argument lands in that parameter.
func paramIndex(fn *ast.FuncDecl, arg int) (int, bool) {
	n := len(paramNames(fn))
	if n == 0 {
		return 0, false
	}
	if _, variadic := fn.Type.Params.List[len(fn.Type.Params.List)-1].Type.(*ast.Ellipsis); variadic && arg >= n-1 {
		return n - 1, true
	}
	return arg, arg < n
}

// paramType is the declared type of the parameter at a position, as normTypeText.
func paramType(fn *ast.FuncDecl, index int) string {
	at := 0
	for _, field := range fn.Type.Params.List {
		count := max(len(field.Names), 1)
		if index < at+count {
			return normTypeText(fn, field.Type)
		}
		at += count
	}
	return ""
}

// collectGlobals records the package-level variable names, so a selector on one of them is not read as a package name.
func (f *flow) collectGlobals(pkg []srcFile) {
	for _, sf := range pkg {
		for _, decl := range sf.file.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.VAR {
				continue
			}
			for _, spec := range gen.Specs {
				for _, name := range spec.(*ast.ValueSpec).Names {
					f.globals[name.Name] = true
				}
			}
		}
	}
}

// receiverBase names a method's receiver type without pointer and type arguments ("" for a function).
func receiverBase(fn *ast.FuncDecl) string {
	if fn.Recv == nil || len(fn.Recv.List) == 0 {
		return ""
	}
	return strings.SplitN(normTypeText(fn, fn.Recv.List[0].Type), "[", 2)[0]
}

// resolve lists the same-package declarations a call can reach. A plain name reaches the package functions of that
// name, unless a local of the function holds it. `x.M` reaches nothing when x is neither a local nor a package-level
// variable (it is an imported package); otherwise it reaches the methods named M, narrowed to x's receiver type when
// the syntax states that type and a method on it exists.
func (s *scope) resolve(call *ast.CallExpr) []*ast.FuncDecl {
	s.buildAssigns()
	local := func(name string) bool {
		_, declared := s.decl[name]
		_, assigned := s.assigns[name]
		return declared || assigned
	}
	var out []*ast.FuncDecl
	switch fun := call.Fun.(type) {
	case *ast.Ident:
		if local(fun.Name) {
			return nil
		}
		for _, target := range s.flow.decls[fun.Name] {
			if target.Recv == nil {
				out = append(out, target)
			}
		}
	case *ast.SelectorExpr:
		if base, ok := fun.X.(*ast.Ident); ok && !local(base.Name) && !s.flow.globals[base.Name] {
			return nil
		}
		var methods []*ast.FuncDecl
		for _, target := range s.flow.decls[fun.Sel.Name] {
			if target.Recv != nil {
				methods = append(methods, target)
			}
		}
		if known := strings.SplitN(s.typeOf(fun.X), "[", 2)[0]; known != "" {
			for _, target := range methods {
				if receiverBase(target) == known {
					out = append(out, target)
				}
			}
		}
		if len(out) == 0 {
			out = methods
		}
	}
	return out
}

// candidates picks, among the declarations that share a callee name, those a call can reach: when the argument's type
// is known (a declared variable or a composite literal), the declarations whose parameter there has that type or an
// interface type (any, error, or one of the package's own); when it is unknown, or no declaration matches, every one.
// A parameter of another package's interface type is not recognised without go/types, so such a declaration is left
// out when the argument's type is known and differs.
func candidates(s *scope, decls []*ast.FuncDecl, arg int, expr ast.Expr) []*ast.FuncDecl {
	known := s.typeOf(expr)
	if len(decls) < 2 || known == "" {
		return decls
	}
	var out []*ast.FuncDecl
	for _, target := range decls {
		if index, ok := paramIndex(target, arg); ok {
			if declared := paramType(target, index); declared == known || declared == "any" || declared == "error" || declared == "interface{}" || s.flow.interfaces[strings.SplitN(declared, "[", 2)[0]] {
				out = append(out, target)
			}
		}
	}
	if len(out) == 0 {
		return decls
	}
	return out
}

// typeOf names an expression's type when the syntax states it: a variable with a declared type, a composite literal
// (or its address), or a variable whose only assignment is one of those. It returns "" otherwise.
func (s *scope) typeOf(expr ast.Expr) string {
	expr = ast.Unparen(expr)
	if unary, ok := expr.(*ast.UnaryExpr); ok && unary.Op == token.AND {
		expr = unary.X
	}
	switch typed := expr.(type) {
	case *ast.CompositeLit:
		if typed.Type != nil {
			return normTypeText(s.fn, typed.Type)
		}
	case *ast.Ident:
		if declared, ok := s.decl[typed.Name]; ok {
			return declared
		}
		s.buildAssigns()
		if rhs := s.assigns[typed.Name]; len(rhs) == 1 && s.assignAt[typed.Name][0] < 0 {
			if _, ident := ast.Unparen(rhs[0]).(*ast.Ident); !ident {
				return s.typeOf(rhs[0])
			}
		}
	}
	return ""
}

func markParam(set map[*ast.FuncDecl]map[int]bool, callee *ast.FuncDecl, index int) bool {
	if set[callee][index] {
		return false
	}
	if set[callee] == nil {
		set[callee] = map[int]bool{}
	}
	set[callee][index] = true
	return true
}

// propagateParams marks a same-package helper's parameter as holding a queue-row value when a caller passes an
// unguarded one into that position, so the helper's own outputs are checked against it. It runs to a fixpoint,
// because a helper can pass the value on to a further helper.
func (f *flow) propagateParams(fset *token.FileSet, pkg []srcFile) bool {
	grew := false
	for changed := true; changed; {
		changed = false
		for _, sf := range pkg {
			for _, decl := range sf.file.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || fn.Body == nil {
					continue
				}
				scope := f.scope(fn)
				parents := parentMap(fn.Body)
				var guards []*ast.CallExpr
				ast.Inspect(fn.Body, func(node ast.Node) bool {
					if call, ok := node.(*ast.CallExpr); ok && isGuardCall(fset, sf.src, call) {
						guards = append(guards, call)
					}
					return true
				})
				ast.Inspect(fn.Body, func(node ast.Node) bool {
					call, ok := node.(*ast.CallExpr)
					if !ok {
						return true
					}
					targets := scope.resolve(call)
					if len(targets) == 0 || isGuardCall(fset, sf.src, call) {
						return true
					}
					for i, arg := range call.Args {
						if id, plain := arg.(*ast.Ident); plain && (id.Name == "err" || id.Name == "ok") {
							continue // the scope never treats these names as queue-row values
						}
						if !scope.valueTainted(arg) {
							continue
						}
						open := false
						for _, leaf := range scope.leavesOf(fset, sf.src, arg) {
							if !scope.guarded(fset, sf.src, fn.Body, parents, guards, leaf) {
								open = true
							}
						}
						if !open {
							continue
						}
						for _, target := range candidates(scope, targets, i, arg) {
							index, ok := paramIndex(target, i)
							if !ok {
								continue
							}
							if markParam(f.params, target, index) {
								changed, grew = true, true
							}
							if !scope.originOK(arg, []string{paramType(target, index)}) && markParam(f.foreign, target, index) {
								changed, grew = true, true
							}
						}
					}
					return true
				})
			}
		}
	}
	return grew
}

// scope is the taint state of one function: the locals that hold a queue-row
// value, the struct variables that hold one as a field, and the names that
// were assigned from one another.
type scope struct {
	fields  map[string]bool
	flow    *flow
	locals  map[string]bool
	structs map[string]bool
	loggers map[string]bool
	link    map[string]string
	// decl holds the declared type of each parameter, receiver, named result and var of the function, as normTypeText.
	decl map[string]string
	fn   *ast.FuncDecl
	// foreign holds the parameters a caller fills with a queue-row value that its declared type does not describe.
	foreign map[string]bool
	// assigns holds, by variable name, the expressions assigned to the whole variable; fieldAssigns holds those
	// assigned to a field or element of it. Both are built on first use by originOK.
	assigns, fieldAssigns map[string][]ast.Expr
	// assignAt holds, parallel to assigns, the result position a multi-value right side gives the name, or -1.
	assignAt map[string][]int
}

// isLogger reports whether an expression is a logger: a variable or field of a logger type, a call that
// returns one, or a With/WithGroup of one.
func (s *scope) isLogger(expr ast.Expr) bool {
	switch typed := expr.(type) {
	case *ast.Ident:
		return s.loggers[typed.Name]
	case *ast.ParenExpr:
		return s.isLogger(typed.X)
	case *ast.SelectorExpr:
		return s.flow.loggerFields[typed.Sel.Name]
	case *ast.CallExpr:
		switch fun := typed.Fun.(type) {
		case *ast.Ident:
			return s.flow.loggerResults[fun.Name]
		case *ast.SelectorExpr:
			if base, ok := fun.X.(*ast.Ident); ok && (base.Name == "slog" || base.Name == "log") && (fun.Sel.Name == "Default" || fun.Sel.Name == "New") {
				return true
			}
			if fun.Sel.Name == "With" || fun.Sel.Name == "WithGroup" {
				return s.isLogger(fun.X)
			}
			return s.flow.loggerResults[fun.Sel.Name]
		}
	}
	return false
}

var loggerMethod = regexp.MustCompile(`^((Debug|Info|Warn|Error)(Context)?|Log|LogAttrs|With|WithGroup|Print[a-z]*|Fatal[a-z]*|Panic[a-z]*)$`)

// isLoggerSink reports whether a call is a method call on a logger that emits its arguments.
func (s *scope) isLoggerSink(call *ast.CallExpr) bool {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	return ok && loggerMethod.MatchString(sel.Sel.Name) && s.isLogger(sel.X)
}

func (s *scope) find(name string) string {
	for {
		parent, ok := s.link[name]
		if !ok || parent == name {
			return name
		}
		name = parent
	}
}

func (s *scope) union(a, b string) {
	ra, rb := s.find(a), s.find(b)
	if ra != rb {
		s.link[ra] = rb
	}
}

// compositeOf unwraps & and parentheses to a composite literal.
func compositeOf(expr ast.Expr) *ast.CompositeLit {
	switch typed := expr.(type) {
	case *ast.CompositeLit:
		return typed
	case *ast.UnaryExpr:
		return compositeOf(typed.X)
	case *ast.ParenExpr:
		return compositeOf(typed.X)
	}
	return nil
}

// taintedFields names the keyed fields of a composite literal that hold a queue-row value; "*" stands for a positional one.
func (s *scope) taintedFields(lit *ast.CompositeLit) []string {
	var out []string
	for _, elt := range lit.Elts {
		if kv, ok := elt.(*ast.KeyValueExpr); ok {
			if key, ok := kv.Key.(*ast.Ident); ok && s.tainted(kv.Value) {
				out = append(out, key.Name)
			}
			continue
		}
		if s.tainted(elt) {
			out = append(out, "*")
		}
	}
	return out
}

func (s *scope) fieldPrefix(path string) bool {
	for key := range s.fields {
		if strings.HasPrefix(key, path+".") {
			return true
		}
	}
	return false
}

// markFrom records the tainted fields a value carries into a variable.
func (s *scope) markFrom(name string, rhs ast.Expr, mark func(string, map[string]bool)) {
	if lit := compositeOf(rhs); lit != nil {
		for _, field := range s.taintedFields(lit) {
			mark(name+"."+field, s.fields)
		}
	}
	if call, ok := rhs.(*ast.CallExpr); ok {
		var callee string
		switch fun := call.Fun.(type) {
		case *ast.Ident:
			callee = fun.Name
		case *ast.SelectorExpr:
			callee = fun.Sel.Name
		}
		for field := range s.flow.helperFields[callee] {
			mark(name+"."+field, s.fields)
		}
	}
}

func rootIdent(expr ast.Expr) *ast.Ident {
	for {
		switch typed := expr.(type) {
		case *ast.Ident:
			return typed
		case *ast.SelectorExpr:
			expr = typed.X
		case *ast.IndexExpr:
			expr = typed.X
		case *ast.StarExpr:
			expr = typed.X
		case *ast.ParenExpr:
			expr = typed.X
		default:
			return nil
		}
	}
}

func typeName(expr ast.Expr) string {
	switch typed := expr.(type) {
	case *ast.Ident:
		return typed.Name
	case *ast.StarExpr:
		return typeName(typed.X)
	case *ast.UnaryExpr:
		return typeName(typed.X)
	case *ast.CompositeLit:
		return typeName(typed.Type)
	}
	return ""
}

func (f *flow) scope(fn *ast.FuncDecl) *scope {
	s := &scope{flow: f, locals: map[string]bool{}, structs: map[string]bool{}, loggers: map[string]bool{}, fields: map[string]bool{}, link: map[string]string{}, decl: map[string]string{}, fn: fn, foreign: map[string]bool{}}
	fields := []*ast.FieldList{fn.Type.Params, fn.Recv, fn.Type.Results}
	for _, list := range fields {
		if list == nil {
			continue
		}
		for _, field := range list.List {
			for _, name := range field.Names {
				s.decl[name.Name] = normTypeText(fn, field.Type)
				if f.structs[holdsStruct(field.Type)] {
					s.structs[name.Name] = true
				}
				if isLoggerType(field.Type) {
					s.loggers[name.Name] = true
				}
			}
		}
	}
	for i, name := range paramNames(fn) {
		if f.params[fn][i] && name != "_" && !s.structs[name] {
			s.locals[name] = true
		}
		if f.foreign[fn][i] && name != "_" {
			s.foreign[name] = true
		}
	}
	for changed := true; changed; {
		changed = false
		mark := func(name string, set map[string]bool) {
			if name != "_" && name != "err" && name != "ok" && !set[name] {
				set[name] = true
				changed = true
			}
		}
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			switch typed := node.(type) {
			case *ast.AssignStmt:
				for i, lhs := range typed.Lhs {
					rhs := typed.Rhs[0]
					if len(typed.Rhs) == len(typed.Lhs) {
						rhs = typed.Rhs[i]
					}
					if sel, isSel := lhs.(*ast.SelectorExpr); isSel {
						if s.tainted(rhs) {
							mark(selectorPath(sel), s.fields)
						}
						continue
					}
					id, ok := lhs.(*ast.Ident)
					if !ok {
						continue
					}
					if s.isLogger(rhs) {
						mark(id.Name, s.loggers)
					}
					s.markFrom(id.Name, rhs, mark)
					for _, name := range keys(rhs) {
						s.union(id.Name, name)
					}
					if f.structs[typeName(rhs)] {
						mark(id.Name, s.structs)
					}
					if s.tainted(rhs) {
						mark(id.Name, s.locals)
					}
				}
			case *ast.ValueSpec:
				for i, id := range typed.Names {
					if typed.Type != nil {
						s.decl[id.Name] = normTypeText(fn, typed.Type)
					}
					if f.structs[holdsStruct(typed.Type)] {
						mark(id.Name, s.structs)
					}
					if isLoggerType(typed.Type) {
						mark(id.Name, s.loggers)
					}
					if i < len(typed.Values) {
						if s.isLogger(typed.Values[i]) {
							mark(id.Name, s.loggers)
						}
						s.markFrom(id.Name, typed.Values[i], mark)
						for _, name := range keys(typed.Values[i]) {
							s.union(id.Name, name)
						}
						if f.structs[typeName(typed.Values[i])] {
							mark(id.Name, s.structs)
						}
						if s.tainted(typed.Values[i]) {
							mark(id.Name, s.locals)
						}
					}
				}
			case *ast.RangeStmt:
				if s.tainted(typed.X) {
					for _, side := range []ast.Expr{typed.Key, typed.Value} {
						if id, ok := side.(*ast.Ident); ok {
							mark(id.Name, s.locals)
						}
					}
				}
			}
			return true
		})
	}
	return s
}

// keys names the identifiers and selector paths an expression reads.
func keys(expr ast.Expr) []string {
	var out []string
	ast.Inspect(expr, func(node ast.Node) bool {
		switch typed := node.(type) {
		case *ast.SelectorExpr:
			out = append(out, selectorPath(typed))
			return false
		case *ast.Ident:
			out = append(out, typed.Name)
		}
		return true
	})
	return out
}

func selectorPath(sel *ast.SelectorExpr) string {
	switch base := sel.X.(type) {
	case *ast.Ident:
		return base.Name + "." + sel.Sel.Name
	case *ast.SelectorExpr:
		return selectorPath(base) + "." + sel.Sel.Name
	}
	return sel.Sel.Name
}

// tainted reports whether an expression carries a queue-row value: a field
// or variable with a row-key name, a local assigned from one, a struct that
// holds one, or the result of a helper that returns one.
func (s *scope) tainted(expr ast.Expr) bool {
	switch typed := expr.(type) {
	case *ast.Ident:
		return rowKeyName.MatchString(typed.Name) || s.locals[typed.Name] || s.structs[typed.Name] || s.fieldPrefix(typed.Name)
	case *ast.SelectorExpr:
		if rowKeyName.MatchString(typed.Sel.Name) {
			return true
		}
		path := selectorPath(typed)
		if s.fields[path] || s.fieldPrefix(path) {
			return true
		}
		if root := rootIdent(typed); root != nil && s.fields[root.Name+".*"] {
			return true
		}
		return false
	case *ast.CompositeLit:
		for _, elt := range typed.Elts {
			if kv, ok := elt.(*ast.KeyValueExpr); ok {
				elt = kv.Value
			}
			if s.tainted(elt) {
				return true
			}
		}
		return false
	case *ast.ParenExpr:
		return s.tainted(typed.X)
	case *ast.StarExpr:
		return s.tainted(typed.X)
	case *ast.UnaryExpr:
		return s.tainted(typed.X)
	case *ast.IndexExpr:
		return s.tainted(typed.X)
	case *ast.BinaryExpr:
		return s.tainted(typed.X) || s.tainted(typed.Y)
	case *ast.CallExpr:
		switch fun := typed.Fun.(type) {
		case *ast.Ident:
			if s.flow.helpers[fun.Name] {
				return true
			}
		case *ast.SelectorExpr:
			if s.flow.helpers[fun.Sel.Name] {
				return true
			}
			if s.tainted(fun.X) && !s.structsOnly(fun.X) {
				return true
			}
		}
		if id, ok := typed.Fun.(*ast.Ident); ok && (id.Name == "string" || id.Name == "append") {
			for _, arg := range typed.Args {
				if s.tainted(arg) {
					return true
				}
			}
		}
		if sel, ok := typed.Fun.(*ast.SelectorExpr); ok {
			if base, ok := sel.X.(*ast.Ident); ok && (base.Name == "strings" || base.Name == "attribute" || (base.Name == "fmt" && strings.HasPrefix(sel.Sel.Name, "Sprint"))) {
				for _, arg := range typed.Args {
					if s.tainted(arg) {
						return true
					}
				}
			}
		}
		return false
	}
	return false
}

// valueTainted is tainted without the whole-struct rule: a helper that hands
// back a struct is not a helper that hands back a queue-row value.
func (s *scope) valueTainted(expr ast.Expr) bool {
	if id, ok := expr.(*ast.Ident); ok && s.structs[id.Name] && !s.locals[id.Name] && !rowKeyName.MatchString(id.Name) {
		return false
	}
	return s.tainted(expr)
}

func (s *scope) structsOnly(expr ast.Expr) bool {
	id, ok := expr.(*ast.Ident)
	return ok && s.structs[id.Name] && !s.locals[id.Name] && !rowKeyName.MatchString(id.Name)
}

func isGuardCall(fset *token.FileSet, src []byte, call *ast.CallExpr) bool {
	return exprText(fset, src, call.Fun) == strings.TrimSuffix(guardCall, "(")
}

// leaves lists the tainted sub-expressions of one sink argument.
func (s *scope) leaves(fset *token.FileSet, src []byte, expr ast.Expr) []ast.Expr {
	if expr == nil {
		return nil
	}
	if call, ok := expr.(*ast.CallExpr); ok && isGuardCall(fset, src, call) {
		return nil
	}
	if s.tainted(expr) {
		return []ast.Expr{expr}
	}
	var out []ast.Expr
	switch typed := expr.(type) {
	case *ast.CallExpr:
		if sel, ok := typed.Fun.(*ast.SelectorExpr); ok {
			if _, plain := sel.X.(*ast.Ident); !plain {
				out = append(out, s.leaves(fset, src, sel.X)...)
			}
		}
		for _, arg := range typed.Args {
			out = append(out, s.leaves(fset, src, arg)...)
		}
	case *ast.SelectorExpr:
		if _, plain := typed.X.(*ast.Ident); !plain {
			out = append(out, s.leaves(fset, src, typed.X)...)
		}
	case *ast.BinaryExpr:
		out = append(out, s.leaves(fset, src, typed.X)...)
		out = append(out, s.leaves(fset, src, typed.Y)...)
	case *ast.ParenExpr:
		out = append(out, s.leaves(fset, src, typed.X)...)
	case *ast.StarExpr:
		out = append(out, s.leaves(fset, src, typed.X)...)
	case *ast.UnaryExpr:
		out = append(out, s.leaves(fset, src, typed.X)...)
	case *ast.IndexExpr:
		out = append(out, s.leaves(fset, src, typed.X)...)
	case *ast.KeyValueExpr:
		out = append(out, s.leaves(fset, src, typed.Value)...)
	case *ast.CompositeLit:
		for _, elt := range typed.Elts {
			out = append(out, s.leaves(fset, src, elt)...)
		}
	}
	return out
}

// leavesOf is leaves with a composite literal taken element by element.
func (s *scope) leavesOf(fset *token.FileSet, src []byte, expr ast.Expr) []ast.Expr {
	if lit := compositeOf(expr); lit != nil {
		var out []ast.Expr
		for _, elt := range lit.Elts {
			if kv, ok := elt.(*ast.KeyValueExpr); ok {
				elt = kv.Value
			}
			out = append(out, s.leaves(fset, src, elt)...)
		}
		return out
	}
	return s.leaves(fset, src, expr)
}

func parentMap(root ast.Node) map[ast.Node]ast.Node {
	parents := map[ast.Node]ast.Node{}
	var stack []ast.Node
	ast.Inspect(root, func(node ast.Node) bool {
		if node == nil {
			stack = stack[:len(stack)-1]
			return true
		}
		if len(stack) > 0 {
			parents[node] = stack[len(stack)-1]
		}
		stack = append(stack, node)
		return true
	})
	return parents
}

func stmtList(node ast.Node) ([]ast.Stmt, bool) {
	switch typed := node.(type) {
	case *ast.BlockStmt:
		return typed.List, true
	case *ast.CaseClause:
		return typed.Body, true
	case *ast.CommClause:
		return typed.Body, true
	}
	return nil, false
}

func within(outer, inner ast.Node) bool {
	return outer != nil && inner != nil && inner.Pos() >= outer.Pos() && inner.End() <= outer.End()
}

// evaluatedFirst reports whether the guard sits where the statement always
// evaluates it before the statement's own body: the init, condition or tag of
// a compound statement, or anywhere in a simple statement.
func evaluatedFirst(stmt ast.Stmt, guard ast.Node) bool {
	switch typed := stmt.(type) {
	case *ast.IfStmt:
		return within(typed.Init, guard) || within(typed.Cond, guard)
	case *ast.ForStmt:
		return within(typed.Init, guard) || within(typed.Cond, guard)
	case *ast.SwitchStmt:
		return within(typed.Init, guard) || within(typed.Tag, guard)
	case *ast.RangeStmt:
		return within(typed.X, guard)
	case *ast.BlockStmt, *ast.TypeSwitchStmt, *ast.SelectStmt:
		return false
	}
	return true
}

// dominates reports whether the guard call runs on every path that reaches
// the sink: the guard's statement is in the same or an enclosing statement
// list and ahead of the sink, or the guard is evaluated first by the
// statement that holds the sink. A guard in a sibling branch, or after the
// sink, does not dominate it.
func dominates(parents map[ast.Node]ast.Node, guard, sink ast.Node) bool {
	stmt := guard
	var container ast.Node
	for {
		parent := parents[stmt]
		if parent == nil {
			return false
		}
		if _, ok := stmtList(parent); ok {
			container = parent
			break
		}
		stmt = parent
	}
	list, _ := stmtList(container)
	for i, candidate := range list {
		if ast.Node(candidate) != stmt {
			continue
		}
		if within(candidate, sink) {
			return sink.Pos() >= guard.End() && evaluatedFirst(candidate, guard)
		}
		for _, later := range list[i+1:] {
			if within(later, sink) {
				return true
			}
		}
	}
	return false
}

func unguardedOutputs(fset *token.FileSet, pkg []srcFile, seen map[string]bool) []string {
	fl := newFlow(fset, pkg)
	found := map[string]bool{}
	for _, sf := range pkg {
		for _, decl := range sf.file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			where := sf.path + "#" + fn.Name.Name
			sc := fl.scope(fn)
			parents := parentMap(fn.Body)
			var guards []*ast.CallExpr
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				if call, ok := node.(*ast.CallExpr); ok && isGuardCall(fset, sf.src, call) {
					guards = append(guards, call)
				}
				return true
			})
			check := func(sink ast.Node, args []ast.Expr) {
				for _, arg := range args {
					for _, leaf := range sc.leaves(fset, sf.src, arg) {
						text := exprText(fset, sf.src, leaf)
						if sc.guarded(fset, sf.src, fn.Body, parents, guards, leaf) {
							continue
						}
						if exception, ok := decodedRowFieldOutputs[where]; ok && slices.Contains(exception.values, text) && sc.originOK(leaf, exception.origins) {
							seen[where+"|"+text] = true
							continue
						}
						found[where+" formats "+text+" into an error or log line whose guard result does not control it ("+guardCall+")"] = true
					}
				}
			}
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				switch typed := node.(type) {
				case *ast.CallExpr:
					if sinkCall.MatchString(exprText(fset, sf.src, typed.Fun)) || sc.isLoggerSink(typed) {
						check(typed, typed.Args)
					}
				case *ast.CompositeLit:
					if typed.Type != nil && sinkLiteral.MatchString(exprText(fset, sf.src, typed.Type)) {
						check(typed, typed.Elts)
					}
				}
				return true
			})
		}
	}
	out := make([]string, 0, len(found))
	for finding := range found {
		out = append(out, finding)
	}
	return out
}

// terminates reports whether a branch ends every path in a statement that leaves the enclosing flow.
func terminates(block ast.Stmt) bool {
	body, ok := block.(*ast.BlockStmt)
	if !ok || len(body.List) == 0 {
		return false
	}
	switch last := body.List[len(body.List)-1].(type) {
	case *ast.ReturnStmt:
		return true
	case *ast.BranchStmt:
		return true
	case *ast.ExprStmt:
		if call, ok := last.X.(*ast.CallExpr); ok {
			if id, ok := call.Fun.(*ast.Ident); ok && id.Name == "panic" {
				return true
			}
		}
	}
	return false
}

// overwrites reports whether a branch assigns an expression with no queue-row value to the named variable.
func (s *scope) overwrites(block ast.Stmt, name string) bool {
	body, ok := block.(*ast.BlockStmt)
	if !ok {
		return false
	}
	found := false
	ast.Inspect(body, func(node ast.Node) bool {
		if assign, ok := node.(*ast.AssignStmt); ok {
			for i, lhs := range assign.Lhs {
				if id, ok := lhs.(*ast.Ident); ok && id.Name == name && len(assign.Rhs) == len(assign.Lhs) && !s.tainted(assign.Rhs[i]) {
					found = true
				}
			}
		}
		return true
	})
	return found
}

// reassigned reports whether the variable is given a different queue-row value between two positions. A value built
// only from the guarded value (a plain copy, a copy of a copy, a constructor around it) keeps the guard; an assignment
// that reads any other queue-row value drops it, and so does an assignment of anything else to a name that held a copy.
func (s *scope) reassigned(body ast.Node, fset *token.FileSet, src []byte, name string, guard *ast.CallExpr, from, to token.Pos) bool {
	copies := map[string]bool{}
	for _, arg := range guard.Args {
		copies[exprText(fset, src, arg)] = true
	}
	found := false
	ast.Inspect(body, func(node ast.Node) bool {
		assign, ok := node.(*ast.AssignStmt)
		if !ok || assign.Pos() < from || assign.End() > to || len(assign.Rhs) != len(assign.Lhs) {
			return true
		}
		for i, lhs := range assign.Lhs {
			id, ok := lhs.(*ast.Ident)
			if !ok {
				continue
			}
			foreign, fromCopy := false, false
			for _, path := range keys(assign.Rhs[i]) {
				switch {
				case copies[path]:
					fromCopy = true
				case s.pathNeedsOrigin(path):
					foreign = true
				}
			}
			if foreign || !fromCopy {
				delete(copies, id.Name)
			} else {
				copies[id.Name] = true
			}
			if foreign && id.Name == name && s.tainted(assign.Rhs[i]) {
				found = true
			}
		}
		return true
	})
	return found
}

type implies int

const (
	impliesNothing implies = iota
	impliesTrue
	impliesFalse
)

func firstKnown(values ...implies) implies {
	for _, v := range values {
		if v != impliesNothing {
			return v
		}
	}
	return impliesNothing
}

// truthOf reports what the detector's result is when the condition is true and when it is false. A negation swaps them; `a && b` being true makes every conjunct true, and `a || b` being false makes every disjunct false;
// the other direction proves nothing, because a short-circuit can skip the detector.
func truthOf(cond ast.Expr, isDetector func(ast.Expr) bool) (whenTrue, whenFalse implies) {
	switch typed := cond.(type) {
	case *ast.ParenExpr:
		return truthOf(typed.X, isDetector)
	case *ast.UnaryExpr:
		if typed.Op == token.NOT {
			t, f := truthOf(typed.X, isDetector)
			return f, t
		}
	case *ast.BinaryExpr:
		lt, lf := truthOf(typed.X, isDetector)
		rt, rf := truthOf(typed.Y, isDetector)
		switch typed.Op {
		case token.LAND:
			return firstKnown(lt, rt), impliesNothing
		case token.LOR:
			return impliesNothing, firstKnown(lf, rf)
		}
	}
	if isDetector(cond) {
		return impliesTrue, impliesFalse
	}
	return impliesNothing, impliesNothing
}

// gateOf finds the if statement the detector's result controls: the call is in its condition, directly, under a
// negation, or as an operand of && or ||, or it is assigned to a variable that is such an operand. It returns the
// statement and what the detector's result is when the body is taken and when the else path is taken.
func gateOf(fset *token.FileSet, src []byte, fn ast.Node, parents map[ast.Node]ast.Node, guard *ast.CallExpr) (gate *ast.IfStmt, body, other implies) {
	var found *ast.IfStmt
	ast.Inspect(fn, func(node ast.Node) bool {
		ifs, ok := node.(*ast.IfStmt)
		if !ok || found != nil {
			return true
		}
		whenTrue, whenFalse := truthOf(ifs.Cond, func(e ast.Expr) bool { return e == ast.Expr(guard) })
		if whenTrue == impliesNothing && whenFalse == impliesNothing {
			if parent, ok := parents[guard].(*ast.AssignStmt); ok && len(parent.Lhs) == 1 && len(parent.Rhs) == 1 && parent.Pos() < ifs.Pos() {
				if id, ok := parent.Lhs[0].(*ast.Ident); ok {
					whenTrue, whenFalse = truthOf(ifs.Cond, func(e ast.Expr) bool { ident, ok := e.(*ast.Ident); return ok && ident.Name == id.Name })
				}
			}
		}
		if whenTrue != impliesNothing || whenFalse != impliesNothing {
			found, body, other = ifs, whenTrue, whenFalse
		}
		return true
	})
	return found, body, other
}

// guarded reports whether a detector call, whose arguments reach the same value as the leaf, has its result control the
// path to the leaf: the value is not reassigned afterwards, and either the leaf is only on a branch where the detector
// was false, or the path that reaches it past the if statement is one where the detector was false and the other
// branch leaves the flow or overwrites the value.
func (s *scope) guarded(fset *token.FileSet, src []byte, fn ast.Node, parents map[ast.Node]ast.Node, guards []*ast.CallExpr, leaf ast.Expr) bool {
	leafKeys := keys(leaf)
	root := rootIdent(leaf)
	for _, guard := range guards {
		covers := false
		for _, arg := range guard.Args {
			for _, guardKey := range keys(arg) {
				for _, leafKey := range leafKeys {
					if s.find(guardKey) == s.find(leafKey) ||
						strings.HasPrefix(guardKey, leafKey+".") || strings.HasPrefix(leafKey, guardKey+".") {
						covers = true
					}
				}
			}
		}
		if !covers {
			continue
		}
		gate, body, other := gateOf(fset, src, fn, parents, guard)
		if gate == nil {
			continue
		}
		if body == impliesFalse && within(gate.Body, leaf) || other == impliesFalse && gate.Else != nil && within(gate.Else, leaf) {
			return true
		}
		if !dominates(parents, gate, leaf) {
			continue
		}
		if root != nil && s.reassigned(fn, fset, src, root.Name, guard, gate.End(), leaf.Pos()) {
			continue
		}
		leaves := func(branch ast.Stmt) bool {
			return branch != nil && (terminates(branch) || (root != nil && s.overwrites(branch, root.Name)))
		}
		if other == impliesFalse && leaves(gate.Body) || body == impliesFalse && leaves(gate.Else) {
			return true
		}
	}
	return false
}

func treeSHA(t *testing.T, root string, dirs []string) string {
	t.Helper()
	sum := sha256.New()
	for _, dir := range dirs {
		_ = filepath.WalkDir(filepath.Join(root, dir), func(path string, entry fs.DirEntry, err error) error {
			if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") {
				return nil
			}
			data, rerr := os.ReadFile(path)
			if rerr != nil {
				t.Fatal(rerr)
			}
			sum.Write([]byte(path))
			sum.Write(data)
			return nil
		})
	}
	return hex.EncodeToString(sum.Sum(nil))
}

const fixtureHead = `package fx

import (
	"fmt"
	"log"
	"log/slog"

	"github.com/full-chaos/dev-health-ops/internal/platform/credshape"
)

type Row struct {
	DedupeKey     string
	CorrelationID string
	N             int
}

`

// plantCensus writes the files under a temporary tree and runs the census on it.
func plantCensus(t *testing.T, files map[string]string) []string {
	t.Helper()
	root := t.TempDir()
	for rel, body := range files {
		path := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(fixtureHead+body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	findings, _ := formatCensus(t, root, censusDirs)
	return findings
}

var formatPlants = []struct {
	name  string
	body  string
	clean string
}{
	{"F01-renamed-local",
		"func f(row Row) { k := row.DedupeKey; slog.Info(\"x\", \"k\", k) }\n",
		"func f(row Row) { k := row.DedupeKey; if credshape.AnyCredentialShaped(k) { return }; slog.Info(\"x\", \"k\", k) }\n"},
	{"F02-slog-default",
		"func f(row Row) { slog.Default().Warn(\"x\", \"k\", row.DedupeKey) }\n",
		"func f(row Row) { slog.Default().Warn(\"x\", \"k\", row.N) }\n"},
	{"F03-helper-return",
		"var cur Row\nfunc keyOf() string { return cur.DedupeKey }\nfunc f() { slog.Info(\"x\", \"k\", keyOf()) }\n",
		"var cur Row\nfunc keyOf() string { return cur.DedupeKey }\nfunc f() { if credshape.AnyCredentialShaped(keyOf()) { return }; slog.Info(\"x\", \"k\", keyOf()) }\n"},
	{"F04-whole-struct",
		"func f(r Row) error { return fmt.Errorf(\"bad %+v\", r) }\n",
		"func f(r Row) error { return fmt.Errorf(\"bad %d\", r.N) }\n"},
	{"F05-format-before-guard",
		"func f(row Row) { slog.Info(\"x\", \"k\", row.DedupeKey); if credshape.AnyCredentialShaped(row.DedupeKey) { return } }\n",
		"func f(row Row) { if credshape.AnyCredentialShaped(row.DedupeKey) { return }; slog.Info(\"x\", \"k\", row.DedupeKey) }\n"},
	{"F06-guard-in-other-branch",
		"func f(row Row, c bool) { if c { if credshape.AnyCredentialShaped(row.DedupeKey) { return } } else { slog.Info(\"x\", \"k\", row.DedupeKey) } }\n",
		"func f(row Row, c bool) { if credshape.AnyCredentialShaped(row.DedupeKey) { return }; if c { slog.Info(\"x\", \"k\", row.DedupeKey) } }\n"},
	{"F07-guard-of-other-value",
		"func f(row Row) { if credshape.AnyCredentialShaped(row.CorrelationID) { return }; slog.Info(\"x\", \"k\", row.DedupeKey) }\n",
		"func f(row Row) { if credshape.AnyCredentialShaped(row.DedupeKey) { return }; slog.Info(\"x\", \"k\", row.DedupeKey) }\n"},
	{"F08-logger-with",
		"func f(logger *slog.Logger, row Row) { l := logger.With(\"k\", row.DedupeKey); _ = l }\n",
		"func f(logger *slog.Logger, row Row) { l := logger.With(\"k\", row.N); _ = l }\n"},
	{"F09-log-printf",
		"func f(row Row) { log.Printf(\"x %s\", row.DedupeKey) }\n",
		"func f(row Row) { log.Printf(\"x %d\", row.N) }\n"},
	{"F10-fmt-print",
		"func f(row Row) { fmt.Print(row.DedupeKey) }\n",
		"func f(row Row) { fmt.Print(row.N) }\n"},
	{"F11-span-attribute",
		"func f(span Span, row Row) { span.SetAttributes(attribute.String(\"k\", row.DedupeKey)) }\n",
		"func f(span Span, row Row) { span.SetAttributes(attribute.Int(\"k\", row.N)) }\n"},
	{"F12-metric-label",
		"func f(counter Vec, row Row) { counter.WithLabelValues(row.DedupeKey).Inc() }\n",
		"func f(counter Vec, row Row) { counter.WithLabelValues(\"fixed\").Inc() }\n"},
}

func TestQueueRowFormatCensusFindsEachPlantedShape(t *testing.T) {
	root := repoRoot(t)
	before := treeSHA(t, root, censusDirs)
	for _, plant := range formatPlants {
		t.Run(plant.name, func(t *testing.T) {
			if got := plantCensus(t, map[string]string{"internal/joboutbox/x.go": plant.body}); len(got) == 0 {
				t.Errorf("%s: the census did not turn red", plant.name)
			}
		})
		t.Run(plant.name+"-clean", func(t *testing.T) {
			if got := plantCensus(t, map[string]string{"internal/joboutbox/x.go": plant.clean}); len(got) != 0 {
				t.Errorf("%s: the clean twin is reported: %v", plant.name, got)
			}
		})
	}
	if after := treeSHA(t, root, censusDirs); after != before {
		t.Errorf("the plants changed the real tree: sha256 %s != %s", before, after)
	}
}

func TestQueueRowFormatCensusRecursesIntoSubpackages(t *testing.T) {
	t.Run("R01-subpackage", func(t *testing.T) {
		got := plantCensus(t, map[string]string{"internal/joboutbox/sub/deep/x.go": "func f(row Row) { slog.Info(\"x\", \"k\", row.DedupeKey) }\n"})
		if len(got) == 0 {
			t.Error("R01-subpackage: a plant in a subpackage was not found")
		}
	})
}

func TestQueueRowSQLCensusFindsArgsAtAnyPosition(t *testing.T) {
	cases := []struct {
		name, sql string
		want      bool
	}{
		{"S01-last-in-projection", "SELECT id, args FROM jobs", true},
		{"S02-returning", "UPDATE jobs SET state = 'x' RETURNING id, args", true},
		{"S03-first-in-projection", "SELECT args, id FROM jobs", true},
		{"S04-before-newline-from", "SELECT id,\n  args\nFROM jobs", true},
		{"S05-in-call", "SELECT coalesce(args) FROM jobs", true},
		{"S06-unrelated-column", "SELECT id, state FROM jobs", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := false
			for _, key := range sqlKeys(c.sql) {
				got = got || key == "args"
			}
			if got != c.want {
				t.Errorf("sqlKeys(%q) names args = %v, want %v", c.sql, got, c.want)
			}
		})
	}
}

type cellPair struct{ name, plant, clean string }

var r1FormatCells = []cellPair{
	{"F13-guard-result-ignored",
		"func f(row Row) { _ = credshape.AnyCredentialShaped(row.DedupeKey); slog.Info(\"x\", \"k\", row.DedupeKey) }\n",
		"func f(row Row) { if credshape.AnyCredentialShaped(row.DedupeKey) { return }; slog.Info(\"x\", \"k\", row.DedupeKey) }\n"},
	{"F14-output-on-the-detector-true-branch",
		"func f(row Row) { if credshape.AnyCredentialShaped(row.DedupeKey) { slog.Info(\"x\", \"k\", row.DedupeKey) } }\n",
		"func f(row Row) { if credshape.AnyCredentialShaped(row.DedupeKey) { slog.Info(\"refused\") } else { slog.Info(\"x\", \"k\", row.DedupeKey) } }\n"},
	{"F15-alias-reassigned-after-the-guard",
		"func f(row, other Row) { k := row.DedupeKey; if credshape.AnyCredentialShaped(k) { return }; k = other.DedupeKey; slog.Info(\"x\", \"k\", k) }\n",
		"func f(row, other Row) { k := row.DedupeKey; if credshape.AnyCredentialShaped(k) { return }; slog.Info(\"x\", \"k\", k) }\n"},
	{"F16-sanitized-in-the-refused-branch",
		"func f(row Row) { org := row.DedupeKey; refused := credshape.AnyCredentialShaped(row.DedupeKey); if refused { slog.Info(\"x\", \"k\", org) } }\n",
		"func f(row Row) { org := row.DedupeKey; refused := credshape.AnyCredentialShaped(row.DedupeKey); if refused { org = \"\" }; slog.Info(\"x\", \"k\", org) }\n"},
	{"F17-wrapper-composite",
		"type Holder struct{ value string }\nfunc f(row Row) { h := Holder{value: row.DedupeKey}; slog.Info(\"x\", \"v\", h.value) }\n",
		"type Holder struct{ value string }\nfunc f(row Row) { h := Holder{value: \"fixed\"}; slog.Info(\"x\", \"v\", h.value) }\n"},
	{"F18-nested-struct-formatted-whole",
		"type Holder struct{ r Row }\nfunc f(h Holder) error { return fmt.Errorf(\"bad %+v\", h) }\n",
		"type Holder struct{ r Row }\nfunc f(h Holder) error { return fmt.Errorf(\"bad %d\", h.r.N) }\n"},
	{"F18b-field-assigned-from-a-row",
		"type Holder struct{ value string }\nfunc f(row Row) { var h Holder; h.value = row.DedupeKey; slog.Info(\"x\", \"v\", h.value) }\n",
		"type Holder struct{ value string }\nfunc f(row Row) { var h Holder; h.value = \"fixed\"; slog.Info(\"x\", \"v\", h.value) }\n"},
	{"F19-helper-returns-a-holder",
		"type Holder struct{ value string }\nfunc wrap(row Row) Holder { return Holder{value: row.DedupeKey} }\nfunc f(row Row) { h := wrap(row); slog.Info(\"x\", \"v\", h.value) }\n",
		"type Holder struct{ value string }\nfunc wrap(row Row) Holder { return Holder{value: \"fixed\"} }\nfunc f(row Row) { h := wrap(row); slog.Info(\"x\", \"v\", h.value) }\n"},
	{"F20-logger-local-renamed",
		"func f(logger *slog.Logger, row Row) { l := logger.With(\"a\", \"b\"); l.Info(\"x\", \"k\", row.DedupeKey) }\n",
		"func f(logger *slog.Logger, row Row) { l := logger.With(\"a\", \"b\"); l.Info(\"x\", \"k\", row.N) }\n"},
	{"F21-logger-param-renamed",
		"func f(lg *slog.Logger, row Row) { lg.Warn(\"x\", \"k\", row.DedupeKey) }\n",
		"func f(lg *slog.Logger, row Row) { lg.Warn(\"x\", \"k\", row.N) }\n"},
	{"F22-logger-field-renamed",
		"type S struct{ out *slog.Logger }\nfunc (s *S) f(row Row) { s.out.Info(\"x\", \"k\", row.DedupeKey) }\n",
		"type S struct{ out *slog.Logger }\nfunc (s *S) f(row Row) { s.out.Info(\"x\", \"k\", row.N) }\n"},
}

func TestQueueRowFormatCensusFollowsTheGuardResultAndTheValue(t *testing.T) {
	for _, c := range r1FormatCells {
		t.Run(c.name, func(t *testing.T) {
			if got := plantCensus(t, map[string]string{"internal/joboutbox/x.go": c.plant}); len(got) == 0 {
				t.Errorf("%s: the census did not turn red", c.name)
			}
		})
		t.Run(c.name+"-clean", func(t *testing.T) {
			if got := plantCensus(t, map[string]string{"internal/joboutbox/x.go": c.clean}); len(got) != 0 {
				t.Errorf("%s: the clean twin is reported: %v", c.name, got)
			}
		})
	}
}

var exceptionCells = []struct{ name, path, plant, clean string }{
	{"X01-execute", "internal/jobruntime/adapter.go",
		"func execute(row Row) { slog.Info(\"x\", \"k\", row.DedupeKey) }\n",
		"func execute(envelope jobcontract.Envelope) { slog.Info(\"x\", \"k\", envelope.CorrelationID) }\n"},
	{"X02-logFinish", "internal/jobruntime/adapter.go",
		"func logFinish(row Row) { slog.Info(\"x\", \"k\", row.DedupeKey) }\n",
		"func logFinish(envelope jobcontract.Envelope) { attributes := []any{\"c\", envelope.CorrelationID}; slog.Info(\"x\", attributes...) }\n"},
	{"X03-startJobSpan", "internal/jobruntime/adapter.go",
		"func startJobSpan(row Row, span Span) { span.SetAttributes(attribute.String(\"k\", row.DedupeKey)) }\n",
		"func startJobSpan(envelope jobcontract.Envelope) { attributes := []attribute.KeyValue{attribute.String(\"k\", envelope.CorrelationID)}; _ = oteltrace.WithAttributes(attributes...) }\n"},
	{"X04-writeManualInvestmentTrigger", "internal/workersctl/trigger_investment.go",
		"func writeManualInvestmentTrigger(row Row) { slog.Info(\"x\", \"k\", row.DedupeKey) }\n",
		"func writeManualInvestmentTrigger(request workgraph.Request) { generation := request.IdempotencyKey; slog.Info(\"x\", \"g\", generation) }\n"},
	{"X05-writeManualWorkgraphTrigger", "internal/workersctl/trigger_workgraph.go",
		"func writeManualWorkgraphTrigger(row Row) { slog.Info(\"x\", \"k\", row.DedupeKey) }\n",
		"func writeManualWorkgraphTrigger(request workgraph.Request) { generation := request.IdempotencyKey; slog.Info(\"x\", \"g\", generation) }\n"},
	{"X06-rollbackTriggerTx", "internal/workersctl/trigger_workgraph.go",
		"func rollbackTriggerTx(row Row) { slog.Info(\"x\", \"k\", row.DedupeKey) }\n",
		"func rollbackTriggerTx(attrs []any) { args := make([]any, 0); args = append(args, attrs...); slog.Default().Error(\"x\", args...) }\n"},
}

func TestQueueRowFormatExceptionsCoverOnlyTheirOwnValue(t *testing.T) {
	for _, c := range exceptionCells {
		t.Run(c.name, func(t *testing.T) {
			if got := plantCensus(t, map[string]string{c.path: c.plant}); len(got) == 0 {
				t.Errorf("%s: an unlisted queue-row value in the exempt function was accepted", c.name)
			}
		})
		t.Run(c.name+"-clean", func(t *testing.T) {
			if got := plantCensus(t, map[string]string{c.path: c.clean}); len(got) != 0 {
				t.Errorf("%s: the listed value is reported: %v", c.name, got)
			}
		})
	}
}

func TestQueueRowSQLCensusStatementsAndOperators(t *testing.T) {
	cases := []struct {
		name, sql string
		want      string
	}{
		{"S07-delete-returning", "DELETE FROM jobs WHERE state = 'x' RETURNING args", "args"},
		{"S08-delete-returning-key", "DELETE FROM jobs WHERE dedupe_key = $1 RETURNING dedupe_key", "dedupe_key"},
		{"S09-jsonb-minus", "SELECT args - 'legacy' FROM jobs", "args"},
		{"S10-jsonb-path-minus", "SELECT args #- '{domain,id}' FROM jobs", "args"},
		{"S11-jsonb-contains", "SELECT id FROM jobs WHERE args @> '{}'", "args"},
		{"S12-quoted-literal-and-longer-column", "SELECT 'args' AS label, args_hash FROM jobs", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := strings.Join(queueRowSQLKeys(c.sql), ",")
			if got != c.want {
				t.Errorf("queueRowSQLKeys(%q) = %q, want %q", c.sql, got, c.want)
			}
		})
	}
}

// normTypeText names the declared type of a variable for the exception origins: a pointer, slice or variadic marker
// is dropped, and a type parameter of the function reads "typeparam <constraint>".
func normTypeText(fn *ast.FuncDecl, expr ast.Expr) string {
	for {
		switch typed := expr.(type) {
		case *ast.StarExpr:
			expr = typed.X
			continue
		case *ast.ArrayType:
			expr = typed.Elt
			continue
		case *ast.Ellipsis:
			expr = typed.Elt
			continue
		}
		break
	}
	text := types.ExprString(expr)
	if fn.Type.TypeParams != nil {
		for _, field := range fn.Type.TypeParams.List {
			for _, name := range field.Names {
				if name.Name == text {
					return "typeparam " + types.ExprString(field.Type)
				}
			}
		}
	}
	return text
}

func (s *scope) buildAssigns() {
	if s.assigns != nil {
		return
	}
	s.assigns, s.fieldAssigns, s.assignAt = map[string][]ast.Expr{}, map[string][]ast.Expr{}, map[string][]int{}
	add := func(name string, rhs ast.Expr, at int) {
		s.assigns[name] = append(s.assigns[name], rhs)
		s.assignAt[name] = append(s.assignAt[name], at)
	}
	ast.Inspect(s.fn.Body, func(node ast.Node) bool {
		switch typed := node.(type) {
		case *ast.AssignStmt:
			for i, lhs := range typed.Lhs {
				rhs, at := typed.Rhs[0], i
				if len(typed.Rhs) == len(typed.Lhs) {
					rhs, at = typed.Rhs[i], -1
				}
				if id, ok := lhs.(*ast.Ident); ok {
					add(id.Name, rhs, at)
				} else if root := rootIdent(lhs); root != nil {
					s.fieldAssigns[root.Name] = append(s.fieldAssigns[root.Name], rhs)
				}
			}
		case *ast.ValueSpec:
			for i, id := range typed.Names {
				if i < len(typed.Values) {
					add(id.Name, typed.Values[i], -1)
				} else if len(typed.Values) == 1 {
					add(id.Name, typed.Values[0], i)
				}
			}
		case *ast.RangeStmt:
			for _, side := range []ast.Expr{typed.Key, typed.Value} {
				if id, ok := side.(*ast.Ident); ok {
					add(id.Name, typed.X, -1)
				}
			}
		}
		return true
	})
}

// pathNeedsOrigin reports whether a selector path or name carries a queue-row value by its own spelling or taint.
func (s *scope) pathNeedsOrigin(path string) bool {
	parts := strings.Split(path, ".")
	for _, part := range parts {
		if rowKeyName.MatchString(part) {
			return true
		}
	}
	return s.tainted(&ast.Ident{Name: parts[0]}) || s.fields[path] || s.fieldPrefix(path)
}

// originOK reports whether every queue-row value the leaf reads comes from a variable whose declared type is one of
// the origins: the value's origin is its Go type (a decoded envelope, a typed request), not the text of the expression.
// A variable without a declared type is followed to the expressions assigned to it; a field assigned into a variable
// of an allowed type is followed too.
func (s *scope) originOK(leaf ast.Expr, origins []string) bool {
	s.buildAssigns()
	return s.originExpr(leaf, origins, map[string]bool{})
}

func (s *scope) originExpr(expr ast.Expr, origins []string, seen map[string]bool) bool {
	for _, path := range keys(expr) {
		if !s.pathNeedsOrigin(path) {
			continue
		}
		root := strings.Split(path, ".")[0]
		if !s.originName(root, origins, seen) {
			return false
		}
	}
	return true
}

func (s *scope) originName(name string, origins []string, seen map[string]bool) bool {
	if seen[name] {
		return true
	}
	seen[name] = true
	if s.foreign[name] {
		return false
	}
	if declared, ok := s.decl[name]; ok {
		if !slices.Contains(origins, declared) {
			return false
		}
		for i, rhs := range s.assigns[name] {
			if !s.originRHS(rhs, s.assignAt[name][i], origins, seen) {
				return false
			}
		}
		for _, rhs := range s.fieldAssigns[name] {
			if !s.originExpr(rhs, origins, seen) {
				return false
			}
		}
		return true
	}
	rhsList, assigned := s.assigns[name]
	if !assigned {
		return false
	}
	for i, rhs := range rhsList {
		if !s.originRHS(rhs, s.assignAt[name][i], origins, seen) {
			return false
		}
	}
	for _, rhs := range s.fieldAssigns[name] {
		if !s.originExpr(rhs, origins, seen) {
			return false
		}
	}
	return true
}

// originRHS checks one value assigned to a name. For a call to a function of the same package, that function's own
// return values at the position must have the origin; any other expression is checked by the values it reads.
func (s *scope) originRHS(rhs ast.Expr, at int, origins []string, seen map[string]bool) bool {
	call, ok := ast.Unparen(rhs).(*ast.CallExpr)
	if !ok {
		return s.originExpr(rhs, origins, seen)
	}
	targets := s.resolve(call)
	if len(targets) == 0 {
		return s.originExpr(rhs, origins, seen)
	}
	position := max(at, 0)
	for _, target := range targets {
		if !s.flow.returnsOrigin(target, position, origins) {
			return false
		}
	}
	return true
}

// resultFields lists a function's results in order: the declared type and the name ("" when unnamed).
func resultFields(fn *ast.FuncDecl) (typesOut, names []string) {
	if fn.Type.Results == nil {
		return nil, nil
	}
	for _, field := range fn.Type.Results.List {
		if len(field.Names) == 0 {
			typesOut, names = append(typesOut, normTypeText(fn, field.Type)), append(names, "")
		}
		for _, name := range field.Names {
			typesOut, names = append(typesOut, normTypeText(fn, field.Type)), append(names, name.Name)
		}
	}
	return typesOut, names
}

// returnsOrigin reports whether every value a function returns at a position (a named result included) comes from one
// of the origins. A function already on the stack counts as proven, so recursion ends.
func (f *flow) returnsOrigin(fn *ast.FuncDecl, position int, origins []string) bool {
	resultTypes, resultNames := resultFields(fn)
	if position >= len(resultTypes) || fn.Body == nil {
		return false
	}
	if f.returning == nil {
		f.returning = map[*ast.FuncDecl]bool{}
	}
	if f.returning[fn] {
		return true
	}
	f.returning[fn] = true
	defer delete(f.returning, fn)
	callee := f.scope(fn)
	callee.buildAssigns()
	if name := resultNames[position]; name != "" && name != "_" && !callee.originName(name, origins, map[string]bool{}) {
		return false
	}
	proven := true
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		switch typed := node.(type) {
		case *ast.FuncLit:
			return false
		case *ast.ReturnStmt:
			switch {
			case len(typed.Results) == len(resultTypes):
				proven = proven && callee.originRHS(typed.Results[position], -1, origins, map[string]bool{})
			case len(typed.Results) == 1:
				proven = proven && callee.originRHS(typed.Results[0], position, origins, map[string]bool{})
			}
		}
		return proven
	})
	return proven
}
