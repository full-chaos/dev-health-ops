package authschema

import (
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// This guard is keyed on what the operands ARE, not on which SQL keywords the
// format string happens to contain. A fmt.Sprintf whose result reaches a
// database call (Exec, Query, QueryRow, Queue, or a package helper that
// forwards a parameter to one) may only be given operands that are constants,
// results of the identifier renderer (quoteIdentifier / Quote), or ints. Any
// other operand -- a bare string, a field, a call result -- is a violation:
// it is exactly how the old `%q` fixtures bypassed ValidatedIdentifier while
// compiling unchanged.
//
// Flow is tracked inside one top-level function: directly as a sink argument,
// through a variable the result is stored in (assignment, append, composite
// literal), through a range over that variable, or through a return value
// (conservatively treated as reaching a sink, since the caller is unknown).

type sqlOperandExemption struct {
	file    string
	fn      string
	operand string
	reason  string
}

// Every entry must match a real violation; an entry that matches nothing
// fails the test, so a fixed or deleted call site cannot leave a silent hole.
var sqlOperandExemptions = []sqlOperandExemption{
	{
		file: "catalog_integration_test.go", fn: "TestPredefinedRoleReachableThroughAnIntermediaryIsDetected", operand: "testCase.grantOptions",
		reason: "GRANT option keywords (WITH ADMIN OPTION and the like) from the test's own table, not an identifier",
	},
	{
		file: "publicreads.go", fn: "publicReadStatements", operand: `strings.Join(columns, ", ")`,
		reason: "columns holds only quoteIdentifier results of validated manifest names",
	},
	{
		file: "public_reads_integration_test.go", fn: "TestPublicReadsRuntimeReadsExactlyTheManifestColumns", operand: `strings.Join(quoted, ", ")`,
		reason: "test query: every entry is a Quote'd manifest column",
	},
	{
		file: "grants.go", fn: "applyRuntimeGrants", operand: `strings.Join(granted, ", ")`,
		reason: "privilege keywords from the closed RuntimePosture manifest (TablePrivilege.privileges), not an identifier",
	},
	{
		file: "catalog_integration_test.go", fn: "TestPredefinedRoleReachableThroughAnIntermediaryIsDetected",
		operand: "testCase.roleAttribute", reason: "role attribute keyword (NOSUPERUSER, NOINHERIT) from the test's own table, not an identifier",
	},
}

type sprintfViolation struct {
	file, fn, operand, format string
	line                      int
}

func TestNoUnvalidatedOperandReachesSQLThroughSprintf(t *testing.T) {
	violations := scanSprintfSQLViolations(t)

	used := make([]bool, len(sqlOperandExemptions))
	var unexempt []string
	for _, v := range violations {
		matched := false
		for i, ex := range sqlOperandExemptions {
			if ex.file == v.file && ex.fn == v.fn && ex.operand == v.operand {
				used[i], matched = true, true
			}
		}
		if !matched {
			unexempt = append(unexempt, v.file+":"+strconv.Itoa(v.line)+" in "+v.fn+": operand `"+v.operand+
				"` is not a constant, an int or a quoteIdentifier/Quote result, in "+v.format)
		}
	}
	for i, ex := range sqlOperandExemptions {
		if !used[i] {
			t.Errorf("stale exemption matches nothing, remove it: %s %s `%s` (%s)", ex.file, ex.fn, ex.operand, ex.reason)
		}
		if strings.TrimSpace(ex.reason) == "" {
			t.Errorf("exemption %s %s `%s` has no reason", ex.file, ex.fn, ex.operand)
		}
	}
	sort.Strings(unexempt)
	for _, u := range unexempt {
		t.Error(u)
	}
}

// TestSprintfSQLGuardCatchesTheOldBypass runs the same scanner over source
// text, so a guard that silently stopped matching cannot pass unnoticed.
func TestSprintfSQLGuardCatchesTheOldBypass(t *testing.T) {
	const bad = `package p
import "fmt"
func f(admin interface{ Exec(string) }, role string) {
	admin.Exec(fmt.Sprintf("CREATE ROLE %q", role))
}
func viaVar(admin interface{ Exec(string) }, role string) {
	stmts := []string{fmt.Sprintf("GRANT x TO %s", role)}
	for _, s := range stmts {
		admin.Exec(s)
	}
}
func good(admin interface{ Exec(string) }, id ValidatedIdentifier) {
	admin.Exec(fmt.Sprintf("CREATE ROLE %s LIMIT %d", quoteIdentifier(id), 3))
}
type fake struct{ db interface{ Exec(string) } }

func (f fake) run(sql string) { f.db.Exec(sql) }
func viaMethodWrapper(f fake, role string) {
	f.run(fmt.Sprintf("GRANT x TO %s", role))
}
func buildStatement(role string) string { return fmt.Sprintf("GRANT x TO %s", role) }
func viaHelperReturn(admin interface{ Exec(string) }, role string) {
	statement := buildStatement(role)
	admin.Exec(statement)
}
func buildLocal(role string) string {
	statement := fmt.Sprintf("GRANT y TO %s", role)
	return statement
}
func viaLocalReturn(admin interface{ Exec(string) }, role string) {
	statement := buildLocal(role)
	admin.Exec(statement)
}
func viaRangeLiteral(admin interface{ Exec(string) }, role string) {
	for _, s := range []string{fmt.Sprintf("GRANT x TO %s", role)} {
		admin.Exec(s)
	}
}
func message(role string) string { return "" }
func notSQL(role string) {
	_ = fmt.Sprintf("hello %s", role)
}
`
	got := scanSprintfSource(t, "bad.go", bad)
	var fns []string
	for _, v := range got {
		fns = append(fns, v.fn+"/"+v.operand)
	}
	sort.Strings(fns)
	want := []string{"buildLocal/role", "buildStatement/role", "f/role", "viaMethodWrapper/role", "viaRangeLiteral/role", "viaVar/role"}
	if strings.Join(fns, ",") != strings.Join(want, ",") {
		t.Fatalf("violations = %v, want %v", fns, want)
	}
}

func scanSprintfSQLViolations(t *testing.T) []sprintfViolation {
	t.Helper()
	paths, err := filepath.Glob("*.go")
	if err != nil || len(paths) == 0 {
		t.Fatalf("no package sources found: %v", err)
	}
	fset := token.NewFileSet()
	var files []*ast.File
	for _, path := range paths {
		file, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		files = append(files, file)
	}
	return scanSprintf(fset, files)
}

func scanSprintfSource(t *testing.T, name, src string) []sprintfViolation {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, name, src, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	return scanSprintf(fset, []*ast.File{file})
}

var sqlSinkMethods = map[string]bool{"Exec": true, "Query": true, "QueryRow": true, "Queue": true}

func scanSprintf(fset *token.FileSet, files []*ast.File) []sprintfViolation {
	consts := map[string]bool{"true": true, "false": true, "nil": true}
	for _, file := range files {
		for _, decl := range file.Decls {
			if gen, ok := decl.(*ast.GenDecl); ok && gen.Tok == token.CONST {
				for _, spec := range gen.Specs {
					for _, name := range spec.(*ast.ValueSpec).Names {
						consts[name.Name] = true
					}
				}
			}
		}
	}

	// Package helpers that forward a parameter into a sink are sinks too.
	sinkFuncs := map[string]bool{}
	for changed := true; changed; {
		changed = false
		for _, file := range files {
			for _, decl := range file.Decls {
				fd, ok := decl.(*ast.FuncDecl)
				if !ok || fd.Body == nil || sinkFuncs[fd.Name.Name] {
					continue
				}
				params := map[string]bool{}
				for _, field := range fd.Type.Params.List {
					for _, name := range field.Names {
						params[name.Name] = true
					}
				}
				forwards := false
				ast.Inspect(fd.Body, func(n ast.Node) bool {
					if call, ok := n.(*ast.CallExpr); ok && isSink(call, sinkFuncs) {
						for _, arg := range call.Args {
							if mentionsAny(arg, params) {
								forwards = true
							}
						}
					}
					return true
				})
				if forwards {
					sinkFuncs[fd.Name.Name], changed = true, true
				}
			}
		}
	}

	// A function that returns a Sprintf result reaches a sink when some caller
	// passes a call to that function into one.
	sinkArgFuncs := map[string]bool{}
	// A call whose result is assigned to a variable that a sink call then
	// receives counts the same as a call placed in the sink's arguments.
	for _, file := range files {
		for _, decl := range file.Decls {
			fd, ok := decl.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				as, ok := n.(*ast.AssignStmt)
				if !ok {
					return true
				}
				held := map[string]bool{}
				for _, lhs := range as.Lhs {
					if id, ok := lhs.(*ast.Ident); ok {
						held[id.Name] = true
					}
				}
				// Follow the held names through later assignments and ranges.
				for changed := true; changed; {
					changed = false
					ast.Inspect(fd.Body, func(m ast.Node) bool {
						switch node := m.(type) {
						case *ast.AssignStmt:
							for i, rhs := range node.Rhs {
								if mentionsAny(rhs, held) && i < len(node.Lhs) {
									if id, ok := node.Lhs[i].(*ast.Ident); ok && !held[id.Name] {
										held[id.Name], changed = true, true
									}
								}
							}
						case *ast.RangeStmt:
							if mentionsAny(node.X, held) {
								for _, v := range []ast.Expr{node.Key, node.Value} {
									if id, ok := v.(*ast.Ident); ok && !held[id.Name] {
										held[id.Name], changed = true, true
									}
								}
							}
						}
						return true
					})
				}
				reachesSink := false
				ast.Inspect(fd.Body, func(m ast.Node) bool {
					if call, ok := m.(*ast.CallExpr); ok && isSink(call, sinkFuncs) {
						for _, arg := range call.Args {
							if mentionsAny(arg, held) {
								reachesSink = true
							}
						}
					}
					return true
				})
				if !reachesSink {
					return true
				}
				for _, rhs := range as.Rhs {
					ast.Inspect(rhs, func(m ast.Node) bool {
						if call, ok := m.(*ast.CallExpr); ok {
							switch fun := call.Fun.(type) {
							case *ast.Ident:
								sinkArgFuncs[fun.Name] = true
							case *ast.SelectorExpr:
								sinkArgFuncs[fun.Sel.Name] = true
							}
						}
						return true
					})
				}
				return true
			})
		}
	}

	for _, file := range files {
		ast.Inspect(file, func(n ast.Node) bool {
			if call, ok := n.(*ast.CallExpr); ok && isSink(call, sinkFuncs) {
				for _, arg := range call.Args {
					ast.Inspect(arg, func(m ast.Node) bool {
						if inner, ok := m.(*ast.CallExpr); ok {
							switch fun := inner.Fun.(type) {
							case *ast.Ident:
								sinkArgFuncs[fun.Name] = true
							case *ast.SelectorExpr:
								sinkArgFuncs[fun.Sel.Name] = true
							}
						}
						return true
					})
				}
			}
			return true
		})
	}

	var out []sprintfViolation
	for _, file := range files {
		filename := filepath.Base(fset.Position(file.Pos()).Filename)
		for _, decl := range file.Decls {
			fd, ok := decl.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}
			out = append(out, scanFunc(fset, filename, fd, consts, sinkFuncs, sinkArgFuncs)...)
		}
	}
	return out
}

func isSink(call *ast.CallExpr, sinkFuncs map[string]bool) bool {
	switch fun := call.Fun.(type) {
	case *ast.SelectorExpr:
		return sqlSinkMethods[fun.Sel.Name] || sinkFuncs[fun.Sel.Name]
	case *ast.Ident:
		return sinkFuncs[fun.Name]
	}
	return false
}

func mentionsAny(node ast.Node, names map[string]bool) bool {
	found := false
	ast.Inspect(node, func(n ast.Node) bool {
		if id, ok := n.(*ast.Ident); ok && names[id.Name] {
			found = true
		}
		return !found
	})
	return found
}

func isSprintf(call *ast.CallExpr) bool {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "Sprintf" {
		return false
	}
	pkg, ok := sel.X.(*ast.Ident)
	return ok && pkg.Name == "fmt"
}

func scanFunc(fset *token.FileSet, file string, fd *ast.FuncDecl, consts, sinkFuncs, sinkArgFuncs map[string]bool) []sprintfViolation {
	// Assignments, parameter types and parent links for the whole function,
	// including nested function literals.
	assigned := map[string][]ast.Expr{}
	paramSafe := map[string]bool{}
	addParams := func(list *ast.FieldList) {
		if list == nil {
			return
		}
		for _, field := range list.List {
			if typeIsSafeScalar(field.Type) {
				for _, name := range field.Names {
					paramSafe[name.Name] = true
				}
			}
		}
	}
	addParams(fd.Type.Params)
	parent := map[ast.Node]ast.Node{}
	var stack []ast.Node
	ast.Inspect(fd.Body, func(n ast.Node) bool {
		if n == nil {
			stack = stack[:len(stack)-1]
			return true
		}
		if len(stack) > 0 {
			parent[n] = stack[len(stack)-1]
		}
		stack = append(stack, n)
		switch node := n.(type) {
		case *ast.FuncLit:
			addParams(node.Type.Params)
		case *ast.AssignStmt:
			if len(node.Lhs) == len(node.Rhs) {
				for i, lhs := range node.Lhs {
					if id, ok := lhs.(*ast.Ident); ok {
						assigned[id.Name] = append(assigned[id.Name], node.Rhs[i])
					}
				}
			}
		case *ast.ValueSpec:
			for i, name := range node.Names {
				if i < len(node.Values) {
					assigned[name.Name] = append(assigned[name.Name], node.Values[i])
				} else if node.Type != nil && typeIsSafeScalar(node.Type) {
					paramSafe[name.Name] = true
				}
			}
		}
		return true
	})

	var safe func(expr ast.Expr, depth int) bool
	safe = func(expr ast.Expr, depth int) bool {
		if depth > 8 {
			return false
		}
		switch e := expr.(type) {
		case *ast.BasicLit:
			return true
		case *ast.ParenExpr:
			return safe(e.X, depth+1)
		case *ast.UnaryExpr:
			return safe(e.X, depth+1)
		case *ast.BinaryExpr:
			return safe(e.X, depth+1) && safe(e.Y, depth+1)
		case *ast.CallExpr:
			switch fun := e.Fun.(type) {
			case *ast.Ident:
				return fun.Name == "quoteIdentifier" || fun.Name == "len"
			case *ast.SelectorExpr:
				return fun.Sel.Name == "Quote" && isIdentNamed(fun.X, "authschema")
			}
			return false
		case *ast.Ident:
			if consts[e.Name] || paramSafe[e.Name] {
				return true
			}
			rhs := assigned[e.Name]
			if len(rhs) == 0 {
				return false
			}
			for _, r := range rhs {
				if !safe(r, depth+1) {
					return false
				}
			}
			return true
		}
		return false
	}
	// A bare `Quote(...)` call inside the package is the exported renderer.
	origSafe := safe
	safe = func(expr ast.Expr, depth int) bool {
		if call, ok := expr.(*ast.CallExpr); ok {
			if id, ok := call.Fun.(*ast.Ident); ok && id.Name == "Quote" {
				return true
			}
		}
		return origSafe(expr, depth)
	}

	// Which names hold a Sprintf result, and which Sprintf results reach a sink.
	type found struct {
		call   *ast.CallExpr
		holder map[string]bool
		direct bool
	}
	var sprintfs []*found
	ast.Inspect(fd.Body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok || !isSprintf(call) {
			return true
		}
		f := &found{call: call, holder: map[string]bool{}}
		for cur := ast.Node(call); cur != nil; cur = parent[cur] {
			switch p := parent[cur].(type) {
			case *ast.CallExpr:
				if p != call && isSink(p, sinkFuncs) {
					f.direct = true
				}
				if id, ok := p.Fun.(*ast.Ident); ok && id.Name == "panic" {
					return true // a panic message is not SQL
				}
			case *ast.ReturnStmt:
				f.direct = sinkArgFuncs[fd.Name.Name]
			case *ast.AssignStmt:
				for _, lhs := range p.Lhs {
					if id, ok := lhs.(*ast.Ident); ok {
						f.holder[id.Name] = true
					}
				}
			case *ast.ValueSpec:
				for _, name := range p.Names {
					f.holder[name.Name] = true
				}
			case *ast.RangeStmt:
				if cur == p.X {
					for _, v := range []ast.Expr{p.Key, p.Value} {
						if id, ok := v.(*ast.Ident); ok {
							f.holder[id.Name] = true
						}
					}
				}
			}
		}
		sprintfs = append(sprintfs, f)
		return true
	})

	var sinkCalls []*ast.CallExpr
	ast.Inspect(fd.Body, func(n ast.Node) bool {
		if call, ok := n.(*ast.CallExpr); ok && isSink(call, sinkFuncs) {
			sinkCalls = append(sinkCalls, call)
		}
		return true
	})

	var out []sprintfViolation
	for _, f := range sprintfs {
		reaches := f.direct
		tainted := f.holder
		for changed := true; changed && !reaches; {
			changed = false
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				if rs, ok := n.(*ast.RangeStmt); ok && mentionsAny(rs.X, tainted) {
					for _, v := range []ast.Expr{rs.Key, rs.Value} {
						if id, ok := v.(*ast.Ident); ok && !tainted[id.Name] {
							tainted[id.Name], changed = true, true
						}
					}
				}
				if as, ok := n.(*ast.AssignStmt); ok {
					for i, rhs := range as.Rhs {
						if mentionsAny(rhs, tainted) && i < len(as.Lhs) {
							if id, ok := as.Lhs[i].(*ast.Ident); ok && !tainted[id.Name] {
								tainted[id.Name], changed = true, true
							}
						}
					}
				}
				return true
			})
		}
		for _, call := range sinkCalls {
			for _, arg := range call.Args {
				if mentionsAny(arg, tainted) {
					reaches = true
				}
			}
		}
		if !reaches && sinkArgFuncs[fd.Name.Name] {
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				if ret, ok := n.(*ast.ReturnStmt); ok {
					for _, result := range ret.Results {
						if mentionsAny(result, tainted) {
							reaches = true
						}
					}
				}
				return true
			})
		}
		if !reaches {
			continue
		}
		format := ""
		if len(f.call.Args) > 0 {
			format = types.ExprString(f.call.Args[0])
			if len(format) > 60 {
				format = format[:60] + "..."
			}
		}
		for _, operand := range f.call.Args[1:] {
			if !safe(operand, 0) {
				out = append(out, sprintfViolation{
					file: file, fn: fd.Name.Name, operand: types.ExprString(operand), format: format,
					line: fset.Position(operand.Pos()).Line,
				})
			}
		}
	}
	return out
}

func isIdentNamed(expr ast.Expr, name string) bool {
	id, ok := expr.(*ast.Ident)
	return ok && id.Name == name
}

func typeIsSafeScalar(expr ast.Expr) bool {
	switch t := expr.(type) {
	case *ast.Ident:
		switch t.Name {
		case "int", "int8", "int16", "int32", "int64", "uint", "uint32", "uint64", "bool", "ValidatedIdentifier":
			return true
		}
	case *ast.SelectorExpr:
		return t.Sel.Name == "ValidatedIdentifier"
	}
	return false
}
