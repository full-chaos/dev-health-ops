package affected

import (
	"go/ast"
	"go/token"
	"strings"
)

// A pure function only computes: its body resolves in the scan, it reads, runs, opens and writes no file, and every call
// in it is a builtin, a conversion, a string or path function of the allowlist or another pure function of the module.
// A method call, a function value, a closure, a goroutine and a call of any other package make it not pure. A cycle of
// functions that call each other is pure when no member is otherwise not.

const pureDepth = 12

// pureStdlib: the functions of the standard library a pure function may call (a package prefix ending in a dot, or one
// full name). The ones that take a function value are left out.
var pureStdlibPrefixes = []string{"strings.", "bytes.", "path.", "unicode.", "unicode/utf8.", "strconv.", "math."}

var pureStdlibNames = map[string]bool{
	"fmt.Sprintf": true, "fmt.Sprint": true, "fmt.Sprintln": true, "fmt.Errorf": true, "errors.New": true,
	"sort.Strings": true, "sort.Ints": true, "sort.Float64s": true, "sort.SearchStrings": true,
	"path/filepath.Join": true, "path/filepath.Base": true, "path/filepath.Ext": true, "path/filepath.Clean": true,
	"path/filepath.Dir": true, "path/filepath.ToSlash": true, "path/filepath.FromSlash": true, "path/filepath.Match": true,
	"path/filepath.IsAbs": true, "path/filepath.Rel": true, "path/filepath.SplitList": true, "path/filepath.VolumeName": true,
}

var pureBuiltins = map[string]bool{"len": true, "cap": true, "append": true, "make": true, "new": true, "copy": true, "min": true,
	"max": true, "panic": true, "string": true, "byte": true, "rune": true, "delete": true, "clear": true}

func purePkgCall(full string) bool {
	if pureStdlibNames[full] {
		return true
	}
	for _, p := range pureStdlibPrefixes {
		if strings.HasPrefix(full, p) && !strings.HasSuffix(full, "Func") && full != "strings.Map" && full != "bytes.Map" {
			return true
		}
	}
	return false
}

// pureCall reports whether a call is to a pure function of this directory or of a package of the module.
func (c *fileCtx) pureCall(x *ast.CallExpr) bool {
	switch f := ast.Unparen(x.Fun).(type) {
	case *ast.Ident:
		return c.g.pureFunc(f.Name, c, nil)
	case *ast.SelectorExpr:
		if og, _, ok := c.modulePackage(f, nil); ok {
			return og.pureFunc(f.Sel.Name, nil, nil)
		}
	}
	return false
}

// pureFunc reports whether the function name of the directory is pure. visiting holds the functions being decided, which
// count as pure for the cycle that reaches them again.
func (g *dirGroup) pureFunc(name string, _ *fileCtx, visiting map[*ast.FuncDecl]bool) bool {
	decls := g.funcs[name]
	if len(decls) != 1 || decls[0].decl.Recv != nil || decls[0].decl.Body == nil {
		return false
	}
	fd := decls[0]
	if visiting[fd.decl] {
		return true
	}
	if v, ok := g.pure[fd.decl]; ok && visiting == nil {
		return v
	}
	top := visiting == nil
	if top {
		visiting = map[*ast.FuncDecl]bool{}
	}
	if len(visiting) >= pureDepth {
		return false
	}
	visiting[fd.decl] = true
	ok := g.pureBody(fd, visiting)
	delete(visiting, fd.decl)
	if top {
		if g.pure == nil {
			g.pure = map[*ast.FuncDecl]bool{}
		}
		g.pure[fd.decl] = ok
	}
	return ok
}

func (g *dirGroup) pureBody(fd *funcDecl, visiting map[*ast.FuncDecl]bool) bool {
	c := &fileCtx{g: g, s: fd.file, pkg: g.pkgOf[fd.file]}
	pure := true
	callFun := map[ast.Expr]bool{}
	ast.Inspect(fd.decl.Body, func(n ast.Node) bool {
		if x, ok := n.(*ast.CallExpr); ok {
			callFun[ast.Unparen(x.Fun)] = true
		}
		return true
	})
	ast.Inspect(fd.decl.Body, func(n ast.Node) bool {
		if !pure {
			return false
		}
		switch x := n.(type) {
		case *ast.FuncLit, *ast.GoStmt, *ast.DeferStmt, *ast.SendStmt, *ast.SelectStmt:
			pure = false
		case *ast.UnaryExpr:
			if x.Op == token.ARROW {
				pure = false
			}
		case *ast.Ident:
			// a function of the directory used as a value may be called by anything it is handed to
			if !callFun[x] && g.funcs[x.Name] != nil && g.pkgVarKindless(x.Name) {
				pure = false
			}
		case *ast.CallExpr:
			if !c.pureCallIn(x, visiting) {
				pure = false
			}
		}
		return pure
	})
	return pure
}

// pkgVarKindless is true for a name that no package variable or constant of the directory declares: a function name.
func (g *dirGroup) pkgVarKindless(name string) bool {
	for pkg := range g.vars {
		if _, ok := g.vars[pkg][name]; ok {
			return false
		}
		if _, ok := g.consts[pkg][name]; ok {
			return false
		}
	}
	return true
}

func (c *fileCtx) pureCallIn(x *ast.CallExpr, visiting map[*ast.FuncDecl]bool) bool {
	switch f := ast.Unparen(x.Fun).(type) {
	case *ast.Ident:
		switch {
		case c.declaresName(f.Name) && c.g.funcs[f.Name] == nil && !c.g.typeNames[f.Name]:
			return false // a local value called: a function value
		case pureBuiltins[f.Name] || numericTypes[f.Name] || c.g.typeNames[f.Name]:
			return true
		}
		return c.g.pureFunc(f.Name, c, visiting)
	case *ast.ArrayType, *ast.MapType, *ast.InterfaceType, *ast.StarExpr, *ast.FuncType, *ast.ChanType:
		return true // a conversion
	case *ast.SelectorExpr:
		if id, ok := f.X.(*ast.Ident); ok {
			if _, imp := c.s.imports[id.Name]; imp && !c.declaresName(id.Name) {
				if purePkgCall(c.s.callName(f)) {
					return true
				}
				if og, _, ok := c.modulePackage(f, nil); ok {
					return og.pureFunc(f.Sel.Name, nil, visiting)
				}
			}
		}
	}
	return false
}

// modulePure reports whether the function name of the package imp, a package of the module, is pure.
func (g *dirGroup) modulePure(imp, name string) bool {
	rest, ok := strings.CutPrefix(imp, g.module+"/")
	if !ok || g.module == "" {
		return false
	}
	og := g.world[rest]
	return og != nil && og.pureFunc(name, nil, nil)
}
