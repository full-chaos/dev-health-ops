package affected

import (
	"fmt"
	"go/ast"
	"go/constant"
	"go/token"
	"path"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
)

// The dynamic-read analysis: what a Go file reads, walks, runs or loads through a path the literal scan cannot see. It
// reads the AST only (no type check). Every path argument of a read, walk, exec or load call is given a kind:
//
//	const    a constant string: literals, constants, +, path.Join/filepath.Join, Clean/Dir/Base, fmt.Sprintf of constants
//	         (folded with go/constant); resolved against the directory the call runs in
//	repo     a repository path: a root locator (repoRoot(t), moduleRoot(), ...) or the file's own directory
//	         (runtime.Caller) joined with constant parts; a part that leaves the repository makes it dyn
//	temp     under t.TempDir/os.MkdirTemp/os.TempDir: no repository read
//	walkpath a path a walk hands its callback (or a directory entry of os.ReadDir): under that walk's root
//	dyn      anything else: the read is unbounded
//
// A constant relative path is resolved against the package directory in a _test.go file (go test runs a package's
// tests there) unless the package calls os.Chdir or t.Chdir; in non-test code the directory is the one of whichever
// test binary runs it, so a relative path that leaves "." is unbounded. Parameters of a function that only its own
// package can call (unexported, or declared in a _test.go file) take the kinds of the arguments at every call site
// (a fixed point; a function used as a value keeps unbounded parameters). Calls of other APIs that read a path from a
// string are not modelled; a string literal naming a classed repository path is still a literal read (scan.go).

const (
	kBottom = ""
	kConst  = "const"
	kRepo   = "repo"
	kTemp   = "temp"
	kOwn    = "ownfile"
	kWalk   = "walkpath"
	kList   = "dirlist"  // the result of os.ReadDir(root)
	kEntry  = "direntry" // one element of a dirlist
	kName   = "entryname"
	kGlob   = "globlist" // the result of filepath.Glob(pattern): paths under its root
	kGoMod  = "gomod"    // a go.mod or go.sum in an unbounded directory
	kDyn    = "dyn"
	kSet    = "set" // two or more of const, repo and temp: value holds their elements, see setElems
)

const (
	depthTree   = "tree"
	depthDir    = "dir"
	unexpressed = "unexpressed"
)

type pathInfo struct {
	kind  string
	value string // const: the value; repo: the repository path; walkpath/dirlist/direntry/entryname: the root
	depth string // walkpath: tree or dir
	// walkpath: the base-name glob every path the walk hands on past its callback's gate matches ("" = any)
	filter string
}

var dynInfo = pathInfo{kind: kDyn}

func join(a, b pathInfo) pathInfo {
	switch {
	case a.kind == kBottom:
		return b
	case b.kind == kBottom || a == b:
		return a
	}
	if ea, ok := setElems(a); ok {
		if eb, ok := setElems(b); ok {
			return mkSet(append(ea, eb...))
		}
	}
	return dynInfo
}

// rootLocator: a function whose name says it returns the repository root. A walk or read rooted at its result is
// treated as rooted at the repository root, the widest root there is, whatever the function returns.
var rootLocator = regexp.MustCompile(`^(?i:(find|get)?_?(repo|module|git|repository)_?root(dir|path)?|reporoot|moduleroot)$`)

var tempMethods = map[string]bool{"t.TempDir": true, "b.TempDir": true, "tb.TempDir": true, "f.TempDir": true}

// Calls that read the path in their first argument (parser.ParseFile: the second).
var readArg = map[string]int{
	"os.ReadFile": 0, "os.Open": 0, "os.OpenFile": 0, "os.Stat": 0, "os.Lstat": 0, "os.Readlink": 0,
	"io/ioutil.ReadFile": 0, "path/filepath.EvalSymlinks": 0, "archive/zip.OpenReader": 0, "go/parser.ParseFile": 1, "net/http.ServeFile": 2,
}

// Calls that list a directory (dir) or a tree (tree) rooted at their first argument (parser.ParseDir: the second).
var walkArg = map[string]struct {
	arg   int
	depth string
}{
	"path/filepath.WalkDir": {0, depthTree}, "path/filepath.Walk": {0, depthTree}, "path/filepath.Glob": {0, depthDir},
	"os.ReadDir": {0, depthDir}, "io/ioutil.ReadDir": {0, depthDir}, "os.DirFS": {0, depthTree}, "net/http.Dir": {0, depthTree},
	"os.OpenRoot": {0, depthTree}, "os.OpenInRoot": {0, depthTree},
	"go/parser.ParseDir": {1, depthDir}, "go/build.ImportDir": {0, depthDir},
}

// Packages whose functions read files named by configuration: any call into them is an unbounded read.
var opaqueReaders = []string{
	"github.com/99designs/gqlgen/api", "github.com/99designs/gqlgen/codegen", "github.com/compose-spec/compose-go/v2/loader",
	"github.com/compose-spec/compose-go/v2/cli", "go/build",
}

// Calls that start a program other than through exec.Command: always an unbounded run.
var otherExec = map[string]bool{"os.StartProcess": true, "syscall.Exec": true, "syscall.ForkExec": true}

// srcFile is one parsed Go file of the scan.
type srcFile struct {
	rel      string
	fset     *token.FileSet
	file     *ast.File
	test     bool
	imports  map[string]string // local name -> import path
	allPaths []string          // every import path, blank and dot imports included
	dotIO    bool              // dot-imports a package whose calls read paths
	declared map[string]bool   // declaresName answers
}

func newSrcFile(rel string, fset *token.FileSet, f *ast.File) *srcFile {
	s := &srcFile{rel: rel, fset: fset, file: f, test: strings.HasSuffix(rel, "_test.go"), imports: map[string]string{}}
	for _, im := range f.Imports {
		p, err := strconv.Unquote(im.Path.Value)
		if err != nil {
			continue
		}
		s.allPaths = append(s.allPaths, p)
		name := path.Base(p)
		if i := strings.LastIndexByte(name, '.'); strings.HasPrefix(p, "gopkg.in/") && i > 0 {
			name = name[:i]
		}
		if im.Name != nil {
			name = im.Name.Name
		}
		switch name {
		case "_":
			continue
		case ".":
			switch p {
			case "os", "io/ioutil", "path/filepath", "os/exec", "go/parser", "golang.org/x/tools/go/packages", "text/template",
				"html/template", "net/http", "archive/zip", "go/build":
				s.dotIO = true
			}
			continue
		}
		s.imports[name] = p
	}
	return s
}

// callName names a call: import path and function for a package function (`path/filepath.Join`), `recv.Method` for a
// method on a plain identifier, `.Method` for any other method, the identifier for a local function.
func (s *srcFile) callName(fun ast.Expr) string {
	switch f := fun.(type) {
	case *ast.SelectorExpr:
		if x, ok := f.X.(*ast.Ident); ok {
			if p, ok := s.imports[x.Name]; ok {
				return p + "." + f.Sel.Name
			}
			return x.Name + "." + f.Sel.Name
		}
		return "." + f.Sel.Name
	case *ast.Ident:
		return f.Name
	case *ast.IndexExpr:
		return s.callName(f.X)
	case *ast.IndexListExpr:
		return s.callName(f.X)
	case *ast.ParenExpr:
		return s.callName(f.X)
	}
	return ""
}

func shortName(name string) string {
	if i := strings.LastIndexByte(name, '.'); i >= 0 {
		return name[i+1:]
	}
	return name
}

// dirGroup is the Go files of one directory: a package and its external test package. Constants, package variables
// and function parameters are resolved within it.
type dirGroup struct {
	dir    string
	module string
	files  []*srcFile
	consts map[string]map[string]ast.Expr // package name -> constant -> value
	// constFile: the file that declares each constant; strTypes: the package-level `type T string` names
	constFile map[string]map[string]*srcFile
	strTypes  map[string]map[string]bool
	// world: every directory group of the scan by directory, to fold a constant of another package of the module
	world map[string]*dirGroup
	vars  map[string]map[string]ast.Expr // package name -> package variable assigned once at its declaration
	funcs map[string][]*funcDecl         // name -> declarations of that name (functions and methods, both packages)
	chdir bool
	// underProgram: the package is a package main or in the import closure of one (execProgramDirs)
	underProgram bool
	// chdirOpen: the package changes directory to somewhere that is not a temp dir (or not provably one)
	chdirOpen bool
	pkgOf     map[*srcFile]string
	params    map[*ast.FuncDecl][]pathInfo
	bindOK    map[*ast.FuncDecl]bool
	varKind   map[string]map[string]pathInfo
	immut     map[immutKey]bool // immutableTable/immutableValue, which every bind pass asks again
	inlining  map[*ast.FuncDecl]bool
	// execScopes: the bound scope of each function execInTemp asked about; bound: bindParams and boundChdirs ran
	execScopes map[*ast.FuncDecl]*scope
	bound      bool
	pure       map[*ast.FuncDecl]bool // pureFunc answers
	typeNames  map[string]bool        // the package-level types of the directory
}

type funcDecl struct {
	decl *ast.FuncDecl
	file *srcFile
}

// constZero reports whether e folds to the integer constant 0 (the skip of runtime.Caller that names the call's own file).
func (c *fileCtx) constZero(e ast.Expr, sc *scope) bool {
	v, ok := c.fold(e, sc, 0)
	if !ok || v.Kind() != constant.Int {
		return false
	}
	n, exact := constant.Int64Val(v)
	return exact && n == 0
}

// execProgramDirs is the package directories of every package main (non-test file) and of every in-module package they
// import, directly or not: code any program can run under a Cmd.Dir of its caller's choosing.
func execProgramDirs(byDir map[string][]*srcFile, module string) map[string]bool {
	imports := func(dir string) []string {
		var out []string
		for _, s := range byDir[dir] {
			if s.test {
				continue
			}
			for _, p := range s.allPaths {
				if rest, ok := strings.CutPrefix(p, module+"/"); ok && byDir[rest] != nil {
					out = append(out, rest)
				}
			}
		}
		return out
	}
	seen := map[string]bool{}
	var visit func(string)
	visit = func(d string) {
		if seen[d] {
			return
		}
		seen[d] = true
		for _, n := range imports(d) {
			visit(n)
		}
	}
	for d, files := range byDir {
		for _, s := range files {
			if !s.test && s.file.Name.Name == "main" {
				visit(d)
				break
			}
		}
	}
	return seen
}

func newDirGroup(dir, module string, files []*srcFile) *dirGroup {
	g := &dirGroup{dir: dir, module: module, files: files, consts: map[string]map[string]ast.Expr{}, vars: map[string]map[string]ast.Expr{},
		constFile: map[string]map[string]*srcFile{}, strTypes: map[string]map[string]bool{}, typeNames: map[string]bool{},
		funcs: map[string][]*funcDecl{}, pkgOf: map[*srcFile]string{}, params: map[*ast.FuncDecl][]pathInfo{}, bindOK: map[*ast.FuncDecl]bool{},
		varKind: map[string]map[string]pathInfo{}}
	assigned := map[string]bool{} // names assigned or address-taken anywhere in the directory
	valueUse := map[string]bool{} // function names used other than as the function of a call
	for _, s := range files {
		pkg := s.file.Name.Name
		g.pkgOf[s] = pkg
		if g.consts[pkg] == nil {
			g.consts[pkg], g.vars[pkg] = map[string]ast.Expr{}, map[string]ast.Expr{}
			g.constFile[pkg], g.strTypes[pkg] = map[string]*srcFile{}, map[string]bool{}
		}
		for _, d := range s.file.Decls {
			switch d := d.(type) {
			case *ast.GenDecl:
				var prev []ast.Expr // a const spec with no value repeats the expressions of the previous spec
				for _, spec := range d.Specs {
					if ts, ok := spec.(*ast.TypeSpec); ok {
						g.typeNames[ts.Name.Name] = true
						if id, isID := ts.Type.(*ast.Ident); isID && id.Name == "string" && !ts.Assign.IsValid() {
							g.strTypes[pkg][ts.Name.Name] = true
						}
						continue
					}
					vs, ok := spec.(*ast.ValueSpec)
					if !ok {
						continue
					}
					values := vs.Values
					if d.Tok == token.CONST {
						if len(values) == 0 && vs.Type == nil {
							values = prev
						} else {
							prev = values
						}
					}
					for i, n := range vs.Names {
						var v ast.Expr
						if i < len(values) && len(values) == len(vs.Names) {
							v = values[i]
						}
						switch {
						case d.Tok == token.CONST && v != nil:
							g.consts[pkg][n.Name] = v
							g.constFile[pkg][n.Name] = s
						case d.Tok == token.VAR && v != nil:
							g.vars[pkg][n.Name] = v
						}
					}
				}
			case *ast.FuncDecl:
				g.funcs[d.Name.Name] = append(g.funcs[d.Name.Name], &funcDecl{decl: d, file: s})
			}
		}
		calls := map[ast.Expr]bool{}
		ast.Inspect(s.file, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.CallExpr:
				calls[x.Fun] = true
				name := s.callName(x.Fun)
				if isChdir(name) {
					g.chdir, g.chdirOpen = true, true
				}
			case *ast.AssignStmt:
				for _, l := range x.Lhs {
					if id, ok := l.(*ast.Ident); ok {
						assigned[id.Name] = true
					}
				}
			case *ast.UnaryExpr:
				if id, ok := x.X.(*ast.Ident); ok && x.Op == token.AND {
					assigned[id.Name] = true
				}
			case *ast.IncDecStmt:
				if id, ok := x.X.(*ast.Ident); ok {
					assigned[id.Name] = true
				}
			}
			return true
		})
		ast.Inspect(s.file, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.FuncDecl:
				// the declared name is not a use
				if x.Body != nil {
					ast.Inspect(x.Body, func(m ast.Node) bool { return markValueUse(m, calls, valueUse) })
				}
				if x.Recv != nil {
					ast.Inspect(x.Recv, func(m ast.Node) bool { return markValueUse(m, calls, valueUse) })
				}
				return false
			case *ast.GenDecl:
				ast.Inspect(x, func(m ast.Node) bool { return markValueUse(m, calls, valueUse) })
				return false
			}
			return true
		})
	}
	for pkg, vars := range g.vars {
		for name := range vars {
			if assigned[name] {
				delete(g.vars[pkg], name)
			}
		}
	}
	for name, decls := range g.funcs {
		for _, fd := range decls {
			exported := ast.IsExported(name)
			g.bindOK[fd.decl] = !valueUse[name] && (!exported || fd.file.test) && name != "init" && name != "main"
		}
	}
	return g
}

func markValueUse(n ast.Node, calls map[ast.Expr]bool, out map[string]bool) bool {
	switch x := n.(type) {
	case *ast.Ident:
		if !calls[x] {
			out[x.Name] = true
		}
	case *ast.SelectorExpr:
		if !calls[x] {
			out[x.Sel.Name] = true
		}
		ast.Inspect(x.X, func(m ast.Node) bool { return markValueUse(m, calls, out) })
		return false
	}
	return true
}

// scope is the flat scope of one top-level function (closures included): a name assigned two different kinds is dyn.
// A closure's parameters live in a child scope.
type scope struct {
	parent *scope
	vals   map[string]pathInfo
	fixed  map[string]bool // closure parameters: an assignment makes them dyn
	tables map[string]*ast.CompositeLit
	views  map[string]*tableView
}

func newScope(parent *scope) *scope {
	return &scope{parent: parent, vals: map[string]pathInfo{}, fixed: map[string]bool{}}
}

func (sc *scope) lookup(name string) (pathInfo, bool) {
	for s := sc; s != nil; s = s.parent {
		if v, ok := s.vals[name]; ok {
			return v, true
		}
	}
	return pathInfo{}, false
}

// set joins v into name; it reports whether the value changed.
func (sc *scope) set(name string, v pathInfo) bool {
	if name == "_" {
		return false
	}
	for s := sc; s != nil; s = s.parent {
		if s.fixed[name] {
			if old := s.vals[name]; old.kind != kDyn {
				s.vals[name] = dynInfo
				return true
			}
			return false
		}
	}
	old, ok := sc.vals[name]
	if !ok {
		sc.vals[name] = v
		return true
	}
	n := join(old, v)
	if n != old {
		sc.vals[name] = n
		return true
	}
	return false
}

// fileCtx evaluates the expressions of one file.
type fileCtx struct {
	g   *dirGroup
	s   *srcFile
	pkg string
}

// fold evaluates a constant expression with go/constant.
func (c *fileCtx) fold(e ast.Expr, sc *scope, depth int) (constant.Value, bool) {
	if depth > 32 {
		return nil, false
	}
	switch x := e.(type) {
	case *ast.BasicLit:
		v := constant.MakeFromLiteral(x.Value, x.Kind, 0)
		return v, v.Kind() != constant.Unknown
	case *ast.Ident:
		if sc != nil {
			if v, ok := sc.lookup(x.Name); ok {
				if v.kind == kConst {
					return constant.MakeString(v.value), true
				}
				return nil, false
			}
		}
		if ce, ok := c.g.consts[c.pkg][x.Name]; ok {
			return c.fold(ce, nil, depth+1)
		}
		return nil, false
	case *ast.ParenExpr:
		return c.fold(x.X, sc, depth+1)
	case *ast.SelectorExpr:
		og, pkg, ok := c.modulePackage(x, sc)
		if !ok {
			return nil, false
		}
		ce, ok := og.consts[pkg][x.Sel.Name]
		if !ok || og.constFile[pkg][x.Sel.Name].test {
			return nil, false
		}
		return (&fileCtx{g: og, s: og.constFile[pkg][x.Sel.Name], pkg: pkg}).fold(ce, nil, depth+1)
	case *ast.BinaryExpr:
		a, ok1 := c.fold(x.X, sc, depth+1)
		b, ok2 := c.fold(x.Y, sc, depth+1)
		if !ok1 || !ok2 {
			return nil, false
		}
		switch x.Op {
		case token.ADD, token.SUB, token.MUL:
			if a.Kind() != b.Kind() || (x.Op != token.ADD && a.Kind() != constant.Int) {
				return nil, false
			}
			v := constant.BinaryOp(a, x.Op, b)
			return v, v.Kind() != constant.Unknown
		}
		return nil, false
	case *ast.CallExpr:
		name := c.s.callName(x.Fun)
		callArgs := x.Args
		if x.Ellipsis.IsValid() {
			el, ok := c.spreadArgs(x)
			if !ok {
				return nil, false
			}
			callArgs = el
		}
		if c.stringType(x.Fun, sc) && len(callArgs) == 1 && !x.Ellipsis.IsValid() {
			name = "string"
		}
		args := make([]constant.Value, 0, len(callArgs))
		for _, a := range callArgs {
			v, ok := c.fold(a, sc, depth+1)
			if !ok {
				return nil, false
			}
			args = append(args, v)
		}
		strs := func() ([]string, bool) {
			var out []string
			for _, a := range args {
				if a.Kind() != constant.String {
					return nil, false
				}
				out = append(out, constant.StringVal(a))
			}
			return out, true
		}
		switch name {
		case "path/filepath.Join", "path.Join":
			if s, ok := strs(); ok {
				return constant.MakeString(path.Join(s...)), true
			}
		case "path/filepath.Clean", "path.Clean", "path/filepath.FromSlash", "path/filepath.ToSlash", "string",
			"path/filepath.Dir", "path.Dir", "path/filepath.Base", "path.Base":
			if s, ok := strs(); ok && len(s) == 1 {
				switch name {
				case "path/filepath.Clean", "path.Clean":
					return constant.MakeString(path.Clean(s[0])), true
				case "path/filepath.Dir", "path.Dir":
					return constant.MakeString(path.Dir(s[0])), true
				case "path/filepath.Base", "path.Base":
					return constant.MakeString(path.Base(s[0])), true
				}
				return constant.MakeString(s[0]), true
			}
		case "fmt.Sprintf":
			if len(args) == 0 || args[0].Kind() != constant.String {
				return nil, false
			}
			var vals []any
			for _, a := range args[1:] {
				switch a.Kind() {
				case constant.String:
					vals = append(vals, constant.StringVal(a))
				case constant.Int:
					n, exact := constant.Int64Val(a)
					if !exact {
						return nil, false
					}
					vals = append(vals, n)
				case constant.Bool:
					vals = append(vals, constant.BoolVal(a))
				default:
					return nil, false
				}
			}
			out := fmt.Sprintf(constant.StringVal(args[0]), vals...)
			if strings.Contains(out, "%!") {
				return nil, false
			}
			return constant.MakeString(out), true
		}
	}
	return nil, false
}

func (c *fileCtx) foldString(e ast.Expr, sc *scope) (string, bool) {
	v, ok := c.fold(e, sc, 0)
	if !ok || v.Kind() != constant.String {
		return "", false
	}
	return constant.StringVal(v), true
}

// safeRel reports whether a constant relative part stays below the path it is joined to.
func safeRel(v string) bool {
	if strings.HasPrefix(v, "/") {
		return false
	}
	c := path.Clean(v)
	return c != ".." && !strings.HasPrefix(c, "../")
}

func (c *fileCtx) pathKind(e ast.Expr, sc *scope) pathInfo {
	if v, ok := c.foldString(e, sc); ok {
		return pathInfo{kind: kConst, value: v}
	}
	switch x := e.(type) {
	case *ast.SelectorExpr:
		if id, ok := x.X.(*ast.Ident); ok {
			if v, ok := sc.view(id.Name); ok {
				return c.fieldKind(v, x.Sel.Name, sc)
			}
		}
		return dynInfo
	case *ast.ParenExpr:
		return c.pathKind(x.X, sc)
	case *ast.Ident:
		if v, ok := sc.lookup(x.Name); ok {
			return v
		}
		if v, ok := c.g.varKind[c.pkg][x.Name]; ok {
			return v
		}
		return dynInfo
	case *ast.BinaryExpr:
		if x.Op != token.ADD {
			return dynInfo
		}
		left := c.pathKind(x.X, sc)
		rest, ok := c.foldString(x.Y, sc)
		if !ok || !strings.HasPrefix(rest, "/") {
			return dynInfo
		}
		return c.under(left, strings.TrimPrefix(rest, "/"))
	case *ast.CallExpr:
		name := c.s.callName(x.Fun)
		switch {
		case tempMethods[name] || name == "os.MkdirTemp" || name == "os.TempDir":
			return pathInfo{kind: kTemp}
		case isLocator(name):
			return pathInfo{kind: kRepo, value: "."}
		}
		if sel, ok := x.Fun.(*ast.SelectorExpr); ok && len(x.Args) == 0 {
			if id, ok := sel.X.(*ast.Ident); ok {
				v, _ := sc.lookup(id.Name)
				switch {
				case sel.Sel.Name == "Name" && v.kind == kTemp:
					return v
				case sel.Sel.Name == "Name" && v.kind == kEntry:
					return pathInfo{kind: kName, value: v.value}
				case v.kind == kBottom && sel.Sel.Name == "Name":
					if _, found := sc.lookup(id.Name); found {
						return pathInfo{}
					}
				}
			}
		}
		if x.Ellipsis.IsValid() {
			return dynInfo
		}
		switch name {
		case "path/filepath.Join", "path.Join":
			if len(x.Args) == 0 {
				return dynInfo
			}
			root := c.pathKind(x.Args[0], sc)
			if len(x.Args) == 2 && root.kind != kBottom {
				if n := c.pathKind(x.Args[1], sc); n.kind == kName && (root.kind == kRepo && n.value == root.value || root.kind == kWalk && n.value == root.value) {
					return pathInfo{kind: kWalk, value: n.value, depth: depthDir}
				}
			}
			if last, ok := c.foldString(x.Args[len(x.Args)-1], sc); ok && len(x.Args) > 1 && goModFiles[path.Base(last)] {
				return pathInfo{kind: kGoMod} // a module file: any change to one plans FULL
			}
			if len(x.Args) == 2 && (root.kind == kRepo || root.kind == kConst) {
				base := root
				if base.kind == kConst {
					base = c.resolve(base.value)
				}
				if n := c.pathKind(x.Args[1], sc); n.kind == kSet && base.kind == kRepo {
					var out []string
					for _, m := range members(n) {
						el, ok := setElems(c.under(base, m.value))
						if m.kind != kConst || !ok {
							return dynInfo
						}
						out = append(out, el...)
					}
					return mkSet(out)
				}
			}
			var rest []string
			for _, a := range x.Args[1:] {
				v, ok := c.foldString(a, sc)
				if !ok {
					// a package variable assigned once, to a constant
					if k := c.pathKind(a, sc); k.kind == kConst {
						v, ok = k.value, true
					}
				}
				if !ok {
					if root.kind == kBottom {
						return root
					}
					return dynInfo
				}
				rest = append(rest, v)
			}
			return c.under(root, path.Join(rest...))
		case "path/filepath.Dir", "path.Dir":
			if len(x.Args) != 1 {
				return dynInfo
			}
			in := c.pathKind(x.Args[0], sc)
			switch in.kind {
			case kBottom:
				return in
			case kOwn:
				return pathInfo{kind: kRepo, value: c.g.dir}
			case kTemp:
				return in
			case kRepo:
				if in.value != "." {
					return pathInfo{kind: kRepo, value: path.Dir(in.value)}
				}
			}
			return dynInfo
		case "path/filepath.Abs":
			// a relative path is made absolute against the directory the call runs in
			if len(x.Args) == 1 {
				in := c.pathKind(x.Args[0], sc)
				if in.kind == kConst {
					return c.resolve(in.value)
				}
				return in
			}
		case "path/filepath.Clean", "path.Clean", "path/filepath.FromSlash", "path/filepath.ToSlash":
			if len(x.Args) == 1 {
				return c.pathKind(x.Args[0], sc)
			}
		}
		if id, ok := x.Fun.(*ast.Ident); ok {
			if k, ok := c.inlineCall(id, x, sc); ok {
				return k
			}
		}
	}
	return dynInfo
}

// inlineLimit is the most statements a helper may have to be evaluated at a call site.
const inlineLimit = 16

// inlineCall is the kind of the result of calling a function of the directory: its body bound with the parameters at
// the kinds of this call's arguments, the kinds of its return statements joined. Only a function of one unnamed
// result, no variadic parameter, a small body and no recursion; anything else is left to the caller (dyn).
func (c *fileCtx) inlineCall(id *ast.Ident, call *ast.CallExpr, sc *scope) (pathInfo, bool) {
	if _, local := sc.lookup(id.Name); local || call.Ellipsis.IsValid() {
		return pathInfo{}, false
	}
	decls := c.g.funcs[id.Name]
	if len(decls) != 1 || decls[0].decl.Recv != nil || decls[0].decl.Body == nil || c.g.inlining[decls[0].decl] || isVariadic(decls[0].decl) {
		return pathInfo{}, false
	}
	fd := decls[0]
	if r := fd.decl.Type.Results; r == nil || len(r.List) != 1 || len(r.List[0].Names) > 0 {
		return pathInfo{}, false
	}
	var names []string
	for _, f := range fd.decl.Type.Params.List {
		if len(f.Names) == 0 {
			names = append(names, "_")
		}
		for _, n := range f.Names {
			names = append(names, n.Name)
		}
	}
	if len(names) != len(call.Args) {
		return pathInfo{}, false
	}
	stmts := 0
	ast.Inspect(fd.decl.Body, func(n ast.Node) bool {
		if _, ok := n.(ast.Stmt); ok {
			stmts++
		}
		return true
	})
	if stmts > inlineLimit {
		return pathInfo{}, false
	}
	if c.g.inlining == nil {
		c.g.inlining = map[*ast.FuncDecl]bool{}
	}
	c.g.inlining[fd.decl] = true
	defer delete(c.g.inlining, fd.decl)
	in := newScope(nil)
	for i, n := range names {
		in.vals[n] = c.pathKind(call.Args[i], sc)
	}
	callee := &fileCtx{g: c.g, s: fd.file, pkg: c.g.pkgOf[fd.file]}
	callee.bind(fd.decl.Body, in)
	result := pathInfo{}
	returns := 0
	ast.Inspect(fd.decl.Body, func(n ast.Node) bool {
		switch r := n.(type) {
		case *ast.FuncLit:
			return false
		case *ast.ReturnStmt:
			returns++
			if len(r.Results) != 1 {
				result = dynInfo
				return true
			}
			result = join(result, callee.pathKind(r.Results[0], in))
		}
		return true
	})
	if returns == 0 {
		return pathInfo{}, false
	}
	return result, true
}

// under is base joined with the constant relative part rest; dyn when rest leaves base.
func (c *fileCtx) under(base pathInfo, rest string) pathInfo {
	if rest == "" {
		rest = "."
	}
	switch base.kind {
	case kBottom, kTemp:
		return base
	case kRepo:
		r := path.Clean(base.value + "/" + rest)
		if r == ".." || strings.HasPrefix(r, "../") || strings.HasPrefix(rest, "/") {
			return dynInfo
		}
		return pathInfo{kind: kRepo, value: r}
	case kWalk:
		if !safeRel(rest) {
			return dynInfo
		}
		if path.Clean(rest) != "." {
			base.filter = "" // another path below the walked one: the gate's filter says nothing of it
		}
		return base
	case kSet:
		var out []string
		for _, m := range members(base) {
			if m.kind == kConst {
				return dynInfo
			}
			u := c.under(m, rest)
			el, ok := setElems(u)
			if !ok {
				return dynInfo
			}
			out = append(out, el...)
		}
		return mkSet(out)
	}
	return dynInfo
}

// resolve places a constant path read where the call runs: a repository path, temp (an absolute path outside the
// checkout reads no repository file) or dyn.
func (c *fileCtx) resolve(v string) pathInfo {
	if strings.HasPrefix(v, "/") {
		return pathInfo{kind: kTemp}
	}
	if v == "" {
		v = "."
	}
	if c.g.chdirOpen {
		return dynInfo
	}
	if !c.s.test {
		// non-test code runs in the directory of the test binary that links it: a path that stays below it is read
		// inside that package's directory, anything else is unbounded. Code a program (package main) links also runs
		// under that program's exec.Cmd.Dir, which the test binary's directory says nothing about: unbounded.
		if c.g.underProgram || !safeRel(v) {
			return dynInfo
		}
		return pathInfo{kind: kConst, value: path.Clean(v)}
	}
	r := path.Clean(c.g.dir + "/" + v)
	if r == ".." || strings.HasPrefix(r, "../") {
		return dynInfo
	}
	return pathInfo{kind: kRepo, value: r}
}

// bind evaluates one function body to a fixed point of its locals.
func (c *fileCtx) bind(body ast.Node, sc *scope) {
	for pass := 0; pass < 64; pass++ {
		changed := false
		set := func(name string, v pathInfo) {
			if sc.set(name, v) {
				changed = true
			}
		}
		ast.Inspect(body, func(n ast.Node) bool {
			switch a := n.(type) {
			case *ast.AssignStmt:
				if a.Tok != token.DEFINE && a.Tok != token.ASSIGN {
					for _, l := range a.Lhs {
						if id, ok := l.(*ast.Ident); ok {
							set(id.Name, dynInfo)
						}
					}
					return true
				}
				if len(a.Rhs) == 1 && len(a.Lhs) > 1 {
					var call *ast.CallExpr
					if cl, ok := a.Rhs[0].(*ast.CallExpr); ok {
						call = cl
					}
					name := ""
					if call != nil {
						name = c.s.callName(call.Fun)
					}
					for i, l := range a.Lhs {
						id, ok := l.(*ast.Ident)
						if !ok {
							continue
						}
						v := dynInfo
						switch {
						case name == "runtime.Caller" && i == 1 && len(call.Args) == 1 && c.constZero(call.Args[0], sc):
							v = pathInfo{kind: kOwn}
						case (name == "os.MkdirTemp" || name == "os.CreateTemp") && i == 0:
							v = pathInfo{kind: kTemp}
						case isLocator(name) && i == 0:
							v = pathInfo{kind: kRepo, value: "."}
						case name == "path/filepath.Abs" && i == 0 && len(call.Args) == 1:
							v = c.pathKind(call, sc)
						case name == "path/filepath.EvalSymlinks" && i == 0 && len(call.Args) == 1:
							// the real path of a temp dir is a temp dir; of any other path, unknown
							if k := c.pathKind(call.Args[0], sc); k.kind == kTemp {
								v = k
							}
						case name == "path/filepath.Glob" && i == 0 && len(call.Args) == 1:
							if r := c.walkRoot(call.Args[0], sc, true); r.kind == kRepo {
								v = pathInfo{kind: kGlob, value: r.value, depth: r.depth}
							}
						case (name == "os.ReadDir" || name == "io/ioutil.ReadDir") && i == 0 && len(call.Args) == 1:
							if r := c.walkRoot(call.Args[0], sc, false); r.kind == kRepo || r.kind == kWalk {
								v = pathInfo{kind: kList, value: r.value}
							}
						}
						set(id.Name, v)
					}
					return true
				}
				for i, l := range a.Lhs {
					if id, ok := l.(*ast.Ident); ok && i < len(a.Rhs) {
						if lit := compositeOf(a.Rhs[i]); lit != nil && a.Tok == token.DEFINE && len(a.Lhs) == len(a.Rhs) && c.g.immutable(body, id.Name, a) {
							if sc.tables == nil {
								sc.tables = map[string]*ast.CompositeLit{}
							}
							sc.tables[id.Name] = lit
						}
						set(id.Name, c.pathKind(a.Rhs[i], sc))
					}
				}
			case *ast.ValueSpec:
				for i, id := range a.Names {
					if i < len(a.Values) && len(a.Values) == len(a.Names) {
						set(id.Name, c.pathKind(a.Values[i], sc))
					} else {
						set(id.Name, dynInfo)
					}
				}
			case *ast.RangeStmt:
				val := dynInfo
				key := dynInfo
				var lit *ast.CompositeLit
				if id, ok := a.X.(*ast.Ident); ok {
					v, found := sc.lookup(id.Name)
					local, isTable := sc.table(id.Name)
					switch {
					case isTable:
						lit = local
					case v.kind == kList:
						val = pathInfo{kind: kEntry, value: v.value}
					case v.kind == kGlob:
						val = pathInfo{kind: kWalk, value: v.value, depth: v.depth}
					case !found:
						lit = compositeOf(c.g.vars[c.pkg][id.Name])
					}
				} else if cl, ok := a.X.(*ast.CompositeLit); ok {
					lit = cl
				}
				structTable := false
				if lit != nil {
					if _, isStruct := structElems(lit); isStruct {
						structTable = true
						key, _ = c.elemKinds(lit, sc)
						if id, ok := a.Value.(*ast.Ident); ok && a.Tok == token.DEFINE {
							if c.g.immutable(body, id.Name, nil) {
								sc.addView(id.Name, lit)
							} else {
								sc.addView(id.Name, nil)
							}
						}
					} else {
						key, val = c.elemKinds(lit, sc)
					}
				}
				if id, ok := a.Value.(*ast.Ident); ok && (!structTable || a.Tok != token.DEFINE) {
					sc.addView(id.Name, nil)
				}
				if id, ok := a.Key.(*ast.Ident); ok {
					set(id.Name, key)
				}
				if id, ok := a.Value.(*ast.Ident); ok {
					set(id.Name, val)
				}
			case *ast.TypeSwitchStmt:
				if as, ok := a.Assign.(*ast.AssignStmt); ok {
					for _, l := range as.Lhs {
						if id, ok := l.(*ast.Ident); ok {
							set(id.Name, dynInfo)
						}
					}
				}
			case *ast.FuncLit:
				// a closure parameter shadows any local of its name in the flat scope: both are unbounded
				for _, f := range a.Type.Params.List {
					for _, n := range f.Names {
						set(n.Name, dynInfo)
					}
				}
			}
			return true
		})
		if !changed {
			return
		}
	}
	for k := range sc.vals {
		sc.vals[k] = dynInfo
	}
}

// walkRoot places the root of a walk or a directory read: repo (with the walk's root), temp, walkpath or dyn. A
// glob's root is the part before its first wildcard segment.
func (c *fileCtx) walkRoot(arg ast.Expr, sc *scope, glob bool) pathInfo {
	in := c.pathKind(arg, sc)
	if in.kind == kConst {
		in = c.resolve(in.value)
		if in.kind == kConst { // a relative root in non-test code: under an unknown directory
			return dynInfo
		}
	}
	if glob && in.kind == kRepo {
		segs := strings.Split(in.value, "/")
		for i, s := range segs {
			if strings.ContainsAny(s, "*?[\\") {
				root := path.Join(segs[:i]...)
				if root == "" {
					root = "."
				}
				depth := depthDir
				if i < len(segs)-1 {
					depth = depthTree
				}
				return pathInfo{kind: kRepo, value: root, depth: depth}
			}
		}
		return pathInfo{kind: kRepo, value: path.Dir(in.value), depth: depthDir}
	}
	return in
}

// analyze returns every dynamic read, walk, run and load of the files of one directory.
func (g *dirGroup) analyze() []Read {
	g.prebind()
	var out []Read
	for _, s := range g.files {
		c := &fileCtx{g: g, s: s, pkg: g.pkgOf[s]}
		at := func(n ast.Node) int { return s.fset.Position(n.Pos()).Line }
		add := func(r Read) {
			r.Package, r.File, r.Test = g.dir, s.rel, s.test
			out = append(out, r)
		}
		if s.dotIO {
			add(Read{Kind: ReadDyn, Line: 1, Literal: "dot import of a reading package"})
		}
		c.eachBody(nil, func(body ast.Node, sc *scope) {
			c.record(body, sc, at, add)
		})
	}
	return out
}

// eachBody calls fn with every top-level function body (bound to the current parameter kinds) and every package-level
// variable initializer of the file.
func (c *fileCtx) eachBody(params map[*ast.FuncDecl][]pathInfo, fn func(ast.Node, *scope)) {
	if params == nil {
		params = c.g.params
	}
	for _, d := range c.s.file.Decls {
		switch d := d.(type) {
		case *ast.FuncDecl:
			if d.Body == nil {
				continue
			}
			sc := declScope(d, params)
			c.bind(d.Body, sc)
			fn(d.Body, sc)
		case *ast.GenDecl:
			if d.Tok == token.VAR {
				fn(d, newScope(nil))
			}
		}
	}
}

// declScope is the scope of a function body before it is bound: the parameters at their kinds (dyn when unknown), the
// receiver and the named results unbounded.
func declScope(d *ast.FuncDecl, params map[*ast.FuncDecl][]pathInfo) *scope {
	sc := newScope(nil)
	i := 0
	for _, f := range d.Type.Params.List {
		names := f.Names
		if len(names) == 0 {
			i++
			continue
		}
		for _, n := range names {
			v := dynInfo
			if ps, ok := params[d]; ok && i < len(ps) {
				v = ps[i]
			}
			sc.vals[n.Name] = v
			i++
		}
	}
	if d.Recv != nil {
		for _, f := range d.Recv.List {
			for _, n := range f.Names {
				sc.vals[n.Name] = dynInfo
			}
		}
	}
	if d.Type.Results != nil {
		for _, f := range d.Type.Results.List {
			for _, n := range f.Names {
				sc.vals[n.Name] = dynInfo
			}
		}
	}
	return sc
}

// execInTemp reports whether an exec.Command call runs in a temp directory: the directory it is given is under a temp
// dir at every call of its function.
func (g *dirGroup) execInTemp(s *srcFile, call *ast.CallExpr) bool {
	c := &fileCtx{g: g, s: s, pkg: g.pkgOf[s]}
	fd, ok := c.holder(call).(*ast.FuncDecl)
	if !ok || fd.Body == nil {
		return false
	}
	sc := g.execScopes[fd]
	if sc == nil {
		sc = declScope(fd, g.params)
		c.bind(fd.Body, sc)
		if g.execScopes == nil {
			g.execScopes = map[*ast.FuncDecl]*scope{}
		}
		g.execScopes[fd] = sc
	}
	return c.cmdDir(call, sc).kind == kTemp
}

// prebind computes the parameter kinds and the directory changes of the group once.
func (g *dirGroup) prebind() {
	if g.bound {
		return
	}
	g.bound = true
	g.bindParams()
	g.boundChdirs()
}

// bindParams computes the parameter kinds of the functions only this directory can call, as the least fixed point of
// the argument kinds at their call sites, and the kinds of the package variables assigned only at their declaration.
func (g *dirGroup) bindParams() {
	for pkg, vars := range g.vars {
		g.varKind[pkg] = map[string]pathInfo{}
		for name := range vars {
			g.varKind[pkg][name] = pathInfo{}
		}
	}
	state := map[*ast.FuncDecl][]pathInfo{}
	for _, decls := range g.funcs {
		for _, fd := range decls {
			n := 0
			for _, f := range fd.decl.Type.Params.List {
				if len(f.Names) == 0 {
					n++
				}
				n += len(f.Names)
			}
			ps := make([]pathInfo, n)
			if !g.bindOK[fd.decl] {
				for i := range ps {
					ps[i] = dynInfo
				}
			}
			state[fd.decl] = ps
		}
	}
	for round := 0; ; round++ {
		if round == 32 {
			for d, ps := range state {
				for i := range ps {
					ps[i] = dynInfo
				}
				state[d] = ps
			}
			break
		}
		next := map[*ast.FuncDecl][]pathInfo{}
		for d, ps := range state {
			cp := make([]pathInfo, len(ps))
			if !g.bindOK[d] {
				copy(cp, ps)
			}
			next[d] = cp
		}
		nextVars := map[string]map[string]pathInfo{}
		for _, s := range g.files {
			c := &fileCtx{g: g, s: s, pkg: g.pkgOf[s]}
			if nextVars[c.pkg] == nil {
				nextVars[c.pkg] = map[string]pathInfo{}
			}
			for name, e := range g.vars[c.pkg] {
				nextVars[c.pkg][name] = c.pathKind(e, newScope(nil))
			}
			c.eachBody(state, func(body ast.Node, sc *scope) {
				ast.Inspect(body, func(n ast.Node) bool {
					call, ok := n.(*ast.CallExpr)
					if !ok {
						return true
					}
					var name string
					switch f := call.Fun.(type) {
					case *ast.Ident:
						name = f.Name
					case *ast.SelectorExpr:
						name = f.Sel.Name
					default:
						return true
					}
					for _, fd := range g.funcs[name] {
						if !g.bindOK[fd.decl] {
							continue
						}
						ps := next[fd.decl]
						variadic := fd.decl.Type.Params.NumFields() > 0 && isVariadic(fd.decl)
						for i := range ps {
							switch {
							case call.Ellipsis.IsValid() || (variadic && i == len(ps)-1) || len(call.Args) != len(ps) && !variadic:
								ps[i] = dynInfo
							case i < len(call.Args):
								ps[i] = join(ps[i], c.pathKind(call.Args[i], sc))
							}
						}
					}
					return true
				})
			})
		}
		same := true
		for d, ps := range next {
			for i := range ps {
				if ps[i] != state[d][i] {
					same = false
				}
			}
		}
		for pkg, vars := range nextVars {
			for name, v := range vars {
				if g.varKind[pkg][name] != v {
					same = false
				}
			}
		}
		state = next
		g.varKind = nextVars
		if same {
			break
		}
	}
	for d, ps := range state {
		for i := range ps {
			if ps[i].kind == kBottom {
				ps[i] = dynInfo // no call site binds it
			}
		}
		g.params[d] = ps
	}
	for pkg, vars := range g.varKind {
		for name, v := range vars {
			if v.kind == kBottom {
				g.varKind[pkg][name] = dynInfo
			}
		}
	}
}

func isVariadic(d *ast.FuncDecl) bool {
	l := d.Type.Params.List
	if len(l) == 0 {
		return false
	}
	_, ok := l[len(l)-1].Type.(*ast.Ellipsis)
	return ok
}

// record classifies every read, walk, run and load call of one body.
func (c *fileCtx) record(body ast.Node, sc *scope, at func(ast.Node) int, add func(Read)) {
	funs := map[ast.Expr]bool{}
	ast.Inspect(body, func(n ast.Node) bool {
		if call, ok := n.(*ast.CallExpr); ok {
			funs[call.Fun] = true
		}
		return true
	})
	var visit func(n ast.Node, sc *scope)
	visit = func(n ast.Node, sc *scope) {
		ast.Inspect(n, func(m ast.Node) bool {
			if m == n {
				return true
			}
			switch x := m.(type) {
			case *ast.FuncLit:
				visit(x.Body, sc)
				return false
			case *ast.CompositeLit:
				if name := c.typeName(x.Type); name == "os/exec.Cmd" {
					add(Read{Kind: ReadEdge, Line: at(x), Target: unexpressed, Literal: "exec.Cmd literal"})
				}
			case *ast.SelectorExpr:
				if !funs[x] {
					if name := c.s.callName(x); modelledCall(name) {
						// a reading function used as a value: its calls are out of sight
						add(Read{Kind: ReadDyn, Line: at(x), Literal: name + " as a value"})
						add(Read{Kind: ReadEdge, Line: at(x), Target: unexpressed, Literal: name + " as a value"})
					}
				}
			case *ast.CallExpr:
				name := c.s.callName(x.Fun)
				if (name == "path/filepath.WalkDir" || name == "path/filepath.Walk") && len(x.Args) == 2 {
					if lit, ok := x.Args[1].(*ast.FuncLit); ok {
						root := c.walkRoot(x.Args[0], sc, false)
						filter, gate := c.walkFilter(lit)
						c.call(x, name, sc, at, func(r Read) {
							if r.Kind == ReadWalk {
								r.Filter = filter
							}
							add(r)
						})
						visit(x.Args[0], sc)
						param := dynInfo
						switch root.kind {
						case kRepo:
							param = pathInfo{kind: kWalk, value: root.value, depth: depthTree}
						case kWalk, kTemp:
							param = pathInfo{kind: root.kind, value: root.value, depth: depthTree}
						}
						// The statements before the gate see every entry; from the gate on, only the files it passes.
						inner, gated := newScope(sc), newScope(sc)
						if ps := lit.Type.Params.List; len(ps) > 0 && len(ps[0].Names) > 0 {
							name := ps[0].Names[0].Name
							inner.vals[name], gated.vals[name] = param, param
							if param.kind == kWalk {
								gated.vals[name] = pathInfo{kind: kWalk, value: param.value, depth: param.depth, filter: filter}
							}
							inner.fixed[name], gated.fixed[name] = true, true
							assignedIn(lit.Body, name, func() { inner.vals[name], gated.vals[name] = dynInfo, dynInfo })
						}
						for i, st := range lit.Body.List {
							if filter != "" && i >= gate {
								visit(st, gated)
							} else {
								visit(st, inner)
							}
						}
						return false
					}
				}
				if name == "os.DirFS" && len(x.Args) == 1 {
					if filter, ok := c.fsFilter(x); ok {
						c.call(x, name, sc, at, func(r Read) {
							if r.Kind == ReadWalk {
								r.Filter = filter
							}
							add(r)
						})
						visit(x.Args[0], sc)
						return false
					}
				}
				c.call(x, name, sc, at, add)
			}
			return true
		})
	}
	visit(body, sc)
}

// fsFilter is the walk filter of an os.DirFS value passed directly to a function of the directory whose FS parameter is
// used only as the tree of fs.WalkDir calls with a gated callback and as the FS of fs.ReadFile calls of the callback's
// own gated name: the files those walks act on are the ones the gates pass.
func (c *fileCtx) fsFilter(dirFS *ast.CallExpr) (string, bool) {
	body := c.enclosingBody(dirFS)
	if body == nil {
		return "", false
	}
	var call *ast.CallExpr
	arg := -1
	ast.Inspect(body, func(n ast.Node) bool {
		if p, ok := n.(*ast.CallExpr); ok {
			for i, a := range p.Args {
				if a == dirFS {
					call, arg = p, i
				}
			}
		}
		return true
	})
	if call == nil || call.Ellipsis.IsValid() {
		return "", false
	}
	id, ok := call.Fun.(*ast.Ident)
	if !ok {
		return "", false
	}
	decls := c.g.funcs[id.Name]
	if len(decls) != 1 || decls[0].decl.Recv != nil || decls[0].decl.Body == nil || isVariadic(decls[0].decl) {
		return "", false
	}
	var param string
	i := 0
	for _, f := range decls[0].decl.Type.Params.List {
		for _, n := range f.Names {
			if i == arg {
				param = n.Name
			}
			i++
		}
	}
	if param == "" || param == "_" || i != len(call.Args) {
		return "", false
	}
	callee := &fileCtx{g: c.g, s: decls[0].file, pkg: c.g.pkgOf[decls[0].file]}
	return callee.fsParamFilter(decls[0].decl.Body, param)
}

func (c *fileCtx) fsParamFilter(body *ast.BlockStmt, param string) (string, bool) {
	allowed := map[*ast.Ident]bool{}
	var filters []string
	ok := countAssigns(body, param) == 0 // the parameter is never assigned
	ast.Inspect(body, func(n ast.Node) bool {
		call, isCall := n.(*ast.CallExpr)
		if !isCall || len(call.Args) != 3 || c.pkgCallName(call.Fun) != "io/fs.WalkDir" {
			return true
		}
		fsys, isID := call.Args[0].(*ast.Ident)
		lit, isLit := call.Args[2].(*ast.FuncLit)
		if !isID || fsys.Name != param || !isLit {
			return true
		}
		filter, gate := c.walkFilter(lit)
		if filter == "" {
			ok = false
			return true
		}
		filters = append(filters, filter)
		allowed[fsys] = true
		var name string
		if ps := lit.Type.Params.List; len(ps) > 0 && len(ps[0].Names) > 0 {
			name = ps[0].Names[0].Name
		}
		for i, st := range lit.Body.List {
			ast.Inspect(st, func(m ast.Node) bool {
				rf, isCall := m.(*ast.CallExpr)
				if !isCall || len(rf.Args) != 2 || c.pkgCallName(rf.Fun) != "io/fs.ReadFile" {
					return true
				}
				a0, ok0 := rf.Args[0].(*ast.Ident)
				a1, ok1 := rf.Args[1].(*ast.Ident)
				if ok0 && ok1 && a0.Name == param && i >= gate && name != "" && a1.Name == name && countAssigns(lit.Body, name) == 0 {
					allowed[a0] = true
				}
				return true
			})
		}
		return true
	})
	ast.Inspect(body, func(n ast.Node) bool {
		if id, isID := n.(*ast.Ident); isID && id.Name == param && !allowed[id] {
			ok = false
		}
		return ok
	})
	if !ok || len(filters) == 0 {
		return "", false
	}
	sort.Strings(filters)
	var out []string
	for _, f := range filters {
		for _, part := range strings.Split(f, "|") {
			if !slices.Contains(out, part) {
				out = append(out, part)
			}
		}
	}
	return strings.Join(out, "|"), true
}

// walkFilter finds the gate of a filepath.Walk/WalkDir callback: a statement that returns nil for every file whose
// name lacks a suffix (`if ... || !strings.HasSuffix(path, ".go") || ... { return nil }`, or `if strings.HasSuffix(path,
// ".go") && ... { ... }` followed only by `return nil`), with nothing before it that reads, writes, keeps a name or
// skips a directory for a file. The walk then acts on no other file: it returns the filter "*<suffix>" and the index of
// the gate statement, or "" when the callback has no such gate.
func (c *fileCtx) walkFilter(lit *ast.FuncLit) (string, int) {
	var names []string
	for _, f := range lit.Type.Params.List {
		for _, n := range f.Names {
			names = append(names, n.Name)
		}
	}
	if len(names) == 0 || names[0] == "_" {
		return "", 0
	}
	entry, errName := "", ""
	if len(names) > 1 {
		entry = names[1]
	}
	if len(names) > 2 {
		errName = names[2]
	}
	body := lit.Body
	// a name for the walked path -> its assignments so far, each a top-level statement that keeps the path's suffix
	aliases := map[string]int{names[0]: 0}
	locals := map[string]bool{}
	upto := 0 // the statements before the one being read
	// The entry methods are trusted only on the parameter itself: once the statements so far (the one being read
	// included) declare or assign its name again (`d := probe{}`, `var d probe`, a closure parameter), it may hold
	// another value, whose methods may read.
	trusted := func() string {
		for _, st := range body.List[:upto+1] {
			if countAssigns(st, entry) > 0 {
				return ""
			}
		}
		return entry
	}
	before := func(name string) int {
		n := 0
		for _, st := range body.List[:upto] {
			n += countAssigns(st, name)
		}
		return n
	}
	var subject func(e ast.Expr) bool
	subject = func(e ast.Expr) bool {
		switch x := ast.Unparen(e).(type) {
		case *ast.Ident:
			n, ok := aliases[x.Name]
			return ok && before(x.Name) == n
		case *ast.CallExpr:
			if sel, ok := x.Fun.(*ast.SelectorExpr); ok && len(x.Args) == 0 && sel.Sel.Name == "Name" {
				id, ok := sel.X.(*ast.Ident)
				return ok && trusted() != "" && trusted() != "_" && id.Name == trusted()
			}
			switch c.pkgCallName(x.Fun) {
			case "path/filepath.ToSlash", "path/filepath.FromSlash", "path/filepath.Base", "path/filepath.Clean", "path.Base", "path.Clean":
				return len(x.Args) == 1 && subject(x.Args[0])
			}
		}
		return false
	}
	suffix := func(e ast.Expr) string {
		x, ok := ast.Unparen(e).(*ast.CallExpr)
		if !ok || len(x.Args) != 2 || c.pkgCallName(x.Fun) != "strings.HasSuffix" || !subject(x.Args[0]) {
			return ""
		}
		lit, ok := ast.Unparen(x.Args[1]).(*ast.BasicLit)
		if !ok || lit.Kind != token.STRING {
			return ""
		}
		s, err := strconv.Unquote(lit.Value)
		if err != nil || s == "" || strings.ContainsAny(s, "/*?[\\") {
			return ""
		}
		return s
	}
	// The extension gate: `ext := filepath.Ext(path)` then `if !exts[ext] { return nil }`, exts a package map literal of
	// constant extensions that nothing writes: the walk acts on those extensions only.
	extAliases := map[string]int{}
	isExtOf := func(e ast.Expr) bool {
		x, ok := ast.Unparen(e).(*ast.CallExpr)
		if !ok || len(x.Args) != 1 {
			return false
		}
		n := c.pkgCallName(x.Fun)
		return (n == "path/filepath.Ext" || n == "path.Ext") && subject(x.Args[0])
	}
	extSet := func(e ast.Expr) []string {
		idx, ok := ast.Unparen(e).(*ast.IndexExpr)
		if !ok {
			return nil
		}
		if id, ok := ast.Unparen(idx.Index).(*ast.Ident); ok {
			if n, ok := extAliases[id.Name]; !ok || before(id.Name) != n {
				return nil
			}
		} else if !isExtOf(idx.Index) {
			return nil
		}
		m, ok := idx.X.(*ast.Ident)
		if !ok || countAssigns(body, m.Name) > 0 {
			return nil
		}
		return c.constExtSet(m.Name)
	}
	for i, st := range body.List {
		upto = i
		if ifs, ok := st.(*ast.IfStmt); ok && ifs.Init == nil && ifs.Else == nil {
			// if A || !strings.HasSuffix(p, S) || B { return nil }
			if len(ifs.Body.List) == 1 && returnsNil(ifs.Body.List[0]) {
				ds := chain(ifs.Cond, token.LOR)
				for j, d := range ds {
					if u, ok := ast.Unparen(d).(*ast.UnaryExpr); ok && u.Op == token.NOT {
						if s := suffix(u.X); s != "" && c.allPure(ds[:j], trusted()) {
							return "*" + s, i
						}
						if exts := extSet(u.X); len(exts) > 0 && c.allPure(ds[:j], trusted()) {
							return "*" + strings.Join(exts, "|*"), i
						}
					}
				}
			}
			// if A && strings.HasSuffix(p, S) && B { ... } return nil
			rest := true
			for _, r := range body.List[i+1:] {
				rest = rest && returnsNil(r)
			}
			if cs := chain(ifs.Cond, token.LAND); rest && len(body.List) > i+1 {
				for j, d := range cs {
					if s := suffix(d); s != "" && c.allPure(cs[:j], trusted()) {
						return "*" + s, i
					}
				}
			}
		}
		if !c.preGate(st, false, errName, trusted(), locals) {
			return "", 0
		}
		if a, ok := st.(*ast.AssignStmt); ok {
			for j, l := range a.Lhs {
				id, ok := l.(*ast.Ident)
				if !ok {
					continue
				}
				if a.Tok == token.DEFINE {
					locals[id.Name] = true
				}
				keeps := len(a.Lhs) == len(a.Rhs) && subject(a.Rhs[j])
				if call, ok := a.Rhs[0].(*ast.CallExpr); ok && len(a.Rhs) == 1 && j == 0 && len(call.Args) == 2 &&
					c.pkgCallName(call.Fun) == "path/filepath.Rel" && subject(call.Args[1]) {
					keeps = true
				}
				if keeps {
					aliases[id.Name]++
				} else if id.Name != names[0] {
					delete(aliases, id.Name)
				}
				if len(a.Lhs) == len(a.Rhs) && isExtOf(a.Rhs[j]) {
					extAliases[id.Name]++
				} else {
					delete(extAliases, id.Name)
				}
			}
		}
	}
	return "", 0
}

// preGate reports whether a statement before a walk callback's gate does nothing but return early for some entries:
// pure conditions and values, locals of the callback, `return nil`, the walk's own error, or filepath.SkipDir under an
// IsDir test (a file that returned SkipDir would skip the rest of its directory).
func (c *fileCtx) preGate(st ast.Stmt, dir bool, errName, entry string, locals map[string]bool) bool {
	switch s := st.(type) {
	case *ast.EmptyStmt:
		return true
	case *ast.AssignStmt:
		if s.Tok != token.DEFINE && s.Tok != token.ASSIGN {
			return false
		}
		for _, l := range s.Lhs {
			id, ok := l.(*ast.Ident)
			if !ok || (s.Tok == token.ASSIGN && id.Name != "_" && !locals[id.Name]) {
				return false
			}
		}
		return c.allPure(s.Rhs, entry)
	case *ast.BlockStmt:
		for _, x := range s.List {
			if !c.preGate(x, dir, errName, entry, locals) {
				return false
			}
		}
		return true
	case *ast.IfStmt:
		if !c.allPure([]ast.Expr{s.Cond}, entry) || s.Init != nil && !c.preGate(s.Init, dir, errName, entry, locals) {
			return false
		}
		inDir := dir
		for _, cj := range chain(s.Cond, token.LAND) {
			if call, ok := ast.Unparen(cj).(*ast.CallExpr); ok && entryMethod(call, entry) {
				if sel := call.Fun.(*ast.SelectorExpr); sel.Sel.Name == "IsDir" {
					inDir = true
				}
			}
		}
		if !c.preGate(s.Body, inDir, errName, entry, locals) {
			return false
		}
		return s.Else == nil || c.preGate(s.Else, dir, errName, entry, locals)
	case *ast.RangeStmt:
		// a loop over a pure value, its variables new locals of the loop
		if s.Tok != token.DEFINE || !c.allPure([]ast.Expr{s.X}, entry) {
			return false
		}
		inner := map[string]bool{}
		for k := range locals {
			inner[k] = true
		}
		for _, e := range []ast.Expr{s.Key, s.Value} {
			if id, ok := e.(*ast.Ident); ok {
				inner[id.Name] = true
			}
		}
		return c.preGate(s.Body, dir, errName, entry, inner)
	case *ast.SwitchStmt:
		if s.Init != nil || (s.Tag != nil && !c.allPure([]ast.Expr{s.Tag}, entry)) {
			return false
		}
		for _, cl := range s.Body.List {
			cc, ok := cl.(*ast.CaseClause)
			if !ok || !c.allPure(cc.List, entry) {
				return false
			}
			for _, x := range cc.Body {
				if !c.preGate(x, dir, errName, entry, locals) {
					return false
				}
			}
		}
		return true
	case *ast.ReturnStmt:
		if len(s.Results) != 1 {
			return false
		}
		if returnsNil(s) {
			return true
		}
		switch r := ast.Unparen(s.Results[0]).(type) {
		case *ast.Ident:
			return errName != "" && errName != "_" && r.Name == errName
		case *ast.SelectorExpr:
			n := c.pkgCallName(r)
			return dir && (n == "path/filepath.SkipDir" || n == "io/fs.SkipDir")
		}
	}
	return false
}

// allPure reports whether the expressions call nothing but string and path functions and the Name/IsDir/Type/Mode
// methods of an entry: they read, write and keep nothing.
func (c *fileCtx) allPure(es []ast.Expr, entry string) bool {
	pure := true
	for _, e := range es {
		ast.Inspect(e, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.FuncLit:
				pure = false
			case *ast.CallExpr:
				name := c.pkgCallName(x.Fun)
				switch {
				case strings.HasPrefix(name, "strings.") || strings.HasPrefix(name, "path."):
				case name == "path/filepath.Rel" || name == "path/filepath.ToSlash" || name == "path/filepath.FromSlash" ||
					name == "path/filepath.Base" || name == "path/filepath.Ext" || name == "path/filepath.Clean" ||
					name == "path/filepath.Dir" || name == "path/filepath.Join":
				case c.statsModuleFile(x) || c.pureCall(x):
				default:
					if !entryMethod(x, entry) {
						pure = false
					}
				}
			}
			return pure
		})
	}
	return pure
}

var pureMethods = map[string]bool{"Name": true, "IsDir": true, "Type": true, "Mode": true, "IsRegular": true}

// entryMethod reports whether a call is a Name/IsDir/Type/Mode/IsRegular method of the walk callback's own entry
// parameter (fs.DirEntry or os.FileInfo), or of such a call's result (entry.Mode().IsRegular()): a method of any other
// value may read or write.
func entryMethod(call *ast.CallExpr, entry string) bool {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || len(call.Args) != 0 || !pureMethods[sel.Sel.Name] || entry == "" || entry == "_" {
		return false
	}
	switch x := ast.Unparen(sel.X).(type) {
	case *ast.Ident:
		return x.Name == entry
	case *ast.CallExpr:
		return entryMethod(x, entry)
	}
	return false
}

func returnsNil(st ast.Stmt) bool {
	r, ok := st.(*ast.ReturnStmt)
	if !ok || len(r.Results) != 1 {
		return false
	}
	id, ok := r.Results[0].(*ast.Ident)
	return ok && id.Name == "nil"
}

// chain flattens a || b || c (op LOR) or a && b && c (op LAND) into its operands, in order.
func chain(e ast.Expr, op token.Token) []ast.Expr {
	if b, ok := ast.Unparen(e).(*ast.BinaryExpr); ok && b.Op == op {
		return append(chain(b.X, op), chain(b.Y, op)...)
	}
	return []ast.Expr{e}
}

// countAssigns is the number of assignments to name in body (any assignment, increment or range variable).
func countAssigns(body ast.Node, name string) int {
	n := 0
	ast.Inspect(body, func(m ast.Node) bool {
		switch x := m.(type) {
		case *ast.AssignStmt:
			for _, l := range x.Lhs {
				if id, ok := l.(*ast.Ident); ok && id.Name == name {
					n++
				}
			}
		case *ast.ValueSpec:
			for _, id := range x.Names {
				if id.Name == name {
					n++
				}
			}
		case *ast.Field:
			for _, id := range x.Names {
				if id.Name == name {
					n++
				}
			}
		case *ast.IncDecStmt:
			if id, ok := x.X.(*ast.Ident); ok && id.Name == name {
				n++
			}
		case *ast.RangeStmt:
			for _, e := range []ast.Expr{x.Key, x.Value} {
				if id, ok := e.(*ast.Ident); ok && id.Name == name {
					n++
				}
			}
		case *ast.UnaryExpr:
			if id, ok := x.X.(*ast.Ident); ok && x.Op == token.AND && id.Name == name {
				n += 1000 // its address escapes: never an alias
			}
		}
		return true
	})
	return n
}

// assignedIn calls fn when name is assigned anywhere in body.
func assignedIn(body ast.Node, name string, fn func()) {
	ast.Inspect(body, func(n ast.Node) bool {
		if a, ok := n.(*ast.AssignStmt); ok {
			for _, l := range a.Lhs {
				if id, ok := l.(*ast.Ident); ok && id.Name == name {
					fn()
				}
			}
		}
		return true
	})
}

func (c *fileCtx) typeName(e ast.Expr) string {
	switch t := e.(type) {
	case *ast.SelectorExpr:
		if x, ok := t.X.(*ast.Ident); ok {
			if p, ok := c.s.imports[x.Name]; ok {
				return p + "." + t.Sel.Name
			}
		}
	case *ast.StarExpr:
		return c.typeName(t.X)
	}
	return ""
}

// call classifies one call.
func (c *fileCtx) call(x *ast.CallExpr, name string, sc *scope, at func(ast.Node) int, add func(Read)) {
	line := at(x)
	dyn := func(what string) { add(Read{Kind: ReadDyn, Line: line, Literal: what}) }
	walk := func(r pathInfo, depth, what string) {
		switch r.kind {
		case kRepo:
			if r.depth != "" {
				depth = r.depth
			}
			add(Read{Kind: ReadWalk, Line: line, Root: r.value, Depth: depth, Literal: what})
		case kWalk:
			add(Read{Kind: ReadWalk, Line: line, Root: r.value, Depth: depthTree, Literal: what})
		case kTemp:
		case kSet:
			for _, m := range members(r) {
				switch m.kind {
				case kRepo:
					add(Read{Kind: ReadWalk, Line: line, Root: m.value, Depth: depth, Literal: what})
				case kTemp:
				default:
					add(Read{Kind: ReadWalk, Line: line, Root: unexpressed, Depth: "-", Literal: what})
				}
			}
		default:
			add(Read{Kind: ReadWalk, Line: line, Root: unexpressed, Depth: "-", Literal: what})
		}
	}
	read := func(arg ast.Expr, what string) {
		in := c.pathKind(arg, sc)
		parts := []pathInfo{in}
		if in.kind == kSet {
			parts = members(in)
		}
		for _, p := range parts {
			if p.kind == kConst {
				p = c.resolve(p.value)
			}
			switch p.kind {
			case kRepo:
				add(Read{Kind: ReadPath, Line: line, Path: p.value, Literal: what})
			case kWalk:
				add(Read{Kind: ReadWalk, Line: line, Root: p.value, Depth: p.depth, Filter: p.filter, Literal: what})
			case kTemp, kConst, kGoMod:
				// a temp file, a module file, or a path below the directory of the test binary that runs this code
			default:
				dyn(what)
				c.tailPattern(arg, sc, line, what, add)
			}
		}
	}
	_, isRead := readArg[name]
	_, isWalk := walkArg[name]
	if x.Ellipsis.IsValid() && (isRead || isWalk) {
		dyn(name)
		return
	}
	if i, ok := readArg[name]; ok && i < len(x.Args) {
		read(x.Args[i], name)
		return
	}
	if w, ok := walkArg[name]; ok && w.arg < len(x.Args) {
		root := c.walkRoot(x.Args[w.arg], sc, name == "path/filepath.Glob")
		walk(root, w.depth, name)
		if root.kind != kRepo && root.kind != kWalk && root.kind != kTemp {
			c.tailPattern(x.Args[w.arg], sc, line, name, add)
		}
		return
	}
	if strings.HasSuffix(name, ".ParseFiles") || strings.HasSuffix(name, ".ParseGlob") {
		for _, a := range x.Args {
			if strings.HasSuffix(name, "ParseGlob") {
				walk(c.walkRoot(a, sc, true), depthDir, name)
			} else {
				read(a, name)
			}
		}
		return
	}
	// A directory change moves every relative read of the test binary that runs this code: the call is unbounded.
	if isChdir(name) {
		if len(x.Args) != 1 || c.pathKind(x.Args[0], sc).kind != kTemp {
			dyn(name)
		}
		return
	}
	if otherExec[name] {
		add(Read{Kind: ReadEdge, Line: line, Target: unexpressed, Literal: name})
		return
	}
	switch name {
	case "os/exec.Command", "os/exec.CommandContext":
		c.execEdges(x, name, sc, line, add)
		return
	case "golang.org/x/tools/go/packages.Load":
		c.loadEdges(x, sc, line, add)
		return
	}
	for _, p := range opaqueReaders {
		if strings.HasPrefix(name, p+".") {
			dyn(name)
			return
		}
	}
}

// goFlagValue: flags of the go command that take the next argument as their value.
var goFlagValue = map[string]bool{
	"-o": true, "-tags": true, "-ldflags": true, "-gcflags": true, "-asmflags": true, "-modfile": true, "-mod": true, "-p": true,
	"-exec": true, "-toolexec": true, "-overlay": true, "-pkgdir": true, "-buildmode": true, "-compiler": true,
	"-installsuffix": true, "-coverpkg": true, "-covermode": true, "-coverprofile": true, "-run": true, "-bench": true,
	"-count": true, "-timeout": true, "-cpu": true, "-parallel": true, "-benchtime": true, "-blockprofile": true,
	"-cpuprofile": true, "-memprofile": true, "-mutexprofile": true, "-outputdir": true, "-trace": true, "-shuffle": true,
	"-skip": true, "-fuzz": true, "-fuzztime": true, "-vettool": true, "-f": true, "-pgo": true, "-buildvcs": true,
	"-memprofilerate": true, "-blockprofilerate": true, "-mutexprofilefraction": true, "-fuzzminimizetime": true,
}

// goFlagBool: flags of the go command that take no value.
var goFlagBool = map[string]bool{
	"-a": true, "-n": true, "-race": true, "-msan": true, "-asan": true, "-v": true, "-work": true, "-x": true,
	"-trimpath": true, "-cover": true, "-i": true, "-json": true, "-m": true, "-e": true, "-deps": true, "-test": true,
	"-export": true, "-find": true, "-compiled": true, "-failfast": true, "-short": true, "-benchmem": true, "-c": true,
	"-linkshared": true, "-modcacherw": true, "-reuse": true, "-u": true, "-retracted": true, "-versions": true,
}

// execEdges records what an exec.Command call runs: for `go run|build|install|test|vet|list` with constant arguments
// the packages it builds (an edge to each), for any other program or argument an unbounded run.
func (c *fileCtx) execEdges(x *ast.CallExpr, name string, sc *scope, line int, add func(Read)) {
	edge := func(target, what string) {
		add(Read{Kind: ReadEdge, Line: line, Target: target, Literal: what})
	}
	args := x.Args
	if name == "os/exec.CommandContext" {
		if len(args) == 0 {
			edge(unexpressed, name)
			return
		}
		args = args[1:]
	}
	// A constant argument that names Markdown files (`*.md`, `README.md`; a named constant, a + chain or fmt.Sprintf of
	// constants) is a read of them whatever the program.
	scanArgs := args
	if x.Ellipsis.IsValid() {
		if el, ok := c.spreadArgs(x); ok {
			scanArgs = el
			if name == "os/exec.CommandContext" && len(scanArgs) > 0 {
				scanArgs = scanArgs[1:]
			}
		}
	}
	for _, a := range scanArgs {
		ast.Inspect(a, func(n ast.Node) bool {
			switch n.(type) {
			case *ast.BasicLit, *ast.Ident, *ast.SelectorExpr, *ast.BinaryExpr, *ast.ParenExpr, *ast.CallExpr:
			default:
				return true
			}
			if v, ok := c.foldString(n.(ast.Expr), sc); ok {
				if i := strings.LastIndexByte(v, ':'); i >= 0 {
					v = v[i+1:] // git rev:path and pathspec magic name the path after the colon
				}
				if mdEvidence(v) {
					add(Read{Kind: ReadPattern, Line: line, Path: v, Literal: name})
				}
			}
			return true
		})
	}
	if x.Ellipsis.IsValid() || len(args) == 0 {
		edge(unexpressed, name+" with spread arguments")
		return
	}
	var vals []string
	for _, a := range args {
		v, ok := c.foldString(a, sc)
		if !ok {
			v = unknownArg // allowed only as the value of a flag that does not choose packages
		}
		vals = append(vals, v)
	}
	what := strings.ReplaceAll(strings.Join(vals, " "), unknownArg, "?")
	if vals[0] != "go" || len(vals) > 1 && vals[1] == unknownArg {
		edge(unexpressed, what)
		return
	}
	if len(vals) < 2 {
		edge(unexpressed, what)
		return
	}
	verb := vals[1]
	switch verb {
	case "env", "version", "help":
		return
	case "run", "build", "install", "test", "vet", "list":
	default:
		edge(unexpressed, what)
		return
	}
	var targets []string
	rest := vals[2:]
	for i := 0; i < len(rest); i++ {
		a := rest[i]
		if a == "-args" || a == "--" {
			break
		}
		if a == unknownArg {
			edge(unexpressed, what)
			return
		}
		if strings.HasPrefix(a, "-") {
			flag := a
			if j := strings.IndexByte(a, '='); j >= 0 {
				flag = a[:j]
			}
			flag = "-" + strings.TrimLeft(flag, "-")
			switch {
			case goFlagUnbounded[flag]:
				edge(unexpressed, what)
				return
			case strings.Contains(a, "="):
			case goFlagValue[flag]:
				i++
			case goFlagBool[flag]:
			default:
				edge(unexpressed, what)
				return
			}
			continue
		}
		if verb == "run" {
			if strings.HasSuffix(a, ".go") {
				for ; i < len(rest) && strings.HasSuffix(rest[i], ".go"); i++ {
					targets = append(targets, rest[i])
				}
			} else {
				targets = append(targets, a)
			}
			break
		}
		targets = append(targets, a)
	}
	if len(targets) == 0 {
		targets = []string{"."}
	}
	if c.cmdMutated(x) {
		edge(unexpressed, what+" (its Args or Path is set later)")
		return
	}
	dir := c.cmdDir(x, sc)
	for _, t := range targets {
		edge(c.target(dir, t), what)
	}
}

// cmdDir is the directory an exec.Command result runs in: its Dir field as the function sets it, else the package
// directory (a _test.go file) or an unknown directory (non-test code). A Dir set only on some path joins the default;
// a command passed on, returned or stored may get its directory elsewhere: unknown.
func (c *fileCtx) cmdDir(x *ast.CallExpr, sc *scope) pathInfo {
	def := pathInfo{kind: kRepo, value: c.g.dir}
	if !c.s.test || c.g.chdirOpen {
		def = dynInfo
	}
	holder := c.holder(x)
	if holder == nil {
		return dynInfo
	}
	var v string
	ast.Inspect(holder, func(n ast.Node) bool {
		switch a := n.(type) {
		case *ast.AssignStmt:
			for i, r := range a.Rhs {
				if r == x && i < len(a.Lhs) {
					if id, ok := a.Lhs[i].(*ast.Ident); ok {
						v = id.Name
					}
				}
			}
		case *ast.ValueSpec:
			for i, r := range a.Values {
				if r == x && i < len(a.Names) {
					v = a.Names[i].Name
				}
			}
		}
		return true
	})
	if v == "" || v == "_" {
		// a result not kept in a variable runs in the default directory only when a method is called on it directly
		direct := false
		ast.Inspect(holder, func(n ast.Node) bool {
			if s, ok := n.(*ast.SelectorExpr); ok && s.X == x {
				direct = true
			}
			return true
		})
		if direct {
			return def
		}
		return dynInfo
	}
	return c.fieldDir(holder, v, def, nil, sc)
}

func (c *fileCtx) holder(n ast.Node) ast.Node {
	for _, d := range c.s.file.Decls {
		if d.Pos() <= n.Pos() && n.End() <= d.End() {
			return d
		}
	}
	return nil
}

// fieldDir is the value of v.Dir where v is used: an assignment that follows v's only definition in the same block
// replaces the default, any other assignment joins it. v passed to a call (other than except), returned, sent, stored
// or defined twice may run in another directory: unknown.
func (c *fileCtx) fieldDir(holder ast.Node, v string, def pathInfo, except *ast.CallExpr, sc *scope) pathInfo {
	unconditional := map[*ast.AssignStmt]bool{}
	ast.Inspect(holder, func(n ast.Node) bool {
		var list []ast.Stmt
		switch b := n.(type) {
		case *ast.BlockStmt:
			list = b.List
		case *ast.CaseClause:
			list = b.Body
		case *ast.CommClause:
			list = b.Body
		default:
			return true
		}
		defined := false
		for _, st := range list {
			if definesName(st, v) {
				defined = true
				continue
			}
			if a, ok := st.(*ast.AssignStmt); ok && defined {
				unconditional[a] = true
			}
		}
		return true
	})
	var uncond, cond pathInfo
	defs := 0
	escaped := false
	ast.Inspect(holder, func(n ast.Node) bool {
		switch a := n.(type) {
		case *ast.AssignStmt:
			for i, l := range a.Lhs {
				if id, ok := l.(*ast.Ident); ok && id.Name == v {
					defs++
				}
				if s, ok := l.(*ast.SelectorExpr); ok && s.Sel.Name == "Dir" && refersTo(s.X, v) {
					k := dynInfo
					if i < len(a.Rhs) && len(a.Rhs) == len(a.Lhs) {
						k = c.dirKind(a.Rhs[i], sc)
					}
					if unconditional[a] {
						uncond = join(uncond, k)
					} else {
						cond = join(cond, k)
					}
				}
			}
			for _, r := range a.Rhs {
				if refersTo(r, v) {
					escaped = true
				}
			}
		case *ast.ValueSpec:
			for _, nm := range a.Names {
				if nm.Name == v {
					defs++
				}
			}
			for _, r := range a.Values {
				if refersTo(r, v) {
					escaped = true
				}
			}
		case *ast.CallExpr:
			if a == except {
				return true
			}
			for _, arg := range a.Args {
				if refersTo(arg, v) {
					escaped = true
				}
			}
		case *ast.ReturnStmt:
			for _, r := range a.Results {
				if refersTo(r, v) {
					escaped = true
				}
			}
		case *ast.SendStmt:
			if refersTo(a.Value, v) {
				escaped = true
			}
		case *ast.CompositeLit:
			for _, e := range a.Elts {
				if refersTo(e, v) {
					escaped = true
				}
			}
		}
		return true
	})
	switch {
	case escaped:
		return dynInfo
	case defs != 1:
		return join(join(def, uncond), cond)
	case uncond.kind != kBottom:
		return join(uncond, cond)
	}
	return join(def, cond)
}

func definesName(st ast.Stmt, v string) bool {
	switch s := st.(type) {
	case *ast.AssignStmt:
		for _, l := range s.Lhs {
			if id, ok := l.(*ast.Ident); ok && id.Name == v {
				return true
			}
		}
	case *ast.DeclStmt:
		if g, ok := s.Decl.(*ast.GenDecl); ok {
			for _, spec := range g.Specs {
				if vs, ok := spec.(*ast.ValueSpec); ok {
					for _, n := range vs.Names {
						if n.Name == v {
							return true
						}
					}
				}
			}
		}
	}
	return false
}

func refersTo(e ast.Expr, name string) bool {
	switch x := e.(type) {
	case *ast.Ident:
		return x.Name == name
	case *ast.UnaryExpr:
		return refersTo(x.X, name)
	case *ast.ParenExpr:
		return refersTo(x.X, name)
	case *ast.KeyValueExpr:
		return refersTo(x.Value, name)
	}
	return false
}

// dirKind places a directory a command or a loader runs in.
func (c *fileCtx) dirKind(e ast.Expr, sc *scope) pathInfo {
	in := c.pathKind(e, sc)
	if in.kind == kConst {
		in = c.resolve(in.value)
		if in.kind == kConst {
			return dynInfo
		}
	}
	switch in.kind {
	case kRepo, kTemp:
		return in
	}
	return dynInfo
}

// target resolves one package argument of the go command or of packages.Load run in dir: a repository directory, a
// tree (`dir/...`), a pattern of directories when dir is unknown (`**/rel`), or unexpressed. A standard-library
// package reads no repository file.
func (c *fileCtx) target(dir pathInfo, t string) string {
	if q, ok := strings.CutPrefix(t, "file="); ok {
		t = path.Dir(q)
		if !strings.HasPrefix(t, ".") && !strings.HasPrefix(t, "/") {
			t = "./" + t
		}
	}
	if strings.Contains(t, "=") || strings.Contains(t, "@") || strings.HasPrefix(t, "/") {
		return unexpressed
	}
	if strings.HasSuffix(t, ".go") {
		t = path.Dir(t)
		if !strings.HasPrefix(t, ".") {
			t = "./" + t
		}
	}
	tree := false
	if t == "..." || strings.HasSuffix(t, "/...") {
		tree = true
		t = strings.TrimSuffix(strings.TrimSuffix(t, "..."), "/")
		if t == "" {
			t = "."
		}
	}
	if strings.Contains(t, "...") {
		return unexpressed
	}
	var repo string
	switch {
	case t == "all":
		return "..."
	case t == "std" || t == "cmd":
		return ""
	case t == "." || t == ".." || strings.HasPrefix(t, "./") || strings.HasPrefix(t, "../"):
		switch dir.kind {
		case kRepo:
			repo = path.Clean(dir.value + "/" + t)
			if repo == ".." || strings.HasPrefix(repo, "../") {
				return unexpressed
			}
		case kTemp:
			return unexpressed
		default:
			rel := path.Clean(t)
			for strings.HasPrefix(rel, "../") {
				rel = strings.TrimPrefix(rel, "../")
			}
			if rel == "." || rel == ".." {
				if tree {
					return "..."
				}
				return unexpressed
			}
			repo = "**/" + rel
		}
	case c.g.module != "" && (t == c.g.module || strings.HasPrefix(t, c.g.module+"/")):
		repo = strings.TrimPrefix(strings.TrimPrefix(t, c.g.module), "/")
		if repo == "" {
			repo = "."
		}
	default:
		if first := strings.SplitN(t, "/", 2)[0]; strings.Contains(first, ".") {
			return unexpressed // another module, run against this tree
		}
		return "" // the standard library
	}
	if strings.Contains(repo, "/testdata/") || strings.HasPrefix(repo, "testdata/") || strings.HasSuffix(repo, "/testdata") || repo == "testdata" {
		return unexpressed // a program the scan does not read
	}
	if tree {
		if repo == "." {
			return "..."
		}
		return repo + "/..."
	}
	return repo
}

// loadEdges records the packages a packages.Load call loads, in the directory of its Config.
func (c *fileCtx) loadEdges(x *ast.CallExpr, sc *scope, line int, add func(Read)) {
	edge := func(target string) {
		add(Read{Kind: ReadEdge, Line: line, Target: target, Literal: "packages.Load"})
	}
	if len(x.Args) == 0 || x.Ellipsis.IsValid() {
		edge(unexpressed)
		return
	}
	dir := c.configDir(x.Args[0], sc, x)
	pats := x.Args[1:]
	if len(pats) == 0 {
		edge(c.target(dir, "."))
		return
	}
	for _, p := range pats {
		v, ok := c.patternValue(p, sc)
		if !ok {
			edge(unexpressed)
			continue
		}
		if t := c.target(dir, v); t != "" {
			edge(t)
		}
	}
}

func (c *fileCtx) patternValue(e ast.Expr, sc *scope) (string, bool) {
	if v, ok := c.foldString(e, sc); ok {
		return v, true
	}
	if id, ok := e.(*ast.Ident); ok {
		if v, ok := sc.lookup(id.Name); ok && v.kind == kConst {
			return v.value, true
		}
	}
	return "", false
}

// configDir is the Dir of a packages.Config argument: a composite literal written inline or held by a local that
// takes no other value; the default is the directory the call runs in.
func (c *fileCtx) configDir(e ast.Expr, sc *scope, call *ast.CallExpr) pathInfo {
	def := pathInfo{kind: kRepo, value: c.g.dir}
	if !c.s.test || c.g.chdirOpen {
		def = dynInfo
	}
	lit := compositeOf(e)
	holder := c.holder(call)
	if lit == nil {
		id, ok := e.(*ast.Ident)
		if !ok || holder == nil {
			return dynInfo
		}
		var lits []*ast.CompositeLit
		other := false
		ast.Inspect(holder, func(n ast.Node) bool {
			switch a := n.(type) {
			case *ast.AssignStmt:
				for i, l := range a.Lhs {
					if lid, ok := l.(*ast.Ident); ok && lid.Name == id.Name && i < len(a.Rhs) {
						if cl := compositeOf(a.Rhs[i]); cl != nil {
							lits = append(lits, cl)
						} else {
							other = true
						}
					}
				}
			case *ast.ValueSpec:
				for i, n := range a.Names {
					if n.Name == id.Name {
						if i < len(a.Values) {
							if cl := compositeOf(a.Values[i]); cl != nil {
								lits = append(lits, cl)
								continue
							}
						}
						other = true
					}
				}
			}
			return true
		})
		if other || len(lits) != 1 {
			return dynInfo
		}
		return c.fieldDir(holder, id.Name, c.litDir(lits[0], sc, def), call, sc)
	}
	return c.litDir(lit, sc, def)
}

func (c *fileCtx) litDir(lit *ast.CompositeLit, sc *scope, def pathInfo) pathInfo {
	for _, el := range lit.Elts {
		kv, ok := el.(*ast.KeyValueExpr)
		if !ok {
			return dynInfo
		}
		if k, ok := kv.Key.(*ast.Ident); ok && k.Name == "Dir" {
			return c.dirKind(kv.Value, sc)
		}
	}
	return def
}

func compositeOf(e ast.Expr) *ast.CompositeLit {
	switch x := e.(type) {
	case *ast.UnaryExpr:
		if x.Op == token.AND {
			return compositeOf(x.X)
		}
	case *ast.CompositeLit:
		return x
	case *ast.ParenExpr:
		return compositeOf(x.X)
	}
	return nil
}

// goModFiles: the module files a repository-root search reads; a change to any of them plans FULL (plan.go).
var goModFiles = map[string]bool{"go.mod": true, "go.sum": true, "go.work": true, "go.work.sum": true}

// isLocator: a call whose function, or whose package, is named as a repository-root locator (repoRoot(t),
// moduleroot.Root()).
func isLocator(name string) bool {
	short := shortName(name)
	if rootLocator.MatchString(short) {
		return true
	}
	if i := strings.LastIndexByte(name, '.'); i > 0 {
		return rootLocator.MatchString(path.Base(name[:i]))
	}
	return false
}

// elemKinds is the key and value kinds of ranging over a composite literal: a map's keys and values, a slice's
// elements (its keys are indexes). An element that is not a path is dyn.
func (c *fileCtx) elemKinds(lit *ast.CompositeLit, sc *scope) (key, val pathInfo) {
	_, isMap := lit.Type.(*ast.MapType)
	_, isSlice := lit.Type.(*ast.ArrayType)
	if !isMap && !isSlice || len(lit.Elts) == 0 {
		return dynInfo, dynInfo
	}
	key, val = pathInfo{}, pathInfo{}
	if isSlice {
		key = dynInfo
	}
	for _, el := range lit.Elts {
		if kv, ok := el.(*ast.KeyValueExpr); ok {
			if isMap {
				key = join(key, c.pathKind(kv.Key, sc))
			}
			val = join(val, c.pathKind(kv.Value, sc))
			continue
		}
		val = join(val, c.pathKind(el, sc))
	}
	return key, val
}

// unknownArg stands for an argument of the go command that does not fold to a constant.
const unknownArg = "\x00?"

// goFlagUnbounded: flags of the go command that change the directory, the module or the tools it runs.
var goFlagUnbounded = map[string]bool{"-C": true, "-exec": true, "-toolexec": true, "-modfile": true, "-overlay": true}

// modelledCall: a package function the analysis classifies when it is called.
func modelledCall(name string) bool {
	if _, ok := readArg[name]; ok {
		return true
	}
	if _, ok := walkArg[name]; ok {
		return true
	}
	switch name {
	case "os/exec.Command", "os/exec.CommandContext", "golang.org/x/tools/go/packages.Load":
		return true
	}
	if isChdir(name) {
		return true
	}
	if otherExec[name] {
		return true
	}
	for _, p := range opaqueReaders {
		if strings.HasPrefix(name, p+".") {
			return true
		}
	}
	return false
}

// cmdMutated reports whether the function that holds an exec.Command call sets the Args or Path of its result.
func (c *fileCtx) cmdMutated(x *ast.CallExpr) bool {
	holder := c.holder(x)
	if holder == nil {
		return true
	}
	v := assignedTo(holder, x)
	if v == "" {
		return false
	}
	mutated := false
	ast.Inspect(holder, func(n ast.Node) bool {
		if a, ok := n.(*ast.AssignStmt); ok {
			for _, l := range a.Lhs {
				// any receiver: an alias of the command (c2 := c) is not followed
				if s, ok := l.(*ast.SelectorExpr); ok && (s.Sel.Name == "Args" || s.Sel.Name == "Path") {
					mutated = true
				}
			}
		}
		return true
	})
	return mutated || escapes(holder, v)
}

// assignedTo is the name of the variable x is assigned to in holder ("" when x is not kept in one).
func assignedTo(holder ast.Node, x ast.Expr) string {
	var v string
	ast.Inspect(holder, func(n ast.Node) bool {
		switch a := n.(type) {
		case *ast.AssignStmt:
			for i, r := range a.Rhs {
				if r == x && i < len(a.Lhs) {
					if id, ok := a.Lhs[i].(*ast.Ident); ok {
						v = id.Name
					}
				}
			}
		case *ast.ValueSpec:
			for i, r := range a.Values {
				if r == x && i < len(a.Names) {
					v = a.Names[i].Name
				}
			}
		}
		return true
	})
	return v
}

func isChdir(name string) bool {
	return name == "os.Chdir" || name == "syscall.Chdir" || strings.HasSuffix(name, ".Chdir")
}

// escapes reports whether the variable v is used in holder other than as the receiver of a selector (c.Dir, c.Run()):
// passed on, assigned to another name, returned or stored, so a later write to its Args or Path is out of sight.
func escapes(holder ast.Node, v string) bool {
	found := false
	var visit func(n ast.Node, receiver bool)
	visit = func(n ast.Node, receiver bool) {
		if n == nil || found {
			return
		}
		switch x := n.(type) {
		case *ast.Ident:
			if x.Name == v && !receiver {
				found = true
			}
			return
		case *ast.SelectorExpr:
			visit(x.X, true)
			return
		case *ast.AssignStmt:
			for _, l := range x.Lhs {
				if id, ok := l.(*ast.Ident); ok && id.Name == v {
					continue
				}
				visit(l, false)
			}
			for _, r := range x.Rhs {
				visit(r, false)
			}
			return
		case *ast.ValueSpec:
			for _, r := range x.Values {
				visit(r, false)
			}
			return
		}
		ast.Inspect(n, func(m ast.Node) bool {
			if m == n {
				return true
			}
			visit(m, false)
			return false
		})
	}
	visit(holder, false)
	return found
}

// mdEvidence reports whether a constant names Markdown files: it ends in .md, or it is a glob whose last segment can
// end in .md (no extension, or a wildcard in it). A path-like constant only (a valid patterns.tsv field).
func mdEvidence(v string) bool {
	if v == "" || strings.ContainsAny(v, " \t\n\r\"'=;(){}<>|&$`\\,:") {
		return false // not a path or a pattern: a message, a flag value, a shell line
	}
	base := path.Base(v)
	ext := path.Ext(base)
	if _, err := path.Match(base, ""); err != nil {
		return false
	}
	return ext == ".md" || strings.ContainsAny(base, "*?[") && (ext == "" || strings.ContainsAny(ext, "*?["))
}

// tailPattern records a patterns.tsv row for an unbounded path that ends in a constant naming Markdown files:
// filepath.Join(dir, "docs", "index.md") or dir+"/README.md" reads **/docs/index.md, **/README.md.
func (c *fileCtx) tailPattern(arg ast.Expr, sc *scope, line int, what string, add func(Read)) {
	var tail []string
	switch x := ast.Unparen(arg).(type) {
	case *ast.CallExpr:
		if n := c.s.callName(x.Fun); n == "path/filepath.Join" || n == "path.Join" {
			args := x.Args
			if x.Ellipsis.IsValid() {
				var ok bool
				if args, ok = c.spreadArgs(x); !ok {
					args = nil
				}
			}
			for i := len(args) - 1; i >= 1; i-- {
				v, ok := c.foldString(args[i], sc)
				if !ok {
					break
				}
				tail = append([]string{v}, tail...)
			}
		}
	case *ast.BinaryExpr:
		if v, ok := c.foldString(x.Y, sc); ok && x.Op == token.ADD {
			tail = []string{v}
		}
	}
	if len(tail) == 0 {
		return
	}
	s := strings.TrimLeft(path.Clean(strings.Join(tail, "/")), "/")
	for strings.HasPrefix(s, "../") {
		s = strings.TrimPrefix(s, "../")
	}
	if s != "" && s != "." && s != ".." && mdEvidence(s) {
		add(Read{Kind: ReadPattern, Line: line, Path: "**/" + s, Literal: what})
	}
}

// constExtSet is the keys of the package map variable name when it is a literal of constant extensions (".go": true)
// and no code of the directory writes it, passes it on or takes its address; nil otherwise.
func (c *fileCtx) constExtSet(name string) []string {
	lit := compositeOf(c.g.vars[c.pkg][name])
	if lit == nil {
		return nil
	}
	if _, ok := lit.Type.(*ast.MapType); !ok {
		return nil
	}
	var exts []string
	for _, el := range lit.Elts {
		kv, ok := el.(*ast.KeyValueExpr)
		if !ok {
			return nil
		}
		k, ok := kv.Key.(*ast.BasicLit)
		v, vok := kv.Value.(*ast.Ident)
		if !ok || k.Kind != token.STRING || !vok || v.Name != "true" {
			return nil
		}
		s, err := strconv.Unquote(k.Value)
		if err != nil || !strings.HasPrefix(s, ".") || strings.ContainsAny(s, "/*?[\\| \t\n") {
			return nil
		}
		exts = append(exts, s)
	}
	for _, s := range c.g.files {
		written := false
		var stack []ast.Node
		ast.Inspect(s.file, func(n ast.Node) bool {
			if n == nil {
				stack = stack[:len(stack)-1]
				return true
			}
			parent := ast.Node(nil)
			if len(stack) > 0 {
				parent = stack[len(stack)-1]
			}
			stack = append(stack, n)
			id, ok := n.(*ast.Ident)
			if !ok || id.Name != name {
				return true
			}
			switch p := parent.(type) {
			case *ast.ValueSpec:
				return true // the declaration
			case *ast.RangeStmt:
				if p.X == id {
					return true
				}
			case *ast.IndexExpr:
				if p.X == id && len(stack) >= 3 {
					switch gp := stack[len(stack)-3].(type) {
					case *ast.AssignStmt:
						for _, l := range gp.Lhs {
							if l == p {
								written = true
							}
						}
						return true
					case *ast.IncDecStmt:
						written = true
						return true
					case *ast.UnaryExpr:
						written = written || gp.Op == token.AND
						return true
					}
					return true
				}
			case *ast.SelectorExpr:
				if p.Sel == id {
					return true // a field or method of another value
				}
			case *ast.KeyValueExpr:
				if p.Key == id {
					return true // a struct field name
				}
			}
			written = true
			return true
		})
		if written {
			return nil
		}
	}
	sort.Strings(exts)
	return exts
}

// pkgCallName is callName for a package function whose qualifier the file never declares again (a parameter, :=, var,
// range variable, function or type named like the import): "" when the qualifier may name a local value instead.
func (c *fileCtx) pkgCallName(fun ast.Expr) string {
	sel, ok := ast.Unparen(fun).(*ast.SelectorExpr)
	if !ok {
		return ""
	}
	x, ok := sel.X.(*ast.Ident)
	if !ok {
		return ""
	}
	if _, imp := c.s.imports[x.Name]; !imp || c.declaresName(x.Name) {
		return ""
	}
	return c.s.callName(fun)
}

// declaresName reports whether the file declares name anywhere, at any depth.
func (c *fileCtx) declaresName(name string) bool {
	if v, ok := c.s.declared[name]; ok {
		return v
	}
	found := c.scanDeclares(name)
	if c.s.declared == nil {
		c.s.declared = map[string]bool{}
	}
	c.s.declared[name] = found
	return found
}

func (c *fileCtx) scanDeclares(name string) bool {
	found := false
	ast.Inspect(c.s.file, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.Field:
			for _, id := range x.Names {
				found = found || id.Name == name
			}
		case *ast.AssignStmt:
			if x.Tok == token.DEFINE {
				for _, l := range x.Lhs {
					if id, ok := l.(*ast.Ident); ok && id.Name == name {
						found = true
					}
				}
			}
		case *ast.ValueSpec:
			for _, id := range x.Names {
				found = found || id.Name == name
			}
		case *ast.RangeStmt:
			for _, e := range []ast.Expr{x.Key, x.Value} {
				if id, ok := e.(*ast.Ident); ok && id.Name == name && x.Tok == token.DEFINE {
					found = true
				}
			}
		case *ast.FuncDecl:
			found = found || x.Recv == nil && x.Name.Name == name
		case *ast.TypeSpec:
			found = found || x.Name.Name == name
		}
		return !found
	})
	return found
}

// modulePackage resolves pkg.Name, pkg an import of a package of the module that the scan holds: its directory group and
// the name of its non-test package. The qualifier must not be declared again in the file or the scope.
func (c *fileCtx) modulePackage(x *ast.SelectorExpr, sc *scope) (*dirGroup, string, bool) {
	id, ok := x.X.(*ast.Ident)
	if !ok || c.g.module == "" {
		return nil, "", false
	}
	if sc != nil {
		if _, local := sc.lookup(id.Name); local {
			return nil, "", false
		}
	}
	imp, ok := c.s.imports[id.Name]
	if !ok || c.declaresName(id.Name) {
		return nil, "", false
	}
	rest, ok := strings.CutPrefix(imp, c.g.module+"/")
	if !ok {
		return nil, "", false
	}
	og := c.g.world[rest]
	if og == nil {
		return nil, "", false
	}
	for _, f := range og.files {
		if !f.test {
			return og, og.pkgOf[f], true
		}
	}
	return nil, "", false
}

// stringType reports whether fun names a type declared as `type T string`, in this package or in a package of the module:
// T(v) then converts a string.
func (c *fileCtx) stringType(fun ast.Expr, sc *scope) bool {
	switch f := ast.Unparen(fun).(type) {
	case *ast.Ident:
		if sc != nil {
			if _, local := sc.lookup(f.Name); local {
				return false
			}
		}
		return c.g.strTypes[c.pkg][f.Name]
	case *ast.SelectorExpr:
		if og, pkg, ok := c.modulePackage(f, sc); ok {
			return og.strTypes[pkg][f.Sel.Name]
		}
	}
	return false
}

// spreadArgs are the arguments a call's `s...` stands for, when s is a slice literal, an append of values to one, or a
// local or package variable defined once as one and only spread, ranged over or measured: the arguments before the
// spread, then the elements.
func (c *fileCtx) spreadArgs(call *ast.CallExpr) ([]ast.Expr, bool) {
	if len(call.Args) == 0 {
		return nil, false
	}
	last := call.Args[len(call.Args)-1]
	el, ok := c.sliceElems(last, call, 0)
	if !ok {
		return nil, false
	}
	return append(append([]ast.Expr{}, call.Args[:len(call.Args)-1]...), el...), true
}

func (c *fileCtx) sliceElems(e ast.Expr, at ast.Node, depth int) ([]ast.Expr, bool) {
	if depth > 8 {
		return nil, false
	}
	switch x := ast.Unparen(e).(type) {
	case *ast.CompositeLit:
		if _, ok := x.Type.(*ast.ArrayType); !ok {
			return nil, false
		}
		for _, el := range x.Elts {
			if _, kv := el.(*ast.KeyValueExpr); kv {
				return nil, false
			}
		}
		return x.Elts, true
	case *ast.CallExpr:
		if id, ok := x.Fun.(*ast.Ident); !ok || id.Name != "append" || c.declaresName("append") || len(x.Args) == 0 || x.Ellipsis.IsValid() {
			return nil, false
		}
		base, ok := c.sliceElems(x.Args[0], at, depth+1)
		if !ok {
			return nil, false
		}
		return append(append([]ast.Expr{}, base...), x.Args[1:]...), true
	case *ast.Ident:
		if def := c.packageSlice(x.Name); def != nil {
			return c.sliceElems(def, at, depth+1)
		}
		body := c.enclosingBody(at)
		if body == nil || countAssigns(body, x.Name) != 1 {
			return nil, false
		}
		var def ast.Expr
		ast.Inspect(body, func(n ast.Node) bool {
			switch d := n.(type) {
			case *ast.AssignStmt:
				if len(d.Lhs) == len(d.Rhs) {
					for i, l := range d.Lhs {
						if id, ok := l.(*ast.Ident); ok && id.Name == x.Name {
							def = d.Rhs[i]
						}
					}
				}
			case *ast.ValueSpec:
				if len(d.Names) == len(d.Values) {
					for i, id := range d.Names {
						if id.Name == x.Name {
							def = d.Values[i]
						}
					}
				}
			}
			return true
		})
		if def == nil || !onlySpreadUses(body, x.Name) {
			return nil, false
		}
		return c.sliceElems(def, at, depth+1)
	}
	return nil, false
}

// packageSlice is the initializer of a package variable the directory assigns only at its declaration.
func (c *fileCtx) packageSlice(name string) ast.Expr {
	return c.g.vars[c.pkg][name]
}

// enclosingBody is the body of the top-level function declaration that holds n.
func (c *fileCtx) enclosingBody(n ast.Node) ast.Node {
	for _, d := range c.s.file.Decls {
		if fd, ok := d.(*ast.FuncDecl); ok && fd.Body != nil && fd.Pos() <= n.Pos() && n.End() <= fd.End() {
			return fd.Body
		}
	}
	return nil
}

// onlySpreadUses reports whether every use of the variable name in body is its definition, a spread argument, a range
// operand or the argument of len: nothing can then change its elements.
func onlySpreadUses(body ast.Node, name string) bool {
	ok := true
	allowed := map[*ast.Ident]bool{}
	ast.Inspect(body, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.AssignStmt:
			for _, l := range x.Lhs {
				if id, isID := l.(*ast.Ident); isID {
					allowed[id] = true
				}
			}
		case *ast.ValueSpec:
			for _, id := range x.Names {
				allowed[id] = true
			}
		case *ast.CallExpr:
			if x.Ellipsis.IsValid() && len(x.Args) > 0 {
				if id, isID := x.Args[len(x.Args)-1].(*ast.Ident); isID {
					allowed[id] = true
				}
			}
			if f, isID := x.Fun.(*ast.Ident); isID && f.Name == "len" && len(x.Args) == 1 {
				if id, isID := x.Args[0].(*ast.Ident); isID {
					allowed[id] = true
				}
			}
		case *ast.RangeStmt:
			if id, isID := x.X.(*ast.Ident); isID {
				allowed[id] = true
			}
		}
		return true
	})
	ast.Inspect(body, func(n ast.Node) bool {
		if id, isID := n.(*ast.Ident); isID && id.Name == name && !allowed[id] {
			ok = false
		}
		return ok
	})
	return ok
}

// statsModuleFile reports whether a call asks whether a go.mod, go.sum or go.work file exists below a directory
// (os.Stat(filepath.Join(dir, "go.mod"))): no Markdown change creates or removes one.
func (c *fileCtx) statsModuleFile(x *ast.CallExpr) bool {
	if n := c.pkgCallName(x.Fun); (n != "os.Stat" && n != "os.Lstat") || len(x.Args) != 1 {
		return false
	}
	j, ok := x.Args[0].(*ast.CallExpr)
	if !ok || j.Ellipsis.IsValid() || len(j.Args) < 2 {
		return false
	}
	if n := c.pkgCallName(j.Fun); n != "path/filepath.Join" && n != "path.Join" {
		return false
	}
	v, ok := c.foldString(j.Args[len(j.Args)-1], nil)
	return ok && goModFiles[path.Base(v)]
}
