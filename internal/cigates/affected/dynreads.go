package affected

import (
	"fmt"
	"go/ast"
	"go/constant"
	"go/token"
	"path"
	"regexp"
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
	rel     string
	fset    *token.FileSet
	file    *ast.File
	test    bool
	imports map[string]string // local name -> import path
	dotIO   bool              // dot-imports a package whose calls read paths
}

func newSrcFile(rel string, fset *token.FileSet, f *ast.File) *srcFile {
	s := &srcFile{rel: rel, fset: fset, file: f, test: strings.HasSuffix(rel, "_test.go"), imports: map[string]string{}}
	for _, im := range f.Imports {
		p, err := strconv.Unquote(im.Path.Value)
		if err != nil {
			continue
		}
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
	vars   map[string]map[string]ast.Expr // package name -> package variable assigned once at its declaration
	funcs  map[string][]*funcDecl         // name -> declarations of that name (functions and methods, both packages)
	chdir  bool
	// chdirOpen: the package changes directory to somewhere that is not a temp dir (or not provably one)
	chdirOpen bool
	pkgOf     map[*srcFile]string
	params    map[*ast.FuncDecl][]pathInfo
	bindOK    map[*ast.FuncDecl]bool
	varKind   map[string]map[string]pathInfo
	immut     map[immutKey]bool // immutableTable/immutableValue, which every bind pass asks again
}

type funcDecl struct {
	decl *ast.FuncDecl
	file *srcFile
}

func newDirGroup(dir, module string, files []*srcFile) *dirGroup {
	g := &dirGroup{dir: dir, module: module, files: files, consts: map[string]map[string]ast.Expr{}, vars: map[string]map[string]ast.Expr{},
		funcs: map[string][]*funcDecl{}, pkgOf: map[*srcFile]string{}, params: map[*ast.FuncDecl][]pathInfo{}, bindOK: map[*ast.FuncDecl]bool{},
		varKind: map[string]map[string]pathInfo{}}
	assigned := map[string]bool{} // names assigned or address-taken anywhere in the directory
	valueUse := map[string]bool{} // function names used other than as the function of a call
	for _, s := range files {
		pkg := s.file.Name.Name
		g.pkgOf[s] = pkg
		if g.consts[pkg] == nil {
			g.consts[pkg], g.vars[pkg] = map[string]ast.Expr{}, map[string]ast.Expr{}
		}
		for _, d := range s.file.Decls {
			switch d := d.(type) {
			case *ast.GenDecl:
				for _, spec := range d.Specs {
					vs, ok := spec.(*ast.ValueSpec)
					if !ok {
						continue
					}
					for i, n := range vs.Names {
						var v ast.Expr
						if i < len(vs.Values) && len(vs.Values) == len(vs.Names) {
							v = vs.Values[i]
						}
						switch {
						case d.Tok == token.CONST && v != nil:
							g.consts[pkg][n.Name] = v
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
		args := make([]constant.Value, 0, len(x.Args))
		if x.Ellipsis.IsValid() {
			return nil, false
		}
		for _, a := range x.Args {
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
			var rest []string
			for _, a := range x.Args[1:] {
				v, ok := c.foldString(a, sc)
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
		case "path/filepath.Clean", "path.Clean", "path/filepath.Abs", "path/filepath.FromSlash", "path/filepath.ToSlash":
			if len(x.Args) == 1 {
				return c.pathKind(x.Args[0], sc)
			}
		}
	}
	return dynInfo
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
		// inside that package's directory, anything else is unbounded.
		if !safeRel(v) {
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
						case name == "runtime.Caller" && i == 1:
							v = pathInfo{kind: kOwn}
						case (name == "os.MkdirTemp" || name == "os.CreateTemp") && i == 0:
							v = pathInfo{kind: kTemp}
						case isLocator(name) && i == 0:
							v = pathInfo{kind: kRepo, value: "."}
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
	g.bindParams()
	g.boundChdirs()
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
			c.bind(d.Body, sc)
			fn(d.Body, sc)
		case *ast.GenDecl:
			if d.Tok == token.VAR {
				fn(d, newScope(nil))
			}
		}
	}
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
						c.call(x, name, sc, at, add)
						visit(x.Args[0], sc)
						inner := newScope(sc)
						param := dynInfo
						switch root.kind {
						case kRepo:
							param = pathInfo{kind: kWalk, value: root.value, depth: depthTree}
						case kWalk, kTemp:
							param = pathInfo{kind: root.kind, value: root.value, depth: depthTree}
						}
						if ps := lit.Type.Params.List; len(ps) > 0 && len(ps[0].Names) > 0 {
							name := ps[0].Names[0].Name
							inner.vals[name] = param
							inner.fixed[name] = true
							assignedIn(lit.Body, name, func() { inner.vals[name] = dynInfo })
						}
						visit(lit.Body, inner)
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
				add(Read{Kind: ReadWalk, Line: line, Root: p.value, Depth: p.depth, Literal: what})
			case kTemp, kConst, kGoMod:
				// a temp file, a module file, or a path below the directory of the test binary that runs this code
			default:
				dyn(what)
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
		walk(c.walkRoot(x.Args[w.arg], sc, name == "path/filepath.Glob"), w.depth, name)
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
