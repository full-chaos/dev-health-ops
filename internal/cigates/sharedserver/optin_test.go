package sharedserver_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

const (
	containersImportPath = "github.com/full-chaos/dev-health-ops/internal/testsupport/containers"

	venueoracleImportPath = "github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"

	// sharedTestMain is the TestMain of a package that runs on shared servers.
	sharedTestMain = "package opted\n\nimport (\n\t\"os\"\n\t\"testing\"\n\n\t\"" + containersImportPath + "\"\n)\n\n" +
		"func TestMain(m *testing.M) { os.Exit(containers.RunWithSharedServers(m.Run)) }\n"
)

// optsIn reports whether a test file holds a TestMain that runs the package
// on shared servers: the body of func TestMain(m *testing.M) passes the
// result of the harness's RunWithSharedServers (through the file's real
// import of it) to os.Exit, and every use of m lies inside the argument of
// that call, where the callback is m.Run or a func literal that returns
// only run expressions (m.Run(), venueoracle.RunTests(m), a same-file func
// whose every return is one of these for its *testing.M parameter, or a local
// set once from one of these before any return). Comments and string literals are not code and never count.
func optsIn(src string) (bool, error) {
	file, err := parser.ParseFile(token.NewFileSet(), "main_test.go", src, 0)
	if err != nil {
		return false, err
	}
	imports := map[string]string{}
	for _, spec := range file.Imports {
		path, err := strconv.Unquote(spec.Path.Value)
		if err != nil {
			return false, err
		}
		name := path[strings.LastIndex(path, "/")+1:]
		if spec.Name != nil {
			name = spec.Name.Name
		}
		imports[name] = path
	}
	isCall := func(call *ast.CallExpr, path, function string) bool {
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || selector.Sel.Name != function {
			return false
		}
		pkg, ok := selector.X.(*ast.Ident)
		return ok && pkg.Obj == nil && imports[pkg.Name] == path
	}
	type visit struct {
		fn  *ast.FuncDecl
		arg int
	}
	memo := map[visit]bool{}
	unparen := func(e ast.Expr) ast.Expr {
		for {
			paren, ok := e.(*ast.ParenExpr)
			if !ok {
				return e
			}
			e = paren.X
		}
	}
	isM := func(e ast.Expr, p *ast.Object) bool {
		id, ok := unparen(e).(*ast.Ident)
		return ok && id.Obj == p
	}
	var runExpr func(e ast.Expr, p *ast.Object) bool
	var runnerBody func(body *ast.BlockStmt, p *ast.Object) bool
	runExpr = func(e ast.Expr, p *ast.Object) bool {
		call, ok := unparen(e).(*ast.CallExpr)
		if !ok {
			return false
		}
		plain := *call
		plain.Fun = unparen(call.Fun)
		if selector, ok := plain.Fun.(*ast.SelectorExpr); ok && selector.Sel.Name == "Run" && len(plain.Args) == 0 && isM(selector.X, p) {
			return true
		}
		if isCall(&plain, venueoracleImportPath, "RunTests") {
			return len(plain.Args) == 1 && isM(plain.Args[0], p)
		}
		id, ok := plain.Fun.(*ast.Ident)
		if !ok || id.Obj == nil {
			return false
		}
		fn, ok := id.Obj.Decl.(*ast.FuncDecl)
		if !ok || fn.Recv != nil || fn.Body == nil {
			return false
		}
		for arg, a := range plain.Args {
			if !isM(a, p) {
				continue
			}
			key := visit{fn, arg}
			if done, seen := memo[key]; seen {
				if done {
					return true
				}
				continue
			}
			memo[key] = false
			var param *ast.Object
			index := 0
			for _, field := range fn.Type.Params.List {
				for _, name := range field.Names {
					if star, ok := field.Type.(*ast.StarExpr); ok && index == arg {
						if sel, ok := star.X.(*ast.SelectorExpr); ok && sel.Sel.Name == "M" {
							param = name.Obj
						}
					}
					index++
				}
			}
			if param != nil && runnerBody(fn.Body, param) {
				memo[key] = true
				return true
			}
		}
		return false
	}
	runnerBody = func(body *ast.BlockStmt, p *ast.Object) bool {
		var firstReturn token.Pos = token.NoPos
		ast.Inspect(body, func(node ast.Node) bool {
			switch node := node.(type) {
			case *ast.FuncLit:
				return false
			case *ast.ReturnStmt:
				if firstReturn == token.NoPos || node.Pos() < firstReturn {
					firstReturn = node.Pos()
				}
			}
			return true
		})
		assigned := map[*ast.Object]int{}
		ast.Inspect(body, func(node ast.Node) bool {
			note := func(e ast.Expr) {
				if id, ok := unparen(e).(*ast.Ident); ok && id.Obj != nil {
					assigned[id.Obj]++
				}
			}
			switch node := node.(type) {
			case *ast.AssignStmt:
				for _, lhs := range node.Lhs {
					note(lhs)
				}
			case *ast.IncDecStmt:
				note(node.X)
			case *ast.UnaryExpr:
				if node.Op == token.AND {
					note(node.X)
					note(node.X)
				}
			case *ast.RangeStmt:
				if node.Tok == token.ASSIGN {
					if node.Key != nil {
						note(node.Key)
					}
					if node.Value != nil {
						note(node.Value)
					}
				}
			}
			return true
		})
		locals := map[*ast.Object]bool{}
		for _, stmt := range body.List {
			if stmt.End() > firstReturn {
				continue
			}
			switch def := stmt.(type) {
			case *ast.AssignStmt:
				if def.Tok != token.DEFINE || len(def.Lhs) != 1 || len(def.Rhs) != 1 {
					continue
				}
				if id, ok := def.Lhs[0].(*ast.Ident); ok && id.Obj != nil && assigned[id.Obj] == 1 && runExpr(def.Rhs[0], p) {
					locals[id.Obj] = true
				}
			case *ast.DeclStmt:
				decl, ok := def.Decl.(*ast.GenDecl)
				if !ok || decl.Tok != token.VAR || len(decl.Specs) != 1 {
					continue
				}
				spec, ok := decl.Specs[0].(*ast.ValueSpec)
				if !ok || len(spec.Names) != 1 || len(spec.Values) != 1 {
					continue
				}
				if id := spec.Names[0]; id.Obj != nil && assigned[id.Obj] == 0 && runExpr(spec.Values[0], p) {
					locals[id.Obj] = true
				}
			}
		}
		returns, allRun := 0, true
		ast.Inspect(body, func(node ast.Node) bool {
			switch node := node.(type) {
			case *ast.FuncLit:
				return false
			case *ast.ReturnStmt:
				returns++
				if len(node.Results) != 1 {
					allRun = false
					break
				}
				if id, ok := unparen(node.Results[0]).(*ast.Ident); ok && id.Obj != nil && locals[id.Obj] {
					break
				}
				if !runExpr(node.Results[0], p) {
					allRun = false
				}
			}
			return true
		})
		return returns > 0 && allRun
	}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Recv != nil || fn.Name.Name != "TestMain" || fn.Body == nil || fn.Type.Params.NumFields() != 1 {
			continue
		}
		param := fn.Type.Params.List[0]
		star, ok := param.Type.(*ast.StarExpr)
		if !ok || len(param.Names) != 1 || param.Names[0].Name == "_" {
			continue
		}
		if sel, ok := star.X.(*ast.SelectorExpr); !ok || sel.Sel.Name != "M" {
			continue
		}
		m := param.Names[0].Obj
		var shared []*ast.CallExpr
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			exit, ok := node.(*ast.CallExpr)
			if !ok || !isCall(exit, "os", "Exit") || len(exit.Args) != 1 {
				return true
			}
			if call, ok := exit.Args[0].(*ast.CallExpr); ok && isCall(call, containersImportPath, "RunWithSharedServers") && len(call.Args) == 1 {
				shared = append(shared, call)
			}
			return true
		})
		inside := func(pos token.Pos) bool {
			for _, call := range shared {
				if call.Args[0].Pos() <= pos && pos < call.Args[0].End() {
					return true
				}
			}
			return false
		}
		runs, outside := false, false
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			if id, ok := node.(*ast.Ident); ok && id.Obj == m && !inside(id.Pos()) {
				outside = true
			}
			return true
		})
		for _, call := range shared {
			switch callback := unparen(call.Args[0]).(type) {
			case *ast.SelectorExpr:
				if isM(callback.X, m) && callback.Sel.Name == "Run" {
					runs = true
				}
			case *ast.FuncLit:
				if runnerBody(callback.Body, m) {
					runs = true
				}
			}
		}
		if len(shared) > 0 && runs && !outside {
			return true, nil
		}
	}
	return false, nil
}

// missingFromBatch returns the packages of a batch (internal/<dir>) that are
// not among the opted-in directories (relative to internal).
func missingFromBatch(optedIn, batch []string) []string {
	have := map[string]bool{}
	for _, dir := range optedIn {
		have["internal/"+dir] = true
	}
	var missing []string
	for _, pkg := range batch {
		if !have[pkg] {
			missing = append(missing, pkg)
		}
	}
	return missing
}

func mainFile(imports, body string) string {
	return "package opted\n\nimport (\n" + imports + ")\n\n" + body
}

const (
	stdImports  = "\t\"os\"\n\t\"testing\"\n\n\t\"" + containersImportPath + "\"\n"
	venueImport = "\t\"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle\"\n"
)

func TestOptInNeedsATestMainThatRunsTheTestsInsideTheSharedCall(t *testing.T) {
	cases := []struct {
		name string
		src  string
		want bool
	}{
		{"m.Run passed to the call", sharedTestMain, true},
		{"a func literal around a runner", mainFile(stdImports+venueImport,
			"func TestMain(m *testing.M) {\n\tos.Exit(containers.RunWithSharedServers(func() int { return venueoracle.RunTests(m) }))\n}\n"), true},
		{"setup before the call", mainFile(stdImports,
			"func TestMain(m *testing.M) {\n\tsetup()\n\tos.Exit(containers.RunWithSharedServers(m.Run))\n}\n\nfunc setup() {}\n"), true},
		{"an aliased import", mainFile("\t\"os\"\n\t\"testing\"\n\n\tc \""+containersImportPath+"\"\n",
			"func TestMain(m *testing.M) { os.Exit(c.RunWithSharedServers(m.Run)) }\n"), true},
		{"the marker only in a comment", mainFile(stdImports,
			"// TestMain would call containers.RunWithSharedServers(m.Run) one day.\nfunc TestMain(m *testing.M) {\n\t// containers.RunWithSharedServers(\n\tos.Exit(m.Run())\n}\n"), false},
		{"the marker in a string literal", mainFile(stdImports,
			"func TestMain(m *testing.M) {\n\t_ = \"containers.RunWithSharedServers(\"\n\tos.Exit(m.Run())\n}\n"), false},
		{"an unused reference", mainFile(stdImports,
			"func TestMain(m *testing.M) {\n\t_ = containers.RunWithSharedServers\n\tos.Exit(m.Run())\n}\n"), false},
		{"the call in a func TestMain never calls", mainFile(stdImports,
			"func runShared(m *testing.M) int { return containers.RunWithSharedServers(m.Run) }\n\nfunc TestMain(m *testing.M) { os.Exit(m.Run()) }\n"), false},
		{"m.Run again outside the call", mainFile(stdImports,
			"func TestMain(m *testing.M) {\n\t_ = containers.RunWithSharedServers(m.Run)\n\tos.Exit(m.Run())\n}\n"), false},
		{"a runner given m outside the call", mainFile(stdImports+venueImport,
			"func TestMain(m *testing.M) {\n\tcode := venueoracle.RunTests(m)\n\tos.Exit(containers.RunWithSharedServers(func() int { return code }))\n}\n"), false},
		{"the call result is dropped", mainFile(stdImports,
			"func TestMain(m *testing.M) {\n\tcontainers.RunWithSharedServers(m.Run)\n}\n"), false},
		{"the call runs nothing of m", mainFile(stdImports,
			"func TestMain(m *testing.M) {\n\tos.Exit(containers.RunWithSharedServers(func() int { return 0 }))\n}\n"), false},
		{"a package that is not the harness", mainFile("\t\"os\"\n\t\"testing\"\n\n\t\"example.com/other/containers\"\n",
			"func TestMain(m *testing.M) { os.Exit(containers.RunWithSharedServers(m.Run)) }\n"), false},
		{"a no-op helper given m", mainFile(stdImports,
			"func noop(m *testing.M) int { return 0 }\n\nfunc TestMain(m *testing.M) {\n\tos.Exit(containers.RunWithSharedServers(func() int { return noop(m) }))\n}\n"), false},
		{"a helper from another package given m", mainFile(stdImports+"\t\"example.com/other/runner\"\n",
			"func TestMain(m *testing.M) {\n\tos.Exit(containers.RunWithSharedServers(func() int { return runner.RunTests(m) }))\n}\n"), false},
		{"a same-file helper that drops m", mainFile(stdImports,
			"func drop(m *testing.M) int {\n\t_ = m\n\treturn 0\n}\n\nfunc TestMain(m *testing.M) {\n\tos.Exit(containers.RunWithSharedServers(func() int { return drop(m) }))\n}\n"), false},
		{"a same-file helper that runs m", mainFile(stdImports,
			"func run(m *testing.M) int { return m.Run() }\n\nfunc TestMain(m *testing.M) {\n\tos.Exit(containers.RunWithSharedServers(func() int { return run(m) }))\n}\n"), true},
		{"a same-file helper that hands m to venueoracle.RunTests", mainFile(stdImports+venueImport,
			"func run(m *testing.M) int { return venueoracle.RunTests(m) }\n\nfunc TestMain(m *testing.M) {\n\tos.Exit(containers.RunWithSharedServers(func() int { return run(m) }))\n}\n"), true},
		{"mutual recursion with no run", mainFile(stdImports,
			"func ping(m *testing.M) int { return pong(m) }\n\nfunc pong(m *testing.M) int { return ping(m) }\n\nfunc TestMain(m *testing.M) {\n\tos.Exit(containers.RunWithSharedServers(func() int { return ping(m) }))\n}\n"), false},
		{"a helper chain that reaches a run", mainFile(stdImports,
			"func ping(m *testing.M) int { return pong(m) }\n\nfunc pong(m *testing.M) int {\n\tif cond() {\n\t\treturn m.Run()\n\t}\n\treturn m.Run()\n}\n\nfunc cond() bool { return true }\n\nfunc TestMain(m *testing.M) {\n\tos.Exit(containers.RunWithSharedServers(func() int { return ping(m) }))\n}\n"), true},
		{"a helper whose run is in an uncalled closure", mainFile(stdImports,
			"func run(m *testing.M) int {\n\t_ = func() int { return m.Run() }\n\treturn 0\n}\n\nfunc TestMain(m *testing.M) {\n\tos.Exit(containers.RunWithSharedServers(func() int { return run(m) }))\n}\n"), false},
		{"a helper whose run is in a dead branch", mainFile(stdImports,
			"func run(m *testing.M) int {\n\tif false {\n\t\tm.Run()\n\t}\n\treturn 0\n}\n\nfunc TestMain(m *testing.M) {\n\tos.Exit(containers.RunWithSharedServers(func() int { return run(m) }))\n}\n"), false},
		{"a helper that returns a run on one path only", mainFile(stdImports,
			"func run(m *testing.M, c bool) int {\n\tif c {\n\t\treturn m.Run()\n\t}\n\treturn 0\n}\n\nfunc TestMain(m *testing.M) {\n\tos.Exit(containers.RunWithSharedServers(func() int { return run(m, true) }))\n}\n"), false},
		{"a callback with a run in a dead branch", mainFile(stdImports,
			"func TestMain(m *testing.M) {\n\tos.Exit(containers.RunWithSharedServers(func() int {\n\t\tif false {\n\t\t\tm.Run()\n\t\t}\n\t\treturn 0\n\t}))\n}\n"), false},
		{"a callback with a run in an uncalled closure", mainFile(stdImports,
			"func TestMain(m *testing.M) {\n\tos.Exit(containers.RunWithSharedServers(func() int {\n\t\t_ = func() int { return m.Run() }\n\t\treturn 0\n\t}))\n}\n"), false},
		{"a helper called through parentheses", mainFile(stdImports,
			"func run(m *testing.M) int { return (m.Run)() }\n\nfunc TestMain(m *testing.M) {\n\tos.Exit(containers.RunWithSharedServers(func() int { return (run)(m) }))\n}\n"), true},
		{"a callback that returns a local set from m.Run", mainFile(stdImports,
			"func TestMain(m *testing.M) {\n\tos.Exit(containers.RunWithSharedServers(func() int {\n\t\tcode := m.Run()\n\t\treturn code\n\t}))\n}\n"), true},
		{"a callback whose local gets m.Run only in a dead branch", mainFile(stdImports,
			"func TestMain(m *testing.M) {\n\tos.Exit(containers.RunWithSharedServers(func() int {\n\t\tcode := 0\n\t\tif false {\n\t\t\tcode = m.Run()\n\t\t}\n\t\treturn code\n\t}))\n}\n"), false},
		{"a callback whose local is overwritten after m.Run", mainFile(stdImports,
			"func TestMain(m *testing.M) {\n\tos.Exit(containers.RunWithSharedServers(func() int {\n\t\tcode := m.Run()\n\t\tcode = 0\n\t\treturn code\n\t}))\n}\n"), false},
		{"a callback whose local is set in a nested block", mainFile(stdImports,
			"func TestMain(m *testing.M) {\n\tos.Exit(containers.RunWithSharedServers(func() int {\n\t\tif cond() {\n\t\t\tcode := m.Run()\n\t\t\treturn code\n\t\t}\n\t\treturn 0\n\t}))\n}\n"), false},
		{"a callback with a closure whose own return is no run", mainFile(stdImports,
			"func TestMain(m *testing.M) {\n\tos.Exit(containers.RunWithSharedServers(func() int {\n\t\t_ = func() int { return 0 }\n\t\treturn m.Run()\n\t}))\n}\n"), true},
		{"a helper with a closure whose own return is no run", mainFile(stdImports,
			"func run(m *testing.M) int {\n\t_ = func() int { return 0 }\n\treturn m.Run()\n}\n\nfunc TestMain(m *testing.M) {\n\tos.Exit(containers.RunWithSharedServers(func() int { return run(m) }))\n}\n"), true},
		{"a callback that returns a var set from venueoracle.RunTests", mainFile(stdImports+venueImport,
			"func TestMain(m *testing.M) {\n\tos.Exit(containers.RunWithSharedServers(func() int {\n\t\tvar code = venueoracle.RunTests(m)\n\t\treturn code\n\t}))\n}\n"), true},
		{"a callback that returns a var set from m.Run", mainFile(stdImports+venueImport,
			"func TestMain(m *testing.M) {\n\tos.Exit(containers.RunWithSharedServers(func() int {\n\t\tvar code = m.Run()\n\t\treturn code\n\t}))\n}\n"), true},
		{"a callback whose declared var is assigned later", mainFile(stdImports+venueImport,
			"func TestMain(m *testing.M) {\n\tos.Exit(containers.RunWithSharedServers(func() int {\n\t\tvar code int\n\t\tcode = m.Run()\n\t\treturn code\n\t}))\n}\n"), false},
		{"a callback whose var is overwritten after m.Run", mainFile(stdImports+venueImport,
			"func TestMain(m *testing.M) {\n\tos.Exit(containers.RunWithSharedServers(func() int {\n\t\tvar code = m.Run()\n\t\tcode = 0\n\t\treturn code\n\t}))\n}\n"), false},
		{"a helper that returns a var set from venueoracle.RunTests", mainFile(stdImports+venueImport,
			"func run(m *testing.M) int {\n\tvar code = venueoracle.RunTests(m)\n\treturn code\n}\n\nfunc TestMain(m *testing.M) {\n\tos.Exit(containers.RunWithSharedServers(func() int { return run(m) }))\n}\n"), true},
		{"no TestMain", "package opted\n\nfunc helper() {}\n", false},
	}
	for _, tc := range cases {
		got, err := optsIn(tc.src)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if got != tc.want {
			t.Errorf("%s: optsIn = %v, want %v\n%s", tc.name, got, tc.want, tc.src)
		}
	}
}

func TestABatchRowFailsWhenItsTestMainOnlyMentionsTheCall(t *testing.T) {
	root := t.TempDir()
	write := func(rel, body string) {
		t.Helper()
		writeFixture(t, filepath.Join(root, rel), body)
	}
	write("internal/real/main_test.go", strings.ReplaceAll(sharedTestMain, "package opted", "package real"))
	write("internal/commented/main_test.go", "package commented\n\nimport (\n\t\"os\"\n\t\"testing\"\n)\n\n// containers.RunWithSharedServers(m.Run)\nfunc TestMain(m *testing.M) { os.Exit(m.Run()) }\n")
	write("internal/unused/main_test.go", mainFile(stdImports, "func TestMain(m *testing.M) {\n\t_ = containers.RunWithSharedServers\n\tos.Exit(m.Run())\n}\n"))
	write("internal/outside/main_test.go", mainFile(stdImports, "func TestMain(m *testing.M) {\n\t_ = containers.RunWithSharedServers(m.Run)\n\tos.Exit(m.Run())\n}\n"))
	write("internal/dropsm/main_test.go", mainFile(stdImports+venueImport,
		"func runTests(m *testing.M) int { return 0 }\n\nfunc TestMain(m *testing.M) {\n\tos.Exit(containers.RunWithSharedServers(func() int {\n\t\tcode := runTests(m)\n\t\treturn code\n\t}))\n}\n"))
	write("internal/deadbranch/main_test.go", mainFile(stdImports,
		"func run(m *testing.M) int {\n\tif false {\n\t\tm.Run()\n\t}\n\treturn 0\n}\n\nfunc TestMain(m *testing.M) {\n\tos.Exit(containers.RunWithSharedServers(func() int { return run(m) }))\n}\n"))
	optedIn, _ := scan(t, filepath.Join(root, "internal"))
	batch := []string{"internal/real", "internal/commented", "internal/unused", "internal/outside", "internal/dropsm", "internal/deadbranch"}
	got := strings.Join(missingFromBatch(optedIn, batch), ",")
	if want := "internal/commented,internal/unused,internal/outside,internal/dropsm,internal/deadbranch"; got != want {
		t.Errorf("batch rows missing from the opted-in packages = %q, want %q (opted in: %v)", got, want, optedIn)
	}
}

func writeFixture(t *testing.T, path, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

// A test file that names the opt-in call while its package does not opt in
// is a TestMain that does not run on shared servers however it reads.
func TestNoPackageNamesTheOptInCallWithoutOptingIn(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	optedIn, _ := repoScan(t)
	opted := map[string]bool{}
	for _, dir := range optedIn {
		opted[dir] = true
	}
	const self = "cigates/sharedserver"
	walkErr := filepath.WalkDir(filepath.Join(root, "internal"), func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", "node_modules", "testdata", "vendor":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, "_test.go") {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		dir, _ := filepath.Rel(filepath.Join(root, "internal"), filepath.Dir(path))
		dir = filepath.ToSlash(dir)
		if strings.Contains(string(data), optInCall) && !opted[dir] && dir != self {
			t.Errorf("internal/%s: %s names %s but no TestMain of the package runs its tests inside it",
				dir, entry.Name(), optInCall)
		}
		return nil
	})
	if walkErr != nil {
		t.Fatal(walkErr)
	}
}
