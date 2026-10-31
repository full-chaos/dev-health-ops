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

	// sharedTestMain is the TestMain of a package that runs on shared servers.
	sharedTestMain = "package opted\n\nimport (\n\t\"os\"\n\t\"testing\"\n\n\t\"" + containersImportPath + "\"\n)\n\n" +
		"func TestMain(m *testing.M) { os.Exit(containers.RunWithSharedServers(m.Run)) }\n"
)

// optsIn reports whether a test file holds a TestMain that runs the package
// on shared servers: the body of func TestMain(m *testing.M) passes the
// result of the harness's RunWithSharedServers (through the file's real
// import of it) to os.Exit, and every use of m lies inside the argument of
// that call, where at least one use runs the tests (m.Run, or m handed to a
// runner). Comments and string literals are not code and never count.
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
			switch node := node.(type) {
			case *ast.Ident:
				if node.Obj == m && !inside(node.Pos()) {
					outside = true
				}
			case *ast.CallExpr:
				if !inside(node.Pos()) {
					return true
				}
				if selector, ok := node.Fun.(*ast.SelectorExpr); ok {
					if id, ok := selector.X.(*ast.Ident); ok && id.Obj == m && selector.Sel.Name == "Run" {
						runs = true
					}
				}
				for _, arg := range node.Args {
					if id, ok := arg.(*ast.Ident); ok && id.Obj == m {
						runs = true
					}
				}
			}
			return true
		})
		for _, call := range shared {
			if selector, ok := call.Args[0].(*ast.SelectorExpr); ok {
				if id, ok := selector.X.(*ast.Ident); ok && id.Obj == m && selector.Sel.Name == "Run" {
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
		{"mutual recursion that reaches a run", mainFile(stdImports,
			"func ping(m *testing.M) int { return pong(m) }\n\nfunc pong(m *testing.M) int {\n\tif false {\n\t\treturn ping(m)\n\t}\n\treturn m.Run()\n}\n\nfunc TestMain(m *testing.M) {\n\tos.Exit(containers.RunWithSharedServers(func() int { return ping(m) }))\n}\n"), true},
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
	optedIn, _ := scan(t, filepath.Join(root, "internal"))
	batch := []string{"internal/real", "internal/commented", "internal/unused", "internal/outside", "internal/dropsm"}
	got := strings.Join(missingFromBatch(optedIn, batch), ",")
	if want := "internal/commented,internal/unused,internal/outside,internal/dropsm"; got != want {
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
