package venueoracle

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// openedChildDir names, for a child process of the test below, the directory
// that stands in for the package's testdata.
const openedChildDir = "VENUEORACLE_OPENED_CHECK_DIR"

// binaryTests is what compiledTests read from this binary's testing.M.
var (
	binaryTests    map[string]bool
	binaryTestsErr error
)

func TestMain(m *testing.M) {
	binaryTests, binaryTestsErr = compiledTests(m)
	dir := "testdata"
	if child := os.Getenv(openedChildDir); child != "" {
		dir = child
	}
	os.Exit(runTests(m, dir))
}

func TestTheTestsOfThisBinaryAreRead(t *testing.T) {
	if binaryTestsErr != nil {
		t.Fatal(binaryTestsErr)
	}
	// This test, another one of the package, and nothing that is not a test.
	if !binaryTests[t.Name()] || !binaryTests["TestTheRunSelectionFollowsTheTestFlags"] || binaryTests["TestMain"] || binaryTests["TestThatDoesNotExist"] {
		t.Fatalf("the tests of this binary were not read: %d names, this one %v", len(binaryTests), binaryTests[t.Name()])
	}
	if _, err := compiledTests(nil); err == nil {
		t.Fatal("no testing.M read as an empty list of tests: every golden would pass")
	}
}

func TestTheRunSelectionFollowsTheTestFlags(t *testing.T) {
	for _, c := range []struct {
		run, skip, name string
		want            bool
	}{
		{"", "", "TestA", true},
		{"", "", "TestA/sub", true},
		{"^TestA$", "", "TestA", true},
		{"^TestA$", "", "TestA/sub", true},
		{"^TestA$", "", "TestAB", false},
		{"^(TestA|TestB)$", "", "TestB", true},
		{"^(TestA|TestB)$", "", "TestC", false},
		{"TestA/sub", "", "TestA", true},
		{"TestA/sub", "", "TestA/sub", true},
		{"TestA/sub", "", "TestA/other", false},
		{"TestA/sub", "", "TestB/sub", false},
		{"^TestA$/^(sub|more)$", "", "TestA/more", true},
		{"^TestA$/^(sub|more)$", "", "TestA/other", false},
		{"Test[A/]x/sub", "", "TestAx/sub", true},
		{"^$", "", "TestA", false},
		{"", "^TestA$", "TestA", false},
		{"", "^TestA$", "TestA/sub", false},
		{"", "^TestA$", "TestB", true},
		{"", "TestA/sub", "TestA", true},
		{"", "TestA/sub", "TestA/sub", false},
		{"", "TestA/sub", "TestA/other", true},
		{"^TestA$", "TestA/sub", "TestA/sub", false},
	} {
		if got := testSelection(c.run, c.skip)(c.name); got != c.want {
			t.Errorf("run=%q skip=%q: %s selected=%t, want %t", c.run, c.skip, c.name, got, c.want)
		}
	}
}

func TestAGoldenItsTestDidNotUseIsReported(t *testing.T) {
	dir := t.TempDir()
	write := func(name, content string) string {
		path := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
		absolute, _ := filepath.Abs(path)
		return absolute
	}
	golden := func(test string) string { return `{"header":{"test":"` + test + `","python_build":"b"}}` }
	type use struct{ opened, finished, excused bool }
	uses := map[string]use{
		write("used.json", golden("TestUsed")):                 {true, true, false},
		write("sub/unopened.json", golden("TestUnopened/sub")): {},
		write("unfinished.json", golden("TestUnfinished")):     {true, false, false},
		write("skipped.json", golden("TestSkipped")):           {true, false, true},
		write("not-run.json", golden("TestNotRun")):            {},
		write("other-build.json", golden("TestOtherBuild")):    {},
	}
	write("not-a-golden.json", `{"header":{"test":"TestUnopened"}}`)
	write("not-json.json", `rows`)
	write("notes.txt", golden("TestUnopened"))
	compiled := map[string]bool{"TestUsed": true, "TestUnopened": true, "TestUnfinished": true, "TestSkipped": true, "TestNotRun": true}
	problems, err := unusedGoldens(dir, compiled, func(test string) bool { return test != "TestNotRun" }, func(path string) (bool, bool, bool) {
		u, known := uses[path]
		if !known {
			t.Errorf("asked about %s, which is not a golden", path)
		}
		return u.opened, u.finished, u.excused
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(problems) != 2 || !strings.Contains(problems[0], "sub/unopened.json was not opened by its test TestUnopened/sub") ||
		!strings.Contains(problems[1], "unfinished.json was opened by its test TestUnfinished and never finished") {
		t.Fatalf("problems = %q, want the unopened and the unfinished golden of the tests that ran, by name", problems)
	}
	// A package with no testdata holds no golden; a golden that cannot be read is an error, not a pass.
	if problems, err := unusedGoldens(filepath.Join(dir, "absent"), compiled, func(string) bool { return true }, nil); err != nil || len(problems) != 0 {
		t.Fatalf("no testdata: %v %v", problems, err)
	}
	if err := os.Symlink(filepath.Join(dir, "gone.json"), filepath.Join(dir, "dangling.json")); err != nil {
		t.Fatal(err)
	}
	if _, err := unusedGoldens(dir, compiled, func(string) bool { return true }, func(string) (bool, bool, bool) { return true, true, false }); err == nil {
		t.Fatal("a golden that cannot be read was passed over")
	}
}

// The whole check, in a child process whose goldens are in a directory of its
// own: the subtests below are the tests of a package, each with its golden.
func TestRunTestsFailsForAGoldenItsTestDidNotUse(t *testing.T) {
	requests := []Request{ProgramRequest("corpus", sampleProgram, []byte("abc"), nil)}
	if dir := os.Getenv(openedChildDir); dir != "" {
		t.Setenv(goldenUpdateEnv, "")
		t.Setenv(goldenCandidateEnv, "")
		t.Setenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR", t.TempDir())
		// open writes the subtest's golden under dir and opens it, as a test opens its pinned golden.
		open := func(child *testing.T, name string) *Golden {
			file := goldenFile{Header: goldenHeader{Test: child.Name(), PythonBuild: goldenBuild, ProducerDigest: strings.Repeat("a", 64), Recipe: "record it"}}
			entry := requestKey(requests[0])
			entry.Headers, entry.Body = map[string]string{"stderr": ""}, "ABC\n"
			file.Requests = []goldenRequest{entry}
			if err := os.MkdirAll(filepath.Join(dir, name), 0o755); err != nil {
				child.Fatal(err)
			}
			path, digest := writeGoldenFile(child, filepath.Join(dir, name), file)
			return OpenGolden(child, GoldenSpec{Path: path, PythonBuild: goldenBuild, SHA256: digest, Recipe: "record it"})
		}
		compare := func(child *testing.T, golden *Golden) {
			answers := golden.Produce(child, "/no/python/here", requests, func(*Producer, []Request) []Response { return nil })
			golden.Consumed(child, answers...)
			golden.SkipDiff(child)
		}
		t.Run("uses", func(child *testing.T) {
			golden := open(child, "uses")
			compare(child, golden)
			golden.Finish(child)
		})
		// The comparison was taken out of this test: it passes, and its golden (written by the parent) stays.
		t.Run("forgets", func(child *testing.T) {})
		t.Run("opens-only", func(child *testing.T) { compare(child, open(child, "opens-only")) })
		t.Run("skips", func(child *testing.T) {
			open(child, "skips")
			child.Skip("the gate is off")
		})
		t.Run("other", func(child *testing.T) {})
		return
	}

	run := func(args ...string) (string, error) {
		dir := t.TempDir()
		for _, name := range []string{"forgets", "other"} {
			if err := os.MkdirAll(filepath.Join(dir, name), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, name, "golden.json"), []byte(`{"header":{"test":"`+t.Name()+"/"+name+`","python_build":"`+goldenBuild+`"}}`), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		command := exec.Command(os.Args[0], append([]string{"-test.v"}, args...)...)
		command.Env = append(os.Environ(), openedChildDir+"="+dir)
		output, err := command.CombinedOutput()
		return string(output), err
	}
	only := "-test.run=^" + t.Name() + "$/^"

	// The test whose comparison was removed ran: the run fails and names its golden and the test.
	output, err := run(only + "(uses|forgets)$")
	if err == nil || !strings.Contains(output, "forgets/golden.json was not opened by its test "+t.Name()+"/forgets") {
		t.Fatalf("a test that ran without its golden did not fail the run (err %v):\n%s", err, output)
	}
	// Only that one: not the golden that was used, not the golden of a test the run did not select.
	if strings.Contains(output, "uses/golden.json was") || strings.Contains(output, "other/golden.json") || strings.Count(output, "FAIL: golden ") != 1 {
		t.Fatalf("the run reported more than the one unused golden:\n%s", output)
	}
	// Opened and compared, never finished.
	output, err = run(only + "(uses|opens-only)$")
	if err == nil || !strings.Contains(output, "opens-only/golden.json was opened by its test "+t.Name()+"/opens-only and never finished") || strings.Count(output, "FAIL: golden ") != 1 {
		t.Fatalf("a golden that never reached Finish did not fail the run (err %v):\n%s", err, output)
	}
	// A test that uses its golden, and one that opens it and then skips: the run passes.
	if output, err = run(only + "(uses|skips)$"); err != nil || strings.Contains(output, "FAIL: golden ") {
		t.Fatalf("a used golden and a skipped test failed the run (err %v):\n%s", err, output)
	}
	// -skip takes the tests out of the run, and their goldens out of the check.
	if output, err = run(only+"(uses|forgets|other)$", "-test.skip=^"+t.Name()+"$/^(forgets|other)$"); err != nil || strings.Contains(output, "FAIL: golden ") {
		t.Fatalf("tests taken out with -skip tripped the check (err %v):\n%s", err, output)
	}
	// -list runs nothing.
	if output, err = run("-test.list=."); err != nil || strings.Contains(output, "FAIL: golden ") {
		t.Fatalf("-list tripped the check (err %v):\n%s", err, output)
	}
}

// callsRunTests reports whether the test files of the package in dir define a
// TestMain that calls venueoracle.RunTests (RunTests, in this package).
func callsRunTests(t *testing.T, dir string) bool {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), filepath.Join(dir, entry.Name()), nil, parser.ParseComments|parser.SkipObjectResolution)
		if err != nil {
			t.Fatal(err)
		}
		// A file behind a build tag is not in every test binary of the package: the TestMain must be in all of them.
		tagged := false
		for _, group := range file.Comments {
			for _, line := range group.List {
				if line.Pos() < file.Package && strings.HasPrefix(line.Text, "//go:build") {
					tagged = true
				}
			}
		}
		if tagged {
			continue
		}
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Recv != nil || function.Name.Name != "TestMain" || function.Body == nil {
				continue
			}
			found := false
			ast.Inspect(function.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				if selector, ok := call.Fun.(*ast.SelectorExpr); ok && selector.Sel.Name == "RunTests" {
					if pkg, ok := selector.X.(*ast.Ident); ok && pkg.Name == "venueoracle" {
						found = true
					}
				}
				return true
			})
			if found {
				return true
			}
		}
	}
	return false
}

// goldenPackages lists the package directories under root that hold a golden in their testdata.
func goldenPackages(t *testing.T, root string) []string {
	t.Helper()
	found := map[string]bool{}
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if name := entry.Name(); name == ".git" || name == "node_modules" || name == ".venv" {
				return filepath.SkipDir
			}
			return nil
		}
		slashed := filepath.ToSlash(path)
		at := strings.Index(slashed, "/testdata/")
		if at < 0 || !strings.HasSuffix(path, ".json") {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if strings.Contains(string(raw), `"python_build"`) {
			found[filepath.FromSlash(slashed[:at])] = true
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for dir := range found {
		out = append(out, dir)
	}
	sort.Strings(out)
	return out
}

// Every package that holds a golden runs its tests through RunTests: without
// it nothing asks whether the package's goldens were used.
func TestEveryPackageWithAGoldenRunsItsTestsThroughRunTests(t *testing.T) {
	root, err := filepath.Abs("../../..")
	if err != nil {
		t.Fatal(err)
	}
	packages := goldenPackages(t, root)
	if len(packages) == 0 {
		t.Fatal("the walk found no package with a golden: a guard that checks nothing passes everything")
	}
	for _, dir := range packages {
		if !callsRunTests(t, dir) {
			relative, _ := filepath.Rel(root, dir)
			t.Errorf("%s holds a golden and its tests do not run through venueoracle.RunTests: add, in a test file with no build tag, func TestMain(m *testing.M) { os.Exit(venueoracle.RunTests(m)) }", relative)
		}
	}
}

func TestAPackageWithoutTheRunTestsCallIsFound(t *testing.T) {
	root := t.TempDir()
	write := func(path, content string) {
		full := filepath.Join(root, path)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	golden := `{"header":{"python_build":"b","test":"TestX"}}`
	main := "package p\nimport (\n\t\"os\"\n\t\"testing\"\n\tvenueoracle \"example/venueoracle\"\n)\nfunc TestMain(m *testing.M) { os.Exit(venueoracle.RunTests(m)) }\n"
	for name, c := range map[string]struct {
		source string
		want   bool
	}{
		"calls":       {main, true},
		"plain-run":   {strings.Replace(main, "venueoracle.RunTests(m)", "m.Run()", 1), false},
		"no-testmain": {"package p\n", false},
		"tagged":      {"//go:build integration\n\n" + main, false},
		"other-func":  {strings.Replace(main, "func TestMain(", "func helper(", 1), false},
	} {
		write(filepath.Join(name, "testdata", "g.json"), golden)
		write(filepath.Join(name, "main_test.go"), c.source)
		if got := callsRunTests(t, filepath.Join(root, name)); got != c.want {
			t.Errorf("%s: calls RunTests = %v, want %v", name, got, c.want)
		}
	}
	write(filepath.Join("no-golden", "testdata", "notes.json"), `{"a":1}`)
	if got := goldenPackages(t, root); len(got) != 5 {
		t.Fatalf("packages with a golden = %v, want the five with one", got)
	}
}
