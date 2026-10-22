package affected_test

import (
	"fmt"
	"strings"
	"testing"
)

// CHAOS-10475: a change set with non-Go paths may plan NONE or AFFECTED, so the planner must bound every test that can
// read a changed path. Each case below is one way a reader escapes a naive bound; the fixture is the git tree of
// plan_verdict_test.go (a, b, c imports b, d, x, cmd/tool imports a). The tables the planner reads beside classes.tsv
// and reads.tsv are generated from the code: dynreads.tsv (`package<TAB>site`: the site reads a path the tool cannot
// bound), walks.tsv (`package<TAB>site<TAB>root<TAB>depth`: the site walks root, a repository directory taken from the
// code) and edges.tsv (`package<TAB>site<TAB>target`: the site runs or loads the Go packages of target). A case passes
// only with the exact verdict: FULL where a selection is due fails it as much as a selection that misses a reader.

const tablesDir = "internal/cigates/affected/"

type nongoCase struct {
	name      string
	head      map[string]string
	changed   []string
	want      string   // first line of plan-verdict, or a prefix ending in "*"
	keys      []string // keys after an AFFECTED line
	readsCode int      // plan-reads exit code over the same tree
	readsWant []string // lines plan-reads must print
}

func (c nongoCase) run(t *testing.T) {
	t.Helper()
	work := t.TempDir()
	f, err := newVerdictFixture(work, c.head)
	if err != nil {
		t.Fatal(err)
	}
	res := planReads(t, f.root)
	out := res.stdout + res.stderr
	if res.code != c.readsCode {
		t.Errorf("plan-reads exited %d, want %d:\n%s", res.code, c.readsCode, out)
	}
	lines := map[string]bool{}
	for _, l := range strings.Split(out, "\n") {
		lines[l] = true
	}
	for _, w := range c.readsWant {
		if !lines[w] && !strings.Contains(out, w) {
			t.Errorf("plan-reads output does not hold %q:\n%s", w, out)
		}
	}
	first, keys, err := f.verdict(t, work, "pull_request", nil, c.changed...)
	if err != nil {
		t.Fatal(err)
	}
	if strings.HasSuffix(c.want, "*") {
		if !strings.HasPrefix(first, strings.TrimSuffix(c.want, "*")) {
			t.Fatalf("plan-verdict over %q: %q, want a line starting %q", c.changed, first, strings.TrimSuffix(c.want, "*"))
		}
		return
	}
	if len(keys) == 1 && keys[0] == "" {
		keys = nil
	}
	if first != c.want || strings.Join(keys, " ") != strings.Join(c.keys, " ") {
		t.Fatalf("plan-verdict over %q: %q %q, want %q %q", c.changed, first, keys, c.want, c.keys)
	}
}

func runNongo(t *testing.T, cases []nongoCase) {
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			c.run(t)
		})
	}
}

func withReads(rows string) string { return fixtureReads + rows }

const envReader = "package %s\n\nimport \"os\"\n\nfunc Load() ([]byte, error) { return os.ReadFile(os.Getenv(\"READ_PATH\")) }\n"

// Row 1: a package whose non-test code reads a path the tool cannot bound is linked into every test binary that imports
// it: a non-Go change selects its reverse import closure (b and c), not b alone. A test-file reader stays direct.
func TestNonGoPlanCodeScopeUnboundedReaderSelectsItsImporters(t *testing.T) {
	runNongo(t, []nongoCase{
		{name: "code_reader_selects_its_importer", head: map[string]string{"b/b_dyn.go": fmt.Sprintf(envReader, "b"),
			tablesDir + "dynreads.tsv": "b\tb_dyn.go\n"},
			changed: []string{"AGENTS.md"}, want: "AFFECTED 2", keys: []string{"b", "c"}},
		{name: "test_reader_selects_itself", head: map[string]string{"x/x_dyn_test.go": fmt.Sprintf(envReader, "x"),
			tablesDir + "dynreads.tsv": "x\tx_dyn_test.go\n"},
			changed: []string{"AGENTS.md"}, want: "AFFECTED 1", keys: []string{"x"}},
		{name: "undeclared_code_reader_is_full", head: map[string]string{"b/b_dyn.go": fmt.Sprintf(envReader, "b")},
			changed: []string{"AGENTS.md"}, want: "FULL *", readsCode: 1, readsWant: []string{"b\tb_dyn.go"}},
	})
}

// Row 2: a path is the package's own directory only when it is a constant relative path that stays inside it.
// filepath.Join(".", <env>) and "../docs/a.md" leave it; "testdata/a.txt" does not.
func TestNonGoPlanOwnDirNeedsAConstantPathInsideThePackage(t *testing.T) {
	const join = "package x\n\nimport (\n\t\"os\"\n\t\"path/filepath\"\n)\n\nfunc read() ([]byte, error) { return os.ReadFile(filepath.Join(\".\", %s)) }\n"
	env := fmt.Sprintf(join, `os.Getenv("READ_PATH")`)
	escape := fmt.Sprintf(join, `"../docs/a.md"`)
	inside := fmt.Sprintf(join, `"testdata/a.txt"`)
	caller := "package x\n\nimport (\n\t\"os\"\n\t\"path/filepath\"\n\t\"runtime\"\n)\n\nfunc read() ([]byte, error) {\n\t_, file, _, _ := runtime.Caller(0)\n\treturn os.ReadFile(filepath.Join(filepath.Dir(file), \"..\", \"docs\", \"a.md\"))\n}\n"
	runNongo(t, []nongoCase{
		{name: "env_join_is_unbounded", head: map[string]string{"x/x_own_test.go": env, tablesDir + "dynreads.tsv": "x\tx_own_test.go\n"},
			changed: []string{"docs/a.md"}, want: "AFFECTED 2", keys: []string{"a", "x"}},
		{name: "env_join_without_a_row_is_full", head: map[string]string{"x/x_own_test.go": env},
			changed: []string{"docs/a.md"}, want: "FULL *", readsCode: 1, readsWant: []string{"x\tx_own_test.go"}},
		{name: "literal_escape_reads_docs", head: map[string]string{"x/x_own_test.go": escape, tablesDir + "reads.tsv": withReads("x\ttest\tdocs\n")},
			changed: []string{"docs/a.md"}, want: "AFFECTED 2", keys: []string{"a", "x"}},
		{name: "literal_escape_without_a_row_is_full", head: map[string]string{"x/x_own_test.go": escape},
			changed: []string{"docs/a.md"}, want: "FULL *", readsCode: 1, readsWant: []string{"x\ttest\tdocs"}},
		{name: "caller_dir_escape_without_a_row_is_full", head: map[string]string{"x/x_own_test.go": caller},
			changed: []string{"docs/a.md"}, want: "FULL *", readsCode: 1},
		{name: "testdata_stays_own_dir", head: map[string]string{"x/x_own_test.go": inside},
			changed: []string{"docs/a.md"}, want: "AFFECTED 1", keys: []string{"a"}},
	})
}

// Row 3: constant expressions fold before a path is classified: "../do"+"cs/a.md", a constant built from constants,
// path.Join and fmt.Sprintf of constants all read docs/a.md. An expression that does not fold is an unbounded read.
func TestNonGoPlanConstantExpressionsFold(t *testing.T) {
	const reader = "package x\n\nimport (\n\t\"fmt\"\n\t\"os\"\n\t\"path\"\n)\n\nvar _ = fmt.Sprint\nvar _ = path.Join\n\n%s\n\nfunc read() ([]byte, error) { return os.ReadFile(%s) }\n"
	folded := []struct{ name, decls, expr string }{
		{"concatenation", "", `"../do" + "cs/a.md"`},
		{"constant_of_constants", "const docsDir = \"../do\" + \"cs\"\n\nconst docA string = docsDir + \"/a.md\"", "docA"},
		{"path_join_of_constants", "const up = \"..\"", `path.Join(up, "do"+"cs", "a.md")`},
		{"sprintf_of_constants", "const dir = \"../docs\"", `fmt.Sprintf("%s/%s", dir, "a.md")`},
	}
	var cases []nongoCase
	for _, f := range folded {
		src := fmt.Sprintf(reader, f.decls, f.expr)
		cases = append(cases,
			nongoCase{name: f.name + "_needs_a_docs_row", head: map[string]string{"x/x_cat_test.go": src},
				changed: []string{"docs/a.md"}, want: "FULL *", readsCode: 1, readsWant: []string{"x\ttest\tdocs"}},
			nongoCase{name: f.name + "_selects_its_reader", head: map[string]string{"x/x_cat_test.go": src, tablesDir + "reads.tsv": withReads("x\ttest\tdocs\n")},
				changed: []string{"docs/a.md"}, want: "AFFECTED 2", keys: []string{"a", "x"}})
	}
	open := fmt.Sprintf(reader, "", `fmt.Sprintf("../docs/%s", os.Getenv("F"))`)
	cases = append(cases,
		nongoCase{name: "unfoldable_is_an_unbounded_read", head: map[string]string{"x/x_cat_test.go": open},
			changed: []string{"docs/a.md"}, want: "FULL *", readsCode: 1, readsWant: []string{"x\tx_cat_test.go"}},
		nongoCase{name: "unfoldable_with_its_row_selects_its_reader", head: map[string]string{"x/x_cat_test.go": open, tablesDir + "dynreads.tsv": "x\tx_cat_test.go\n"},
			changed: []string{"docs/a.md"}, want: "AFFECTED 2", keys: []string{"a", "x"}})
	runNongo(t, cases)
}

// Row 4: the walk table is compared with the walks the code makes, root taken from the AST: a row narrower than the
// code's walk fails the guard and the plan is FULL; the row the code calls for selects the walker.
func TestNonGoPlanWalkRowsComeFromTheCode(t *testing.T) {
	const walk = "package x\n\nimport (\n\t\"io/fs\"\n\t\"path/filepath\"\n)\n\nfunc repoRoot() string { return \"..\" }\n\nfunc walk() error {\n\treturn filepath.WalkDir(%s, func(string, fs.DirEntry, error) error { return nil })\n}\n"
	root := fmt.Sprintf(walk, "repoRoot()")
	docs := fmt.Sprintf(walk, `filepath.Join(repoRoot(), "docs")`)
	runNongo(t, []nongoCase{
		{name: "narrow_row_for_a_root_walk_fails", head: map[string]string{"x/x_walk_test.go": root, tablesDir + "walks.tsv": "x\tx_walk_test.go\tdocs\ttree\n"},
			changed: []string{"contracts/auth/v1/x.json"}, want: "FULL *", readsCode: 1, readsWant: []string{"x\tx_walk_test.go\t.\ttree"}},
		{name: "missing_row_for_a_root_walk_fails", head: map[string]string{"x/x_walk_test.go": root},
			changed: []string{"contracts/auth/v1/x.json"}, want: "FULL *", readsCode: 1, readsWant: []string{"x\tx_walk_test.go\t.\ttree"}},
		{name: "root_walk_selects_its_walker", head: map[string]string{"x/x_walk_test.go": root, tablesDir + "walks.tsv": "x\tx_walk_test.go\t.\ttree\n"},
			changed: []string{"contracts/auth/v1/x.json"}, want: "AFFECTED 3", keys: []string{"b", "c", "x"}},
		{name: "wide_row_for_a_docs_walk_fails", head: map[string]string{"x/x_walk_test.go": docs, tablesDir + "reads.tsv": withReads("x\ttest\tdocs\n"), tablesDir + "walks.tsv": "x\tx_walk_test.go\t.\ttree\n"},
			changed: []string{"contracts/auth/v1/x.json"}, want: "FULL *", readsCode: 1, readsWant: []string{"x\tx_walk_test.go\tdocs\ttree"}},
		{name: "docs_walk_skips_a_contracts_change", head: map[string]string{"x/x_walk_test.go": docs, tablesDir + "reads.tsv": withReads("x\ttest\tdocs\n"), tablesDir + "walks.tsv": "x\tx_walk_test.go\tdocs\ttree\n"},
			changed: []string{"contracts/auth/v1/x.json"}, want: "AFFECTED 2", keys: []string{"b", "c"}},
	})
}

// The four packages always.tsv held because their tests read or run other packages' files. Each case plants the same
// read in a package that is not in always.tsv: the analysis alone must select it for a change to what it reads.
const repoRootFunc = "func repoRootDir(t *testing.T) string {\n\tt.Helper()\n\treturn \"..\"\n}\n"

// internal/pgmigrate: the hand-written DDL guard walks the module root and reads every _test.go.
func TestNonGoPlanReproModuleWalkOfEveryTestFile(t *testing.T) {
	const scan = "package %s\n\nimport (\n\t\"io/fs\"\n\t\"os\"\n\t\"path/filepath\"\n\t\"strings\"\n\t\"testing\"\n)\n\n" + repoRootFunc +
		"\nfunc ScanHandTables(t *testing.T) int {\n\tn := 0\n\troot := repoRootDir(t)\n\terr := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {\n\t\tif err != nil || d.IsDir() || !strings.HasSuffix(path, \"_test.go\") {\n\t\t\treturn err\n\t\t}\n\t\traw, err := os.ReadFile(path)\n\t\tn += len(raw)\n\t\treturn err\n\t})\n\tif err != nil {\n\t\tt.Fatal(err)\n\t}\n\treturn n\n}\n"
	runNongo(t, []nongoCase{
		{name: "test_walker_runs_for_another_packages_test_file", head: map[string]string{"x/x_ddl_test.go": fmt.Sprintf(scan, "x"),
			tablesDir + "walks.tsv": "x\tx_ddl_test.go\t.\ttree\n"},
			changed: []string{"a/a_test.go"}, want: "AFFECTED 2", keys: []string{"a", "x"}},
		{name: "code_walker_runs_its_importers", head: map[string]string{"b/b_ddl.go": fmt.Sprintf(scan, "b"),
			tablesDir + "walks.tsv": "b\tb_ddl.go\t.\ttree\n"},
			changed: []string{"a/a_test.go"}, want: "AFFECTED 3", keys: []string{"a", "b", "c"}},
	})
}

// internal/queryapi/server: `go run ./cmd/registrydump` with the command's Dir set to the repository root.
func TestNonGoPlanReproGoRunFromTheRepositoryRoot(t *testing.T) {
	const run = "package x\n\nimport (\n\t\"os/exec\"\n\t\"testing\"\n)\n\n" + repoRootFunc +
		"\nfunc dump(t *testing.T, root string) []byte {\n\tcommand := exec.Command(\"go\", \"run\", \"./cmd/tool\", \"-file\", \"x/x.go\")\n\tcommand.Dir = root\n\tout, err := command.Output()\n\tif err != nil {\n\t\tt.Fatal(err)\n\t}\n\treturn out\n}\n\nfunc TestDump(t *testing.T) { _ = dump(t, repoRootDir(t)) }\n"
	runNongo(t, []nongoCase{
		{name: "command_dependency_selects_the_runner", head: map[string]string{"x/x_dump_test.go": run, tablesDir + "edges.tsv": "x\tx_dump_test.go\tcmd/tool\n"},
			changed: []string{"a/a.go"}, want: "AFFECTED 2", keys: []string{"a", "x"}},
		{name: "missing_edge_row_fails", head: map[string]string{"x/x_dump_test.go": run},
			changed: []string{"a/a.go"}, want: "FULL *", readsCode: 1, readsWant: []string{"x\tx_dump_test.go\tcmd/tool"}},
	})
}

// internal/fixturescli: `go run ../../cmd/dho`, a path relative to the package directory.
func TestNonGoPlanReproGoRunRelativeToThePackage(t *testing.T) {
	const run = "package x\n\nimport (\n\t\"os/exec\"\n\t\"testing\"\n)\n\nfunc TestMigrate(t *testing.T) {\n\tmigrate := exec.Command(\"go\", \"run\", \"../cmd/tool\", \"migrate\")\n\tif out, err := migrate.CombinedOutput(); err != nil {\n\t\tt.Fatalf(\"%v %s\", err, out)\n\t}\n}\n"
	runNongo(t, []nongoCase{
		{name: "command_dependency_selects_the_runner", head: map[string]string{"x/x_migrate_test.go": run, tablesDir + "edges.tsv": "x\tx_migrate_test.go\tcmd/tool\n"},
			changed: []string{"a/a.go"}, want: "AFFECTED 2", keys: []string{"a", "x"}},
		{name: "command_itself_selects_the_runner", head: map[string]string{"x/x_migrate_test.go": run, tablesDir + "edges.tsv": "x\tx_migrate_test.go\tcmd/tool\n"},
			changed: []string{"cmd/tool/main.go"}, want: "AFFECTED 1", keys: []string{"x"}},
	})
}

// internal/queryapi/scancensus: LoadSites loads the source of a package tree (go/packages, Dir from its caller).
func TestNonGoPlanReproSourceLoadOfAPackageTree(t *testing.T) {
	const load = "package x\n\nimport (\n\t\"testing\"\n\n\t\"golang.org/x/tools/go/packages\"\n)\n\nfunc LoadSites(moduleRoot, pattern string) ([]*packages.Package, error) {\n\tcfg := &packages.Config{Mode: packages.NeedSyntax | packages.NeedDeps, Dir: moduleRoot}\n\treturn packages.Load(cfg, pattern, \"./d/...\")\n}\n\nfunc TestCensus(t *testing.T) {\n\tif _, err := LoadSites(\"..\", \"./b/...\"); err != nil {\n\t\tt.Fatal(err)\n\t}\n}\n"
	edges := "x\tx_census_test.go\tb/...\nx\tx_census_test.go\td/...\n"
	runNongo(t, []nongoCase{
		{name: "loaded_package_change_selects_the_loader", head: map[string]string{"x/x_census_test.go": load, tablesDir + "edges.tsv": edges},
			changed: []string{"b/b.go"}, want: "AFFECTED 3", keys: []string{"b", "c", "x"}},
		{name: "unloaded_package_change_skips_the_loader", head: map[string]string{"x/x_census_test.go": load, tablesDir + "edges.tsv": edges},
			changed: []string{"a/a.go"}, want: "AFFECTED 1", keys: []string{"a"}},
		{name: "missing_edge_rows_fail", head: map[string]string{"x/x_census_test.go": load},
			changed: []string{"b/b.go"}, want: "FULL *", readsCode: 1, readsWant: []string{"x\tx_census_test.go\tb/..."}},
	})
}

// CHAOS-10475 review findings: each case reproduces a read the first scanner did not bound, with the exact tables the
// scanner generates for the fixture (readsCode 0: plan-reads passes) and the verdict that selects the reader.

// An unbounded read may name a changed Go file: a Go-only change set selects every unbounded reader.
func TestNonGoPlanGoOnlyChangeSelectsAnUnboundedReader(t *testing.T) {
	const reader = "package b\n\nimport \"os\"\n\nfunc TestReadChanged() { _, _ = os.ReadFile(os.Getenv(\"CHANGED_PATH\")) }\n"
	runNongo(t, []nongoCase{
		{name: "go_only_change_selects_the_unbounded_reader", head: map[string]string{"b/b_changed_test.go": reader, tablesDir + "dynreads.tsv": "b\tb_changed_test.go\n"},
			changed: []string{"a/a.go"}, want: "AFFECTED 2", keys: []string{"a", "b"}},
	})
}

// A directory change in one test binary moves the relative reads of every package it links: the Chdir call is unbounded.
func TestNonGoPlanChdirMakesRelativeReadsUnbounded(t *testing.T) {
	const helper = "package b\n\nimport \"os\"\n\nfunc Read() ([]byte, error) { return os.ReadFile(\"a.md\") }\n"
	const test = "package a\n\nimport (\n\t\"os\"\n\t\"testing\"\n\n\t\"m/b\"\n)\n\nfunc TestRead(t *testing.T) {\n\tif err := os.Chdir(os.Getenv(\"TEST_DIR\")); err != nil {\n\t\tt.Fatal(err)\n\t}\n\t_, _ = b.Read()\n}\n"
	runNongo(t, []nongoCase{
		{name: "chdir_test_selects_itself_for_a_reader_less_path", head: map[string]string{"b/b_help.go": helper, "a/a_chdir_test.go": test, tablesDir + "dynreads.tsv": "a\ta_chdir_test.go\n"},
			changed: []string{".claude/x.json"}, want: "AFFECTED 1", keys: []string{"a"}},
		{name: "chdir_without_its_row_fails_the_guard", head: map[string]string{"b/b_help.go": helper, "a/a_chdir_test.go": test},
			changed: []string{".claude/x.json"}, want: "FULL *", readsCode: 1, readsWant: []string{"a\ta_chdir_test.go"}},
	})
}

// Mutating exec.Cmd.Args through an alias, or handing the command on, leaves the run target unknown.
func TestNonGoPlanCmdArgsMutatedThroughAnAlias(t *testing.T) {
	const alias = "package x\n\nimport (\n\t\"os/exec\"\n\t\"testing\"\n)\n\nfunc TestAlias(t *testing.T) {\n\tc := exec.Command(\"go\", \"run\", \"./cmd/tool\")\n\talias := c\n\talias.Args = []string{\"go\", \"run\", \"./a\"}\n\t_ = c.Run()\n}\n"
	const passed = "package x\n\nimport (\n\t\"os/exec\"\n\t\"testing\"\n)\n\nfunc tune(c *exec.Cmd) { c.Args = []string{\"go\", \"run\", \"./a\"} }\n\nfunc TestPassed(t *testing.T) {\n\tc := exec.Command(\"go\", \"run\", \"./cmd/tool\")\n\ttune(c)\n\t_ = c.Run()\n}\n"
	runNongo(t, []nongoCase{
		{name: "alias_mutation_is_an_unexpressed_run", head: map[string]string{"x/x_alias_test.go": alias, tablesDir + "edges.tsv": "x\tx_alias_test.go\tunexpressed\n"},
			changed: []string{"b/b.go"}, want: "AFFECTED 3", keys: []string{"b", "c", "x"}},
		{name: "passed_on_command_is_an_unexpressed_run", head: map[string]string{"x/x_passed_test.go": passed, tablesDir + "edges.tsv": "x\tx_passed_test.go\tunexpressed\n"},
			changed: []string{"b/b.go"}, want: "AFFECTED 3", keys: []string{"b", "c", "x"}},
		{name: "alias_with_a_stale_target_row_fails_the_guard", head: map[string]string{"x/x_alias_test.go": alias, tablesDir + "edges.tsv": "x\tx_alias_test.go\tcmd/tool\n"},
			changed: []string{"b/b.go"}, want: "FULL *", readsCode: 1, readsWant: []string{"x\tx_alias_test.go\tunexpressed"}},
	})
}

// A read through os.Root is a read of the directory the root opens.
func TestNonGoPlanOsRootReadsAreBounded(t *testing.T) {
	const root = "package x\n\nimport (\n\t\"os\"\n\t\"testing\"\n)\n\nfunc TestRoot(t *testing.T) {\n\tr, err := os.OpenRoot(%s)\n\tif err != nil {\n\t\tt.Fatal(err)\n\t}\n\t_, _ = r.ReadFile(os.Getenv(\"FILE_NAME\"))\n}\n"
	runNongo(t, []nongoCase{
		{name: "env_root_is_an_unbounded_walk", head: map[string]string{"x/x_root_test.go": fmt.Sprintf(root, `os.Getenv("ROOT_DIR")`), tablesDir + "walks.tsv": "x\tx_root_test.go\tunexpressed\t-\n"},
			changed: []string{"docs/a.md"}, want: "AFFECTED 2", keys: []string{"a", "x"}},
		{name: "constant_root_walks_that_directory", head: map[string]string{"x/x_root_test.go": fmt.Sprintf(root, `"../docs"`), tablesDir + "walks.tsv": "x\tx_root_test.go\tdocs\ttree\n", tablesDir + "reads.tsv": withReads("x\ttest\tdocs\n")},
			changed: []string{"docs/a.md"}, want: "AFFECTED 2", keys: []string{"a", "x"}},
		{name: "env_root_without_its_row_fails_the_guard", head: map[string]string{"x/x_root_test.go": fmt.Sprintf(root, `os.Getenv("ROOT_DIR")`)},
			changed: []string{"docs/a.md"}, want: "FULL *", readsCode: 1, readsWant: []string{"x\tx_root_test.go\tunexpressed\t-"}},
	})
}
