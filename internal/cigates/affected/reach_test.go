package affected_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// CHAOS-10547: a code-scope site the tool cannot bound runs only in a program that can call it. plan-reads --write
// writes reach.tsv from the call graph (RTA from each test main and main, with its package init), and plan-verdict
// selects the packages whose programs reach the site, not every importer of its package. An interface call, a function
// value and a run of a main that calls the site all reach it; go:linkname hides calls, so its tree plans FULL.

const reachReader = "package b\n\nimport \"os\"\n\nfunc Load() ([]byte, error) { return os.ReadFile(os.Getenv(\"READ_PATH\")) }\n\n" +
	"func Name() string { return \"b\" }\n\ntype Loader interface{ Load() ([]byte, error) }\n\ntype Impl struct{}\n\n" +
	"func (Impl) Load() ([]byte, error) { return Load() }\n"

const reachCaller = "package c\n\nimport (\n\t\"testing\"\n\n\t\"m/b\"\n)\n\nfunc TestC(t *testing.T) { _, _ = b.Load() }\n"

func reachTest(pkg, body string) string {
	return "package " + pkg + "\n\nimport (\n\t\"testing\"\n\n\t\"m/b\"\n)\n\nfunc TestT(t *testing.T) {\n" + body + "}\n"
}

// reachFixture is the verdict fixture with d, e and f's test binaries linking b, and cmd/tool importing b.
func reachFixture(t *testing.T, head map[string]string) (*verdictFixture, string) {
	t.Helper()
	work := t.TempDir()
	f, err := newVerdictFixture(work, head)
	if err != nil {
		t.Fatal(err)
	}
	mod := map[string]string{"Path": "m"}
	pkg := func(rel string, deps ...string) map[string]any {
		e := map[string]any{"ImportPath": "m/" + rel, "Dir": filepath.Join(f.root, rel), "Module": mod}
		var d []string
		for _, dep := range deps {
			d = append(d, "m/"+dep)
		}
		if len(d) > 0 {
			e["Deps"] = d
		}
		return e
	}
	bin := func(rel string, deps ...string) map[string]any {
		d := []string{"m/" + rel + " [m/" + rel + ".test]"}
		for _, dep := range deps {
			d = append(d, "m/"+dep)
		}
		return map[string]any{"ImportPath": "m/" + rel + ".test", "Deps": d}
	}
	var lines []string
	for _, e := range []map[string]any{
		pkg("a"), pkg("b"), pkg("c"), pkg("d"), pkg("e"), pkg("f"), pkg("x"), pkg("internal/testsupport/h"),
		pkg("internal/cigates/affected"), pkg("cmd/tool", "a", "b"), bin("a"), bin("b"), bin("c", "b"), bin("d", "b"),
		bin("e", "b"), bin("f", "b"), bin("x"),
	} {
		raw, err := json.Marshal(e)
		if err != nil {
			t.Fatal(err)
		}
		lines = append(lines, string(raw))
	}
	if err := os.WriteFile(f.golist, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(f.keys, []byte("a\nb\nc\nd\ne\nf\nx\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	return f, work
}

// writeTables runs plan-reads --write in the fixture: the toolchain loads the fixture module, so the go command's own
// environment (cache, module cache) is inherited.
func writeTables(t *testing.T, root string) cmdResult {
	t.Helper()
	env := inheritedEnv([]string{"GOFLAGS", "GOWORK", "GOTOOLCHAIN"}, "GOFLAGS=-mod=mod", "GOWORK=off", "GOTOOLCHAIN=local",
		"GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null")
	return cmdSpec{dir: root, env: env}.run(cigate(t), "plan-reads", "--root", root, "--write")
}

// One fixture module for every case: one load of the call graph. b reads a path the tool cannot bound in non-test
// code; c's test calls it; d's test links b and never calls it; e's test calls it through an interface; f's test
// through a function value; x's test runs cmd/tool, whose main calls it.
func TestReachSelectsTheProgramsThatCallACodeScopeSite(t *testing.T) {
	head := map[string]string{
		"b/b.go":           reachReader,
		"c/c_test.go":      reachCaller,
		"d/d_test.go":      reachTest("d", "\t_ = b.Name()\n"),
		"e/e.go":           "package e\n",
		"e/e_test.go":      reachTest("e", "\tvar l b.Loader = b.Impl{}\n\t_, _ = l.Load()\n"),
		"f/f.go":           "package f\n",
		"f/f_test.go":      reachTest("f", "\tload := b.Load\n\t_, _ = load()\n"),
		"cmd/tool/main.go": "package main\n\nimport \"m/b\"\n\nfunc main() { _, _ = b.Load() }\n",
		"x/x_test.go":      "package x\n\nimport (\n\t\"os/exec\"\n\t\"testing\"\n)\n\nfunc TestRun(t *testing.T) {\n\t_ = exec.Command(\"go\", \"run\", \"../cmd/tool\").Run()\n}\n",
	}
	f, work := reachFixture(t, head)
	if res := writeTables(t, f.root); res.code != 0 {
		t.Fatalf("plan-reads --write exited %d:\n%s%s", res.code, res.stdout, res.stderr)
	}
	raw, err := os.ReadFile(filepath.Join(f.root, tablesDir, "reach.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	var rows []string
	for _, l := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		if !strings.HasPrefix(l, "#") {
			rows = append(rows, l)
		}
	}
	// d links b and never calls Load: no row; c, e (interface), f (function value) and cmd/tool (main) reach it.
	want := "b\tb.go\tc b\tb.go\tcmd/tool b\tb.go\te b\tb.go\tf"
	if got := strings.Join(rows, " "); got != want {
		t.Fatalf("reach.tsv rows %q, want %q", got, want)
	}
	cases := []struct {
		name string
		keys []string
	}{
		{"a_linker_that_never_calls_the_site_is_not_selected", nil},
		{"an_interface_call_reaches_the_site", []string{"e"}},
		{"a_function_value_reaches_the_site", []string{"f"}},
		{"a_test_that_runs_a_main_that_calls_the_site_is_selected", []string{"x"}},
	}
	first, keys, err := f.verdict(t, work, "pull_request", nil, "AGENTS.md")
	if err != nil {
		t.Fatal(err)
	}
	if want := "c e f x"; first != "AFFECTED 4" || strings.Join(keys, " ") != want {
		t.Fatalf("plan-verdict: %q %q, want AFFECTED 4 %q", first, keys, want)
	}
	selected := map[string]bool{}
	for _, k := range keys {
		selected[k] = true
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if c.keys == nil && selected["d"] {
				t.Fatal("d links b and never calls the site, but is selected")
			}
			for _, k := range c.keys {
				if !selected[k] {
					t.Fatalf("%s reaches the site, but is not selected", k)
				}
			}
		})
	}
}

func TestReachTableDefectsPlanFull(t *testing.T) {
	runNongo(t, []nongoCase{
		{name: "a_code_site_with_no_reach_row_is_full", head: map[string]string{"b/b_dyn.go": reachReader,
			tablesDir + "dynreads.tsv": "b\tb_dyn.go\n"},
			changed: []string{"AGENTS.md"}, want: "FULL *", readsCode: 1, readsWant: []string{"has no reach.tsv row"}},
		{name: "a_reach_row_with_no_code_site_is_full", head: map[string]string{tablesDir + "reach.tsv": "b\tb_gone.go\tc\n"},
			changed: []string{"AGENTS.md"}, want: "FULL *", readsCode: 1, readsWant: []string{"names no code-scope site"}},
		{name: "go_linkname_is_full", head: map[string]string{"e/e.go": "package e\n\nimport _ \"unsafe\"\n\n//go:linkname now time.now\nfunc now() (int64, int32, int64)\n"},
			changed: []string{"AGENTS.md"}, want: "FULL *", readsCode: 1, readsWant: []string{"go:linkname hides calls"}},
		{name: "a_file_built_only_without_the_integration_tag_is_full", head: map[string]string{"e/e.go": "//go:build !integration\n\npackage e\n"},
			changed: []string{"AGENTS.md"}, want: "FULL *", readsCode: 1, readsWant: []string{"a build constraint on !integration"}},
		{name: "a_star_row_keeps_the_importers", head: map[string]string{"b/b_dyn.go": reachReader,
			tablesDir + "dynreads.tsv": "b\tb_dyn.go\n", tablesDir + "reach.tsv": "b\tb_dyn.go\t*\n"},
			changed: []string{"AGENTS.md"}, want: "AFFECTED 2", keys: []string{"b", "c"}},
		{name: "a_dash_row_selects_no_one", head: map[string]string{"b/b_dyn.go": reachReader,
			tablesDir + "dynreads.tsv": "b\tb_dyn.go\n", tablesDir + "reach.tsv": "b\tb_dyn.go\t-\n"},
			changed: []string{"AGENTS.md"}, want: "NONE"},
	})
}

// A relative --root (the form the tool's own messages print) places every read: the table equals the absolute-root one.
func TestReachRelativeRootEqualsAbsoluteRoot(t *testing.T) {
	head := map[string]string{
		"b/b.go":      reachReader,
		"c/c_test.go": reachCaller,
		"d/d_test.go": reachTest("d", "\t_ = b.Name()\n"),
		"e/e.go":      "package e\n",
		"f/f.go":      "package f\n",
	}
	f, _ := reachFixture(t, head)
	if res := writeTables(t, f.root); res.code != 0 {
		t.Fatalf("plan-reads --write exited %d:\n%s%s", res.code, res.stdout, res.stderr)
	}
	reach := filepath.Join(f.root, tablesDir, "reach.tsv")
	absolute, err := os.ReadFile(reach)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(absolute), "b\tb.go\tc") {
		t.Fatalf("absolute-root reach.tsv has no placed row for b:\n%s", absolute)
	}
	if err := os.Remove(reach); err != nil {
		t.Fatal(err)
	}
	env := inheritedEnv([]string{"GOFLAGS", "GOWORK", "GOTOOLCHAIN"}, "GOFLAGS=-mod=mod", "GOWORK=off", "GOTOOLCHAIN=local",
		"GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null")
	if res := (cmdSpec{dir: f.root, env: env}).run(cigate(t), "plan-reads", "--root", ".", "--write"); res.code != 0 {
		t.Fatalf("plan-reads --root . --write exited %d:\n%s%s", res.code, res.stdout, res.stderr)
	}
	relative, err := os.ReadFile(reach)
	if err != nil {
		t.Fatal(err)
	}
	if string(relative) != string(absolute) {
		t.Fatalf("--root . reach.tsv differs from the absolute-root table:\n%s\nwant:\n%s", relative, absolute)
	}
}
