package affected_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// CHAOS-11421: `plan-reads --reach --reach-cache DIR` reuses the stored reach rows only for a tree whose every input
// hashes to the same key, and a reused result is still compared with reach.tsv. These tests run the built cigate on a
// fixture module (the planted faults of reach_test.go's style).

func reachEnv(extra ...string) []string {
	return inheritedEnv([]string{"GOFLAGS", "GOWORK", "GOTOOLCHAIN"}, append([]string{"GOFLAGS=-mod=mod", "GOWORK=off", "GOTOOLCHAIN=local",
		"GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null"}, extra...)...)
}

func reachRun(t *testing.T, root string, env []string, args ...string) cmdResult {
	t.Helper()
	return cmdSpec{dir: root, env: env}.run(cigate(t), append([]string{"plan-reads", "--root", root}, args...)...)
}

func reachKeyOf(t *testing.T, root string, env []string) string {
	t.Helper()
	res := reachRun(t, root, env, "--print-reach-key")
	key := strings.TrimSpace(res.stdout)
	if res.code != 0 || len(key) != 64 {
		t.Fatalf("--print-reach-key exited %d:\n%s%s", res.code, res.stdout, res.stderr)
	}
	return key
}

func appendTo(t *testing.T, path, text string) {
	t.Helper()
	old, err := os.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(old, text...), 0o644); err != nil {
		t.Fatal(err)
	}
}

func reachCacheHead() map[string]string {
	return map[string]string{
		"b/b.go":         reachReader,
		"b/embed.go":     "package b\n\nimport \"embed\"\n\n//go:embed assets/*.txt\nvar assets embed.FS\n",
		"b/assets/x.txt": "x\n",
		"b/notes.txt":    "notes\n",
		"c/c_test.go":    reachCaller,
		"d/d_test.go":    reachTest("d", "\t_ = b.Name()\n"),
		"e/e.go":         "package e\n",
		"f/f.go":         "package f\n",
	}
}

func TestReachCacheKeyCoversEveryInput(t *testing.T) {
	f, _ := reachFixture(t, reachCacheHead())
	if res := writeTables(t, f.root); res.code != 0 {
		t.Fatalf("plan-reads --write exited %d:\n%s%s", res.code, res.stdout, res.stderr)
	}
	env := reachEnv()
	base := reachKeyOf(t, f.root, env)
	if again := reachKeyOf(t, f.root, env); again != base {
		t.Fatalf("the key is not stable: %s then %s", base, again)
	}
	p := func(rel string) string { return filepath.Join(f.root, filepath.FromSlash(rel)) }
	changes := []struct {
		name  string
		apply func()
		env   []string
		undo  func()
	}{
		{name: "a .go file", apply: func() { appendTo(t, p("b/b.go"), "\n// x\n") }},
		{name: "a _test.go file", apply: func() { appendTo(t, p("c/c_test.go"), "\n// x\n") }},
		{name: "a new untracked .go file", apply: func() { appendTo(t, p("e/new.go"), "package e\n") }},
		{name: "go.mod", apply: func() { appendTo(t, p("go.mod"), "\n// x\n") }},
		{name: "go.sum", apply: func() { appendTo(t, p("go.sum"), "x\n") }},
		{name: "edges.tsv", apply: func() { appendTo(t, p(tablesDir+"edges.tsv"), "# x\n") }},
		{name: "dynreads.tsv", apply: func() { appendTo(t, p(tablesDir+"dynreads.tsv"), "# x\n") }},
		{name: "reach.tsv", apply: func() { appendTo(t, p(tablesDir+"reach.tsv"), "# x\n") }},
		{name: "reads.tsv", apply: func() { appendTo(t, p(tablesDir+"reads.tsv"), "# x\n") }},
		{name: "a go:embed file", apply: func() { appendTo(t, p("b/assets/x.txt"), "y\n") }},
		{name: "a non-Go file beside Go files", apply: func() { appendTo(t, p("b/notes.txt"), "y\n") }},
		{name: "GOFLAGS", env: reachEnv("GOFLAGS=-mod=readonly")},
		{name: "GOARCH", env: reachEnv("GOARCH=arm64")},
		{name: "GOOS", env: reachEnv("GOOS=plan9")},
		{name: "CGO_ENABLED", env: reachEnv("CGO_ENABLED=1")},
		{name: "GOEXPERIMENT", env: reachEnv("GOEXPERIMENT=nocoverageredesign")},
	}
	for _, c := range changes {
		t.Run(c.name, func(t *testing.T) {
			if c.apply != nil {
				snap := snapshotTree(t, f.root)
				c.apply()
				defer restoreTree(t, f.root, snap)
			}
			e := env
			if c.env != nil {
				e = c.env
			}
			if got := reachKeyOf(t, f.root, e); got == base {
				t.Fatalf("changing %s kept the key %s: a stale result would be reused", c.name, base)
			}
		})
	}
	for _, rel := range []string{"docs/a.md", ".github/workflows/go.yml", "ci/check_go.sh", "web/readme.txt"} {
		snap := snapshotTree(t, f.root)
		appendTo(t, p(rel), "\nchanged\n")
		got := reachKeyOf(t, f.root, env)
		restoreTree(t, f.root, snap)
		if got != base {
			t.Errorf("changing %s moved the key: a change outside the Go inputs must reuse", rel)
		}
	}
}

func snapshotTree(t *testing.T, root string) map[string][]byte {
	t.Helper()
	out := map[string][]byte{}
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if d.Name() == ".git" {
				return filepath.SkipDir
			}
			return nil
		}
		b, err := os.ReadFile(path)
		out[path] = b
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func restoreTree(t *testing.T, root string, snap map[string][]byte) {
	t.Helper()
	now := snapshotTree(t, root)
	for path := range now {
		if _, ok := snap[path]; !ok {
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
		}
	}
	for path, b := range snap {
		if err := os.WriteFile(path, b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func storeFiles(t *testing.T, dir string) []string {
	t.Helper()
	m, err := filepath.Glob(filepath.Join(dir, "reach-*.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	return m
}

// A stale store never gives a green gate: a changed input misses and the gate goes red on the changed call graph.
func TestReachCacheReusesOnlyAnUnchangedTree(t *testing.T) {
	head := reachCacheHead()
	head["cmd/tool/main.go"] = "package main\n\nimport \"m/b\"\n\nfunc main() { _, _ = b.Load() }\n"
	f, _ := reachFixture(t, head)
	if res := writeTables(t, f.root); res.code != 0 {
		t.Fatalf("plan-reads --write exited %d:\n%s%s", res.code, res.stdout, res.stderr)
	}
	env := reachEnv()
	store := t.TempDir()
	key := reachKeyOf(t, f.root, env)

	first := reachRun(t, f.root, env, "--reach", "--reach-cache", store)
	if first.code != 0 || !strings.Contains(first.stdout, "plan-reads: reach: computed for input hash "+key) {
		t.Fatalf("first run: exit %d, want a computed receipt for %s:\n%s%s", first.code, key, first.stdout, first.stderr)
	}
	if got := storeFiles(t, store); len(got) != 1 || filepath.Base(got[0]) != "reach-"+key+".tsv" {
		t.Fatalf("a green computed run must store reach-%s.tsv, store holds %v", key, got)
	}
	second := reachRun(t, f.root, env, "--reach", "--reach-cache", store)
	if second.code != 0 || !strings.Contains(second.stdout, "plan-reads: reach: reused for input hash "+key) {
		t.Fatalf("second run: exit %d, want a reused receipt for %s:\n%s%s", second.code, key, second.stdout, second.stderr)
	}
	if strings.Contains(second.stdout, "program run(s)") {
		t.Fatalf("a reused run must not compute the call graph:\n%s", second.stdout)
	}

	// An input of the tables moves the key: recomputed, still green (a comment row).
	appendTo(t, filepath.Join(f.root, tablesDir, "edges.tsv"), "# x\n")
	third := reachRun(t, f.root, env, "--reach", "--reach-cache", store)
	if third.code != 0 || strings.Contains(third.stdout, "reach: reused") || !strings.Contains(third.stdout, "reach: computed for input hash ") {
		t.Fatalf("a changed edges.tsv must recompute: exit %d:\n%s%s", third.code, third.stdout, third.stderr)
	}

	// The call graph changes (c's test stops calling the site) while reach.tsv is stale: a different key, recomputed, RED.
	if err := os.WriteFile(filepath.Join(f.root, "c", "c_test.go"), []byte(reachTest("c", "\t_ = b.Name()\n")), 0o644); err != nil {
		t.Fatal(err)
	}
	fourth := reachRun(t, f.root, env, "--reach", "--reach-cache", store)
	if fourth.code == 0 || strings.Contains(fourth.stdout, "reach: reused") || !strings.Contains(fourth.stdout, "does not call for") {
		t.Fatalf("a changed .go file must recompute and fail on the stale reach.tsv: exit %d:\n%s%s", fourth.code, fourth.stdout, fourth.stderr)
	}
}

func TestReachCacheStoreIsNeverTrusted(t *testing.T) {
	f, _ := reachFixture(t, reachCacheHead())
	if res := writeTables(t, f.root); res.code != 0 {
		t.Fatalf("plan-reads --write exited %d:\n%s%s", res.code, res.stdout, res.stderr)
	}
	env := reachEnv()
	store := t.TempDir()
	key := reachKeyOf(t, f.root, env)
	if res := reachRun(t, f.root, env, "--reach", "--reach-cache", store); res.code != 0 {
		t.Fatalf("seed run exit %d:\n%s%s", res.code, res.stdout, res.stderr)
	}
	file := filepath.Join(store, "reach-"+key+".tsv")
	good, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	write := func(b string) {
		if err := os.WriteFile(file, []byte(b), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	// Right key, one row too many: reused, compared with reach.tsv, RED.
	write(string(good) + "b\tghost.go\tc\n")
	if res := reachRun(t, f.root, env, "--reach", "--reach-cache", store); res.code == 0 {
		t.Fatalf("a store with an extra row gave a green gate:\n%s%s", res.stdout, res.stderr)
	}
	// Right key, a row missing: RED.
	if lines := strings.Split(strings.TrimRight(string(good), "\n"), "\n"); len(lines) > 2 {
		write(strings.Join(lines[:len(lines)-1], "\n") + "\n")
		if res := reachRun(t, f.root, env, "--reach", "--reach-cache", store); res.code == 0 {
			t.Fatalf("a store with a missing row gave a green gate:\n%s%s", res.stdout, res.stderr)
		}
	}
	// A header of another key, an empty file, a truncated header: not reused; recomputed, green, store rewritten.
	for name, body := range map[string]string{
		"other key":  strings.Replace(string(good), key, strings.Repeat("0", 64), 1),
		"empty":      "",
		"no header":  "b\tb.go\tc\n",
		"old schema": strings.Replace(string(good), "reach-cache-v1", "reach-cache-v0", 1),
	} {
		write(body)
		res := reachRun(t, f.root, env, "--reach", "--reach-cache", store)
		if res.code != 0 || strings.Contains(res.stdout, "reach: reused") || !strings.Contains(res.stdout, "reach: computed for input hash "+key) {
			t.Errorf("%s: want a recompute and a green gate, exit %d:\n%s%s", name, res.code, res.stdout, res.stderr)
		}
	}
}

func TestReachCacheStoresNothingOnARedRun(t *testing.T) {
	f, _ := reachFixture(t, reachCacheHead())
	if res := writeTables(t, f.root); res.code != 0 {
		t.Fatalf("plan-reads --write exited %d:\n%s%s", res.code, res.stdout, res.stderr)
	}
	reach := filepath.Join(f.root, tablesDir, "reach.tsv")
	b, err := os.ReadFile(reach)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimRight(string(b), "\n"), "\n")
	if len(lines) < 2 {
		t.Fatalf("fixture reach.tsv has no row to remove:\n%s", b)
	}
	if err := os.WriteFile(reach, []byte(strings.Join(lines[:len(lines)-1], "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	store := t.TempDir()
	res := reachRun(t, f.root, reachEnv(), "--reach", "--reach-cache", store)
	if res.code != 1 || !strings.Contains(res.stdout, "missing row") {
		t.Fatalf("a reach.tsv with a row removed must fail with the missing row (exit 1):\n%s%s", res.stdout, res.stderr)
	}
	if got := storeFiles(t, store); len(got) != 0 {
		t.Fatalf("a red run stored %v", got)
	}
}

func TestReachCacheOffByDefault(t *testing.T) {
	f, _ := reachFixture(t, reachCacheHead())
	if res := writeTables(t, f.root); res.code != 0 {
		t.Fatalf("plan-reads --write exited %d:\n%s%s", res.code, res.stdout, res.stderr)
	}
	res := reachRun(t, f.root, reachEnv(), "--reach")
	if res.code != 0 || strings.Contains(res.stdout, "input hash") || !strings.Contains(res.stdout, "program run(s)") {
		t.Fatalf("without --reach-cache the gate must compute as before: exit %d:\n%s%s", res.code, res.stdout, res.stderr)
	}
}
