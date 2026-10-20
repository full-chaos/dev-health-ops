package bigboy_test

import (
	"bytes"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// Every guard in this package is a function of a repo root. One test runs it on the real tree; a table-driven
// test copies the real files a guard reads into a temp tree, plants ONE defect and asserts the guard fires with a
// message naming the defect. The planted-defect cases are the red proofs.

type doc = map[string]any

func realRoot(t testing.TB) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found above the test directory")
		}
		dir = parent
	}
}

// guard accumulates the violations of one check run against one root.
type guard struct {
	root string
	tmp  string
	v    []string
}

func (g *guard) failf(format string, args ...any) { g.v = append(g.v, fmt.Sprintf(format, args...)) }

func (g *guard) path(rel string) string { return filepath.Join(g.root, filepath.FromSlash(rel)) }

func (g *guard) read(rel string) string {
	b, err := os.ReadFile(g.path(rel))
	if err != nil {
		g.failf("cannot read %s: %v", rel, err)
		return ""
	}
	return string(b)
}

// expect records a violation unless cond holds.
func (g *guard) expect(cond bool, format string, args ...any) {
	if !cond {
		g.failf(format, args...)
	}
}

func (g *guard) yamlFile(rel string) doc {
	var out doc
	if err := yaml.Unmarshal([]byte(g.read(rel)), &out); err != nil {
		g.failf("%s is not YAML: %v", rel, err)
	}
	return out
}

func (g *guard) jsonFile(rel string) doc {
	// JSON is YAML; yaml.v3 keeps integers as int, which is what the value comparisons want.
	return g.yamlFile(rel)
}

// --- document access ---

func at(v any, path ...any) any {
	for _, p := range path {
		switch k := p.(type) {
		case string:
			m, ok := v.(map[string]any)
			if !ok {
				return nil
			}
			v = m[k]
		case int:
			l, ok := v.([]any)
			if !ok || k < 0 || k >= len(l) {
				return nil
			}
			v = l[k]
		}
	}
	return v
}

func str(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	if v == nil {
		return ""
	}
	return fmt.Sprint(v)
}

func list(v any) []any { l, _ := v.([]any); return l }

func mp(v any) doc { m, _ := v.(map[string]any); return m }

func strs(v any) []string {
	var out []string
	for _, x := range list(v) {
		out = append(out, str(x))
	}
	return out
}

func kind(d doc) string   { return str(d["kind"]) }
func equal(a, b any) bool { return reflect.DeepEqual(a, b) }

// --- real-tree and planted-defect harness ---

type check struct {
	name string
	run  func(g *guard)
}

func runReal(t *testing.T, checks ...check) {
	t.Helper()
	root := realRoot(t)
	for _, c := range checks {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			g := &guard{root: root, tmp: t.TempDir()}
			c.run(g)
			if len(g.v) > 0 {
				t.Fatalf("%d violation(s):\n  %s", len(g.v), strings.Join(g.v, "\n  "))
			}
		})
	}
}

// planted is one defect: files named in copy are taken from the real tree, mutate then changes them, and
// the check must report a violation containing want.
type planted struct {
	name   string
	copy   []string
	mutate func(t *testing.T, root string)
	check  func(g *guard)
	want   string
}

func runPlanted(t *testing.T, cases []planted) {
	t.Helper()
	real := realRoot(t)
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			for _, p := range c.copy {
				copyPath(t, real, root, p)
			}
			if c.mutate != nil {
				c.mutate(t, root)
			}
			g := &guard{root: root, tmp: t.TempDir()}
			c.check(g)
			if len(g.v) == 0 {
				t.Fatalf("planted defect %q was not caught: the guard passed", c.name)
			}
			for _, v := range g.v {
				if strings.Contains(v, c.want) {
					return
				}
			}
			t.Fatalf("planted defect %q was caught, but no violation contains %q:\n  %s", c.name, c.want, strings.Join(g.v, "\n  "))
		})
	}
}

func copyPath(t *testing.T, from, to, rel string) {
	t.Helper()
	src := filepath.Join(from, filepath.FromSlash(rel))
	info, err := os.Stat(src)
	if err != nil {
		t.Fatalf("copy %s: %v", rel, err)
	}
	if !info.IsDir() {
		copyOne(t, src, filepath.Join(to, filepath.FromSlash(rel)))
		return
	}
	err = filepath.WalkDir(src, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || strings.HasSuffix(p, "_test.go") {
			return nil
		}
		r, _ := filepath.Rel(from, p)
		copyOne(t, p, filepath.Join(to, r))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func copyOne(t *testing.T, src, dst string) {
	t.Helper()
	b, err := os.ReadFile(src)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dst, b, 0o644); err != nil {
		t.Fatal(err)
	}
}

// plant replaces the first occurrence of old with repl in root/rel and fails the test if old is absent:
// a planted defect that did not land would make its case pass for the wrong reason.
func plant(t *testing.T, root, rel, old, repl string) {
	t.Helper()
	p := filepath.Join(root, filepath.FromSlash(rel))
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("plant %s: %v", rel, err)
	}
	if !strings.Contains(string(b), old) {
		t.Fatalf("plant %s: text %q not found, the defect did not land", rel, old)
	}
	if err := os.WriteFile(p, []byte(strings.Replace(string(b), old, repl, 1)), 0o644); err != nil {
		t.Fatal(err)
	}
}

func writeFile(t *testing.T, root, rel, content string) {
	t.Helper()
	p := filepath.Join(root, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func mut(rel, old, repl string) func(*testing.T, string) {
	return func(t *testing.T, root string) { plant(t, root, rel, old, repl) }
}

// all applies several mutations in order: a defense-in-depth rule (template fail plus schema) needs both
// layers removed before the guard has anything to catch.
func all(ms ...func(*testing.T, string)) func(*testing.T, string) {
	return func(t *testing.T, root string) {
		for _, m := range ms {
			m(t, root)
		}
	}
}

func rootPath(root, rel string) string { return filepath.Join(root, filepath.FromSlash(rel)) }

func mustRead(t *testing.T, p string) string {
	t.Helper()
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func exists(p string) bool { _, err := os.Stat(p); return err == nil }

// walkFiles lists every regular file under dir, skipping Go test sources.
func walkFiles(dir string) []string {
	var out []string
	_ = filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() && !strings.HasSuffix(p, "_test.go") {
			out = append(out, p)
		}
		return nil
	})
	return out
}

// scriptRun is one execution of a real script with a controlled environment.
type scriptRun struct {
	stdout, stderr string
	code           int
}

// runScript runs name with args in dir; env entries are appended to the inherited environment.
func runScript(t testing.TB, dir string, env []string, name string, args ...string) scriptRun {
	t.Helper()
	cmd := exec.Command(name, args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), env...)
	var so, se bytes.Buffer
	cmd.Stdout, cmd.Stderr = &so, &se
	code := 0
	if err := cmd.Run(); err != nil {
		ee, ok := err.(*exec.ExitError)
		if !ok {
			t.Fatalf("%s %v: %v", name, args, err)
		}
		code = ee.ExitCode()
	}
	return scriptRun{stdout: so.String(), stderr: se.String(), code: code}
}

// stubBin writes executable stand-in commands into a new directory for use at the front of PATH.
func stubBin(t testing.TB, stubs map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	for name, body := range stubs {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("#!/usr/bin/env bash\n"+body), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}
