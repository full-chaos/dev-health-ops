package chartgates_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

// Every guard in this package is a function of a repo root. One test runs it on the real tree; a
// table-driven test copies the real files a guard reads into a temp tree, plants ONE defect and asserts
// the guard fires with a message naming the defect. The planted-defect cases are the red proofs.

type doc = map[string]any

const (
	chartRel   = "deploy/helm/dev-health"
	backendURL = "http://backend.test:8000"
)

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

type render struct {
	stdout, stderr string
	code           int
}

var helmPath = func() string { p, _ := exec.LookPath("helm"); return p }()

var tracked sync.Map

// trackChart reads every chart file once per root. `go test` keys its result cache on the files the
// test process opens; helm opens the chart in another process, so without this read a chart-only
// change would leave a cached pass in place.
func (g *guard) trackChart() {
	if _, done := tracked.LoadOrStore(g.root, true); done {
		return
	}
	for _, f := range walkFiles(g.path(chartRel)) {
		_, _ = os.ReadFile(f)
	}
}

func (g *guard) helm(env []string, args ...string) render {
	g.trackChart()
	if helmPath == "" {
		g.failf("helm is not installed: a chart guard that cannot render measures nothing")
		return render{code: -1}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, helmPath, args...)
	cmd.Env = append(os.Environ(), env...)
	var so, se bytes.Buffer
	cmd.Stdout, cmd.Stderr = &so, &se
	err := cmd.Run()
	code := 0
	if err != nil {
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			code = ee.ExitCode()
		} else {
			g.failf("helm %v: %v", args, err)
			code = -1
		}
	}
	return render{stdout: so.String(), stderr: se.String(), code: code}
}

// tpl runs `helm template <release> <chart> <args>` with the placeholder web BACKEND_URL the suite always
// gave it (web.env.BACKEND_URL is required while web.enabled, the chart default).
func (g *guard) tpl(release string, args ...string) render {
	full := []string{"template", "--set", "web.env.BACKEND_URL=" + backendURL, "--set", "web.authSecret.secretName=web-auth-placeholder", release, g.path(chartRel)}
	return g.helm(nil, append(full, args...)...)
}

// tplRaw renders with no placeholder and HELM_SHIM_OFF semantics: the chart sees exactly the given args.
func (g *guard) tplRaw(release string, args ...string) render {
	full := []string{"template", release, g.path(chartRel)}
	return g.helm(nil, append(full, args...)...)
}

func parseDocs(s string) ([]doc, error) {
	dec := yaml.NewDecoder(strings.NewReader(s))
	var out []doc
	for {
		var d doc
		err := dec.Decode(&d)
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		if d != nil {
			out = append(out, d)
		}
	}
}

// docs renders and parses; a failed render or unparsable output is a violation and yields nil.
func (g *guard) docs(release string, args ...string) []doc {
	r := g.tpl(release, args...)
	if r.code != 0 {
		g.failf("helm template %v failed: %s", args, firstLines(r.stderr, 3))
		return nil
	}
	out, err := parseDocs(r.stdout)
	if err != nil {
		g.failf("helm template %v output is not YAML: %v", args, err)
		return nil
	}
	return out
}

func firstLines(s string, n int) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	if len(lines) > n {
		lines = lines[:n]
	}
	return strings.Join(lines, " | ")
}

// refusal renders and expects a non-zero exit; it returns stderr, and records a violation if the render passed.
func (g *guard) refusal(why, release string, args ...string) string {
	r := g.tpl(release, args...)
	if r.code == 0 {
		g.failf("%s: the render succeeded but must be refused", why)
	}
	return r.stderr
}

func (g *guard) mustRender(why, release string, args ...string) render {
	r := g.tpl(release, args...)
	if r.code != 0 {
		g.failf("%s: render failed: %s", why, firstLines(r.stderr, 3))
	}
	return r
}

func set(kv ...string) []string {
	var out []string
	for _, x := range kv {
		out = append(out, "--set", x)
	}
	return out
}

func setString(kv ...string) []string {
	var out []string
	for _, x := range kv {
		out = append(out, "--set-string", x)
	}
	return out
}

func setJSON(kv ...string) []string {
	var out []string
	for _, x := range kv {
		out = append(out, "--set-json", x)
	}
	return out
}

func join(parts ...[]string) []string {
	var out []string
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}

func (g *guard) valuesFile(content string) string {
	if g.tmp == "" {
		g.tmp = os.TempDir()
	}
	f, err := os.CreateTemp(g.tmp, "values-*.yaml")
	if err != nil {
		g.failf("cannot write values file: %v", err)
		return ""
	}
	defer f.Close()
	_, _ = f.WriteString(content)
	return f.Name()
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

func kind(d doc) string { return str(d["kind"]) }

func name(d doc) string { return str(at(d, "metadata", "name")) }

func label(d doc, k string) string { return str(at(d, "metadata", "labels", k)) }

func ofKind(docs []doc, k string) []doc {
	var out []doc
	for _, d := range docs {
		if kind(d) == k {
			out = append(out, d)
		}
	}
	return out
}

func named(docs []doc, k, n string) doc {
	for _, d := range docs {
		if kind(d) == k && name(d) == n {
			return d
		}
	}
	return nil
}

func podSpec(d doc) doc { return mp(at(d, "spec", "template", "spec")) }

func containers(d doc) []doc {
	var out []doc
	for _, c := range list(at(d, "spec", "template", "spec", "containers")) {
		out = append(out, mp(c))
	}
	return out
}

func initContainers(d doc) []doc {
	var out []doc
	for _, c := range list(at(d, "spec", "template", "spec", "initContainers")) {
		out = append(out, mp(c))
	}
	return out
}

func envMap(c doc) map[string]doc {
	out := map[string]doc{}
	for _, e := range list(c["env"]) {
		out[str(at(e, "name"))] = mp(e)
	}
	return out
}

func has(m map[string]doc, k string) bool { _, ok := m[k]; return ok }

func equal(a, b any) bool { return reflect.DeepEqual(a, b) }

func sortedHas(l []string, s string) bool {
	for _, x := range l {
		if x == s {
			return true
		}
	}
	return false
}

func sameSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	m := map[string]int{}
	for _, x := range a {
		m[x]++
	}
	for _, x := range b {
		m[x]--
	}
	for _, c := range m {
		if c != 0 {
			return false
		}
	}
	return true
}

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

func chartFile(rel string) string { return chartRel + "/" + rel }

var chartOnly = []string{chartRel}

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
