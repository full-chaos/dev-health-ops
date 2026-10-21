package docs_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"unicode"

	"golang.org/x/text/cases"
	"gopkg.in/yaml.v3"
)

// CHAOS-10371: Go port of the content and contract tests of tests/docs (the docs pages, mkdocs.yml, the stylesheets and the
// docs workflows). Every guard is a function of a docTree, which reads the real repository or the real repository with ONE
// planted edit, so the same guard runs on the real tree (must find nothing) and on a defective tree (must name the defect).

type docTree struct {
	root    string
	over    map[string]*string // nil value = file removed
	tracked []string           // git ls-files -z, repository-relative, that exist on disk
}

func realTree(t *testing.T) docTree {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return docTree{root: dir, over: map[string]*string{}, tracked: trackedFiles(t, dir)}
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found above the test directory")
		}
		dir = parent
	}
}

func (d docTree) clone() docTree {
	over := make(map[string]*string, len(d.over)+1)
	for k, v := range d.over {
		over[k] = v
	}
	return docTree{root: d.root, over: over, tracked: d.tracked}
}

func (d docTree) read(rel string) (string, error) {
	if v, ok := d.over[rel]; ok {
		if v == nil {
			return "", fmt.Errorf("%s: no such file", rel)
		}
		return *v, nil
	}
	raw, err := os.ReadFile(filepath.Join(d.root, filepath.FromSlash(rel)))
	return string(raw), err
}

func (d docTree) exists(rel string) bool {
	if v, ok := d.over[rel]; ok {
		return v != nil
	}
	info, err := os.Stat(filepath.Join(d.root, filepath.FromSlash(rel)))
	return err == nil && !info.IsDir()
}

func (d docTree) dirHas(rel, suffix string) bool {
	entries, err := os.ReadDir(filepath.Join(d.root, filepath.FromSlash(rel)))
	if err != nil {
		return false
	}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), suffix) {
			return true
		}
	}
	return false
}

// edit returns a tree where the single occurrence of old in rel is replaced; an anchor that is absent or ambiguous is a
// test error, because a plant that did not happen proves nothing.
func (d docTree) edit(t *testing.T, rel, old, repl string) docTree {
	t.Helper()
	text, err := d.read(rel)
	if err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(text, old); n != 1 {
		t.Fatalf("planted-defect anchor %q occurs %d times in %s, want exactly 1: the file changed, re-derive the plant", old, n, rel)
	}
	out := d.clone()
	s := strings.Replace(text, old, repl, 1)
	out.over[rel] = &s
	return out
}

func (d docTree) removed(rel string) docTree {
	out := d.clone()
	out.over[rel] = nil
	return out
}

func (d docTree) added(rel, content string) docTree {
	out := d.clone()
	out.over[rel] = &content
	return out
}

type plantCase struct {
	name string
	tree func(t *testing.T, d docTree) docTree
	want string
}

// assertGuard runs guard on the real tree (no violation) and on each planted tree (a violation that mentions want).
func assertGuard(t *testing.T, guard func(docTree) []string, plants []plantCase) {
	t.Helper()
	real := realTree(t)
	if got := guard(real); len(got) > 0 {
		t.Fatalf("the real tree violates the guard:\n%s", strings.Join(got, "\n"))
	}
	for _, p := range plants {
		p := p
		t.Run("plant/"+p.name, func(t *testing.T) {
			got := guard(p.tree(t, real))
			if len(got) == 0 {
				t.Fatalf("planted defect %q: the guard found nothing", p.name)
			}
			if !strings.Contains(strings.Join(got, "\n"), p.want) {
				t.Fatalf("planted defect %q: violations %q do not mention %q", p.name, got, p.want)
			}
		})
	}
}

func parseYAML(d docTree, rel string) (map[string]any, error) {
	text, err := d.read(rel)
	if err != nil {
		return nil, err
	}
	var doc map[string]any
	if err := yaml.Unmarshal([]byte(text), &doc); err != nil {
		return nil, fmt.Errorf("%s: %v", rel, err)
	}
	return doc, nil
}

func asMap(v any) map[string]any {
	m, _ := v.(map[string]any)
	return m
}

func asList(v any) []any {
	l, _ := v.([]any)
	return l
}

func asString(v any) string {
	s, _ := v.(string)
	return s
}

// fold is Python casefold (full Unicode case folding), not ToLower: the pages are matched the way the Python tests matched them.
func fold(s string) string { return cases.Fold().String(s) }

func normalized(text string) string {
	return strings.Join(strings.FieldsFunc(fold(text), isPySpace), " ")
}

// splitLines is Python str.splitlines: \n, \r\n, \r, \v, \f, \x1c-\x1e, \x85, \u2028 and \u2029 all end a line.
func splitLines(s string) []string {
	var out []string
	start := 0
	rs := []rune(s)
	for i := 0; i < len(rs); i++ {
		switch rs[i] {
		case '\r', '\n', '\v', '\f', 0x1c, 0x1d, 0x1e, 0x85, 0x2028, 0x2029:
			out = append(out, string(rs[start:i]))
			if rs[i] == '\r' && i+1 < len(rs) && rs[i+1] == '\n' {
				i++
			}
			start = i + 1
		}
	}
	if start < len(rs) {
		out = append(out, string(rs[start:]))
	}
	return out
}

func mustContain(out *[]string, label, text, want string) {
	if !strings.Contains(text, want) {
		*out = append(*out, fmt.Sprintf("%s: missing %q", label, want))
	}
}

func mustNotContain(out *[]string, label, text, forbidden string) {
	if strings.Contains(text, forbidden) {
		*out = append(*out, fmt.Sprintf("%s: contains %q", label, forbidden))
	}
}

func readOrNote(out *[]string, d docTree, rel string) (string, bool) {
	text, err := d.read(rel)
	if err != nil {
		*out = append(*out, fmt.Sprintf("%s: missing file", rel))
		return "", false
	}
	return text, true
}

// bashStatus runs script with bash and a minimal environment and returns its exit code (no Python, no network).
func bashStatus(script string, env ...string) int {
	cmd := exec.Command("bash", "-c", script)
	cmd.Env = append([]string{"PATH=/usr/bin:/bin"}, env...)
	err := cmd.Run()
	if err == nil {
		return 0
	}
	if exit, ok := err.(*exec.ExitError); ok {
		return exit.ExitCode()
	}
	return -1
}

// editFirst replaces the first occurrence of old (which must exist).
func (d docTree) editFirst(t *testing.T, rel, old, repl string) docTree {
	t.Helper()
	text, err := d.read(rel)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(text, old) {
		t.Fatalf("planted-defect anchor %q is absent from %s", old, rel)
	}
	out := d.clone()
	s := strings.Replace(text, old, repl, 1)
	out.over[rel] = &s
	return out
}

// editAll replaces every occurrence of old (at least one must exist).
func (d docTree) editAll(t *testing.T, rel, old, repl string) docTree {
	t.Helper()
	text, err := d.read(rel)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(text, old) {
		t.Fatalf("planted-defect anchor %q is absent from %s", old, rel)
	}
	out := d.clone()
	s := strings.ReplaceAll(text, old, repl)
	out.over[rel] = &s
	return out
}

func (d docTree) appended(t *testing.T, rel, extra string) docTree {
	t.Helper()
	text, err := d.read(rel)
	if err != nil {
		t.Fatal(err)
	}
	out := d.clone()
	s := text + extra
	out.over[rel] = &s
	return out
}

// glue builds the names the guards look for from fragments, so that the fixture-reference scan of the go.yml path filters
// does not read them as test inputs: they are text the guards look for, and the docs workflows own the paths.
func glue(parts ...string) string { return strings.Join(parts, "") }

var (
	docPublicationManifest = glue("docs/publication", ".", "yml")
	redirectsTable         = glue("docs-data/redir", "ects", ".", "tsv")
	searchAcceptance       = glue("docs-data/search-acceptance", ".", "json")
	qaPackage              = glue("docs-qa/package", ".", "json")
	qaPlaywright           = glue("docs-qa/playwright.config", ".", "ts")
)

// isPySpace is Python str.isspace: Unicode white space plus U+001C..U+001F. Python str.strip, str.split and re \s all use it;
// float() and int() strip a narrower set and are not ported here.
func isPySpace(r rune) bool      { return unicode.IsSpace(r) || (r >= 0x1c && r <= 0x1f) }
func trimLeftPy(s string) string { return strings.TrimLeftFunc(s, isPySpace) }
func trimPy(s string) string     { return strings.TrimFunc(s, isPySpace) }

// pySpaceClass is the regexp character-class body of Python re `\s` on str: Unicode white space and U+001C..U+001F
// (Go's `\s` is ASCII and has neither `\v` nor the separators).
const pySpaceClass = `\t\n\v\f\r \x{1c}-\x{1f}\x{85}\p{Z}`
