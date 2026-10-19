package cigates_test

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// CHAOS-10177 (PR B of CHAOS-10174): shared helpers for the workflow-shape guards ported from tests/tooling.
// Every guard is a function of a repo root, so the real tree is checked by one test and a planted defect is
// checked by copying the real file into a temp tree, breaking exactly one thing, and asserting the guard's message.

type yamlMap = map[string]any

func readFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func parseYAML(t *testing.T, text, what string) yamlMap {
	t.Helper()
	var doc yamlMap
	if err := yaml.Unmarshal([]byte(text), &doc); err != nil {
		t.Fatalf("%s: invalid YAML: %v", what, err)
	}
	if doc == nil {
		doc = yamlMap{}
	}
	return doc
}

func loadWf(t *testing.T, root, name string) yamlMap {
	t.Helper()
	return parseYAML(t, readFile(t, filepath.Join(root, ".github", "workflows", name)), name)
}

func workflowNames(t *testing.T, root string) []string {
	t.Helper()
	var paths []string
	for _, ext := range []string{"*.yml", "*.yaml"} {
		m, err := filepath.Glob(filepath.Join(root, ".github", "workflows", ext))
		if err != nil {
			t.Fatal(err)
		}
		paths = append(paths, m...)
	}
	var names []string
	for _, p := range paths {
		names = append(names, filepath.Base(p))
	}
	sort.Strings(names)
	return names
}

func asMap(v any) yamlMap {
	m, _ := v.(yamlMap)
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

func sortedKeys(m yamlMap) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func jobsOf(doc yamlMap) yamlMap { return asMap(doc["jobs"]) }

// stepsOf returns the steps of a job as maps.
func stepsOf(job any) []yamlMap {
	var out []yamlMap
	for _, s := range asList(asMap(job)["steps"]) {
		if m := asMap(s); m != nil {
			out = append(out, m)
		}
	}
	return out
}

// plantTree copies the named repo files into a temp root, applying each mutation (old -> new, exactly once
// required) to the matching file. It returns the temp root. A mutation whose old text is absent fails the
// test: a planted defect that did not land would make its red proof vacuous.
type mutation struct{ file, old, new string }

// allMarker prefixes a mutation's new text to replace EVERY occurrence (a defect planted in a repeated phrase).
const allMarker = "\x00ALL\x00"

func plantTree(t *testing.T, root string, files []string, muts ...mutation) string {
	t.Helper()
	out := t.TempDir()
	for _, f := range files {
		text := readFile(t, filepath.Join(root, f))
		for _, m := range muts {
			if m.file != f {
				continue
			}
			if !strings.Contains(text, m.old) {
				t.Fatalf("planted defect did not land: %q not found in %s", m.old, f)
			}
			if rest, all := strings.CutPrefix(m.new, allMarker); all {
				text = strings.ReplaceAll(text, m.old, rest)
			} else {
				text = strings.Replace(text, m.old, m.new, 1)
			}
		}
		dst := filepath.Join(out, f)
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(dst, []byte(text), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return out
}

func containsAll(got []string, want ...string) bool {
	joined := strings.Join(got, "\n")
	for _, w := range want {
		if !strings.Contains(joined, w) {
			return false
		}
	}
	return true
}

// expectViolations asserts the guard is silent on the real tree.
func expectNoViolations(t *testing.T, got []string) {
	t.Helper()
	if len(got) != 0 {
		t.Fatalf("guard reports violations on the real tree:\n%s", strings.Join(got, "\n"))
	}
}

// expectViolation asserts the planted defect is reported and the message names it.
func expectViolation(t *testing.T, got []string, mustContain ...string) {
	t.Helper()
	if len(got) == 0 {
		t.Fatalf("planted defect was NOT caught (guard returned no violation)")
	}
	if !containsAll(got, mustContain...) {
		t.Fatalf("guard fired but its message does not name the defect %v:\n%s", mustContain, strings.Join(got, "\n"))
	}
	t.Logf("planted defect caught: %s", got[0])
}

// shellCode returns the code of a shell script or a workflow run value: full-line comments and trailing `# ...`
// comments (outside quotes) are removed, so a token that only appears in a comment never satisfies a guard.
func shellCode(text string) string {
	lines := strings.Split(text, "\n")
	out := make([]string, 0, len(lines))
	for _, l := range lines {
		if strings.HasPrefix(strings.TrimSpace(l), "#") {
			continue
		}
		var q byte
		for j := 0; j < len(l); j++ {
			c := l[j]
			switch {
			case q != 0:
				if c == q {
					q = 0
				}
			case c == '\'' || c == '"':
				q = c
			case c == '#' && j > 0 && (l[j-1] == ' ' || l[j-1] == '\t'):
				l = l[:j]
			}
		}
		out = append(out, l)
	}
	return strings.Join(out, "\n")
}

// shellCommands is shellCode without the lines that only print: an echo or printf of a command is not the command.
func shellCommands(text string) string {
	var out []string
	for _, l := range strings.Split(shellCode(text), "\n") {
		if f := strings.Fields(l); len(f) > 0 && (f[0] == "echo" || f[0] == "printf") {
			continue
		}
		out = append(out, l)
	}
	return strings.Join(out, "\n")
}
