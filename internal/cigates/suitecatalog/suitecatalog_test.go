package suitecatalog_test

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// CHAOS-11422: ci/suites.tsv is the one list of what test suites exist (owner, trigger, command, proof, retirement).
// The guard compares it with the workflows both ways: a test command a workflow runs that has no row, and a row that
// says a workflow runs it when none does, both fail. It proves the lists agree; the receipts prove a suite ran.

const catalogPath = "ci/suites.tsv"

var columns = []string{"suite", "owner", "risk", "command", "inputs", "trigger", "service_needs", "proof", "context", "retirement", "kind", "mode", "why_manual"}

var (
	commandStart = regexp.MustCompile(`(?:^|[\s(!;&|])(?:bash\s+|\./)?((?:ci/(?:check|run)_[A-Za-z0-9_]+\.sh|scripts/[A-Za-z0-9_.-]+_test\.sh|ci/[A-Za-z0-9_.-]+_test\.sh)\b.*|go test\b.*)$`)
	commandCut   = regexp.MustCompile(`\s+(?:\|\||\||&&|;|\d?>|<|\))|;\s.*$|\s\\$`)
	goTestFlags  = regexp.MustCompile(`\s-(?:mod=\S+|trimpath|count=\S+|tags=\S+|timeout[= ]\S+|v|json|race)\b`)
	shellSuite   = regexp.MustCompile(`(?:_test|^test-[A-Za-z0-9_-]+)\.sh$`)
)

type site struct{ workflow, command string }

func repoRoot(t *testing.T) string {
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

func normalize(line string) string {
	line = strings.TrimSpace(line)
	for _, skip := range []string{"#", "echo ", "printf ", "chmod ", "source "} {
		if strings.HasPrefix(line, skip) {
			return ""
		}
	}
	m := commandStart.FindStringSubmatch(line)
	if m == nil {
		return ""
	}
	cmd := m[1]
	if loc := commandCut.FindStringIndex(cmd); loc != nil {
		cmd = cmd[:loc[0]]
	}
	if strings.HasPrefix(cmd, "go test") {
		cmd = strings.TrimSpace(goTestFlags.ReplaceAllString(cmd, ""))
	}
	return strings.Join(strings.Fields(cmd), " ")
}

func workflowSites(t *testing.T, root string) []site {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(root, ".github", "workflows", "*.y*ml"))
	if err != nil {
		t.Fatal(err)
	}
	var out []site
	for _, p := range paths {
		raw, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		var doc struct {
			Jobs map[string]struct {
				Steps []struct {
					Run string `yaml:"run"`
				} `yaml:"steps"`
			} `yaml:"jobs"`
		}
		if err := yaml.Unmarshal(raw, &doc); err != nil {
			t.Fatalf("%s: %v", p, err)
		}
		for _, job := range doc.Jobs {
			for _, step := range job.Steps {
				joined := strings.ReplaceAll(step.Run, "\\\n", " ")
				for _, line := range strings.Split(joined, "\n") {
					if c := normalize(line); c != "" {
						out = append(out, site{filepath.Base(p), c})
					}
				}
			}
		}
	}
	return out
}

type row map[string]string

func readCatalog(root string) ([]row, []string) {
	raw, err := os.ReadFile(filepath.Join(root, catalogPath))
	if err != nil {
		return nil, []string{fmt.Sprintf("%s: %v", catalogPath, err)}
	}
	var rows []row
	var bad []string
	header := false
	for i, line := range strings.Split(string(raw), "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		cells := strings.Split(line, "\t")
		if !header {
			header = true
			if strings.Join(cells, ",") != strings.Join(columns, ",") {
				bad = append(bad, fmt.Sprintf("%s:%d: header must be %s", catalogPath, i+1, strings.Join(columns, "<TAB>")))
			}
			continue
		}
		if len(cells) != len(columns) {
			bad = append(bad, fmt.Sprintf("%s:%d: %d cells, want %d", catalogPath, i+1, len(cells), len(columns)))
			continue
		}
		r := row{}
		for j, c := range columns {
			r[c] = cells[j]
		}
		rows = append(rows, r)
	}
	if !header {
		bad = append(bad, catalogPath+": no header")
	}
	return rows, bad
}

func shellSuiteFiles(root string) []string {
	var out []string
	for _, top := range []string{"ci", "scripts", "tools"} {
		_ = filepath.WalkDir(filepath.Join(root, top), func(p string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() {
				return nil
			}
			if rel, _ := filepath.Rel(root, p); shellSuite.MatchString(d.Name()) && !strings.Contains(rel, "/fixtures/") && !strings.Contains(rel, "/testdata/") {
				out = append(out, filepath.ToSlash(rel))
			}
			return nil
		})
	}
	sort.Strings(out)
	return out
}

func violations(t *testing.T, root string) []string {
	t.Helper()
	rows, bad := readCatalog(root)
	sites := workflowSites(t, root)
	ran := map[string]string{}
	for _, s := range sites {
		ran[s.command] = s.workflow
	}
	seenSuite := map[string]bool{}
	seenCommand := map[string]bool{}
	files := map[string]bool{}
	for _, r := range rows {
		id := r["suite"]
		for _, c := range columns {
			if r[c] == "" && !(c == "why_manual" && r["mode"] == "ci") {
				bad = append(bad, fmt.Sprintf("suite %q: empty %s", id, c))
			}
		}
		if seenSuite[id] {
			bad = append(bad, fmt.Sprintf("suite %q: duplicate suite id", id))
		}
		seenSuite[id] = true
		if seenCommand[r["command"]] {
			bad = append(bad, fmt.Sprintf("suite %q: duplicate command %q", id, r["command"]))
		}
		seenCommand[r["command"]] = true
		switch r["mode"] {
		case "ci":
			if _, ok := ran[r["command"]]; !ok {
				bad = append(bad, fmt.Sprintf("catalog row without caller: suite %q command %q is run by no workflow", id, r["command"]))
			}
		case "manual":
			if _, ok := ran[r["command"]]; ok {
				bad = append(bad, fmt.Sprintf("suite %q is mode=manual but %s runs %q", id, ran[r["command"]], r["command"]))
			}
		default:
			bad = append(bad, fmt.Sprintf("suite %q: mode %q is neither ci nor manual", id, r["mode"]))
		}
		for _, in := range strings.Split(r["inputs"], ";") {
			if in = strings.TrimSpace(in); in == "" || strings.ContainsAny(in, "*{") {
				continue
			}
			files[in] = true
			if _, err := os.Stat(filepath.Join(root, in)); err != nil {
				bad = append(bad, fmt.Sprintf("suite %q: input %s does not exist", id, in))
			}
		}
	}
	for _, s := range sites {
		if !seenCommand[s.command] {
			bad = append(bad, fmt.Sprintf("uncatalogued test command: %s runs %q", s.workflow, s.command))
		}
	}
	for _, f := range shellSuiteFiles(root) {
		if !files[f] {
			bad = append(bad, fmt.Sprintf("shell suite %s is named by no catalog row inputs", f))
		}
	}
	sort.Strings(bad)
	return bad
}

func TestSuiteCatalogMatchesWorkflows(t *testing.T) {
	if v := violations(t, repoRoot(t)); len(v) > 0 {
		t.Fatalf("%d violations:\n  %s", len(v), strings.Join(v, "\n  "))
	}
}

func TestNormalize(t *testing.T) {
	cases := map[string]string{
		`bash ci/check_go.sh auth fuzz "${{ matrix.target }}"`:                                `ci/check_go.sh auth fuzz "${{ matrix.target }}"`,
		`bash ci/check_go.sh venue-oracles "${{ matrix.shard }}" 8 2>&1 | tee "$X/r.log"`:     `ci/check_go.sh venue-oracles "${{ matrix.shard }}" 8`,
		`elif bash ci/check_go.sh auth relevant "${mb}" "${HEAD_SHA}" | tee r.txt | grep -q`:  `ci/check_go.sh auth relevant "${mb}" "${HEAD_SHA}"`,
		`./ci/run_tests.sh live-e2e`:                                                          `ci/run_tests.sh live-e2e`,
		`go test -mod=readonly -trimpath -count=1 -v -run '^TestX$' ./deploy/helm/dev-health`: `go test -run '^TestX$' ./deploy/helm/dev-health`,
		`chmod +x ci/run_tests.sh ci/run_live_backend_e2e.sh`:                                 ``,
		`source ci/mirror_precheck_lib.sh`:                                                    ``,
		`bash ci/check_migration_matrix_test.sh`:                                              `ci/check_migration_matrix_test.sh`,
	}
	for in, want := range cases {
		if got := normalize(in); got != want {
			t.Errorf("normalize(%q) = %q, want %q", in, got, want)
		}
	}
}

// plantTree copies the workflows, the catalog and every file the catalog or the shell-suite scan names into a temp tree.
func plantTree(t *testing.T) string {
	t.Helper()
	root := repoRoot(t)
	dst := t.TempDir()
	copyFile := func(rel string) {
		raw, err := os.ReadFile(filepath.Join(root, rel))
		if err != nil {
			return
		}
		if err := os.MkdirAll(filepath.Dir(filepath.Join(dst, rel)), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dst, rel), raw, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	wf, _ := filepath.Glob(filepath.Join(root, ".github", "workflows", "*.y*ml"))
	for _, p := range wf {
		rel, _ := filepath.Rel(root, p)
		copyFile(rel)
	}
	copyFile(catalogPath)
	rows, _ := readCatalog(root)
	for _, r := range rows {
		for _, in := range strings.Split(r["inputs"], ";") {
			copyFile(strings.TrimSpace(in))
		}
	}
	for _, f := range shellSuiteFiles(root) {
		copyFile(f)
	}
	return dst
}

func edit(t *testing.T, path string, fn func(string) string) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	out := fn(string(raw))
	if out == string(raw) {
		t.Fatalf("plant changed nothing in %s", path)
	}
	if err := os.WriteFile(path, []byte(out), 0o644); err != nil {
		t.Fatal(err)
	}
}

func wantViolation(t *testing.T, root, substr string) {
	t.Helper()
	v := strings.Join(violations(t, root), "\n")
	if !strings.Contains(v, substr) {
		t.Fatalf("want a violation containing %q, got:\n%s", substr, v)
	}
}

func TestPlantedTreeBaselineIsClean(t *testing.T) {
	if v := violations(t, plantTree(t)); len(v) > 0 {
		t.Fatalf("unplanted copy is not clean:\n  %s", strings.Join(v, "\n  "))
	}
}

func TestPlantedFaults(t *testing.T) {
	t.Run("workflow runs a command with no row", func(t *testing.T) {
		root := plantTree(t)
		edit(t, filepath.Join(root, ".github/workflows/go-quality.yml"), func(s string) string {
			return strings.Replace(s, "bash ci/check_river_compat_static.sh", "bash ci/check_river_compat_static.sh\n          bash ci/check_go.sh newverb", 1)
		})
		wantViolation(t, root, `uncatalogued test command: go-quality.yml runs "ci/check_go.sh newverb"`)
	})
	t.Run("row without a caller", func(t *testing.T) {
		root := plantTree(t)
		edit(t, filepath.Join(root, catalogPath), func(s string) string {
			return strings.Replace(s, "ci/check_river_compat_static.sh\t", "ci/check_river_compat_gone.sh\t", 1)
		})
		wantViolation(t, root, "catalog row without caller")
		wantViolation(t, root, `uncatalogued test command: go-quality.yml runs "ci/check_river_compat_static.sh"`)
	})
	t.Run("mirror suite row deleted", func(t *testing.T) {
		root := plantTree(t)
		edit(t, filepath.Join(root, catalogPath), func(s string) string {
			var keep []string
			for _, l := range strings.Split(s, "\n") {
				if !strings.Contains(l, "mirror_precheck_lib_test.sh") {
					keep = append(keep, l)
				}
			}
			return strings.Join(keep, "\n")
		})
		wantViolation(t, root, "ci/mirror_precheck_lib_test.sh is named by no catalog row")
		wantViolation(t, root, `uncatalogued test command: mirror-test-images.yml runs "ci/run_shell_tests.sh mirror-precheck-lib"`)
	})
	t.Run("new shell suite with no row", func(t *testing.T) {
		root := plantTree(t)
		if err := os.WriteFile(filepath.Join(root, "scripts", "foo_test.sh"), []byte("#!/usr/bin/env bash\n"), 0o755); err != nil {
			t.Fatal(err)
		}
		wantViolation(t, root, "shell suite scripts/foo_test.sh is named by no catalog row")
	})
	t.Run("manual row without a reason", func(t *testing.T) {
		root := plantTree(t)
		edit(t, filepath.Join(root, catalogPath), func(s string) string {
			var out []string
			for _, l := range strings.Split(s, "\n") {
				if strings.Contains(l, "\tmanual\t") {
					cells := strings.Split(l, "\t")
					cells[len(cells)-1] = ""
					l = strings.Join(cells, "\t")
				}
				out = append(out, l)
			}
			return strings.Join(out, "\n")
		})
		wantViolation(t, root, "empty why_manual")
	})
	t.Run("a refactor that keeps every command passes", func(t *testing.T) {
		root := plantTree(t)
		edit(t, filepath.Join(root, ".github/workflows/go-quality.yml"), func(s string) string {
			return strings.Replace(s, "name: Validate River compatibility harness", "name: Check the River harness", 1)
		})
		if v := violations(t, root); len(v) > 0 {
			t.Fatalf("a step rename must not fail the guard:\n  %s", strings.Join(v, "\n  "))
		}
	})
}
