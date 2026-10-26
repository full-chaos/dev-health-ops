package suitecatalog_test

import (
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path"
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
	goTestFlags  = regexp.MustCompile(`\s-(?:mod=\S+|trimpath|count=\S+|timeout[= ]\S+|v|json|race)\b`)
	segmentSplit = regexp.MustCompile(`\s+(?:&&|\|\||\|)\s+|;\s+|;$`)
	yamlToken    = regexp.MustCompile(`[A-Za-z0-9*_./-]+\.ya?ml`)
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

func normalizeAll(line string) []string {
	var out []string
	for _, seg := range segmentSplit.Split(line, -1) {
		if c := normalize(seg); c != "" {
			out = append(out, c)
		}
	}
	return out
}

type runDoc struct {
	Jobs map[string]struct {
		Steps []struct {
			Run string `yaml:"run"`
		} `yaml:"steps"`
	} `yaml:"jobs"`
	Runs struct {
		Steps []struct {
			Run string `yaml:"run"`
		} `yaml:"steps"`
	} `yaml:"runs"`
}

type runBlock struct{ file, run string }

// allRuns returns every run block of the workflows and of the composite actions under .github/actions.
func allRuns(t *testing.T, root string) []runBlock {
	t.Helper()
	var paths []string
	for _, g := range []string{filepath.Join(root, ".github", "workflows", "*.y*ml"), filepath.Join(root, ".github", "actions", "*", "action.y*ml")} {
		m, err := filepath.Glob(g)
		if err != nil {
			t.Fatal(err)
		}
		paths = append(paths, m...)
	}
	var out []runBlock
	for _, p := range paths {
		raw, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		var doc runDoc
		if err := yaml.Unmarshal(raw, &doc); err != nil {
			t.Fatalf("%s: %v", p, err)
		}
		name := filepath.Base(p)
		if strings.Contains(filepath.ToSlash(p), "/.github/actions/") {
			name = filepath.Base(filepath.Dir(p)) + "/" + name
		}
		var runs []string
		for _, job := range doc.Jobs {
			for _, step := range job.Steps {
				runs = append(runs, step.Run)
			}
		}
		for _, step := range doc.Runs.Steps {
			runs = append(runs, step.Run)
		}
		for _, run := range runs {
			out = append(out, runBlock{name, run})
		}
	}
	return out
}

// workflowSites reads every run step of the workflows and of the composite actions.
func workflowSites(t *testing.T, root string) []site {
	t.Helper()
	var out []site
	for _, b := range allRuns(t, root) {
		for _, line := range strings.Split(strings.ReplaceAll(b.run, "\\\n", " "), "\n") {
			for _, c := range normalizeAll(line) {
				out = append(out, site{b.file, c})
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
	ranIn := map[string]map[string]bool{}
	for _, s := range sites {
		ran[s.command] = s.workflow
		if ranIn[s.command] == nil {
			ranIn[s.command] = map[string]bool{}
		}
		ranIn[s.command][s.workflow] = true
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
			bad = append(bad, triggerViolations(id, r["trigger"], r["command"], ranIn[r["command"]])...)
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
		if st, err := os.Stat(filepath.Join(root, rel)); err == nil && st.IsDir() {
			if err := os.MkdirAll(filepath.Join(dst, rel), 0o755); err != nil {
				t.Fatal(err)
			}
			return
		}
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

// plantRowEdit breaks the first mode=ci shell row in one way (kind selects which) and asserts the message.
func plantRowEdit(t *testing.T, a, b string, kind int, want string) {
	t.Helper()
	root := plantTree(t)
	edit(t, filepath.Join(root, catalogPath), func(s string) string {
		lines := strings.Split(s, "\n")
		for i, l := range lines {
			cells := strings.Split(l, "\t")
			if len(cells) != len(columns) || cells[0] != "migration-matrix-selftest" {
				continue
			}
			switch kind {
			case 0:
				cells[0] = "go-worker-fixture-selftest"
			case 1:
				cells[1] = ""
			case 2:
				cells[4] = cells[4] + ";ci/does_not_exist.sh"
			case 3:
				cells[11] = "manual"
				cells[12] = "planted"
			case 4:
				cells[3] = "ci/go_worker_fixture_test.sh"
			}
			lines[i] = strings.Join(cells, "\t")
		}
		return strings.Join(lines, "\n")
	})
	wantViolation(t, root, want)
}

func TestPlantedFaults(t *testing.T) {
	t.Run("workflow runs a command with no row", func(t *testing.T) {
		root := plantTree(t)
		edit(t, filepath.Join(root, ".github/workflows/go-quality.yml"), func(s string) string {
			return strings.Replace(s, "run: bash ci/check_river_compat_static.sh", "run: |\n          bash ci/check_river_compat_static.sh\n          bash ci/check_go.sh newverb", 1)
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
	t.Run("a second command after && is seen", func(t *testing.T) {
		root := plantTree(t)
		edit(t, filepath.Join(root, ".github/workflows/go-quality.yml"), func(s string) string {
			return strings.Replace(s, "run: bash ci/check_river_compat_static.sh", "run: bash ci/check_river_compat_static.sh && bash ci/check_go.sh hidden || true", 1)
		})
		wantViolation(t, root, `uncatalogued test command: go-quality.yml runs "ci/check_go.sh hidden"`)
	})
	t.Run("a command in a composite action is seen", func(t *testing.T) {
		root := plantTree(t)
		dir := filepath.Join(root, ".github", "actions", "probe")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		action := "runs:\n  using: composite\n  steps:\n    - shell: bash\n      run: bash ci/check_go.sh actionhidden\n"
		if err := os.WriteFile(filepath.Join(dir, "action.yml"), []byte(action), 0o644); err != nil {
			t.Fatal(err)
		}
		wantViolation(t, root, `uncatalogued test command: probe/action.yml runs "ci/check_go.sh actionhidden"`)
	})
	t.Run("a command moved to another workflow than the row names", func(t *testing.T) {
		root := plantTree(t)
		edit(t, filepath.Join(root, catalogPath), func(s string) string {
			return strings.Replace(s, "go-quality.yml static leg (every relevant run)\tnone\tthe frozen CHAOS-3034", "go.yml static leg (every relevant run)\tnone\tthe frozen CHAOS-3034", 1)
		})
		wantViolation(t, root, "go-quality.yml runs")
		wantViolation(t, root, "names go.yml")
	})
	t.Run("duplicate suite id", func(t *testing.T) {
		plantRowEdit(t, "migration-matrix-selftest", "go-worker-fixture-selftest", 0, "duplicate suite id")
	})
	t.Run("empty column", func(t *testing.T) { plantRowEdit(t, "", "", 1, "empty owner") })
	t.Run("input that does not exist", func(t *testing.T) { plantRowEdit(t, "", "", 2, "does not exist") })
	t.Run("manual row that a workflow runs", func(t *testing.T) { plantRowEdit(t, "", "", 3, "is mode=manual but") })
	t.Run("duplicate command", func(t *testing.T) { plantRowEdit(t, "", "", 4, "duplicate command") })
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

func TestRunShellTestsForwardsArgumentsToANamedSuite(t *testing.T) {
	root := t.TempDir()
	for _, d := range []string{"ci", "scripts"} {
		if err := os.MkdirAll(filepath.Join(root, d), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	real, err := os.ReadFile(filepath.Join(repoRoot(t), "ci", "run_shell_tests.sh"))
	if err != nil {
		t.Fatal(err)
	}
	cat := strings.Join(columns, "\t") + "\n" +
		strings.Join([]string{"probe", "ci", "low", "scripts/probe_test.sh", "scripts/probe_test.sh", "none", "none", "p", "none", "r", "shell", "manual", "needs an argument"}, "\t") + "\n"
	files := map[string]string{
		"ci/run_shell_tests.sh": string(real),
		"ci/suites.tsv":         cat,
		"scripts/probe_test.sh": "#!/usr/bin/env bash\necho \"args=$*\"\n",
	}
	for p, c := range files {
		if err := os.WriteFile(filepath.Join(root, p), []byte(c), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	out, err := exec.Command("bash", filepath.Join(root, "ci", "run_shell_tests.sh"), "probe", "wrapper-path", "second").CombinedOutput()
	if err != nil {
		t.Fatalf("named suite failed: %v\n%s", err, out)
	}
	if !strings.Contains(string(out), "args=wrapper-path second") {
		t.Fatalf("arguments were not forwarded:\n%s", out)
	}
}

// triggerViolations holds a row's trigger text to the workflows that really run its command: every workflow that runs it
// must be named (a glob token such as venue-oracles*.yml counts), and every named workflow must run it.
func triggerViolations(id, trigger, command string, actual map[string]bool) []string {
	var bad []string
	tokens := yamlToken.FindAllString(trigger, -1)
	for wf := range actual {
		named := false
		for _, tok := range tokens {
			if ok, _ := path.Match(tok, wf); ok {
				named = true
			}
		}
		if !named {
			bad = append(bad, fmt.Sprintf("suite %q: %s runs %q but its trigger names %v", id, wf, command, tokens))
		}
	}
	for _, tok := range tokens {
		hit := false
		for wf := range actual {
			if ok, _ := path.Match(tok, wf); ok {
				hit = true
			}
		}
		if !hit {
			bad = append(bad, fmt.Sprintf("suite %q: its trigger names %s but no such workflow runs %q", id, tok, command))
		}
	}
	return bad
}

const exceptionsPath = "ci/suites_dynamic_exceptions.tsv"

func plantDynamic(t *testing.T, level, line string) string {
	t.Helper()
	root := plantTree(t)
	wf := filepath.Join(root, ".github/workflows/go-quality.yml")
	edit(t, wf, func(s string) string {
		step := "run: |\n          bash ci/check_river_compat_static.sh\n          " + line
		switch level {
		case "step":
			step = "env:\n          RUNNER: go\n        " + step
		case "job":
			s = strings.Replace(s, "  go-quality-leg:\n", "  go-quality-leg:\n    env:\n      RUNNER: go\n", 1)
		case "workflow":
			s = strings.Replace(s, "\njobs:\n", "\nenv:\n  RUNNER: go\njobs:\n", 1)
		}
		return strings.Replace(s, "run: bash ci/check_river_compat_static.sh", step, 1)
	})
	return root
}

func TestDynamicTestCommandsAreRefused(t *testing.T) {
	lines := map[string]string{
		"env variable":         `$RUNNER test ./internal/cigates/suitecatalog`,
		"braced variable":      `${RUNNER} test ./internal/cigates/suitecatalog`,
		"command substitution": `$(echo go) test ./internal/cigates/suitecatalog`,
		"backtick":             "`echo go` test ./internal/cigates/suitecatalog",
		"bash -c variable":     `bash -c "$CMD"`,
		"eval":                 `eval "$CMD"`,
		"xargs":                `echo ./x | xargs go test`,
		"function":             "runit() { go test ./x; }\n          runit",
	}
	for name, line := range lines {
		for _, level := range []string{"step", "job", "workflow"} {
			t.Run(name+"/"+level, func(t *testing.T) {
				wantViolation(t, plantDynamic(t, level, line), "variable-built command")
			})
		}
	}
	t.Run("a literal command with a variable argument stays clean", func(t *testing.T) {
		root := plantDynamic(t, "step", `go test ./internal/cigates/suitecatalog "$EXTRA"`)
		if v := strings.Join(violations(t, root), "\n"); strings.Contains(v, "variable-built command") {
			t.Fatalf("a variable argument is not a variable command:\n%s", v)
		}
	})
}

func TestDynamicExceptionTable(t *testing.T) {
	t.Run("an exception with an empty reason is refused", func(t *testing.T) {
		root := plantTree(t)
		edit(t, filepath.Join(root, exceptionsPath), func(s string) string {
			lines := strings.Split(s, "\n")
			for i, l := range lines {
				if cells := strings.Split(l, "\t"); len(cells) == 3 && !strings.HasPrefix(l, "#") && cells[0] != "file" {
					cells[2] = ""
					lines[i] = strings.Join(cells, "\t")
					break
				}
			}
			return strings.Join(lines, "\n")
		})
		wantViolation(t, root, "empty reason")
	})
	t.Run("an exception that matches no workflow line is refused", func(t *testing.T) {
		root := plantTree(t)
		edit(t, filepath.Join(root, exceptionsPath), func(s string) string {
			return s + "go.yml\t\"${GONE}/tool\"\tplanted stale row\n"
		})
		wantViolation(t, root, "matches no workflow line")
	})
	t.Run("removing a reviewed exception makes its line a violation", func(t *testing.T) {
		root := plantTree(t)
		edit(t, filepath.Join(root, exceptionsPath), func(s string) string {
			var keep []string
			for _, l := range strings.Split(s, "\n") {
				if !strings.Contains(l, "gitleaks") {
					keep = append(keep, l)
				}
			}
			return strings.Join(keep, "\n")
		})
		wantViolation(t, root, "variable-built command: security-scan.yml")
	})
}
