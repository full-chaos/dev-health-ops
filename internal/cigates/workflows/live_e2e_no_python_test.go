package workflows_test

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// The Live Backend E2E workflow (both jobs: live-e2e and metrics-executed-proof) runs on the Go binaries alone: no
// Python toolchain is installed, no script it reaches starts a Python interpreter, and its path filter does not
// trigger on Python-only files.

var (
	pythonUseRe      = regexp.MustCompile(`\bpython3?\b|\brun_python\b|PY_PROG|py_program|\.py\b|\buv\b|\bpipx?3?\b|\.venv|PYTHONPATH|UV_`)
	pythonOnlyFilter = regexp.MustCompile(`^(pyproject\.toml|requirements.*\.txt|uv\.lock|pytest\.ini|alembic\.ini|Makefile|tests/.*)$`)
	contractDirRe    = regexp.MustCompile(`contracts/[a-z0-9-]+/[a-z0-9-]+`)
	liveE2EScripts   = []string{
		"ci/run_live_backend_e2e.sh",
		"ci/run_metrics_executed_proof.sh",
		"ci/lib/go_worker_fixture.sh",
		"ci/lib/go_api_prove_e2e.sh",
		"ci/lib/e2e_fixture.sh",
	}
)

func liveE2ENoPythonViolations(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	doc := loadWf(t, root, e2eWorkflow)
	for _, id := range e2eJobs {
		job := jobsOf(doc)[id]
		if job == nil {
			out = append(out, fmt.Sprintf("%s has no job %q", e2eWorkflow, id))
			continue
		}
		for _, s := range stepsOf(job) {
			uses := asString(s["uses"])
			if strings.Contains(uses, "setup-uv") || strings.Contains(uses, "setup-python") {
				out = append(out, fmt.Sprintf("%s:%s sets up a Python toolchain (%s)", e2eWorkflow, id, uses))
			}
			if run, ok := s["run"].(string); ok && pythonUseRe.MatchString(shellCode(run)) {
				out = append(out, fmt.Sprintf("%s:%s step %q runs Python tooling: %q", e2eWorkflow, id, asString(s["name"]), pythonUseRe.FindString(shellCode(run))))
			}
			for k := range asMap(s["env"]) {
				if strings.HasPrefix(k, "UV_") || k == "PYTHONPATH" {
					out = append(out, fmt.Sprintf("%s:%s step %q sets %s", e2eWorkflow, id, asString(s["name"]), k))
				}
			}
		}
	}
	for _, rel := range liveE2EScripts {
		raw, err := os.ReadFile(filepath.Join(root, rel))
		if err != nil {
			out = append(out, fmt.Sprintf("cannot read %s: %v", rel, err))
			continue
		}
		if m := pythonUseRe.FindString(shellCode(string(raw))); m != "" {
			out = append(out, fmt.Sprintf("%s starts or prepares Python (%q): the live-e2e jobs install no Python", rel, m))
		}
	}
	raw, err := os.ReadFile(filepath.Join(root, "ci/run_tests.sh"))
	if err != nil {
		out = append(out, fmt.Sprintf("cannot read ci/run_tests.sh: %v", err))
	} else if body := functionBody(string(raw), "live_e2e_tests"); body == "" {
		out = append(out, "ci/run_tests.sh has no live_e2e_tests function")
	} else if m := pythonUseRe.FindString(shellCode(body)); m != "" {
		out = append(out, fmt.Sprintf("ci/run_tests.sh live_e2e_tests uses Python (%q)", m))
	}
	listed := map[string]bool{}
	for _, jn := range sortedKeys(jobsOf(doc)) {
		for _, st := range stepsOf(jobsOf(doc)[jn]) {
			f := asString(asMap(st["with"])["filters"])
			if f == "" {
				continue
			}
			for _, e := range asList(parseYAML(t, f, "paths filters")["code"]) {
				listed[asString(e)] = true
			}
		}
	}
	for p := range listed {
		if pythonOnlyFilter.MatchString(p) {
			out = append(out, fmt.Sprintf("the live-e2e path filter lists %s, a Python-only file nothing in the jobs reads", p))
		}
	}
	var tracked []string
	if tp, err := trackedPaths(repoRoot(t), repoRoot(t)); err != nil {
		out = append(out, "cannot list tracked files: "+err.Error())
	} else {
		tracked = tp
	}
	for _, p := range sortedKeysBool(listed) {
		re, err := githubGlobToRegexp(p)
		if err != nil {
			out = append(out, err.Error())
			continue
		}
		for _, f := range tracked {
			if (strings.HasSuffix(f, ".py") || strings.HasSuffix(f, ".pyi")) && re.MatchString(f) {
				out = append(out, fmt.Sprintf("the live-e2e path filter entry %s matches the tracked Python file %s: a Python-only change must not run the jobs", p, f))
				break
			}
		}
	}
	var entryRes []*regexp.Regexp
	for _, p := range sortedKeysBool(listed) {
		if re, err := githubGlobToRegexp(p); err == nil {
			entryRes = append(entryRes, re)
		}
	}
	covered := func(f string) bool {
		for _, re := range entryRes {
			if re.MatchString(f) {
				return true
			}
		}
		return false
	}
	// Every contracts directory the jobs build or read must trigger them: one that holds a Go package (embedded into
	// the binaries) or that a runner script names.
	contractDirs := map[string]bool{}
	for _, f := range tracked {
		if strings.HasPrefix(f, "contracts/") && strings.HasSuffix(f, ".go") {
			contractDirs[path.Dir(f)] = true
		}
	}
	for _, rel := range liveE2EScripts {
		if raw, err := os.ReadFile(filepath.Join(root, rel)); err == nil {
			for _, d := range contractDirRe.FindAllString(shellCode(string(raw)), -1) {
				contractDirs[d] = true
			}
		}
	}
	for _, d := range sortedKeysBool(contractDirs) {
		for _, f := range tracked {
			if strings.HasPrefix(f, d+"/") && !strings.HasSuffix(f, ".py") && !strings.HasSuffix(f, ".md") && !covered(f) {
				out = append(out, fmt.Sprintf("the live-e2e path filter does not cover %s, a file of %s that the jobs build or read", f, d))
				break
			}
		}
	}
	for _, p := range []string{"cmd/**", "internal/**/*.go", "ci/run_*.sh", "ci/lib/**", "scripts/worker/**", "src/dev_health_ops/config/**", "go.mod", "go.sum", ".github/workflows/live-e2e.yml"} {
		if !listed[p] {
			out = append(out, fmt.Sprintf("the live-e2e path filter must list %s", p))
		}
	}
	return out
}

// functionBody returns the text of a top-level shell function `name() {` ... `}` (the closing brace in column 0).
func functionBody(script, name string) string {
	start := strings.Index(script, "\n"+name+"() {")
	if start < 0 {
		return ""
	}
	rest := script[start+1:]
	end := strings.Index(rest, "\n}\n")
	if end < 0 {
		return ""
	}
	return rest[:end]
}

func TestLiveE2EInstallsNoPython(t *testing.T) {
	expectNoViolations(t, liveE2ENoPythonViolations(t, repoRoot(t)))
}

func TestLiveE2ENoPythonPlantedDefects(t *testing.T) {
	root := repoRoot(t)
	const wf = ".github/workflows/live-e2e.yml"
	const live = "ci/run_live_backend_e2e.sh"
	const prove = "ci/lib/go_api_prove_e2e.sh"
	const runTests = "ci/run_tests.sh"
	files := append([]string{wf, runTests}, liveE2EScripts...)
	cases := []struct {
		name string
		muts []mutation
		want []string
	}{
		{"a setup-uv step returns", []mutation{{wf, "      - name: Run live-e2e tier\n", "      - name: Install uv\n        uses: astral-sh/setup-uv@v1\n\n      - name: Run live-e2e tier\n"}}, []string{"sets up a Python toolchain"}},
		{"a setup-python step returns", []mutation{{wf, "      - name: Run live-e2e tier\n", "      - name: Python\n        uses: actions/setup-python@v6\n\n      - name: Run live-e2e tier\n"}}, []string{"sets up a Python toolchain"}},
		{"a run step syncs the venv", []mutation{{wf, "          chmod +x ci/run_tests.sh ci/run_live_backend_e2e.sh\n", "          uv sync --frozen\n          chmod +x ci/run_tests.sh ci/run_live_backend_e2e.sh\n"}}, []string{"runs Python tooling"}},
		{"a run step puts the venv on the path", []mutation{{wf, "          chmod +x ci/run_tests.sh ci/run_live_backend_e2e.sh\n", "          echo \"$PWD/.venv/bin\" >> \"$GITHUB_PATH\"\n          chmod +x ci/run_tests.sh ci/run_live_backend_e2e.sh\n"}}, []string{"runs Python tooling"}},
		{"the live runner calls python3", []mutation{{live, `"${BIN_DIR}/cigate" live-e2e assert-health "${HEALTH_FILE}"`, `python3 assert_health.py "${HEALTH_FILE}"`}}, []string{live + " starts or prepares Python"}},
		{"the live runner sets PYTHONPATH", []mutation{{live, "require_cmd curl\n", "require_cmd curl\nexport PYTHONPATH=src\n"}}, []string{live + " starts or prepares Python"}},
		{"the prove helper calls run_python", []mutation{{prove, `"${BIN_DIR}/cigate" live-e2e edge-request "${dir}/documents.json"`, `run_python edge-request.py "${dir}/documents.json"`}}, []string{prove + " starts or prepares Python"}},
		{"the metrics runner calls python3", []mutation{{"ci/run_metrics_executed_proof.sh", "require_cmd jq\n", "require_cmd jq\npython3 -c 'print(1)'\n"}}, []string{"ci/run_metrics_executed_proof.sh starts or prepares Python"}},
		{"the live tier function calls python", []mutation{{runTests, `run_step "live backend e2e" bash ./ci/run_live_backend_e2e.sh`, `run_step "live backend e2e" python3 ci/live.py`}}, []string{"live_e2e_tests uses Python"}},
		{"the path filter lists uv.lock again", []mutation{{wf, "              - 'cmd/**'\n", "              - 'cmd/**'\n              - 'uv.lock'\n"}}, []string{"lists uv.lock"}},
		{"a broad filter glob matches Python files", []mutation{{wf, "              - 'cmd/**'\n", "              - 'cmd/**'\n              - 'scripts/**'\n"}}, []string{"matches the tracked Python file"}},
		{"the path filter drops the sync-dispatch contracts", []mutation{{wf, "              - 'contracts/sync-dispatch/v1/**'\n", ""}}, []string{"does not cover contracts/sync-dispatch/v1"}},
		{"the path filter drops cmd/**", []mutation{{wf, "              - 'cmd/**'\n", ""}}, []string{"must list cmd/**"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tree := plantTree(t, root, files, c.muts...)
			expectViolation(t, liveE2ENoPythonViolations(t, tree), c.want...)
		})
	}
	t.Run("a comment that names uv is not a violation", func(t *testing.T) {
		tree := plantTree(t, root, files, mutation{live, "require_cmd curl\n", "require_cmd curl\n# no python3 and no uv here\n"})
		expectNoViolations(t, liveE2ENoPythonViolations(t, tree))
	})
}

func sortedKeysBool(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
