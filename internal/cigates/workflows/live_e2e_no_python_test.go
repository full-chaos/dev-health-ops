package workflows_test

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// The Live Backend E2E workflow (both jobs: live-e2e and metrics-executed-proof) runs on the Go binaries alone: no
// Python toolchain is installed, no script it reaches starts a Python interpreter, and its path filter does not
// trigger on Python-only files.

var (
	pythonUseRe       = regexp.MustCompile(`\bpython3?\b|\brun_python\b|PY_PROG|py_program|\.py\b|\buv\b|\bpipx?3?\b|\.venv|PYTHONPATH|UV_`)
	pythonOnlyFilters = []string{"pyproject.toml", "requirements.txt", "uv.lock", "pytest.ini", "alembic.ini", "Makefile"}
	liveE2EScripts    = []string{
		"ci/run_live_backend_e2e.sh",
		"ci/run_metrics_executed_proof.sh",
		"ci/lib/go_worker_fixture.sh",
		"ci/lib/go_api_prove_e2e.sh",
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
	for _, p := range pythonOnlyFilters {
		if listed[p] {
			out = append(out, fmt.Sprintf("the live-e2e path filter lists %s, a Python-only file nothing in the jobs reads", p))
		}
	}
	for _, p := range []string{"ci/**", "cmd/**", "internal/**", "go.mod", "go.sum", ".github/workflows/live-e2e.yml"} {
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
