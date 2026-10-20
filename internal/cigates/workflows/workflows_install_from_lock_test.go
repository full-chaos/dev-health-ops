package workflows_test

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// CHAOS-10177: Go port of tests/tooling/test_workflows_install_from_lock.py (CHAOS-6589). requirements.txt is
// `-e .[dev]`: installing from it resolves every specifier against PyPI on every run and never reads uv.lock, so
// a PyPI release changed the tested set with no diff. A job installs the locked set with `uv sync --frozen`.
// allowedUnlocked is empty since CHAOS-6646; an entry needs a reason and goes stale-checked.

var allowedUnlocked = map[[2]string]string{}

const (
	e2eWorkflow = "live-e2e.yml"
)

var e2eJobs = []string{"live-e2e", "metrics-executed-proof"}

var (
	rootRequirements = regexp.MustCompile(`(?:-r|--requirement)[ =]+(?:\./)?requirements\.txt([^\w.-]|$)`)
	editableProject  = regexp.MustCompile(`\bpip3?\s+install\b[^\n]*\s-e\s+\.`)
)

func installsUnlocked(script string) bool {
	for _, line := range strings.Split(strings.ReplaceAll(script, "\\\n", " "), "\n") {
		code, _, _ := strings.Cut(line, "#")
		if !strings.Contains(code, "install") {
			continue
		}
		if rootRequirements.MatchString(code) || editableProject.MatchString(code) {
			return true
		}
	}
	return false
}

func runStepsOf(job any) []string {
	var runs []string
	for _, s := range stepsOf(job) {
		if r, ok := s["run"].(string); ok {
			runs = append(runs, r)
		}
	}
	return runs
}

func unlockedJobs(t *testing.T, root string) (found [][2]string, scanned int) {
	t.Helper()
	for _, name := range workflowNames(t, root) {
		jobs := jobsOf(loadWf(t, root, name))
		for _, jn := range sortedKeys(jobs) {
			unlocked := false
			for _, r := range runStepsOf(jobs[jn]) {
				scanned++
				if installsUnlocked(r) {
					unlocked = true
				}
			}
			if unlocked {
				found = append(found, [2]string{name, jn})
			}
		}
	}
	return found, scanned
}

func installFromLockViolations(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	found, scanned := unlockedJobs(t, root)
	seen := map[[2]string]bool{}
	var unlisted []string
	for _, p := range found {
		seen[p] = true
		if _, ok := allowedUnlocked[p]; !ok {
			unlisted = append(unlisted, p[0]+":"+p[1])
		}
	}
	sort.Strings(unlisted)
	if len(unlisted) > 0 {
		out = append(out, fmt.Sprintf("workflow job(s) install from the unlocked requirements.txt, so a PyPI release can change the tested set with no diff (CHAOS-6589); use `uv sync --frozen --all-extras --dev`: %v", unlisted))
	}
	for p := range allowedUnlocked {
		if !seen[p] {
			out = append(out, fmt.Sprintf("allowedUnlocked names job %v that no longer installs unlocked: delete the entry", p))
		}
	}
	if scanned < 50 {
		out = append(out, fmt.Sprintf("the scan read %d workflow run: steps, want >= 50: it is broken", scanned))
	}
	return out
}

func TestWorkflowsInstallFromLock(t *testing.T) {
	expectNoViolations(t, installFromLockViolations(t, repoRoot(t)))
}

// The detector itself: baseline shapes pass, every unlocked shape is caught.
func TestInstallsUnlockedDetector(t *testing.T) {
	for _, run := range []string{
		"uv sync --frozen --all-extras --dev\n",
		"python -m pip install --no-deps -r ci/requirements-live-python-oracles.txt\n",
		"pip install -r requirements-docs.txt\n",
		"uv pip install --python .venv/bin/python -r requirements-docs.txt\n",
		"# pip install -r requirements.txt (a comment)\n",
	} {
		if installsUnlocked(run) {
			t.Errorf("detector flags a locked/other install: %q", run)
		}
	}
	for _, run := range []string{
		"pip install -r requirements.txt\n",
		"python -m pip install --upgrade pip\npip install -r requirements.txt\n",
		"pip install --requirement=requirements.txt\n",
		"uv pip install -r ./requirements.txt\n",
		"pip install -e .[dev]\n",
		"pip install \\\n  -r requirements.txt\n",
	} {
		if !installsUnlocked(run) {
			t.Errorf("detector misses an unlocked install: %q", run)
		}
	}
}
