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

var (
	uvSyncRe   = regexp.MustCompile(`(?m)^\s*uv sync --frozen\b`)
	venvPathRe = regexp.MustCompile(`(?m)^\s*echo\s+"[^"]*\.venv/bin"\s*>>\s*"?\$GITHUB_PATH"?\s*$`)
)

// filterListsPath reports whether any dorny/paths-filter `code` filter of the workflow lists the path; the filters
// are a YAML document inside a string, so they are parsed (a commented-out entry is not an entry).
func filterListsPath(t *testing.T, doc yamlMap, want string) bool {
	t.Helper()
	jobs := jobsOf(doc)
	for _, jn := range sortedKeys(jobs) {
		for _, st := range stepsOf(jobs[jn]) {
			f := asString(asMap(st["with"])["filters"])
			if f == "" {
				continue
			}
			for _, e := range asList(parseYAML(t, f, "paths filters")["code"]) {
				if asString(e) == want {
					return true
				}
			}
		}
	}
	return false
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
	e2e := loadWf(t, root, e2eWorkflow)
	for _, id := range e2eJobs {
		job := jobsOf(e2e)[id]
		if job == nil {
			out = append(out, fmt.Sprintf("%s has no job %q", e2eWorkflow, id))
			continue
		}
		var runs []string
		for _, r := range runStepsOf(job) {
			runs = append(runs, shellCode(r))
		}
		any := func(f func(string) bool) bool {
			for _, r := range runs {
				if f(r) {
					return true
				}
			}
			return false
		}
		if !any(uvSyncRe.MatchString) {
			out = append(out, fmt.Sprintf("%s:%s must install with `uv sync --frozen ...` as a command (not text in an echo or a comment)", e2eWorkflow, id))
		}
		if !any(venvPathRe.MatchString) {
			out = append(out, fmt.Sprintf("%s:%s must put .venv/bin on GITHUB_PATH with `echo ... >> \"$GITHUB_PATH\"`: its scripts call python3 from PATH", e2eWorkflow, id))
		}
		if any(installsUnlocked) {
			out = append(out, fmt.Sprintf("%s:%s installs unlocked", e2eWorkflow, id))
		}
		setup := false
		for _, s := range stepsOf(job) {
			if strings.Contains(asString(s["uses"]), "astral-sh/setup-uv") {
				setup = true
			}
		}
		if !setup {
			out = append(out, fmt.Sprintf("%s:%s must set up uv", e2eWorkflow, id))
		}
	}
	if !filterListsPath(t, e2e, "uv.lock") {
		out = append(out, "the live-e2e path filter `code` must list uv.lock (as a list entry, not a comment): the install reads the lock, so a lock-only change must run the job")
	}
	return out
}

func TestWorkflowsInstallFromLock(t *testing.T) {
	expectNoViolations(t, installFromLockViolations(t, repoRoot(t)))
}

func TestWorkflowsInstallFromLockPlantedDefects(t *testing.T) {
	root := repoRoot(t)
	files := []string{}
	for _, n := range workflowNames(t, root) {
		files = append(files, ".github/workflows/"+n)
	}
	const e2e = ".github/workflows/live-e2e.yml"
	cases := []struct {
		name string
		muts []mutation
		want []string
	}{
		{"e2e job installs from requirements.txt", []mutation{{e2e, "uv sync --frozen", "pip install -r requirements.txt #"}}, []string{"installs unlocked", "unlocked requirements.txt"}},
		{"e2e uv sync survives only as an echoed string", []mutation{{e2e, "uv sync --frozen --all-extras --dev", allMarker + "echo \"uv sync --frozen --all-extras --dev\""}}, []string{"must install with `uv sync --frozen"}},
		{"e2e PATH step survives only as a comment-shaped string", []mutation{{e2e, `echo "$PWD/.venv/bin" >> "$GITHUB_PATH"`, allMarker + `echo "$PWD/.venv/bin # GITHUB_PATH"`}}, []string{"must put .venv/bin on GITHUB_PATH"}},
		{"e2e path filter lists uv.lock only in a comment", []mutation{{e2e, "              - 'uv.lock'\n", "              # - 'uv.lock'\n"}}, []string{"must list uv.lock"}},
		{"e2e path filter drops uv.lock", []mutation{{e2e, "- 'uv.lock'", "- 'pyproject.toml'"}}, []string{"uv.lock"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tree := plantTree(t, root, files, c.muts...)
			expectViolation(t, installFromLockViolations(t, tree), c.want...)
		})
	}
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
