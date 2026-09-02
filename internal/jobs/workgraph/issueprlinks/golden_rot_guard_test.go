package issueprlinks

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestIssuePRLinksGoldenMatchesLivePython is the rot guard for
// tests/fixtures/issue_pr_links_python_golden.json.
//
// TestDeriveMatchesFrozenPythonGoldenExhaustively proves Go matches the frozen
// file. Nothing there proves PYTHON still does -- the frozen rows were captured
// from the deployed producer once, and a later change to
// `_derive_issue_pr_links_from_dependencies` would leave the Go test green
// while the two planes silently diverged. This runs the producer again and
// compares.
//
// # Why this guard replays instead of re-querying
//
// The house pattern (repouser/golden_rot_guard_test.go) re-runs the generator
// and byte-diffs the whole file. That works there because that generator is
// hermetic. This one reads ClickHouse, and those tables move continuously --
// RMT inserts, syncs, materializer runs -- so a re-query guard would fail on
// DATA drift and say nothing about PYTHON drift, the exact
// "comparisons across a time gap are inadmissible" trap.
//
// So the generator has a --replay mode: it feeds the golden's OWN frozen
// inputs back through the deployed producer and prints the links that come
// out. No ClickHouse, no clock, no drift -- the only thing that can move the
// result is the producer's logic, which is precisely what this guard is for.
//
// The comparison is on the `links` array alone, because that is what replay
// recomputes; the frozen inputs are an input to the guard, not an output of it.
func TestIssuePRLinksGoldenMatchesLivePython(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	proofDirectory := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if proofDirectory == "" {
		t.Fatal("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR is required")
	}

	repoRoot := repositoryRootPath(t)
	python := issuePRLinksLivePython(t, repoRoot)
	generator := filepath.Join(repoRoot, "tests", "fixtures", "generate_issue_pr_links_python_golden.py")
	if info, err := os.Stat(generator); err != nil || !info.Mode().IsRegular() {
		t.Fatalf("golden generator is missing at %s: %v", generator, err)
	}
	goldenPath := filepath.Join(repoRoot, "tests", "fixtures", "issue_pr_links_python_golden.json")

	cmd := exec.Command(python, generator, "--replay", goldenPath)
	cmd.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(repoRoot, "src"))
	rendered, err := cmd.Output()
	if err != nil {
		var stderr []byte
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			stderr = exitErr.Stderr
		}
		t.Fatalf("replay the golden through live Python: %v: %s", err, stderr)
	}

	frozen, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatal(err)
	}
	var golden struct {
		Links json.RawMessage `json:"links"`
	}
	if err := json.Unmarshal(frozen, &golden); err != nil {
		t.Fatalf("decode frozen golden: %v", err)
	}

	// Both sides are the same Python-rendered compact JSON of the same
	// structure, so a canonical byte comparison is sufficient and exact.
	if bytes.Equal(canonicalJSON(t, golden.Links), canonicalJSON(t, rendered)) {
		if writeErr := os.WriteFile(
			filepath.Join(proofDirectory, "issue-pr-links-golden"), []byte("executed"), 0o644,
		); writeErr != nil {
			t.Fatalf("write live-python-oracle proof: %v", writeErr)
		}
		return
	}
	t.Error(
		"live Python no longer reproduces the frozen golden's links from the frozen inputs.\n" +
			"This is PYTHON drift, not a Go bug, and not data drift: the replay feeds the golden's own\n" +
			"frozen reads back through the deployed producer, so ClickHouse cannot be the cause.\n" +
			"If the change was intended, regenerate against live data with\n" +
			"    docker cp tests/fixtures/generate_issue_pr_links_python_golden.py dev-health-api-1:/tmp/\n" +
			"    docker exec -i dev-health-api-1 python /tmp/generate_issue_pr_links_python_golden.py --stdout\n" +
			"and re-run the Go golden test, which must still pass against the new file.",
	)
}

// canonicalJSON re-encodes a JSON document so an insignificant whitespace or
// key-order difference between the two renderings cannot be reported as drift.
func canonicalJSON(t *testing.T, raw []byte) []byte {
	t.Helper()
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		t.Fatalf("decode json for canonicalisation: %v", err)
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("re-encode json for canonicalisation: %v", err)
	}
	return encoded
}

func issuePRLinksLivePython(t *testing.T, repoRoot string) string {
	t.Helper()
	resolved := os.Getenv("PYTHON")
	if resolved == "" {
		path, err := exec.LookPath("python3")
		if err != nil {
			t.Fatalf("python3 is required for the golden rot guard: %v", err)
		}
		resolved = path
	}
	cmd := exec.Command(resolved, "-c", "import dev_health_ops, sys; sys.stdout.write(dev_health_ops.__file__)")
	cmd.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(repoRoot, "src"))
	located, err := cmd.Output()
	if err != nil {
		t.Fatalf("resolve dev_health_ops with %s: %v", resolved, err)
	}
	module := string(located)
	if !strings.HasPrefix(module, repoRoot+string(os.PathSeparator)) {
		t.Fatalf(
			"%s resolves dev_health_ops to %s, which is OUTSIDE this checkout (%s) -- "+
				"the guard would be comparing another worktree's producer against this "+
				"worktree's frozen golden; set PYTHONPATH to this checkout's src",
			resolved, module, repoRoot,
		)
	}
	return resolved
}
