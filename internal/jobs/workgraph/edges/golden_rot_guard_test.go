package edges

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

// TestWorkgraphIssueEdgesGoldenMatchesLivePython is the rot guard for
// tests/fixtures/workgraph_issue_edges_python_golden.json (CHAOS-4766).
//
// The tests in golden_full_test.go assert that GO agrees with the frozen file.
// Nothing there asserts that PYTHON still does. That gap matters here because
// the port lands one sub-builder at a time while Python keeps writing the same
// tables: a change to _build_issue_issue_edges that Go does not follow would
// leave both planes producing edges, silently disagreeing, with no crash.
//
// The guard REPLAYS the golden's own frozen reads through the deployed producer
// rather than re-querying ClickHouse. That distinction is the whole design: the
// live tables move continuously (RMT inserts, syncs), so a re-querying guard
// would fail on data drift and say nothing about Python drift. Replaying
// isolates the only question worth asking — does the deployed producer still
// turn THESE rows into THOSE edges, that watermark and those mutations.
func TestWorkgraphIssueEdgesGoldenMatchesLivePython(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	proofDirectory := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if proofDirectory == "" {
		t.Fatal("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR is required")
	}

	repoRoot := repositoryRootPath(t)
	python := workgraphEdgesLivePython(t, repoRoot)
	generator := filepath.Join(
		repoRoot, "tests", "fixtures", "generate_workgraph_issue_edges_python_golden.py",
	)
	if info, err := os.Stat(generator); err != nil || !info.Mode().IsRegular() {
		t.Fatalf("golden generator is missing at %s: %v", generator, err)
	}
	goldenPath := filepath.Join(repoRoot, "tests", "fixtures", goldenFixture)

	command := exec.Command(python, generator, "--replay", goldenPath)
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(repoRoot, "src"))
	rendered, err := command.Output()
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
		Edges          json.RawMessage `json:"edges"`
		ProjectionRuns json.RawMessage `json:"projection_runs"`
		Mutations      json.RawMessage `json:"mutations"`
	}
	if err := json.Unmarshal(frozen, &golden); err != nil {
		t.Fatalf("decode frozen golden: %v", err)
	}
	var replayed struct {
		Edges          json.RawMessage `json:"edges"`
		ProjectionRuns json.RawMessage `json:"projection_runs"`
		Mutations      json.RawMessage `json:"mutations"`
	}
	if err := json.Unmarshal(rendered, &replayed); err != nil {
		t.Fatalf("decode replayed output: %v", err)
	}

	drifted := make([]string, 0, 3)
	for _, part := range []struct {
		name           string
		frozen, replay json.RawMessage
	}{
		{"edges", golden.Edges, replayed.Edges},
		{"projection_runs", golden.ProjectionRuns, replayed.ProjectionRuns},
		{"mutations", golden.Mutations, replayed.Mutations},
	} {
		if !bytes.Equal(canonicalJSON(t, part.frozen), canonicalJSON(t, part.replay)) {
			drifted = append(drifted, part.name)
		}
	}
	if len(drifted) == 0 {
		if writeErr := os.WriteFile(
			filepath.Join(proofDirectory, "workgraph-issue-edges-golden"), []byte("executed"), 0o644,
		); writeErr != nil {
			t.Fatalf("write live-python-oracle proof: %v", writeErr)
		}
		return
	}
	t.Errorf(
		"live Python no longer reproduces the frozen golden's %s from the frozen inputs.\n"+
			"This is PYTHON drift, not a Go bug, and not data drift: the replay feeds the golden's own\n"+
			"frozen reads back through the deployed producer, so ClickHouse cannot be the cause.\n"+
			"If the change was intended, regenerate against live data with\n"+
			"    docker cp tests/fixtures/generate_workgraph_issue_edges_python_golden.py dev-health-api-1:/tmp/\n"+
			"    docker exec -i dev-health-api-1 python /tmp/generate_workgraph_issue_edges_python_golden.py --stdout\n"+
			"and re-run the Go golden tests, which must still pass against the new file.",
		strings.Join(drifted, ", "),
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

func workgraphEdgesLivePython(t *testing.T, repoRoot string) string {
	t.Helper()
	resolved := os.Getenv("PYTHON")
	if resolved == "" {
		path, err := exec.LookPath("python3")
		if err != nil {
			t.Fatalf("python3 is required for the golden rot guard: %v", err)
		}
		resolved = path
	}
	command := exec.Command(
		resolved, "-c", "import dev_health_ops, sys; sys.stdout.write(dev_health_ops.__file__)",
	)
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(repoRoot, "src"))
	located, err := command.Output()
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
