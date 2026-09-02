package main

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestBuildScopeParityTableMatchesLivePython is the rot guard for
// tests/fixtures/build_scope_parity_table.json.
//
// # What was missing, and why it mattered
//
// TestBuildScopeMatchesTheBridgeAdmission proves the Go adapter matches the
// frozen table. NOTHING proved the table still matches PYTHON. The 1,492 cases
// were measured from the bridge once; a change to `_scope_arguments`, to
// `run_work_graph_build`'s window derivation, or a `fromisoformat` change on an
// interpreter bump would leave the fixture stale, the Go tests green, and the
// adapter silently wrong.
//
// So the corpus asserted "Go matches what Python did ONCE", not "Go matches
// what Python DOES" -- and a frozen fixture with no guard is indistinguishable
// from a live one right up until the reference moves.
//
// This is not hypothetical for this fixture in particular. The round-8 P1 (Go
// accepting malformed offset suffixes the bridge rejects) was visible ONLY
// because the corpus happened to contain those spellings. A corpus that has
// drifted from the reference cannot surface that class at all, so the guard
// protects the thing that found the bug rather than merely the bug.
//
// # Why this one REGENERATES where the issue/PR golden REPLAYS
//
// internal/jobs/workgraph/issueprlinks/golden_rot_guard_test.go cannot
// re-measure: its generator reads ClickHouse, and those tables move
// continuously, so a re-query guard would fail on DATA drift while saying
// nothing about PYTHON drift. It needs a --replay mode.
//
// This generator has no such problem. It imports the bridge and derives its
// window against a FROZEN `now`, so it is hermetic: the only thing that can
// change its output is the reference's own behaviour. A straight
// regenerate-and-byte-compare is therefore both sufficient and exact, and it
// re-measures every one of the 1,492 cases rather than a replayed subset.
//
// # The comparison is on BYTES, deliberately
//
// The generator sorts keys and fixes the indent, so its output is canonical by
// construction. Comparing bytes therefore also catches a change to the
// generator's own SHAPE -- a renamed field, a dropped column, a changed schema
// string -- which a structural comparison of the cases alone would silently
// accept. `measured_on` is part of that: an interpreter bump IS reference
// drift, and the guard should say so rather than hide it.
func TestBuildScopeParityTableMatchesLivePython(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	proofDirectory := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if proofDirectory == "" {
		t.Fatal("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR is required")
	}

	repoRoot := repositoryRoot(t)
	python := scopeParityLivePython(t, repoRoot)
	generator := filepath.Join(repoRoot, "tests", "fixtures", "generate_build_scope_parity_table.py")
	if info, err := os.Stat(generator); err != nil || !info.Mode().IsRegular() {
		t.Fatalf("scope parity generator is missing at %s: %v", generator, err)
	}
	tablePath := filepath.Join(repoRoot, "tests", "fixtures", "build_scope_parity_table.json")

	command := exec.Command(python, generator, "--stdout")
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(repoRoot, "src"))
	regenerated, err := command.Output()
	if err != nil {
		var stderr []byte
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			stderr = exitErr.Stderr
		}
		t.Fatalf("regenerate the scope parity table through live Python: %v: %s", err, stderr)
	}

	frozen, err := os.ReadFile(tablePath)
	if err != nil {
		t.Fatal(err)
	}

	if bytes.Equal(frozen, regenerated) {
		if writeErr := os.WriteFile(
			filepath.Join(proofDirectory, "build-scope-parity-table"), []byte("executed"), 0o644,
		); writeErr != nil {
			t.Fatalf("write live-python-oracle proof: %v", writeErr)
		}
		return
	}

	t.Errorf(
		"live Python no longer reproduces the frozen build-scope parity table.\n"+
			"This is REFERENCE drift, not a Go bug and not data drift: the generator uses a frozen\n"+
			"`now` and touches no database, so its output can only move if the bridge's own admission\n"+
			"or window derivation moved.\n"+
			"frozen %d bytes, regenerated %d bytes; first difference at byte %d.\n"+
			"If the change was intended, regenerate and re-run the differential, which must still pass\n"+
			"against the new table (and will name any case where Go now disagrees):\n"+
			"    docker cp tests/fixtures/generate_build_scope_parity_table.py dev-health-api-1:/tmp/\n"+
			"    docker exec -i dev-health-api-1 python /tmp/generate_build_scope_parity_table.py --stdout \\\n"+
			"        > tests/fixtures/build_scope_parity_table.json",
		len(frozen), len(regenerated), firstDifferingByte(frozen, regenerated),
	)
}

// firstDifferingByte reports where two documents part company, so a failure
// points at the change instead of at 16,000 lines of JSON.
func firstDifferingByte(left, right []byte) int {
	limit := min(len(left), len(right))
	for index := range limit {
		if left[index] != right[index] {
			return index
		}
	}
	return limit
}

// scopeParityLivePython resolves the interpreter and REFUSES one that imports
// the bridge from outside this checkout.
//
// Without that check the guard would compare another worktree's producer
// against this worktree's frozen table -- green when it should be red, or red
// when nothing here changed. With several lane worktrees of the same repo on
// one machine, that is a live hazard rather than a theoretical one.
func scopeParityLivePython(t *testing.T, repoRoot string) string {
	t.Helper()
	resolved := os.Getenv("PYTHON")
	if resolved == "" {
		path, err := exec.LookPath("python3")
		if err != nil {
			t.Fatalf("python3 is required for the scope parity rot guard: %v", err)
		}
		resolved = path
	}
	command := exec.Command(
		resolved, "-c", "import dev_health_ops, sys; sys.stdout.write(dev_health_ops.__file__)")
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(repoRoot, "src"))
	located, err := command.Output()
	if err != nil {
		t.Fatalf("resolve dev_health_ops with %s: %v", resolved, err)
	}
	module := string(located)
	if !strings.HasPrefix(module, repoRoot+string(os.PathSeparator)) {
		t.Fatalf(
			"%s resolves dev_health_ops to %s, which is OUTSIDE this checkout (%s) -- "+
				"the guard would be comparing another worktree's bridge against this "+
				"worktree's frozen table; set PYTHONPATH to this checkout's src",
			resolved, module, repoRoot,
		)
	}
	return resolved
}
