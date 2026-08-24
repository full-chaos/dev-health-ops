package syncdispatchruntime

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
)

// livePythonOracleProofFile names this package's proof marker for
// ci/check_go.sh's `live-python-oracles` verb -- see the check_live_python_oracles
// block added for internal/syncdispatchruntime alongside the existing
// providersync/scheduler-sync/synccoverage blocks. A checked-in oracle test
// the gate never actually runs is the exact "measurement that did not
// happen" AGENTS.md warns reads as coverage without being any -- this proof
// file is how the shell script confirms this test body, not just its
// package, executed.
const livePythonOracleProofFile = "sync-dispatch-finalize"

// Live-Python oracle for finalize_sync_run's zero-unit classification
// (CHAOS-4175), same shape as internal/providersync/capabilities_test.go and
// internal/scheduler/sync/planner_oracle_test.go's oracle gating (PR #1879's
// scheduler eligibility pin) -- each oracle-bearing package carries its own
// copy of this gate rather than sharing one, matching those two precedents.
const (
	livePythonOraclesEnv     = "DEV_HEALTH_LIVE_PYTHON_ORACLES"
	livePythonOracleProofDir = "DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR"
)

func requireLivePythonOracles(t *testing.T) {
	t.Helper()
	if os.Getenv(livePythonOraclesEnv) != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	if os.Getenv(livePythonOracleProofDir) == "" {
		t.Fatal("live Python oracle opt-in requires a proof directory from ci/check_go.sh")
	}
}

var (
	pythonProducerOnce   sync.Once
	pythonProducerOrigin string
	pythonProducerErr    error
)

// pythonExecutable resolves the interpreter AND proves it would import
// dev_health_ops from THIS worktree -- see capabilities_test.go's
// assertPythonProducerIsThisWorktree for the reproduced multi-worktree
// stale-interpreter failure this guards against (a green run comparing half
// against one checkout and half against another, on a machine with many
// worktrees of this repo -- which this machine has).
func pythonExecutable(t *testing.T) string {
	t.Helper()
	requireLivePythonOracles(t)
	resolved := ""
	if configured := os.Getenv("PYTHON"); configured != "" {
		resolved = configured
	} else if path, err := exec.LookPath("python3"); err == nil {
		resolved = path
	}
	if resolved == "" {
		t.Fatal("python3 is required for the finalize_sync_run zero-unit oracle")
	}
	assertPythonProducerIsThisWorktree(t, resolved)
	return resolved
}

func assertPythonProducerIsThisWorktree(t *testing.T, python string) {
	t.Helper()
	pythonProducerOnce.Do(func() {
		output, err := exec.Command(python, "-c",
			"import importlib.util;s=importlib.util.find_spec('dev_health_ops');"+
				"print(s.origin if s else '')").CombinedOutput()
		pythonProducerOrigin = strings.TrimSpace(string(output))
		pythonProducerErr = err
	})
	if pythonProducerErr != nil {
		t.Fatalf("cannot determine which dev_health_ops %s would import: %v: %s",
			python, pythonProducerErr, pythonProducerOrigin)
	}
	if pythonProducerOrigin == "" {
		t.Fatalf("%s cannot import dev_health_ops at all -- the live-Python oracle "+
			"would compare against nothing. Set PYTHONPATH to this worktree's src/, "+
			"or run through ci/check_go.sh, which does it for you", python)
	}
	_, currentFile, _, _ := runtime.Caller(0)
	root := filepath.Dir(filepath.Dir(filepath.Dir(currentFile)))
	if !strings.HasPrefix(pythonProducerOrigin, root+string(filepath.Separator)) {
		t.Fatalf("live-Python oracle producer is NOT in this worktree:\n"+
			"  interpreter    %s\n"+
			"  dev_health_ops %s\n"+
			"  this worktree  %s\n"+
			"Set PYTHONPATH=%s/src (ci/check_go.sh does it for you), or point PYTHON "+
			"at this worktree's .venv.",
			python, pythonProducerOrigin, root, root)
	}
}

type oracleAggregateCase struct {
	Total   int    `json:"total"`
	Success int    `json:"success"`
	Failed  int    `json:"failed"`
	Status  string `json:"status"`
}

type oracleReasonCase struct {
	PlannerResult map[string]any `json:"planner_result"`
	ReasonOut     string         `json:"reason_out"`
}

type finalizeZeroUnitOracle struct {
	Aggregate []oracleAggregateCase `json:"aggregate"`
	Reason    []oracleReasonCase    `json:"reason"`
}

// TestAggregateRunStatusAndZeroUnitReasonMatchLivePython is the CHAOS-4175
// live-Python oracle: it executes testdata/finalize_zero_unit_oracle.py,
// which calls the REAL, unmodified
// dev_health_ops.workers.sync_units._aggregate_run_status and
// ._zero_unit_reason, and diffs Go's aggregateRunStatus/zeroUnitReasonFrom
// against every case the script produced. This is what caught (before this
// test even existed -- by hand-running the oracle script first) that
// zeroUnitReasonFrom needs a whitespace-trimming blank check while two
// sibling checks in Finalize deliberately do NOT trim, matching three
// distinct predicates in the same Python function family.
func TestAggregateRunStatusAndZeroUnitReasonMatchLivePython(t *testing.T) {
	python := pythonExecutable(t)
	_, currentFile, _, _ := runtime.Caller(0)
	packageDir := filepath.Dir(currentFile)
	oracleScript := filepath.Join(packageDir, "testdata", "finalize_zero_unit_oracle.py")
	output, err := exec.Command(python, oracleScript).CombinedOutput()
	if err != nil {
		t.Fatalf("execute Python zero-unit oracle: %v: %s", err, output)
	}
	var want finalizeZeroUnitOracle
	if err := json.Unmarshal(output, &want); err != nil {
		t.Fatalf("decode Python zero-unit oracle: %v: %s", err, output)
	}
	if len(want.Aggregate) == 0 || len(want.Reason) == 0 {
		t.Fatalf("oracle produced no cases: %s", output)
	}

	for _, oracleCase := range want.Aggregate {
		got := aggregateRunStatus(oracleCase.Total, oracleCase.Success, oracleCase.Failed)
		if got != oracleCase.Status {
			t.Errorf("aggregateRunStatus(%d,%d,%d)=%q want=%q (Python _aggregate_run_status)",
				oracleCase.Total, oracleCase.Success, oracleCase.Failed, got, oracleCase.Status)
		}
	}
	for _, oracleCase := range want.Reason {
		got := zeroUnitReasonFrom(oracleCase.PlannerResult)
		if got != oracleCase.ReasonOut {
			t.Errorf("zeroUnitReasonFrom(%#v)=%q want=%q (Python _zero_unit_reason)",
				oracleCase.PlannerResult, got, oracleCase.ReasonOut)
		}
	}

	proof := filepath.Join(os.Getenv(livePythonOracleProofDir), livePythonOracleProofFile)
	if err := os.WriteFile(proof, []byte("executed\n"), 0o600); err != nil {
		t.Fatalf("write live Python zero-unit oracle proof: %v", err)
	}
}
