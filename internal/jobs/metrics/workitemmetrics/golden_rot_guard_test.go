package workitemmetrics

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

const (
	livePythonOraclesEnv     = "DEV_HEALTH_LIVE_PYTHON_ORACLES"
	livePythonOracleProofDir = "DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR"

	// This guard used to also write a SECOND marker here
	// ("work-item-estimate-golden"), checked separately so work_item_estimate
	// stayed independently accountable to ci/check_go.sh even though both
	// families shared one generator and one whole-document comparison.
	// CHAOS-5323/CHAOS-3092 deleted compute_estimate_coverage_metrics_daily
	// entirely (fully native, no remaining Python caller) and extracted its
	// frozen cases into their own file with no generator -- there is no more
	// live Python for that family to compare against, so its marker is
	// retired along with the family. This one marker now covers exactly
	// what this guard actually still checks.
	workItemGoldenProofFile = "work-item-golden"
)

// TestWorkItemGoldenMatchesLivePython is the rot guard for
// tests/fixtures/daily_work_item_python_golden.json (CHAOS-4283), in the shape
// TestTeamWellbeingGoldenMatchesLivePython established.
//
// The golden was generated from REAL production Python and FROZEN.
// TestComputeDailyTripletMatchesPythonGolden only proves Go matches that
// FILE -- nothing proves PYTHON still matches it. Without this guard, the
// moment compute_work_item_metrics_daily or compute_work_item_team_attributions
// changes behaviour, the frozen golden keeps encoding the OLD behaviour, Go
// keeps matching the frozen file, and the parity test stays green while the
// two implementations have actually diverged.
//
// This used to also cover work_item_estimate (one shared generator, one
// whole-document comparison, two proof markers for independent CI
// accountability) until CHAOS-5323/CHAOS-3092 deleted
// compute_estimate_coverage_metrics_daily entirely -- see
// TestComputeEstimateCoverageMatchesPythonGolden in golden_test.go, now a
// pure frozen-bits check with no live Python counterpart at all.
func TestWorkItemGoldenMatchesLivePython(t *testing.T) {
	if os.Getenv(livePythonOraclesEnv) != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	proofDirectory := os.Getenv(livePythonOracleProofDir)
	if proofDirectory == "" {
		t.Fatalf("%s is required", livePythonOracleProofDir)
	}

	repoRoot := repositoryRoot(t)
	python := livePython(t, repoRoot)
	generator := filepath.Join(
		repoRoot, "tests", "fixtures", "generate_daily_work_item_python_golden.py",
	)
	if info, err := os.Stat(generator); err != nil || !info.Mode().IsRegular() {
		t.Fatalf("golden generator is missing at %s: %v", generator, err)
	}

	rendered, err := exec.Command(python, generator, "--stdout").Output()
	if err != nil {
		var stderr []byte
		if exitErr, ok := err.(*exec.ExitError); ok {
			stderr = exitErr.Stderr
		}
		t.Fatalf("run the golden generator against live Python: %v: %s", err, stderr)
	}

	frozen, err := os.ReadFile(goldenPath())
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(bytes.TrimSpace(frozen), bytes.TrimSpace(rendered)) {
		t.Fatalf(
			"live Python no longer reproduces the frozen work_item golden.\n"+
				"This is Python drift, not a Go bug: "+
				"tests/fixtures/daily_work_item_python_golden.json was generated from "+
				"production Python and frozen, and the parity tests only prove Go matches "+
				"the FILE. Regenerate with\n"+
				"    python tests/fixtures/generate_daily_work_item_python_golden.py\n"+
				"and review the diff as a real behaviour change -- if Go should follow, "+
				"change internal/jobs/metrics/workitemmetrics too; if it should not, the "+
				"Python change is the bug.\n"+
				"first differing line: %s",
			firstDifferingLine(frozen, rendered),
		)
	}

	if err := os.WriteFile(
		filepath.Join(proofDirectory, workItemGoldenProofFile), []byte("executed\n"), 0o600,
	); err != nil {
		t.Fatalf("write live Python oracle proof %s: %v", workItemGoldenProofFile, err)
	}
}

// repositoryRoot walks up from this test file to the module root.
func repositoryRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate this test file")
	}
	root := filepath.Dir(file)
	for depth := 0; depth < 10; depth++ {
		if _, err := os.Stat(filepath.Join(root, "go.mod")); err == nil {
			return root
		}
		root = filepath.Dir(root)
	}
	t.Fatal("cannot locate the repository root (no go.mod found walking up)")
	return ""
}

// livePython resolves the interpreter the rot guard shells out to.
//
// It prefers the worktree's own .venv, then $PYTHON, then a bare python3 --
// because the generator imports dev_health_ops and its third-party
// dependencies, which a bare system python3 does not have. ci/check_go.sh sets
// PYTHON explicitly for exactly this reason (a PATH prepend alone does NOT
// reach here: the script reads PYTHON itself).
func livePython(t *testing.T, repoRoot string) string {
	t.Helper()
	venv := filepath.Join(repoRoot, ".venv", "bin", "python3")
	if info, err := os.Stat(venv); err == nil && !info.IsDir() {
		return venv
	}
	if configured := strings.TrimSpace(os.Getenv("PYTHON")); configured != "" {
		return configured
	}
	return "python3"
}

// firstDifferingLine reports the first line at which two renderings diverge, so
// a drift failure names the field rather than dumping two large documents.
func firstDifferingLine(left, right []byte) string {
	leftLines := strings.Split(string(left), "\n")
	rightLines := strings.Split(string(right), "\n")
	for index := 0; index < len(leftLines) && index < len(rightLines); index++ {
		if leftLines[index] != rightLines[index] {
			return "line " + itoa(index+1) + ": frozen=" + leftLines[index] +
				" live=" + rightLines[index]
		}
	}
	if len(leftLines) != len(rightLines) {
		return "frozen has " + itoa(len(leftLines)) + " lines, live has " + itoa(len(rightLines))
	}
	return "(no differing line found; the difference is in trailing whitespace)"
}

func itoa(value int) string {
	if value == 0 {
		return "0"
	}
	digits := ""
	for value > 0 {
		digits = string(rune('0'+value%10)) + digits
		value /= 10
	}
	return digits
}
