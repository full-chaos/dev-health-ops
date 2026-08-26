package numerical

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

const wellbeingGoldenProofFile = "daily-wellbeing-golden"

// TestTeamWellbeingGoldenMatchesLivePython is the rot guard for
// tests/fixtures/daily_wellbeing_python_golden.json (CHAOS-4276), mirroring
// TestRemainingMetricsGoldenMatchesLivePython's shape (golden_rot_guard_test.go)
// for one family.
//
// tests/fixtures/daily_wellbeing_python_golden.json was generated from REAL
// production Python (compute_team_wellbeing_metrics_daily) and frozen.
// TestTeamWellbeingGoldenParity only proves Go matches that FILE -- nothing
// proves PYTHON still matches it. Without this guard, the moment
// compute_team_wellbeing_metrics_daily's behaviour changes, the frozen golden
// keeps encoding the OLD behaviour, Go keeps matching the frozen file, and
// the parity test stays green while the two implementations have actually
// diverged.
func TestTeamWellbeingGoldenMatchesLivePython(t *testing.T) {
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
		repoRoot, "tests", "fixtures", "generate_daily_wellbeing_python_golden.py",
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

	frozen, err := os.ReadFile(wellbeingGoldenPath())
	if err != nil {
		t.Fatal(err)
	}

	if bytes.Equal(bytes.TrimSpace(frozen), bytes.TrimSpace(rendered)) {
		if err := os.WriteFile(
			filepath.Join(proofDirectory, wellbeingGoldenProofFile), []byte("executed\n"), 0o600,
		); err != nil {
			t.Fatalf("write live Python oracle proof: %v", err)
		}
		return
	}

	t.Errorf(
		"live Python no longer reproduces the frozen team_wellbeing golden.\n"+
			"This is Python drift, not a Go bug: tests/fixtures/daily_wellbeing_python_golden.json "+
			"was generated from production Python and frozen, and TestTeamWellbeingGoldenParity only "+
			"proves Go matches the FILE. Regenerate with\n"+
			"    python tests/fixtures/generate_daily_wellbeing_python_golden.py\n"+
			"and review the diff as a real behaviour change -- if Go should follow, change Go too; "+
			"if it should not, the Python change is the bug.\n"+
			"first differing line: %s",
		firstDifferingLine(frozen, rendered),
	)
}
