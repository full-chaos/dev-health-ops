package numerical

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

const capacityGoldenProofFile = "capacity-forecast-golden"

// TestCapacityForecastGoldenMatchesLivePython is the rot guard for
// tests/fixtures/capacity_forecast_golden.json.
//
// The capacity port arrived with the same gap the numerical golden had before
// TestRemainingMetricsGoldenMatchesLivePython closed it, and with the same
// generator --check mode wired to nothing. Two claims are needed and they are
// different:
//
//	Go      == recording   (TestCapacityForecastMatchesPythonGolden, no interpreter)
//	CPython == recording   (this test, live interpreter, lane-gated)
//
// Only both together mean "Go reproduces Python". With only the first, a change
// to the Python forecast semantics leaves the frozen file encoding the OLD
// behaviour, Go keeps matching the file, and the parity claim stays green while
// the two implementations have diverged. That failure is silent by
// construction: nothing goes red, the proof this PR rests on just quietly stops
// being true.
//
// The RNG family got this guard when its vectors were frozen. Capacity did not,
// which is the asymmetry this closes.
func TestCapacityForecastGoldenMatchesLivePython(t *testing.T) {
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
		repoRoot, "tests", "fixtures", "generate_capacity_forecast_golden.py",
	)
	if info, err := os.Stat(generator); err != nil || !info.Mode().IsRegular() {
		t.Fatalf("capacity golden generator is missing at %s: %v", generator, err)
	}
	golden := filepath.Join(
		repoRoot, "tests", "fixtures", "capacity_forecast_golden.json",
	)

	// --check re-derives every case from the LIVE interpreter and compares.
	// The generator owns that comparison so there is one definition of what
	// the cases are, rather than a second copy here free to drift from the
	// producer in its own way.
	command := exec.Command(python, generator, "--check", golden)
	command.Dir = repoRoot
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf(
			"the recorded capacity forecasts no longer match live Python.\n"+
				"This is producer drift, not necessarily a Go bug: the frozen file "+
				"was generated from production Python, and the parity test only "+
				"proves Go matches the FILE. Regenerate with\n"+
				"    python tests/fixtures/generate_capacity_forecast_golden.py\n"+
				"and read the diff as a real behaviour change -- if Go should "+
				"follow, Go changes too; if it should not, the Python change is the "+
				"bug.\n%s",
			output,
		)
	}
	if !strings.Contains(string(output), "CAPACITY_FORECAST_GOLDEN_CURRENT") {
		t.Fatalf(
			"the check produced no positive marker, so a silent no-op cannot be "+
				"distinguished from a pass:\n%s", output)
	}

	// Only on a PASS. t.Fatalf above already halts, but a later t.Errorf would
	// not, and a marker that can mean either outcome is not evidence.
	if t.Failed() {
		return
	}
	if err := os.WriteFile(
		filepath.Join(proofDirectory, capacityGoldenProofFile), []byte("executed\n"), 0o600,
	); err != nil {
		t.Fatalf("write live Python oracle proof: %v", err)
	}
}
