package numerical

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/oraclecompare"
)

const (
	livePythonOraclesEnv     = "DEV_HEALTH_LIVE_PYTHON_ORACLES"
	livePythonOracleProofDir = "DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR"
	goldenProofFile          = "numerical-golden"
)

// TestRemainingMetricsGoldenMatchesLivePython is the rot guard for
// tests/fixtures/remaining_metrics_python_golden.json (CHAOS-3092 P0b).
//
// That file was generated on 2026-07-23 from REAL production Python and then
// frozen. TestPythonNumericalGoldenParity asserts Go matches it. Nothing
// asserted that PYTHON still matches it -- so the moment
// compute_dora_metrics_daily, compute_percentiles, _build_snapshots or
// _compute_confidence changes its numbers, the frozen `expected` keeps
// encoding the OLD Python behaviour, Go keeps matching the frozen file, and
// the parity test stays green while the two implementations have actually
// diverged. A golden file with no regeneration guard measures history, not
// parity, and it degrades silently: nothing fails, the credit it supplies to
// R1/R2/R3 just stops being true.
//
// The generator already had a --check mode. It had never been wired to
// anything. This runs it against the live interpreter and reports WHERE the
// drift is rather than a bare exit code.
func TestRemainingMetricsGoldenMatchesLivePython(t *testing.T) {
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
		repoRoot, "tests", "fixtures", "generate_remaining_metrics_python_golden.py",
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

	goldenPath := filepath.Join(
		repoRoot, "tests", "fixtures", "remaining_metrics_python_golden.json",
	)
	frozen, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatal(err)
	}

	// Decoded with UseNumber on both sides: a numeric golden compared through
	// float64 would let a genuine precision drift round away to equal, which
	// is exactly the class of change this guard exists to catch.
	live := decodeExact(t, rendered, "live Python output")
	checkedIn := decodeExact(t, frozen, "checked-in golden")

	if oraclecompare.TypedValuesEqual(checkedIn, live) {
		writeProof(t, proofDirectory)
		return
	}

	// Attribute the drift to a family before dumping any text, so the failure
	// names which producer moved rather than leaving a reader to eyeball two
	// JSON documents.
	for _, message := range oraclecompare.DiffRows(
		"remaining_metrics_python_golden.json",
		checkedIn, live, nil, nil,
	) {
		t.Error(message)
	}
	t.Errorf(
		"live Python no longer reproduces the frozen golden.\n"+
			"This is Python drift, not a Go bug: %s was generated from production "+
			"Python and frozen, and TestPythonNumericalGoldenParity only proves Go "+
			"matches the FILE. Regenerate with\n"+
			"    python tests/fixtures/generate_remaining_metrics_python_golden.py\n"+
			"and review the diff as a real behaviour change -- if Go should follow, "+
			"change Go too; if it should not, the Python change is the bug.\n"+
			"first differing line: %s",
		"tests/fixtures/remaining_metrics_python_golden.json",
		firstDifferingLine(frozen, rendered),
	)
}

// decodeExact decodes JSON without collapsing numbers into float64.
func decodeExact(t *testing.T, raw []byte, label string) map[string]any {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value map[string]any
	if err := decoder.Decode(&value); err != nil {
		t.Fatalf("decode %s: %v", label, err)
	}
	return value
}

// firstDifferingLine points at the first line that changed, so a failure is
// actionable without a separate diff step.
func firstDifferingLine(frozen, rendered []byte) string {
	frozenLines := strings.Split(string(frozen), "\n")
	renderedLines := strings.Split(string(rendered), "\n")
	for index := 0; index < len(frozenLines) && index < len(renderedLines); index++ {
		if frozenLines[index] != renderedLines[index] {
			return "line " + itoa(index+1) +
				"\n  frozen: " + strings.TrimSpace(frozenLines[index]) +
				"\n  live:   " + strings.TrimSpace(renderedLines[index])
		}
	}
	if len(frozenLines) != len(renderedLines) {
		return "the documents have different lengths (" +
			itoa(len(frozenLines)) + " frozen vs " + itoa(len(renderedLines)) + " live)"
	}
	return "(no textual difference -- the divergence is structural)"
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

// repositoryRoot walks up from this package to the checkout root.
func repositoryRoot(t *testing.T) string {
	t.Helper()
	working, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for directory := working; ; {
		if _, err := os.Stat(filepath.Join(directory, "go.mod")); err == nil {
			return directory
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			t.Fatalf("no go.mod above %s", working)
		}
		directory = parent
	}
}

// livePython resolves the interpreter AND proves it resolves dev_health_ops
// inside THIS checkout.
//
// Without that second half the guard is worse than nothing: on a machine with
// several worktrees -- the normal case here -- an ambient interpreter can
// supply another checkout's dev_health_ops while this test compares the result
// against THIS checkout's frozen file, and report drift, or the absence of it,
// about the wrong producer entirely. internal/providersync makes the same
// check for the same reason; it is duplicated rather than shared because that
// one is entangled with its dataset-registry logic.
func livePython(t *testing.T, repoRoot string) string {
	t.Helper()
	resolved := os.Getenv("PYTHON")
	if resolved == "" {
		path, err := exec.LookPath("python3")
		if err != nil {
			t.Fatalf("python3 is required for the golden rot guard: %v", err)
		}
		resolved = path
	}
	located, err := exec.Command(
		resolved, "-c", "import dev_health_ops, sys; sys.stdout.write(dev_health_ops.__file__)",
	).Output()
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

// writeProof records that the comparison actually executed. ci/check_go.sh
// requires this marker, so a skipped or short-circuited guard cannot satisfy
// the lane by staying quiet.
func writeProof(t *testing.T, proofDirectory string) {
	t.Helper()
	if err := os.WriteFile(
		filepath.Join(proofDirectory, goldenProofFile), []byte("executed\n"), 0o600,
	); err != nil {
		t.Fatalf("write live Python oracle proof: %v", err)
	}
}
