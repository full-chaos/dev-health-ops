package numerical

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/oraclecompare"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/rotguard"
)

// TestRemainingMetricsGoldenMatchesLivePython is the rot guard for
// tests/fixtures/remaining_metrics_python_golden.json (CHAOS-3092 P0b).
//
// That file was generated on 2026-07-23 from REAL production Python and then
// frozen. TestPythonNumericalGoldenParity asserts Go matches it. Nothing
// asserted that PYTHON still matches it -- so the moment
// compute_dora_metrics_daily, compute_percentiles or _compute_confidence
// changes its numbers, the frozen `expected` keeps encoding the OLD Python
// behaviour, Go keeps matching the frozen file, and the parity test stays
// green while the two implementations have actually diverged. A golden file
// with no regeneration guard measures history, not parity, and it degrades
// silently: nothing fails, the credit it supplies to R1/R2/R3 just stops
// being true.
//
// CHAOS-4291 dropped this golden's "complexity" section (and Go's own
// AggregateComplexity/ComplexityFile/ComplexitySummary, TestPythonNumerical
// GoldenParity's matching case) along with job_complexity_db.py's
// _build_snapshots -- the native ComplexityExecutor has no Python fallback
// left to guard against drifting from.
//
// The generator already had a --check mode. It had never been wired to
// anything. This runs it against the live interpreter and reports WHERE the
// drift is rather than a bare exit code.
//
// The generator's stdout was executed once on the last build that carried the Python
// sources and is frozen in testdata/golden/remaining_metrics_rot_guard.json; a frozen
// run compares that recorded stdout with the checked-in fixture (no Python runs), so
// the guard still fails when the fixture is edited without recording the producer again.
func TestRemainingMetricsGoldenMatchesLivePython(t *testing.T) {
	root := repositoryRoot(t)
	spec := rotguard.Spec("testdata/golden/remaining_metrics_rot_guard.json", "",
		"./internal/jobs/metrics/numerical/", "^TestRemainingMetricsGoldenMatchesLivePython$")
	rendered := []byte(rotguard.Run(t, spec, root, rotguard.Generator{
		Name: "remaining metrics golden generator",
		Path: "tests/fixtures/generate_remaining_metrics_python_golden.py",
		Args: []string{"--stdout"},
	})[0])

	frozen, err := os.ReadFile(filepath.Join(root, "tests", "fixtures", "remaining_metrics_python_golden.json"))
	if err != nil {
		t.Fatal(err)
	}

	// Decoded with UseNumber on both sides: a numeric golden compared through
	// float64 would let a genuine precision drift round away to equal, which
	// is exactly the class of change this guard exists to catch.
	live := decodeExact(t, rendered, "recorded generator output")
	checkedIn := decodeExact(t, frozen, "checked-in golden")

	if oraclecompare.TypedValuesEqual(checkedIn, live) {
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
		"the recorded Python render no longer equals the checked-in golden.\n"+
			"Either %s was edited by hand or the producer moved, and TestPythonNumericalGoldenParity only proves Go "+
			"matches the FILE. Regenerate with\n"+
			"    python tests/fixtures/generate_remaining_metrics_python_golden.py\n"+
			"review the diff as a real behaviour change -- if Go should follow, change Go too; "+
			"if it should not, the Python change is the bug -- and record the producer again (the golden's recipe).\n"+
			"first differing line: %s",
		"tests/fixtures/remaining_metrics_python_golden.json",
		firstDifferingLine(frozen, rendered),
	)
}

// firstDifferingLine points at the first line that changed, so a failure is
// actionable without a separate diff step.
func firstDifferingLine(frozen, rendered []byte) string {
	frozenLines := strings.Split(string(frozen), "\n")
	renderedLines := strings.Split(string(rendered), "\n")
	for index := 0; index < len(frozenLines) && index < len(renderedLines); index++ {
		if frozenLines[index] != renderedLines[index] {
			return "line " + strconv.Itoa(index+1) +
				"\n  frozen: " + strings.TrimSpace(frozenLines[index]) +
				"\n  recorded: " + strings.TrimSpace(renderedLines[index])
		}
	}
	if len(frozenLines) != len(renderedLines) {
		return "the documents have different lengths (" +
			strconv.Itoa(len(frozenLines)) + " frozen vs " + strconv.Itoa(len(renderedLines)) + " recorded)"
	}
	return "(no textual difference -- the divergence is structural)"
}
