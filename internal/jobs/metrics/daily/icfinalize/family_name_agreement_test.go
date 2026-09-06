package icfinalize

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// FamilyName must equal the literal families.json declares AND the literal the
// Python finalize gate checks. If those three drift apart the mechanism fails
// SILENTLY into two writers: Go computes and writes, Python does not recognise
// its key, recomputes, and its rows supersede via computed_at DESC LIMIT 1 BY.
// Nothing errors and nothing goes red.
//
// This cannot be left to the two-writer integration test, which registers the
// family under the same literal it asserts on and therefore cannot detect a
// mismatch. It is asserted here, against the files themselves.

// FIVE levels, not four. From internal/jobs/metrics/daily/icfinalize the
// ascent is daily, metrics, jobs, internal, root -- an earlier revision used
// four and landed on internal/, so every read of job_daily.py failed and BOTH
// Python-side assertions errored on the clean tree. A path that is wrong fails
// loudly here, but only because the test reads a file it requires; a path used
// for an OPTIONAL read would have silently skipped instead.
func repoRoot(t *testing.T) string {
	t.Helper()
	return filepath.Join("..", "..", "..", "..", "..")
}

func TestFamilyNameMatchesFamiliesJSON(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "families.json"))
	if err != nil {
		t.Fatalf("read families.json: %v", err)
	}
	var registry struct {
		Families []struct {
			Name string `json:"name"`
		} `json:"families"`
	}
	if err := json.Unmarshal(data, &registry); err != nil {
		t.Fatalf("decode families.json: %v", err)
	}
	for _, family := range registry.Families {
		if family.Name == FamilyName {
			return
		}
	}
	t.Fatalf("families.json declares no family named %q -- the registry and the "+
		"Go constant have drifted", FamilyName)
}

// pythonGateLine is the shape run_daily_metrics_finalize USED to gate on,
// before compute_ic.py's deletion (CHAOS-4290 PR3, CHAOS-3092 no-straddle).
// Kept as a named constant so both tests below (and their comments) stay
// anchored to the exact literal, not a restated copy of it.
const pythonGateLine = `if "` + FamilyName + `" not in skip_families:`

// TestPythonFinalizeGateNoLongerExists is TestPythonFinalizeGateUsesTheSameLiteral's
// replacement (PR3): compute_ic_metrics_daily/compute_ic_landscape_rolling
// are deleted from job_daily.py entirely, not merely gated, so there is no
// bridge call left for a skip_families entry to prevent -- a gate line here
// would be dead code asserting protection against a write path that no
// longer exists. This is the INVERSE of the old assertion, checked here
// rather than deleted outright so a Python compute path silently
// reintroduced for this family (without updating this test) fails loudly:
// TestICFinalizeMatchesTheFrozenPythonGolden is what actually proves parity
// now, and a live gate line would mean the parity proof and the runtime
// behaviour have drifted apart again.
func TestPythonFinalizeGateNoLongerExists(t *testing.T) {
	path := filepath.Join(repoRoot(t), "src", "dev_health_ops", "metrics", "job_daily.py")
	source, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read job_daily.py: %v", err)
	}
	if strings.Contains(string(source), pythonGateLine) {
		t.Fatalf("job_daily.py still contains the gate line %q, but compute_ic.py "+
			"(and the compute it would have gated) was deleted in this PR -- "+
			"either the deletion is incomplete, or this family's Python compute "+
			"was reintroduced without updating this test.", pythonGateLine)
	}
}

// NEGATIVE CONTROL. The two assertions above must FAIL for a wrong constant --
// otherwise they would pass for any string that happens to appear somewhere in
// those files, and the guard would be decorative.
func TestTheAgreementAssertionsActuallyDiscriminate(t *testing.T) {
	const wrong = "ic_finalize_TYPO"

	data, err := os.ReadFile(filepath.Join("..", "families.json"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), `"name": "`+wrong+`"`) {
		t.Fatalf("families.json unexpectedly declares %q, so the positive test "+
			"above could pass for the wrong reason", wrong)
	}

	// The job_daily.py half of this control (a typo'd gate line must not be
	// present either) was retired with TestPythonFinalizeGateUsesTheSameLiteral
	// (PR3): job_daily.py never contains ANY ic_finalize gate line now, typo'd
	// or not, so checking for the typo'd one specifically would no longer
	// discriminate anything -- TestPythonFinalizeGateNoLongerExists (above)
	// is the assertion that actually exercises this file's contents now.
}
