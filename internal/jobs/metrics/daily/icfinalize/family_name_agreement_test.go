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

// pythonGateLine is the exact line run_daily_metrics_finalize gates on. Pinned
// as a whole line rather than a substring so that a change to the CONDITION
// (say, inverting it) is caught too, not merely a change to the name.
const pythonGateLine = `if "` + FamilyName + `" not in skip_families:`

func TestPythonFinalizeGateUsesTheSameLiteral(t *testing.T) {
	path := filepath.Join(repoRoot(t), "src", "dev_health_ops", "metrics", "job_daily.py")
	source, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read job_daily.py: %v", err)
	}
	if !strings.Contains(string(source), pythonGateLine) {
		t.Fatalf("job_daily.py does not contain the gate line %q.\n"+
			"Go registers the family under %q; if Python gates on a different "+
			"literal it will RECOMPUTE and its rows supersede the native ones "+
			"silently, because user_metrics_daily is append-only and the later "+
			"writer wins.", pythonGateLine, FamilyName)
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

	path := filepath.Join(repoRoot(t), "src", "dev_health_ops", "metrics", "job_daily.py")
	source, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	wrongGate := `if "` + wrong + `" not in skip_families:`
	if strings.Contains(string(source), wrongGate) {
		t.Fatalf("job_daily.py unexpectedly contains %q", wrongGate)
	}
}
