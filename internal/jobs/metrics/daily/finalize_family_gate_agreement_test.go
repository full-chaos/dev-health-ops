package daily

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The Go and Python processes cannot share a value, so
// pythonRecognisedFinalizeFamilies is a hand-written copy of what
// run_daily_metrics_finalize gates on. A copy that nothing checks is a copy
// that drifts, and the drift is silent in the worst possible way: a name Python
// does not recognise means the native family runs AND the bridge runs, both
// succeed, and the later writer wins on an append-only table.
//
// So this reads the Python SOURCE and asserts the gate line is really there for
// every name Go believes in. It is deliberately a source read rather than a
// list restated in a fixture -- a fixture would be a THIRD copy, and the whole
// defect class here is copies that stop agreeing.
//
// CHAOS-4290, #2241 r1 Finding 2.

func pythonFinalizeSource(t *testing.T) string {
	t.Helper()
	// internal/jobs/metrics/daily -> repo root is four levels up.
	path := filepath.Join("..", "..", "..", "..",
		"src", "dev_health_ops", "metrics", "job_daily.py")
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading the Python finalize source at %s: %v", path, err)
	}
	if len(body) == 0 {
		t.Fatalf("%s is empty -- an empty file would make every gate assertion below vacuous", path)
	}
	return string(body)
}

// gateLineFor is the exact shape the Python gate takes. Built here rather than
// matched loosely, because a substring search for the family name alone would
// pass on a comment, a docstring, or a log message -- none of which gate
// anything.
func gateLineFor(family string) string {
	return `if "` + family + `" not in skip_families:`
}

func TestEveryRecognisedFinalizeFamilyHasAPythonGate(t *testing.T) {
	source := pythonFinalizeSource(t)
	if len(pythonGatedFinalizeFamilies) == 0 {
		t.Fatal("pythonGatedFinalizeFamilies is empty -- the guard would admit " +
			"nothing and this test would assert nothing")
	}
	for _, family := range pythonGatedFinalizeFamilies {
		if !strings.Contains(source, gateLineFor(family)) {
			t.Errorf("Go believes finalize family %q still has a live Python gate, but "+
				"job_daily.py has no gate line %q. Either the Python compute for it was "+
				"deleted (remove it from pythonGatedFinalizeFamilies too, CHAOS-5141-style) "+
				"or the gate line was lost by accident (registering it natively would then "+
				"send a skip entry Python ignores, so the native family and the bridge would "+
				"BOTH write).", family, gateLineFor(family))
		}
	}
}

// CHAOS-5141 (team_cognitive_load) and CHAOS-4290 PR3 (ic_finalize) both
// deleted a registerable-native family's Python compute entirely rather than
// merely gating it -- each must NOT have a live gate line for the test above
// to ever find, or a future accidental re-add of the Python compute would
// silently re-introduce the two-writer hazard this whole file exists to
// prevent. This loop covers BOTH (and any future family in the same shape)
// by construction: it is every pythonRecognisedFinalizeFamilies entry that
// is NOT in pythonGatedFinalizeFamilies, not a hand-maintained list of names.
func TestDeletedPythonComputeFamilyHasNoGateLine(t *testing.T) {
	source := pythonFinalizeSource(t)
	for _, family := range pythonRecognisedFinalizeFamilies {
		gated := false
		for _, g := range pythonGatedFinalizeFamilies {
			if g == family {
				gated = true
				break
			}
		}
		if gated {
			continue
		}
		if strings.Contains(source, gateLineFor(family)) {
			t.Errorf("family %q is NOT in pythonGatedFinalizeFamilies (its Python compute is "+
				"supposed to be deleted), but job_daily.py still has gate line %q -- either "+
				"add it back to pythonGatedFinalizeFamilies, or finish deleting the Python "+
				"compute behind that gate.", family, gateLineFor(family))
		}
	}
}

// The negative control. Without it the test above passes just as happily
// against a matcher that returns true for everything, which is the failure mode
// a source-scanning assertion actually has.
func TestTheFinalizeGateMatcherRejectsAFamilyPythonDoesNotGate(t *testing.T) {
	source := pythonFinalizeSource(t)
	const absent = "ic_finalise" // the plausible typo, one letter from the real name
	if strings.Contains(source, gateLineFor(absent)) {
		t.Fatalf("control failed: job_daily.py appears to gate on %q, which should not exist. "+
			"Either the typo was committed to Python, or this matcher matches anything.", absent)
	}
}

// The guard has to REFUSE, not merely warn: a dropped-but-registered family
// would be the same two-writer bug wearing a different hat.
func TestRegisteringAnUnrecognisedFinalizeFamilyIsRefused(t *testing.T) {
	handler, err := NewFinalizeHandler(finalizeStoreWithClaim(), &fakeCompatibility{})
	if err != nil {
		t.Fatal(err)
	}
	err = handler.SetNativeFinalizeFamilies(map[string]NativeFinalizeFamilyExecutor{
		"ic_finalise": &stubFinalizeFamily{},
	})
	if err == nil {
		t.Fatal("registering the typo'd name succeeded; the bridge would recompute the " +
			"family while the native executor also wrote it")
	}
	// Registration is all-or-nothing: a refused call must leave NOTHING behind,
	// or the caller believes a family is native when it is not.
	if len(handler.nativeFinalizeFamilyNames) != 0 {
		t.Fatalf("refused registration still recorded %v", handler.nativeFinalizeFamilyNames)
	}
}
