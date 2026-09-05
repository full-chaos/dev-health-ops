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

// finalizeFamiliesWithPythonComputeDeleted names every pythonRecognisedFinalizeFamilies
// entry whose Python compute has been fully DELETED (CHAOS-3092 no-straddle),
// not merely gated -- there is no bridge call left to prevent a double-write
// against, so requiring a gate line for it would demand dead code. ic_finalize
// is the first (CHAOS-4290 PR3: compute_ic.py deleted, parity proved by
// icfinalize's TestICFinalizeMatchesTheFrozenPythonGolden instead of a live
// Python fallback). A family newly ADDED to pythonRecognisedFinalizeFamilies
// still gets the full "must have a gate line" check below unless it is also
// listed here, so this stays an explicit, reasoned exemption rather than a
// silent hole in the guard.
var finalizeFamiliesWithPythonComputeDeleted = map[string]struct{}{
	"ic_finalize": {},
}

func TestEveryRecognisedFinalizeFamilyHasAPythonGate(t *testing.T) {
	source := pythonFinalizeSource(t)
	if len(pythonRecognisedFinalizeFamilies) == 0 {
		t.Fatal("pythonRecognisedFinalizeFamilies is empty -- the guard would admit " +
			"nothing and this test would assert nothing")
	}
	for _, family := range pythonRecognisedFinalizeFamilies {
		if _, deleted := finalizeFamiliesWithPythonComputeDeleted[family]; deleted {
			continue
		}
		if !strings.Contains(source, gateLineFor(family)) {
			t.Errorf("Go recognises finalize family %q, but job_daily.py has no gate line %q. "+
				"Registering it would send a skip entry Python ignores, so the native family "+
				"and the bridge would BOTH write.", family, gateLineFor(family))
		}
	}
}

// TestFinalizeFamiliesWithPythonComputeDeletedReallyHaveNoGate is the mirror
// of the loop above's skip: a family stays exempt only while its Python gate
// line genuinely does not exist. If Python's compute were ever reintroduced
// for an exempted family without also removing it from
// finalizeFamiliesWithPythonComputeDeleted, the two-writer risk the main
// test exists to catch would be silently unchecked for exactly that family --
// this fails loudly instead, the same "stale exemption" shape this fleet
// checks for elsewhere (e.g. the shard-manifest and orphan-definition guards).
func TestFinalizeFamiliesWithPythonComputeDeletedReallyHaveNoGate(t *testing.T) {
	source := pythonFinalizeSource(t)
	for family := range finalizeFamiliesWithPythonComputeDeleted {
		if strings.Contains(source, gateLineFor(family)) {
			t.Errorf("finalizeFamiliesWithPythonComputeDeleted lists %q as Python-compute-deleted, "+
				"but job_daily.py DOES contain its gate line %q -- the exemption is stale "+
				"(Python's compute for this family exists again, or never left), remove it "+
				"from the exemption so the main test actually checks this family.",
				family, gateLineFor(family))
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
