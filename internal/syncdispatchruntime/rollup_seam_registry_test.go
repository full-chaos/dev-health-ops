package syncdispatchruntime

import "testing"

// The cross-package terminal-status-write registry and completeness/
// coverage guard (formerly package-local here) moved to
// internal/syncrunrollup/cross_package_registry_test.go (CHAOS-4586, chris:
// "the guard must be the thing that would have caught line 1144" --
// syncreconciler's unreclaimable sweep, a different package). This file
// keeps only the path-vocabulary check specific to this package's own
// rollupPath* constants.

// TestRollupBumpPathVocabularyMatchesMetrics pins that every path label
// this package ever passes to RecordSyncRunRollupBumped is a member of
// providerfoundation's own closed vocabulary -- a mismatch here would
// silently fold a real path into the "other" bucket in production
// (CHAOS-4586).
func TestRollupBumpPathVocabularyMatchesMetrics(t *testing.T) {
	for path := range rollupBumpPathVocabulary {
		if path != rollupPathDenied && path != rollupPathUnroutable &&
			path != rollupPathInvalidClaim && path != rollupPathBudgetExhausted &&
			path != rollupPathReferenceDiscoveryFailed && path != rollupPathFeatureDisabled {
			t.Fatalf("rollupBumpPathVocabulary has a path %q not produced by any known rollupPath* constant", path)
		}
	}
	want := []string{
		rollupPathDenied, rollupPathUnroutable, rollupPathInvalidClaim,
		rollupPathBudgetExhausted, rollupPathReferenceDiscoveryFailed, rollupPathFeatureDisabled,
	}
	if len(rollupBumpPathVocabulary) != len(want) {
		t.Fatalf("rollupBumpPathVocabulary has %d entries, want %d -- keep it in exact sync with the rollupPath* constants",
			len(rollupBumpPathVocabulary), len(want))
	}
	for _, path := range want {
		if !rollupBumpPathVocabulary[path] {
			t.Fatalf("rollupBumpPathVocabulary is missing %q", path)
		}
	}
}
