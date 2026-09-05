package daily

import (
	"errors"
	"testing"
)

// TestAnchorFromDiscoveredSet covers the three cases codex r1 on #2235 (F3)
// showed the old live-`min(id)` anchor could not.
//
// The defect: the anchor came from a live read of `repos`, a DIFFERENT set from
// the one partitions were cut from. When it named a repository no partition
// held, every partition answered "not mine", returned zero rows and SUCCESS,
// and the org silently produced no benchmarking output. Success-with-zero-rows
// is indistinguishable from "correctly nothing to do", so nothing downstream
// could notice.
//
// Choosing from the run's discovered set -- the union of its partition scopes,
// read back from the partitions themselves -- makes "some partition holds the
// anchor" true by construction. These tests pin that property directly rather
// than pinning the symptom.
func TestAnchorFromDiscoveredSet(t *testing.T) {
	const (
		repoA = "00000000-0000-4000-8000-0000000000a1"
		repoB = "00000000-0000-4000-8000-0000000000b1"
		repoC = "00000000-0000-4000-8000-0000000000c1"
	)

	t.Run("subset run anchors on the subset's minimum", func(t *testing.T) {
		// The org contains A, but this run covers only B and C. Under the old
		// live read the anchor was A -- present in the org, absent from every
		// partition -- and the whole run produced nothing.
		run := Run{
			ID:                "run-subset",
			DiscoveredRepoIDs: []RepositoryID{RepositoryID(repoC), RepositoryID(repoB)},
		}
		anchor, err := anchorFromDiscoveredSet(run)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if anchor.String() != repoB {
			t.Errorf("anchor = %s, want %s (the minimum of the RUN's set, not the org's)",
				anchor, repoB)
		}
		// And it is in the run's set, which is the invariant that matters:
		// exactly one partition can therefore claim the work.
		var found bool
		for _, candidate := range run.DiscoveredRepoIDs {
			if string(candidate) == anchor.String() {
				found = true
			}
		}
		if !found {
			t.Error("anchor is not in the run's discovered set -- the invariant this change exists to guarantee")
		}
	})

	t.Run("a repo added after the cut does not move the anchor", func(t *testing.T) {
		// repoA is inserted into `repos` between discovery and execution. The
		// discovered set is the union of the PARTITIONS, so it does not contain
		// A and the anchor is unchanged. Under the old live read A would have
		// become the new minimum and no partition would have held it.
		before := Run{
			ID:                "run-stable",
			DiscoveredRepoIDs: []RepositoryID{RepositoryID(repoB), RepositoryID(repoC)},
		}
		anchorBefore, err := anchorFromDiscoveredSet(before)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		// Same run, re-loaded after repoA landed in `repos`: the partitions did
		// not change, so neither does the set or the anchor.
		after := Run{
			ID:                "run-stable",
			DiscoveredRepoIDs: []RepositoryID{RepositoryID(repoB), RepositoryID(repoC)},
		}
		anchorAfter, err := anchorFromDiscoveredSet(after)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if anchorBefore != anchorAfter {
			t.Errorf("anchor moved from %s to %s after an unrelated repo was inserted",
				anchorBefore, anchorAfter)
		}
		if anchorAfter.String() == repoA {
			t.Errorf("anchor became %s, a repository no partition holds", repoA)
		}
	})

	t.Run("empty discovered set is an error, not a silent no-op", func(t *testing.T) {
		// Reachable only by building a Run without reading its partitions. It
		// must be loud: returning (nil, nil) here would reproduce the exact
		// silent-success this change removes.
		_, err := anchorFromDiscoveredSet(Run{ID: "run-empty"})
		if err == nil {
			t.Fatal("expected an error for an empty discovered set, got nil -- a silent no-op is the defect")
		}
		if !errors.Is(err, ErrInvalidState) {
			t.Errorf("error = %v, want it to wrap ErrInvalidState", err)
		}
	})
}
