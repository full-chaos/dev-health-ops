package remaining

import (
	"reflect"
	"testing"
)

func TestMembershipRunsToPruneKeepsLatestN(t *testing.T) {
	// completed_at DESC order, as the SQL supplies it.
	runIDs := []string{"run-4", "run-3", "run-2", "run-1"}
	dropMarkers, dropRows := membershipRunsToPrune(runIDs, 2)

	wantMarkers := []string{"run-2", "run-1"}
	if !reflect.DeepEqual(dropMarkers, wantMarkers) {
		t.Errorf("dropMarkers = %v, want %v", dropMarkers, wantMarkers)
	}
	if !reflect.DeepEqual(dropRows, wantMarkers) {
		t.Errorf("dropRows = %v, want %v (no legacy id present)", dropRows, wantMarkers)
	}
}

func TestMembershipRunsToPruneNothingBelowThreshold(t *testing.T) {
	dropMarkers, dropRows := membershipRunsToPrune([]string{"run-2", "run-1"}, 2)
	if dropMarkers != nil || dropRows != nil {
		t.Errorf("expected nothing pruned at exactly `keep`, got markers=%v rows=%v", dropMarkers, dropRows)
	}
}

func TestMembershipRunsToPruneDedupsUnmergedVersions(t *testing.T) {
	// ReplacingMergeTree may surface more than one physical row for the same
	// run_id (read without FINAL) -- the newest completed_at sorts first, so
	// the duplicate is a REPEAT of a run_id already seen, not a new entry.
	runIDs := []string{"run-3", "run-3", "run-2", "run-2", "run-1"}
	dropMarkers, _ := membershipRunsToPrune(runIDs, 1)
	want := []string{"run-2", "run-1"}
	if !reflect.DeepEqual(dropMarkers, want) {
		t.Errorf("dropMarkers = %v, want %v (deduped, order preserved)", dropMarkers, want)
	}
}

func TestMembershipRunsToPruneTranslatesLegacyMarker(t *testing.T) {
	// The legacy marker's row run_id is "" (047's column default), never the
	// literal "__legacy__" the seed migration stamped onto the MARKER row.
	runIDs := []string{"run-2", "run-1", legacyMembershipRunID}
	dropMarkers, dropRows := membershipRunsToPrune(runIDs, 2)

	wantMarkers := []string{legacyMembershipRunID}
	if !reflect.DeepEqual(dropMarkers, wantMarkers) {
		t.Errorf("dropMarkers = %v, want %v", dropMarkers, wantMarkers)
	}
	wantRows := []string{""}
	if !reflect.DeepEqual(dropRows, wantRows) {
		t.Errorf("dropRows = %v, want %v (legacy translated to empty string)", dropRows, wantRows)
	}
}

func TestMembershipRunsToPruneEmptyInput(t *testing.T) {
	dropMarkers, dropRows := membershipRunsToPrune(nil, 2)
	if dropMarkers != nil || dropRows != nil {
		t.Errorf("expected nothing pruned for an empty run list, got markers=%v rows=%v", dropMarkers, dropRows)
	}
}
