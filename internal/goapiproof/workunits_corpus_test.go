package goapiproof

import (
	"encoding/json"
	"os"
	"testing"
)

// This file exercises workUnitsTeamScopeSubsetDefect's own
// DisplacementLimit/DisplacementValueField admission (teamreposubset.go)
// against a real captured GET /api/v1/work-units?scope_type=team
// response pair: both `data` lists saturate the route's own default
// LIMIT 200, and the candidate side carries work units the baseline's
// own organization-wide top-200 (by effort.value) does not -- the
// mismatch this fixture pins.

const (
	workUnitsTeamScopedBaselinePath  = "testdata/workunits_teamscoped_baseline_9ab7b0f2.json"
	workUnitsTeamScopedCandidatePath = "testdata/workunits_teamscoped_candidate_e75aeb17.json"
)

// workUnitsSubsetTicket reads the ticket workUnitsTeamScopeSubsetDefect
// actually carries, rather than a literal copy that could drift out of
// sync with workunits_corpus.go.
func workUnitsSubsetTicket(t *testing.T) string {
	t.Helper()
	if workUnitsTeamScopeSubsetDefect.TeamRepoSubsetShape == nil {
		t.Fatal("workUnitsTeamScopeSubsetDefect carries no TeamRepoSubsetShape")
	}
	return workUnitsTeamScopeSubsetDefect.Ticket
}

func readWorkUnitsFixture(t *testing.T, path string) []byte {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return raw
}

func workUnitsSnapshotFromFixture(t *testing.T, path string) Snapshot {
	t.Helper()
	snapshot, err := DecodeRESTSnapshot(readWorkUnitsFixture(t, path))
	if err != nil {
		t.Fatalf("decode REST fixture %s: %v", path, err)
	}
	return snapshot
}

// TestWorkUnitsTeamScopeSubsetDefect_RealCapturedCaseIsFullyAdmitted
// pins the real captured case this admission exists to cover: 14
// baseline-only work units (ordinary subset absences, already admitted
// before this change) and 14 candidate-only work units (each ranking at
// or below the baseline list's own minimum effort.value, the new
// admission) are ALL admitted, and nothing stays outside.
func TestWorkUnitsTeamScopeSubsetDefect_RealCapturedCaseIsFullyAdmitted(t *testing.T) {
	wantTicket := workUnitsSubsetTicket(t)
	baseline := workUnitsSnapshotFromFixture(t, workUnitsTeamScopedBaselinePath)
	candidate := workUnitsSnapshotFromFixture(t, workUnitsTeamScopedCandidatePath)

	result := Compare(baseline, candidate, workUnitsTeamScopedParity)

	if result.TerminalState != TerminalStateMismatch {
		t.Fatalf("terminal = %q, want mismatch -- a declared defect never converts one", result.TerminalState)
	}
	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("outside = %d, want 0: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
	sawSubset := false
	for _, ticket := range result.BaselineDefectsMatched {
		if ticket == wantTicket {
			sawSubset = true
		}
	}
	if !sawSubset {
		t.Fatalf("matched = %v, want %q present -- the admitted presence pairs must be admitted, not merely absent from this capture", result.BaselineDefectsMatched, wantTicket)
	}
}

// TestWorkUnitsTeamScopeSubsetDefect_RealCapturedCaseWithoutDisplacementLeaves28Outside
// is the RED half of the pair, pinned rather than only asserted in prose:
// the SAME two real bodies, compared under a copy of the declaration with
// DisplacementLimit/DisplacementValueField cleared, reproduce exactly the
// pre-fix count this ticket's own captured evidence recorded -- 28
// findings outside every declaration, all ShapePresence. This is what
// TestWorkUnitsTeamScopeSubsetDefect_RealCapturedCaseIsFullyAdmitted
// looked like before this admission existed, kept here so a future
// change to the admission's own preconditions has a live regression
// pin, not just a captured number in a doc comment.
func TestWorkUnitsTeamScopeSubsetDefect_RealCapturedCaseWithoutDisplacementLeaves28Outside(t *testing.T) {
	baseline := workUnitsSnapshotFromFixture(t, workUnitsTeamScopedBaselinePath)
	candidate := workUnitsSnapshotFromFixture(t, workUnitsTeamScopedCandidatePath)

	undisplacedShape := *workUnitsTeamScopeSubsetDefect.TeamRepoSubsetShape
	undisplacedShape.DisplacementLimit = 0
	undisplacedShape.DisplacementValueField = ""
	undisplacedDefect := workUnitsTeamScopeSubsetDefect
	undisplacedDefect.TeamRepoSubsetShape = &undisplacedShape
	undisplacedParity := Options{
		NumericLeavesDeclared: workUnitsTeamScopedParity.NumericLeavesDeclared,
		FloatTierB:            workUnitsTeamScopedParity.FloatTierB,
		FloatExactLeaves:      workUnitsTeamScopedParity.FloatExactLeaves,
		BaselineDefects:       []BaselineDefect{undisplacedDefect},
		OrderInsensitiveLists: workUnitsTeamScopedParity.OrderInsensitiveLists,
	}

	result := Compare(baseline, candidate, undisplacedParity)

	if result.DifferencesOutsideBaselineDefect != 28 {
		t.Fatalf("outside = %d, want 28 -- this pins the exact pre-fix captured count (14 baseline-only + 14 candidate-only), all refused by rule 3 before the displacement admission existed: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
	for _, f := range result.Findings {
		if f.Shape != ShapePresence {
			t.Fatalf("finding shape = %q, want every pre-fix finding to be presence: %+v", f.Shape, f)
		}
	}
}

// workUnitsFixtureBytesRoundTrip guards the two testdata files' own
// decodability outside DecodeRESTSnapshot's stricter guards (non-finite
// literals, UTF-8, trailing bytes) -- a plain encoding/json parse, so a
// future edit to either fixture that breaks basic JSON syntax fails here
// with a clearer message than DecodeRESTSnapshot's own wrapped error.
func TestWorkUnitsTeamScopedFixtures_DecodeAsJSONArrays(t *testing.T) {
	for _, path := range []string{workUnitsTeamScopedBaselinePath, workUnitsTeamScopedCandidatePath} {
		var rows []any
		if err := json.Unmarshal(readWorkUnitsFixture(t, path), &rows); err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		if len(rows) != 200 {
			t.Fatalf("%s: %d rows, want 200 (the route's own default LIMIT, and this admission's declared DisplacementLimit)", path, len(rows))
		}
	}
}
