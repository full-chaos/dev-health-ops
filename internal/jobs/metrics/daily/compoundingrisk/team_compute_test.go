package compoundingrisk

import (
	"testing"
)

// TestMeanOrNoneSkipsNilsAndReturnsNilWhenAllNil ports _mean_or_none's own
// two branches directly (compounding_risk.py:544-548): filter Nones, mean the
// rest; empty (or all-None) input is None, not zero or a divide-by-zero panic.
func TestMeanOrNoneSkipsNilsAndReturnsNilWhenAllNil(t *testing.T) {
	if got := MeanOrNone(nil); got != nil {
		t.Fatalf("MeanOrNone(nil) = %v, want nil", *got)
	}
	if got := MeanOrNone([]*float64{nil, nil}); got != nil {
		t.Fatalf("MeanOrNone(all nil) = %v, want nil", *got)
	}
	got := MeanOrNone([]*float64{ptr(1.0), nil, ptr(3.0)})
	if got == nil {
		t.Fatal("MeanOrNone with two present values returned nil")
	}
	// mean(1.0, 3.0) == 2.0, and the None in between must not count toward
	// len() the way a naive sum(values)/len(values) over the RAW slice would.
	if *got != 2.0 {
		t.Fatalf("MeanOrNone([1.0, nil, 3.0]) = %v, want 2.0 (nil must not count "+
			"toward the divisor)", *got)
	}
}

// TestMeanOrNoneUsesCompensatedSummationNotNaiveAddition pins MeanOrNone to
// pythonparity.Sum rather than a `total += v` loop -- the same divergence
// class TestComputeMatchesFrozenPythonGolden's siblings guard for the
// weighted sum, here for the mean's summation instead. 0.1 ten times is the
// textbook case: naive accumulation drifts to 0.9999999999999999, CPython's
// compensated sum() (and therefore the correct mean here) lands on exactly
// 1.0/10 == 0.1.
func TestMeanOrNoneUsesCompensatedSummationNotNaiveAddition(t *testing.T) {
	values := make([]*float64, 10)
	for i := range values {
		values[i] = opaquePtr(0.1)
	}
	got := MeanOrNone(values)
	if got == nil {
		t.Fatal("MeanOrNone returned nil for 10 present values")
	}
	if *got != 0.1 {
		t.Fatalf("MeanOrNone(ten 0.1s) = %.20f, want exactly 0.1 -- this is the "+
			"naive-vs-compensated-sum divergence MeanOrNone exists to avoid", *got)
	}
}

// TestBuildTeamRowsSkipsRepoWithEmptyStringTeamID: repoToTeam can (in
// principle) carry an empty-string value for a key -- ResolveFromOwnershipMap
// never writes one, but BuildTeamRows must not trust that invariant blindly,
// since Python's own `if not team_id: continue` treats an empty string the
// same as an absent key.
func TestBuildTeamRowsSkipsRepoWithEmptyStringTeamID(t *testing.T) {
	repoInputs := []RepoInputs{
		{RepoID: "repo-a", Inputs: Inputs{ReworkChurn: ptr(0.5)}},
	}
	repoToTeam := map[string]string{"repo-a": ""}
	records := BuildTeamRows(
		goldenDay(), teamGoldenOrgID, repoInputs, repoToTeam, goldenStamp(),
		DefaultWeights, DefaultThresholds, DefaultReferences,
	)
	if len(records) != 0 {
		t.Fatalf("got %d team rows for a repo whose team_id is the empty string, want 0: %v",
			len(records), scopeIDs(records))
	}
}

// TestBuildTeamRowsEmissionOrderIsFirstOccurrenceNotSorted pins the row-order
// guarantee BuildTeamRows' doc comment makes: team rows come out in
// first-occurrence-in-repoInputs order, matching Python dict-insertion order
// for by_team -- NOT sorted by team id. "zzz-team" appears first in the input
// order here and must appear first in the output despite sorting last
// alphabetically.
func TestBuildTeamRowsEmissionOrderIsFirstOccurrenceNotSorted(t *testing.T) {
	repoInputs := []RepoInputs{
		{RepoID: "repo-1", Inputs: Inputs{ReworkChurn: ptr(0.1)}},
		{RepoID: "repo-2", Inputs: Inputs{ReworkChurn: ptr(0.2)}},
		{RepoID: "repo-3", Inputs: Inputs{ReworkChurn: ptr(0.3)}},
	}
	repoToTeam := map[string]string{
		"repo-1": "zzz-team",
		"repo-2": "aaa-team",
		"repo-3": "zzz-team", // second repo-1's team recurs; must not move zzz-team's position
	}
	records := BuildTeamRows(
		goldenDay(), teamGoldenOrgID, repoInputs, repoToTeam, goldenStamp(),
		DefaultWeights, DefaultThresholds, DefaultReferences,
	)
	want := []string{"zzz-team", "aaa-team"}
	got := scopeIDs(records)
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("team row order = %v, want %v (first-occurrence order, not sorted)", got, want)
	}
}

// TestComputeTeamTagsScopeTeamNotRepo is the minimal direct proof that
// ComputeTeam and Compute diverge in exactly one place: the persisted scope
// tag. Everything else routes through the identical computeScored core, which
// TestBuildTeamRowsMatchesFrozenPythonGolden already exercises end to end;
// this test exists so a future refactor that accidentally hardcodes
// ScopeRepo inside computeScored (undoing the CHAOS-5084 parameterisation)
// fails here specifically, with a message naming the actual defect, rather
// than only failing indirectly via the golden comparison.
func TestComputeTeamTagsScopeTeamNotRepo(t *testing.T) {
	inputs := Inputs{
		ReworkChurn: ptr(0.1), ComplexityDelta: ptr(0.1), ReviewLatencyP90H: ptr(1.0),
		SingleOwnerRatio: ptr(0.1),
	}
	repoRecord := Compute(goldenDay(), "repo-x", teamGoldenOrgID, inputs, goldenStamp(),
		DefaultWeights, DefaultThresholds, DefaultReferences)
	teamRecord := ComputeTeam(goldenDay(), "team-x", teamGoldenOrgID, inputs, goldenStamp(),
		DefaultWeights, DefaultThresholds, DefaultReferences)

	if repoRecord.Scope != ScopeRepo {
		t.Errorf("Compute's Scope = %q, want %q", repoRecord.Scope, ScopeRepo)
	}
	if teamRecord.Scope != ScopeTeam {
		t.Errorf("ComputeTeam's Scope = %q, want %q", teamRecord.Scope, ScopeTeam)
	}
	// Everything but Scope/ScopeID must be identical for identical inputs --
	// proof the two functions share one formula.
	if repoRecord.CompoundingRisk == nil || teamRecord.CompoundingRisk == nil {
		t.Fatal("expected both scores to be non-nil for a fully-populated Inputs")
	}
	if *repoRecord.CompoundingRisk != *teamRecord.CompoundingRisk {
		t.Errorf("Compute and ComputeTeam scored the SAME inputs differently: %v vs %v -- "+
			"they must share the identical formula", *repoRecord.CompoundingRisk, *teamRecord.CompoundingRisk)
	}
}
