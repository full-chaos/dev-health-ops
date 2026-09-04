package icfinalize

import (
	"math"
	"testing"
)

func recordFor(records []LandscapeRecord, identity, mapName string) *LandscapeRecord {
	for i := range records {
		if records[i].IdentityID == identity && records[i].MapName == mapName {
			return &records[i]
		}
	}
	return nil
}

// Three maps per identity, because map_name is part of the table's sorting key.
func TestComputeLandscapeEmitsThreeMapsPerIdentity(t *testing.T) {
	records := ComputeLandscape([]RollingStat{
		{IdentityID: "a", TeamID: "t", ChurnLOC30d: 10, DeliveryUnits30: 2, CycleP5030dHrs: 4, WIPMax30d: 1},
		{IdentityID: "b", TeamID: "t", ChurnLOC30d: 20, DeliveryUnits30: 5, CycleP5030dHrs: 8, WIPMax30d: 3},
	}, nil)
	if len(records) != 6 {
		t.Fatalf("got %d records, want 6 (2 identities x 3 maps)", len(records))
	}
	for _, name := range mapNames {
		if recordFor(records, "a", name) == nil {
			t.Fatalf("identity a is missing map %q", name)
		}
	}
}

// Normalization is PER TEAM.
//
// The teams must have DIFFERENT distributions for this to discriminate, and
// the first revision of this test got that wrong: it gave both teams {1, 100},
// where the per-team and global answers are BOTH 0.25 -- per-team is
// (0 + 0.5*1)/2, and globally a1 ties b1 so it is (0 + 0.5*2)/4. Identical.
// A global-normalization mutation SURVIVED that fixture, which is how the flaw
// was found rather than by inspection.
//
// With A={1,2} and B={100,200} nothing ties across teams, so a1 is
// (0 + 0.5*1)/2 = 0.25 per team against (0 + 0.5*1)/4 = 0.125 globally.
func TestNormalizationIsPerTeamNotGlobal(t *testing.T) {
	records := ComputeLandscape([]RollingStat{
		{IdentityID: "a1", TeamID: "A", ChurnLOC30d: 1, DeliveryUnits30: 1},
		{IdentityID: "a2", TeamID: "A", ChurnLOC30d: 2, DeliveryUnits30: 2},
		{IdentityID: "b1", TeamID: "B", ChurnLOC30d: 100, DeliveryUnits30: 100},
		{IdentityID: "b2", TeamID: "B", ChurnLOC30d: 200, DeliveryUnits30: 200},
	}, nil)
	got := recordFor(records, "a1", "churn_throughput").XNorm
	if got != 0.25 {
		t.Fatalf("a1 XNorm = %v, want 0.25 -- bottom of its own 2-member team", got)
	}
	if got == 0.125 {
		t.Fatal("normalization is GLOBAL -- a1 was ranked against all four identities")
	}
	// The top of the OTHER team is likewise ranked within B, not globally.
	if got := recordFor(records, "b2", "churn_throughput").XNorm; got != 0.75 {
		t.Fatalf("b2 XNorm = %v, want 0.75 (top of team B); globally it would be 0.875", got)
	}
}

// A one-member team ranks 0.5 on every axis, the same value an empty vector
// gives. Pinned because the two are indistinguishable downstream and a reader
// of the output cannot tell them apart.
func TestOneMemberTeamRanksAHalfOnEveryAxis(t *testing.T) {
	records := ComputeLandscape([]RollingStat{
		{IdentityID: "solo", TeamID: "T", ChurnLOC30d: 999, DeliveryUnits30: 7, CycleP5030dHrs: 3, WIPMax30d: 2},
	}, nil)
	if len(records) != 3 {
		t.Fatalf("got %d records, want 3", len(records))
	}
	for _, record := range records {
		if record.XNorm != 0.5 || record.YNorm != 0.5 {
			t.Fatalf("map %s: XNorm=%v YNorm=%v, want 0.5/0.5", record.MapName, record.XNorm, record.YNorm)
		}
	}
}

// The axis asymmetry survives composition: log1p on churn and cycle, RAW wip.
func TestLandscapeRawAxesKeepTheReferenceAsymmetry(t *testing.T) {
	records := ComputeLandscape([]RollingStat{
		{IdentityID: "a", TeamID: "t", ChurnLOC30d: 100, DeliveryUnits30: 9, CycleP5030dHrs: 48, WIPMax30d: 5},
	}, nil)
	if got := recordFor(records, "a", "churn_throughput").XRaw; got != math.Log1p(100) {
		t.Fatalf("churn XRaw = %v, want log1p(100)", got)
	}
	if got := recordFor(records, "a", "cycle_throughput").XRaw; got != math.Log1p(48) {
		t.Fatalf("cycle XRaw = %v, want log1p(48)", got)
	}
	if got := recordFor(records, "a", "wip_throughput").XRaw; got != 5 {
		t.Fatalf("wip XRaw = %v, want RAW 5 -- log1p must NOT be applied to wip", got)
	}
	for _, name := range mapNames {
		if got := recordFor(records, "a", name).YRaw; got != 9 {
			t.Fatalf("%s YRaw = %v, want delivery 9", name, got)
		}
	}
}

// The team fallback chain is the reference's: blank team_id resolves via
// team_map, then "unassigned"; a blank/"unknown" identity keeps a blank team.
func TestTeamFallbackChainMatchesTheReference(t *testing.T) {
	records := ComputeLandscape([]RollingStat{
		{IdentityID: "mapped", TeamID: ""},
		{IdentityID: "unmapped", TeamID: ""},
		{IdentityID: "unknown", TeamID: ""},
		{IdentityID: "", TeamID: ""},
		{IdentityID: "explicit", TeamID: "given"},
	}, map[string]string{"mapped": "from-map"})

	for identity, wantTeam := range map[string]string{
		"mapped":   "from-map",
		"unmapped": "unassigned",
		"unknown":  "", // literal "unknown" identity keeps a blank team
		"":         "", // blank identity likewise
		"explicit": "given",
	} {
		record := recordFor(records, identity, "churn_throughput")
		if record == nil {
			t.Fatalf("identity %q produced no record", identity)
		}
		if record.TeamID != wantTeam {
			t.Fatalf("identity %q -> team %q, want %q", identity, record.TeamID, wantTeam)
		}
	}
}
