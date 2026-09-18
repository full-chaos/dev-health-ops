package goapiproof

import "testing"

// TestExplainParityDeclarationsCarryShapes pins the actual registered
// corpus declarations for GET/POST /api/v1/explain -- not a hand-built
// Options -- so a future edit that silently drops a shape field, or
// forgets that Options fields other than BaselineDefects do NOT cascade
// from explainParity to explainRepoTeamScopedParity (both are built
// independently, not by embedding), fails here rather than only being
// noticed by a live prove run reporting false findings or an
// unexplained refusal again.
func TestExplainParityDeclarationsCarryShapes(t *testing.T) {
	if len(explainParity.FloatTierB) != 6 {
		t.Fatalf("explainParity.FloatTierB = %+v, want 6 entries", explainParity.FloatTierB)
	}
	for _, path := range []string{"data.value", "data.delta_pct", "data.drivers.value", "data.drivers.delta_pct", "data.contributors.value", "data.contributors.delta_pct"} {
		if _, ok := explainParity.FloatTierB[path]; !ok {
			t.Fatalf("explainParity.FloatTierB missing %q", path)
		}
	}
	if len(explainRepoTeamScopedParity.FloatTierB) != 6 {
		t.Fatalf("explainRepoTeamScopedParity.FloatTierB = %+v, want 6 entries -- Options fields do not cascade, this must be wired independently", explainRepoTeamScopedParity.FloatTierB)
	}
	if !explainParity.NumericLeavesDeclared {
		t.Fatal("explainParity.NumericLeavesDeclared = false, want true -- Options fields do not cascade, this must be wired independently")
	}
	if !explainRepoTeamScopedParity.NumericLeavesDeclared {
		t.Fatal("explainRepoTeamScopedParity.NumericLeavesDeclared = false, want true -- Options fields do not cascade, this must be wired independently")
	}

	if len(explainParity.OrderInsensitiveLists) != 2 {
		t.Fatalf("explainParity.OrderInsensitiveLists = %+v, want 2 entries (data.drivers, data.contributors)", explainParity.OrderInsensitiveLists)
	}
	if len(explainRepoTeamScopedParity.OrderInsensitiveLists) != 2 {
		t.Fatalf("explainRepoTeamScopedParity.OrderInsensitiveLists = %+v, want its own 2 entries -- does not cascade from explainParity", explainRepoTeamScopedParity.OrderInsensitiveLists)
	}

	var driversShape, contributorsShape *KeyedDirectionShape
	for _, defect := range explainParity.BaselineDefects {
		if defect.Ticket != "CHAOS-5818" || defect.KeyedDirectionShape == nil {
			continue
		}
		switch defect.KeyedDirectionShape.ValuePath {
		case "data.drivers.value":
			driversShape = defect.KeyedDirectionShape
		case "data.contributors.value":
			contributorsShape = defect.KeyedDirectionShape
		}
	}
	if driversShape == nil {
		t.Fatal("explainParity carries no CHAOS-5818 KeyedDirectionShape for data.drivers.value")
	}
	if !driversShape.CandidateMustBeGreater {
		t.Fatal("data.drivers.value's CHAOS-5818 shape lost CandidateMustBeGreater -- the avg-vs-sum direction is CANDIDATE greater, the reverse of every other KeyedDirectionShape in this file")
	}
	if contributorsShape == nil {
		t.Fatal("explainParity carries no CHAOS-5818 KeyedDirectionShape for data.contributors.value")
	}
	if !contributorsShape.CandidateMustBeGreater {
		t.Fatal("data.contributors.value's CHAOS-5818 shape lost CandidateMustBeGreater")
	}
}
