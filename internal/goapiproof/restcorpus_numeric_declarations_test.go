package goapiproof

import (
	"strconv"
	"testing"
)

// This file pins the numeric-leaf declarations added to explain, quadrant,
// flame, flame/aggregated's throughput mode and the zero-leaf routes
// (filters/options, people search) directly against the REGISTERED corpus
// Options -- not a hand-built stand-in -- so a future edit that drops a
// declaration or the NumericLeavesDeclared marker fails here rather than
// only being noticed by a live prove run reporting a false Tier-A mismatch
// or an unexplained refusal.

// TestExplainParity_ContributorsDeltaPctIsTolerantOfULPNoise pins the leaf
// this ticket added: data.contributors.delta_pct shares Contributor's own
// struct with data.drivers.delta_pct (schemas.py), so it must tolerate the
// identical ULP-scale noise class explainAggregateFloats already declares
// for its sibling.
func TestExplainParity_ContributorsDeltaPctIsTolerantOfULPNoise(t *testing.T) {
	body := func(deltaPct float64) string {
		return `{"metric":"cycle_time","label":"Cycle Time","unit":"hours","value":1,"delta_pct":1,"drivers":[],"contributors":[` +
			`{"id":"c1","label":"c1","value":1.263200628679553,"delta_pct":` + jsonFloat(deltaPct) + `,"evidence_link":"","display_name":null}` +
			`],"drilldown_links":{}}`
	}
	baseline := explainRESTSnapshot(t, body(10.587654440876777))
	candidate := explainRESTSnapshot(t, body(10.587654440876802))

	result := Compare(baseline, candidate, explainParity)
	if result.TerminalState != TerminalStateMatch {
		t.Fatalf("terminal = %q, want match -- ULP noise on data.contributors.delta_pct must be tolerated: findings %+v", result.TerminalState, result.Findings)
	}
	if len(result.UndeclaredNumericLeaves) != 0 {
		t.Fatalf("data.contributors.delta_pct must count as declared, got %v", result.UndeclaredNumericLeaves)
	}
}

// RED PROOF: with explainAggregateFloats' own contributors.delta_pct entry
// removed, the same body must refuse as undeclared rather than silently
// falling back to Tier A -- proving the declaration, not luck, is what
// makes the test above pass.
func TestExplainParity_ContributorsDeltaPctRefusesWhenDeclarationRemoved(t *testing.T) {
	body := func(deltaPct float64) string {
		return `{"metric":"cycle_time","label":"Cycle Time","unit":"hours","value":1,"delta_pct":1,"drivers":[],"contributors":[` +
			`{"id":"c1","label":"c1","value":1.263200628679553,"delta_pct":` + jsonFloat(deltaPct) + `,"evidence_link":"","display_name":null}` +
			`],"drilldown_links":{}}`
	}
	stripped := map[string]string{}
	for k, v := range explainAggregateFloats {
		if k == "data.contributors.delta_pct" {
			continue
		}
		stripped[k] = v
	}
	opts := explainParity
	opts.FloatTierB = stripped

	baseline := explainRESTSnapshot(t, body(10.587654440876777))
	candidate := explainRESTSnapshot(t, body(10.587654440876802))
	result := Compare(baseline, candidate, opts)
	if len(result.UndeclaredNumericLeaves) != 1 || result.UndeclaredNumericLeaves[0] != "data.contributors.delta_pct" {
		t.Fatalf("removing the declaration must report data.contributors.delta_pct as undeclared, got %v", result.UndeclaredNumericLeaves)
	}
}

// TestQuadrantRepoDedupParity_PointXIsTolerantOfULPNoise pins
// quadrantPointFloats' own data.points.x declaration, reachable because
// quadrantRepoDedupParity carries NumericLeavesDeclared.
func TestQuadrantRepoDedupParity_PointXIsTolerantOfULPNoise(t *testing.T) {
	body := func(x float64) string {
		return `{"points":[{"entity_id":"e1","entity_label":"e1","x":` + jsonFloat(x) + `,"y":1,"window_start":"2026-01-01","window_end":"2026-01-07","evidence_link":""}]}`
	}
	baseline := restSnapshotFromJSON(t, body(1.263200628679553))
	candidate := restSnapshotFromJSON(t, body(1.2632006286795534))

	result := Compare(baseline, candidate, quadrantRepoDedupParity)
	if result.TerminalState != TerminalStateMatch {
		t.Fatalf("terminal = %q, want match -- ULP noise on data.points.x must be tolerated: findings %+v", result.TerminalState, result.Findings)
	}
}

// RED PROOF: with NumericLeavesDeclared turned off (the pre-this-ticket
// state), the SAME leaf is read as plain opt-in FloatTierB, unchanged --
// proving the marker is what turns on enforcement, not that FloatTierB
// alone already implied it. Removing quadrantPointFloats' own x entry
// while the marker stays on must refuse as undeclared.
func TestQuadrantRepoDedupParity_PointXRefusesWhenDeclarationRemoved(t *testing.T) {
	body := `{"points":[{"entity_id":"e1","entity_label":"e1","x":1.5,"y":1,"window_start":"2026-01-01","window_end":"2026-01-07","evidence_link":""}]}`
	stripped := map[string]string{}
	for k, v := range quadrantPointFloats {
		if k == "data.points.x" {
			continue
		}
		stripped[k] = v
	}
	opts := quadrantRepoDedupParity
	opts.FloatTierB = stripped

	snap := restSnapshotFromJSON(t, body)
	result := Compare(snap, snap, opts)
	if len(result.UndeclaredNumericLeaves) != 1 || result.UndeclaredNumericLeaves[0] != "data.points.x" {
		t.Fatalf("removing the declaration must report data.points.x as undeclared, got %v", result.UndeclaredNumericLeaves)
	}
}

// TestFlamePRIDBoundParity_EntityNumberDifferenceIsAFinding pins
// flamePRIDBoundEntityInts' own data.entity.number declaration: an
// integer leaf must still mismatch on a real difference, tolerance never
// applies to it.
func TestFlamePRIDBoundParity_EntityNumberDifferenceIsAFinding(t *testing.T) {
	body := func(number int) string {
		return `{"entity":{"repo_id":"r1","number":` + jsonInt(number) + `,"title":"t","state":"open"},"timeline":{"start":"2026-01-01T00:00:00Z","end":"2026-01-02T00:00:00Z"},"frames":[]}`
	}
	baseline := restSnapshotFromJSON(t, body(42))
	candidate := restSnapshotFromJSON(t, body(43))

	result := Compare(baseline, candidate, flamePRIDBoundParity)
	if result.TerminalState != TerminalStateMismatch {
		t.Fatalf("terminal = %q, want mismatch -- an integer-declared leaf must still catch a real 1-count difference", result.TerminalState)
	}
	if len(result.UndeclaredNumericLeaves) != 0 {
		t.Fatalf("data.entity.number must count as declared, got %v", result.UndeclaredNumericLeaves)
	}
}

// RED PROOF: with flamePRIDBoundEntityInts' own entry removed, the same
// leaf refuses as undeclared.
func TestFlamePRIDBoundParity_EntityNumberRefusesWhenDeclarationRemoved(t *testing.T) {
	body := `{"entity":{"repo_id":"r1","number":42,"title":"t","state":"open"},"timeline":{"start":"2026-01-01T00:00:00Z","end":"2026-01-02T00:00:00Z"},"frames":[]}`
	opts := flamePRIDBoundParity
	opts.IntegerLeaves = map[string]string{}

	snap := restSnapshotFromJSON(t, body)
	result := Compare(snap, snap, opts)
	if len(result.UndeclaredNumericLeaves) != 1 || result.UndeclaredNumericLeaves[0] != "data.entity.number" {
		t.Fatalf("removing the declaration must report data.entity.number as undeclared, got %v", result.UndeclaredNumericLeaves)
	}
}

// TestAggFlameThroughputParity_RootValueDifferenceIsAFinding pins
// aggFlameThroughputInts' own three fixed leaf-depths as integer: a real
// difference at the root must still be caught.
func TestAggFlameThroughputParity_RootValueDifferenceIsAFinding(t *testing.T) {
	body := func(rootValue int) string {
		return `{"mode":"throughput","unit":"items","root":{"name":"Work Delivered","value":` + jsonInt(rootValue) + `,"children":[]},"meta":{"window_start":"2026-01-01","window_end":"2026-01-07","filters":{},"notes":[],"approximation":{"used":false}}}`
	}
	baseline := restSnapshotFromJSON(t, body(10))
	candidate := restSnapshotFromJSON(t, body(11))

	result := Compare(baseline, candidate, aggFlameThroughputParity)
	if result.TerminalState != TerminalStateMismatch {
		t.Fatalf("terminal = %q, want mismatch -- data.root.value is declared integer, a real 1-count difference must be caught", result.TerminalState)
	}
}

// RED PROOF: with aggFlameThroughputInts' own root-value entry removed,
// the same leaf refuses as undeclared.
func TestAggFlameThroughputParity_RootValueRefusesWhenDeclarationRemoved(t *testing.T) {
	body := `{"mode":"throughput","unit":"items","root":{"name":"Work Delivered","value":10,"children":[]},"meta":{"window_start":"2026-01-01","window_end":"2026-01-07","filters":{},"notes":[],"approximation":{"used":false}}}`
	stripped := map[string]string{}
	for k, v := range aggFlameThroughputInts {
		if k == "data.root.value" {
			continue
		}
		stripped[k] = v
	}
	opts := aggFlameThroughputParity
	opts.IntegerLeaves = stripped

	snap := restSnapshotFromJSON(t, body)
	result := Compare(snap, snap, opts)
	if len(result.UndeclaredNumericLeaves) != 1 || result.UndeclaredNumericLeaves[0] != "data.root.value" {
		t.Fatalf("removing the declaration must report data.root.value as undeclared, got %v", result.UndeclaredNumericLeaves)
	}
}

// TestZeroLeafRoutesDeclareEmptyNotAbsent pins the "declared empty"
// convention filters/options and people search follow: the marker is
// on with both numeric tables absent, matching opportunities/
// work-unit-explain's own corpus precedent, so a future numeric field
// added to either shape is reported rather than silently compared exact.
func TestZeroLeafRoutesDeclareEmptyNotAbsent(t *testing.T) {
	spec, err := SpecForREST("REST:GET:/api/v1/filters/options")
	if err != nil {
		t.Fatalf("SpecForREST(filters/options): %v", err)
	}
	if len(spec.Requests) == 0 || !spec.Requests[0].Parity.NumericLeavesDeclared {
		t.Fatal("filters/options' own live entry must carry NumericLeavesDeclared")
	}
	if len(spec.Requests[0].Parity.FloatTierB) != 0 || len(spec.Requests[0].Parity.IntegerLeaves) != 0 {
		t.Fatalf("filters/options carries zero numeric leaves, want both tables empty, got FloatTierB=%v IntegerLeaves=%v", spec.Requests[0].Parity.FloatTierB, spec.Requests[0].Parity.IntegerLeaves)
	}

	if !peopleParity.NumericLeavesDeclared {
		t.Fatal("peopleParity must carry NumericLeavesDeclared")
	}
	if len(peopleParity.FloatTierB) != 0 || len(peopleParity.IntegerLeaves) != 0 {
		t.Fatalf("peopleParity carries zero numeric leaves, want both tables empty, got FloatTierB=%v IntegerLeaves=%v", peopleParity.FloatTierB, peopleParity.IntegerLeaves)
	}
}

// jsonInt is jsonFloat's integer twin: a plain decimal literal, no
// exponent or trailing zero that could nudge json.Decoder toward a
// different number kind than the fixture intends.
func jsonInt(v int) string {
	return strconv.Itoa(v)
}
