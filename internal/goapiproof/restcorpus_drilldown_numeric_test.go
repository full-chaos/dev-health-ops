package goapiproof

import "testing"

// This file pins the numeric-leaf declarations added to drilldown/prs,
// drilldown/issues and their person-scoped siblings against the
// REGISTERED corpus Options.

// TestDrilldownPRsParity_ReviewLatencyHoursDifferenceIsAFinding pins
// drilldownPRsIntegerLeaves' own data.items.review_latency_hours
// declaration: dateDiff is integer domain, so a real 1-hour difference
// must still be caught, never tolerated.
func TestDrilldownPRsParity_ReviewLatencyHoursDifferenceIsAFinding(t *testing.T) {
	body := func(hours int) string {
		return `{"items":[{"repo_id":"r1","number":1,"title":null,"author":null,"created_at":"2026-01-01T00:00:00Z","merged_at":null,"first_review_at":null,"review_latency_hours":` + jsonInt(hours) + `,"link":null}]}`
	}
	baseline := restSnapshotFromJSON(t, body(5))
	candidate := restSnapshotFromJSON(t, body(6))

	result := Compare(baseline, candidate, drilldownPRsParity)
	if result.TerminalState != TerminalStateMismatch {
		t.Fatalf("terminal = %q, want mismatch -- an integer-declared leaf must still catch a real 1-hour difference", result.TerminalState)
	}
	if len(result.UndeclaredNumericLeaves) != 0 {
		t.Fatalf("data.items.review_latency_hours must count as declared, got %v", result.UndeclaredNumericLeaves)
	}
}

// RED PROOF: with drilldownPRsIntegerLeaves' own entries removed, the
// same leaves refuse as undeclared.
func TestDrilldownPRsParity_RefusesWhenDeclarationsRemoved(t *testing.T) {
	body := `{"items":[{"repo_id":"r1","number":1,"title":null,"author":null,"created_at":"2026-01-01T00:00:00Z","merged_at":null,"first_review_at":null,"review_latency_hours":5,"link":null}]}`
	opts := drilldownPRsParity
	opts.IntegerLeaves = map[string]string{}

	snap := restSnapshotFromJSON(t, body)
	result := Compare(snap, snap, opts)
	if len(result.UndeclaredNumericLeaves) != 2 {
		t.Fatalf("removing both declarations must report data.items.number and data.items.review_latency_hours as undeclared, got %v", result.UndeclaredNumericLeaves)
	}
}

// TestDrilldownIssuesParity_CycleTimeHoursULPDifferenceIsAFinding pins
// drilldownIssuesFloatExact's own Tier-A behaviour: cycle_time_hours is a
// plain per-row column read, never merged, so even a ULP-scale difference
// must mismatch -- unlike a genuinely merged Float64 aggregate, there is
// no engine nondeterminism here to tolerate.
func TestDrilldownIssuesParity_CycleTimeHoursULPDifferenceIsAFinding(t *testing.T) {
	body := func(hours float64) string {
		return `{"items":[{"work_item_id":"w1","provider":"github","status":"done","team_id":null,"cycle_time_hours":` + jsonFloat(hours) + `,"lead_time_hours":null,"started_at":null,"completed_at":null}]}`
	}
	baseline := restSnapshotFromJSON(t, body(12.000000000000002))
	candidate := restSnapshotFromJSON(t, body(12.000000000000004))

	result := Compare(baseline, candidate, drilldownIssuesParity)
	if result.TerminalState != TerminalStateMismatch {
		t.Fatalf("terminal = %q, want mismatch -- FloatExactLeaves must compare exactly even at ULP scale: findings %+v", result.TerminalState, result.Findings)
	}
	if len(result.UndeclaredNumericLeaves) != 0 {
		t.Fatalf("data.items.cycle_time_hours must count as declared, got %v", result.UndeclaredNumericLeaves)
	}

	same := restSnapshotFromJSON(t, body(12.000000000000002))
	if !Compare(same, same, drilldownIssuesParity).IsMatch() {
		t.Fatal("identical values must still match under FloatExactLeaves")
	}
}

// RED PROOF: with drilldownIssuesFloats/drilldownIssuesFloatExact
// stripped, the same leaf refuses as undeclared.
func TestDrilldownIssuesParity_RefusesWhenDeclarationsRemoved(t *testing.T) {
	body := `{"items":[{"work_item_id":"w1","provider":"github","status":"done","team_id":null,"cycle_time_hours":12.0,"lead_time_hours":null,"started_at":null,"completed_at":null}]}`
	opts := drilldownIssuesParity
	opts.FloatTierB = map[string]string{}
	opts.FloatExactLeaves = map[string]string{}

	snap := restSnapshotFromJSON(t, body)
	result := Compare(snap, snap, opts)
	if len(result.UndeclaredNumericLeaves) != 1 || result.UndeclaredNumericLeaves[0] != "data.items.cycle_time_hours" {
		t.Fatalf("removing the declaration must report data.items.cycle_time_hours as undeclared, got %v", result.UndeclaredNumericLeaves)
	}
}

// TestPersonDrilldownPRsParity_NumberDifferenceIsAFinding pins
// personDrilldownPRsIntegerLeaves' own data.items.number declaration.
func TestPersonDrilldownPRsParity_NumberDifferenceIsAFinding(t *testing.T) {
	body := func(number int) string {
		return `{"items":[{"repo_id":"r1","number":` + jsonInt(number) + `,"title":null,"author":null,"created_at":"2026-01-01T00:00:00Z","merged_at":null,"first_review_at":null,"review_latency_hours":null,"link":null}]}`
	}
	baseline := restSnapshotFromJSON(t, body(42))
	candidate := restSnapshotFromJSON(t, body(43))

	result := Compare(baseline, candidate, personDrilldownPRsParity)
	if result.TerminalState != TerminalStateMismatch {
		t.Fatalf("terminal = %q, want mismatch -- an integer-declared leaf must still catch a real 1-count difference", result.TerminalState)
	}
}

// RED PROOF for personDrilldownPRsParity.
func TestPersonDrilldownPRsParity_RefusesWhenDeclarationsRemoved(t *testing.T) {
	body := `{"items":[{"repo_id":"r1","number":42,"title":null,"author":null,"created_at":"2026-01-01T00:00:00Z","merged_at":null,"first_review_at":null,"review_latency_hours":null,"link":null}]}`
	opts := personDrilldownPRsParity
	opts.IntegerLeaves = map[string]string{}

	snap := restSnapshotFromJSON(t, body)
	result := Compare(snap, snap, opts)
	if len(result.UndeclaredNumericLeaves) != 1 || result.UndeclaredNumericLeaves[0] != "data.items.number" {
		t.Fatalf("removing the declaration must report data.items.number as undeclared, got %v", result.UndeclaredNumericLeaves)
	}
}

// TestPersonDrilldownIssuesParity_CycleTimeHoursULPDifferenceIsAFinding
// pins personDrilldownIssuesParity's own Tier-A behaviour for its float
// leaves, which it binds from drilldownIssuesParity.
func TestPersonDrilldownIssuesParity_CycleTimeHoursULPDifferenceIsAFinding(t *testing.T) {
	body := func(hours float64) string {
		return `{"items":[{"work_item_id":"w1","provider":"github","status":"done","team_id":null,"cycle_time_hours":` + jsonFloat(hours) + `,"lead_time_hours":null,"started_at":null,"completed_at":null}]}`
	}
	baseline := restSnapshotFromJSON(t, body(12.000000000000002))
	candidate := restSnapshotFromJSON(t, body(12.000000000000004))

	result := Compare(baseline, candidate, personDrilldownIssuesParity)
	if result.TerminalState != TerminalStateMismatch {
		t.Fatalf("terminal = %q, want mismatch -- FloatExactLeaves must compare exactly even at ULP scale", result.TerminalState)
	}
}

// RED PROOF for personDrilldownIssuesParity.
func TestPersonDrilldownIssuesParity_RefusesWhenDeclarationsRemoved(t *testing.T) {
	body := `{"items":[{"work_item_id":"w1","provider":"github","status":"done","team_id":null,"cycle_time_hours":12.0,"lead_time_hours":null,"started_at":null,"completed_at":null}]}`
	opts := personDrilldownIssuesParity
	opts.FloatTierB = map[string]string{}
	opts.FloatExactLeaves = map[string]string{}

	snap := restSnapshotFromJSON(t, body)
	result := Compare(snap, snap, opts)
	if len(result.UndeclaredNumericLeaves) != 1 || result.UndeclaredNumericLeaves[0] != "data.items.cycle_time_hours" {
		t.Fatalf("removing the declaration must report data.items.cycle_time_hours as undeclared, got %v", result.UndeclaredNumericLeaves)
	}
}
