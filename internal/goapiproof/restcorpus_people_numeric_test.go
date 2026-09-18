package goapiproof

import "testing"

// This file pins the numeric-leaf declarations on
// GET /api/v1/people/{person_id}/summary and .../metric against the
// REGISTERED corpus Options -- peopleSummaryParity (one shared shape) and
// the six per-metric Options .../metric carries, one per metric, rather
// than one shared peopleDetailParity (see peopleDetailParity's own doc
// comment for why the split is required: FloatTierB/IntegerLeaves has no
// Intermittent escape, so a breakdown path one metric never populates
// would report unused and refuse that metric's own request).

func peopleSummaryBody(deltaValue, workMixValue float64) string {
	return `{"person":{"person_id":"p1","display_name":"P","identities":[]},` +
		`"freshness":{"last_ingested_at":null,"sources":{},"coverage":{"repos_covered_pct":50,"prs_linked_to_issues_pct":50,"issues_with_cycle_states_pct":50}},` +
		`"identity_coverage_pct":100,` +
		`"deltas":[{"metric":"cycle_time","label":"Cycle Time","value":` + jsonFloat(deltaValue) + `,"unit":"days","delta_pct":1,"spark":[]}],` +
		`"narrative":[],` +
		`"sections":{"work_mix":[{"key":"feature","name":"Feature","value":` + jsonFloat(workMixValue) + `}],"flow_breakdown":[],"collaboration":{"review_load":[],"handoff_points":[]}}}`
}

// TestPeopleSummaryParity_DeltaValueIsTolerantOfULPNoise pins
// peopleSummaryNumericFloats' own data.deltas.value declaration -- the
// same mixed avg()/sum() provenance homeNumericLeaves already declares
// float for its own identical shape.
func TestPeopleSummaryParity_DeltaValueIsTolerantOfULPNoise(t *testing.T) {
	baseline := restSnapshotFromJSON(t, peopleSummaryBody(1.263200628679553, 10))
	candidate := restSnapshotFromJSON(t, peopleSummaryBody(1.2632006286795534, 10))

	result := Compare(baseline, candidate, peopleSummaryParity)
	if result.TerminalState != TerminalStateMatch {
		t.Fatalf("terminal = %q, want match -- ULP noise on data.deltas.value must be tolerated: findings %+v", result.TerminalState, result.Findings)
	}
}

// TestPeopleSummaryParity_WorkMixValueDifferenceIsAFinding pins
// peopleSummaryNumericInts' own data.sections.work_mix.value declaration:
// fetchPersonWorkMix's toFloat64(count()) is an exact row count, so a
// real 1-count difference must still be caught.
func TestPeopleSummaryParity_WorkMixValueDifferenceIsAFinding(t *testing.T) {
	baseline := restSnapshotFromJSON(t, peopleSummaryBody(1, 10))
	candidate := restSnapshotFromJSON(t, peopleSummaryBody(1, 11))

	result := Compare(baseline, candidate, peopleSummaryParity)
	if result.TerminalState != TerminalStateMismatch {
		t.Fatalf("terminal = %q, want mismatch -- an integer-declared leaf must still catch a real 1-count difference", result.TerminalState)
	}
}

// RED PROOF for peopleSummaryParity: with both numeric tables emptied,
// both leaves refuse as undeclared.
func TestPeopleSummaryParity_RefusesWhenDeclarationsRemoved(t *testing.T) {
	body := peopleSummaryBody(1.26, 10)
	opts := peopleSummaryParity
	opts.FloatTierB = map[string]string{}
	opts.IntegerLeaves = map[string]string{}

	snap := restSnapshotFromJSON(t, body)
	result := Compare(snap, snap, opts)
	if len(result.UndeclaredNumericLeaves) == 0 {
		t.Fatal("removing both numeric tables must report at least one undeclared leaf")
	}
	found := map[string]bool{}
	for _, p := range result.UndeclaredNumericLeaves {
		found[p] = true
	}
	if !found["data.deltas.value"] || !found["data.sections.work_mix.value"] {
		t.Fatalf("expected data.deltas.value and data.sections.work_mix.value among undeclared, got %v", result.UndeclaredNumericLeaves)
	}
}

func peopleMetricBody(timeseriesValue float64, breakdownField, breakdownValue string) string {
	breakdowns := `"by_repo":[],"by_work_type":[],"by_stage":[]`
	if breakdownField != "" {
		breakdowns = `"by_repo":[],"by_work_type":[],"by_stage":[]`
		// Inject the requested breakdown field's own single element.
		element := `[{"label":"l1","value":` + breakdownValue + `}]`
		switch breakdownField {
		case "by_repo":
			breakdowns = `"by_repo":` + element + `,"by_work_type":[],"by_stage":[]`
		case "by_work_type":
			breakdowns = `"by_repo":[],"by_work_type":` + element + `,"by_stage":[]`
		case "by_stage":
			breakdowns = `"by_repo":[],"by_work_type":[],"by_stage":` + element
		}
	}
	return `{"metric":"m","label":"L","definition":{"description":"d","interpretation":"i"},` +
		`"timeseries":[{"day":"2026-01-01","value":` + jsonFloat(timeseriesValue) + `}],` +
		`"breakdowns":{` + breakdowns + `},"drivers":[]}`
}

// TestPeopleMetricChurnParity_TimeseriesAndByRepoAreIntegerDeclared pins
// churn's own sum(loc_touched) shape: both timeseries.value and its
// ByRepo breakdown must be integer, catching a real 1-count difference.
func TestPeopleMetricChurnParity_TimeseriesAndByRepoAreIntegerDeclared(t *testing.T) {
	baseline := restSnapshotFromJSON(t, peopleMetricBody(100, "by_repo", "50"))
	candidate := restSnapshotFromJSON(t, peopleMetricBody(101, "by_repo", "50"))

	result := Compare(baseline, candidate, peopleMetricChurnParity)
	if result.TerminalState != TerminalStateMismatch {
		t.Fatalf("terminal = %q, want mismatch -- churn's timeseries.value is integer, a real 1-count difference must be caught", result.TerminalState)
	}
	if len(result.UndeclaredNumericLeaves) != 0 {
		t.Fatalf("churn's own leaves must count as declared, got %v", result.UndeclaredNumericLeaves)
	}
}

// TestPeopleMetricCycleTimeParity_TimeseriesIsTolerantOfULPNoise pins
// cycle_time's own avg() shape: always Float64 in ClickHouse, so ULP
// noise must be tolerated on both the series and its two breakdowns.
func TestPeopleMetricCycleTimeParity_TimeseriesIsTolerantOfULPNoise(t *testing.T) {
	baseline := restSnapshotFromJSON(t, peopleMetricBody(1.263200628679553, "by_work_type", jsonFloat(2.0)))
	candidate := restSnapshotFromJSON(t, peopleMetricBody(1.2632006286795534, "by_work_type", jsonFloat(2.0)))

	result := Compare(baseline, candidate, peopleMetricCycleTimeParity)
	if result.TerminalState != TerminalStateMatch {
		t.Fatalf("terminal = %q, want match -- avg() is always Float64, ULP noise must be tolerated: findings %+v", result.TerminalState, result.Findings)
	}
}

// TestPeopleMetricWipOverlapParity_DeclaresOnlyTimeseries proves the
// split's own reason for existing: wip_overlap configures no breakdown
// at all, so its Options must declare data.timeseries.value only -- a
// breakdown declaration here would report unused and refuse this
// metric's own live request.
func TestPeopleMetricWipOverlapParity_DeclaresOnlyTimeseries(t *testing.T) {
	if len(peopleMetricWipOverlapFloats) != 1 {
		t.Fatalf("peopleMetricWipOverlapFloats = %v, want exactly data.timeseries.value", peopleMetricWipOverlapFloats)
	}
	if _, ok := peopleMetricWipOverlapFloats["data.timeseries.value"]; !ok {
		t.Fatal("peopleMetricWipOverlapFloats missing data.timeseries.value")
	}

	baseline := restSnapshotFromJSON(t, peopleMetricBody(1.263200628679553, "", ""))
	candidate := restSnapshotFromJSON(t, peopleMetricBody(1.2632006286795534, "", ""))
	result := Compare(baseline, candidate, peopleMetricWipOverlapParity)
	if result.TerminalState != TerminalStateMatch {
		t.Fatalf("terminal = %q, want match", result.TerminalState)
	}
}

// RED PROOF: declaring a breakdown path wip_overlap never populates
// alongside its real Options would report it unused -- demonstrated here
// via the run-level guard's own sibling check, UndeclaredNumericLeaves,
// by instead proving the CONVERSE: removing wip_overlap's own
// timeseries.value declaration refuses that leaf as undeclared, the same
// proof shape as every other route in this ticket.
func TestPeopleMetricWipOverlapParity_RefusesWhenDeclarationRemoved(t *testing.T) {
	body := peopleMetricBody(1.26, "", "")
	opts := peopleMetricWipOverlapParity
	opts.FloatTierB = map[string]string{}

	snap := restSnapshotFromJSON(t, body)
	result := Compare(snap, snap, opts)
	if len(result.UndeclaredNumericLeaves) != 1 || result.UndeclaredNumericLeaves[0] != "data.timeseries.value" {
		t.Fatalf("removing the declaration must report data.timeseries.value as undeclared, got %v", result.UndeclaredNumericLeaves)
	}
}

// TestPeopleMetricThroughputAndBlockedWorkParity_AreIntegerDeclared pins
// the remaining two integer-typed metrics in one pass: both sum() over a
// plain 0/1 or count expression, integer throughout.
func TestPeopleMetricThroughputAndBlockedWorkParity_AreIntegerDeclared(t *testing.T) {
	cases := []struct {
		name  string
		opts  Options
		field string
	}{
		{"throughput", peopleMetricThroughputParity, "by_work_type"},
		{"blocked_work", peopleMetricBlockedWorkParity, "by_work_type"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			baseline := restSnapshotFromJSON(t, peopleMetricBody(10, tc.field, "5"))
			candidate := restSnapshotFromJSON(t, peopleMetricBody(11, tc.field, "5"))
			result := Compare(baseline, candidate, tc.opts)
			if result.TerminalState != TerminalStateMismatch {
				t.Fatalf("%s: terminal = %q, want mismatch -- integer leaf must catch a real 1-count difference", tc.name, result.TerminalState)
			}
		})
	}
}

// TestPeopleMetricReviewLatencyParity_TimeseriesIsTolerantOfULPNoise
// pins review_latency's own avg() shape, the mirror of cycle_time's.
func TestPeopleMetricReviewLatencyParity_TimeseriesIsTolerantOfULPNoise(t *testing.T) {
	baseline := restSnapshotFromJSON(t, peopleMetricBody(1.263200628679553, "by_repo", jsonFloat(2.0)))
	candidate := restSnapshotFromJSON(t, peopleMetricBody(1.2632006286795534, "by_repo", jsonFloat(2.0)))

	result := Compare(baseline, candidate, peopleMetricReviewLatencyParity)
	if result.TerminalState != TerminalStateMatch {
		t.Fatalf("terminal = %q, want match", result.TerminalState)
	}
}
