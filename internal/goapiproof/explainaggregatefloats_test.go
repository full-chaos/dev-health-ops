package goapiproof

import "testing"

// This file pins explainAggregateFloats/explainParity's own FloatTierB
// wiring (restcorpus.go) directly against the declared corpus Options --
// the same discipline heatmapcellfloats_test.go applies to heatmapDedupParity,
// for the same reason: a future edit that drops the field must fail here,
// not only be noticed by a live prove run reporting false findings again.

func explainRESTSnapshot(t *testing.T, body string) Snapshot {
	t.Helper()
	snapshot, err := DecodeRESTSnapshot([]byte(body))
	if err != nil {
		t.Fatalf("DecodeRESTSnapshot(%s): %v", body, err)
	}
	return snapshot
}

func explainBody(value, deltaPct float64) string {
	return `{"metric":"cycle_time","label":"Cycle Time","unit":"hours","value":` +
		jsonFloat(value) + `,"delta_pct":` + jsonFloat(deltaPct) +
		`,"drivers":[],"contributors":[],"drilldown_links":{}}`
}

// A ULP-scale difference at explain's own data.value magnitude (~1.26,
// metricValueProjection's argMax(tuple(col), computed_at) read) must be
// tolerated -- the same CHAOS-5451 class heatmap's own leaf carries.
func TestExplainParity_TolerantOfULPNoiseOnValue(t *testing.T) {
	baseline := explainRESTSnapshot(t, explainBody(1.263200628679553, 10.587654440876777))
	candidate := explainRESTSnapshot(t, explainBody(1.2632006286795534, 10.587654440876802))

	result := Compare(baseline, candidate, explainParity)
	if result.TerminalState != TerminalStateMatch {
		t.Fatalf("terminal = %q, want match -- ULP noise at this magnitude is below float64's own precision floor: findings %+v", result.TerminalState, result.Findings)
	}
}

// Blind-spot pin: a genuine difference at the same magnitude, far above
// the tolerance, must still surface.
func TestExplainParity_StillCatchesGenuineDifferenceOnValue(t *testing.T) {
	baseline := explainRESTSnapshot(t, explainBody(1.26, 10.5))
	candidate := explainRESTSnapshot(t, explainBody(1.30, 10.5))

	result := Compare(baseline, candidate, explainParity)
	if result.TerminalState != TerminalStateMismatch {
		t.Fatalf("terminal = %q, want mismatch -- a 0.04 difference at magnitude 1.26 is ~1e8x the tolerance and must not be swallowed", result.TerminalState)
	}
}

// TestExplainParity_RealCycleTimeULPCaptureIsNowAMatch replays a real
// captured production deployed-vs-deployed prove run (GET /api/v1/explain
// cycle_time_range_days_90, step74): data.value and data.delta_pct each
// differ by 1 ULP (magnitude ~1.26 and ~10.6). This is the request the
// STEP74 record calls "flapped" -- present in an earlier capture, absent,
// back in this one -- and the flap IS this: ULP noise crossing the
// exact-equality boundary run to run, unrelated to drilldown/prs' own
// duplicate-row mechanism. Falsifiable prediction: the next production
// prove of this request comes back a clean match.
func TestExplainParity_RealCycleTimeULPCaptureIsNowAMatch(t *testing.T) {
	baseline := heatmapDirectionSnapshotFromFile(t, "testdata/explain_cycle_time_ulp_baseline_29615ee0.json")
	candidate := heatmapDirectionSnapshotFromFile(t, "testdata/explain_cycle_time_ulp_candidate_e712de26.json")

	result := Compare(baseline, candidate, explainParity)
	if result.TerminalState != TerminalStateMatch {
		t.Fatalf("terminal = %q, want match -- both real captured differences are ULP-scale noise Tier B must absorb: findings %+v", result.TerminalState, result.Findings)
	}
}

// TestExplainParity_RealThroughputAvgVsSumCaptureUnchanged replays a real
// captured production run (GET /api/v1/explain throughput_default,
// step74) whose covered divergence is NOT noise -- CHAOS-5818's own
// avg-vs-sum mechanism (baseline 0.419 vs candidate 57, a ~136x
// difference) -- to prove wiring explainAggregateFloats does not touch
// this route's genuine, large, already-attributed divergence: it is
// seven-plus orders of magnitude above the tolerance and still reaches
// classification exactly as before Tier B existed.
func TestExplainParity_RealThroughputAvgVsSumCaptureUnchanged(t *testing.T) {
	baseline := heatmapDirectionSnapshotFromFile(t, "testdata/explain_throughput_avgvssum_baseline_caeb212b.json")
	candidate := heatmapDirectionSnapshotFromFile(t, "testdata/explain_throughput_avgvssum_candidate_d525f8d8.json")

	result := Compare(baseline, candidate, explainParity)
	if result.TerminalState != TerminalStateMismatch {
		t.Fatalf("terminal = %q, want mismatch -- Tier B must not swallow this route's real avg-vs-sum divergence", result.TerminalState)
	}
}

// explainDriversBody builds a minimal explain response body with the
// given raw `drivers` array JSON (already-formed elements), matching
// Response's own wire shape (explain.go) closely enough for Compare to
// decode and pair by "id".
func explainDriversBody(driversJSON string) string {
	return `{"metric":"throughput","label":"Throughput","unit":"items","value":0,"delta_pct":0,"drivers":[` +
		driversJSON + `],"contributors":[],"drilldown_links":{}}`
}

// explainCHAOS5818Options isolates the SAME shape CHAOS-5818's own
// restcorpus.go declaration wires (KeyedDirectionShape, CandidateMustBeGreater,
// paired OrderInsensitiveLists), without explainParity's OTHER blanket
// entries (CHAOS-5813 also names data.drivers.value in its own Paths,
// unshaped, and would otherwise admit every case below regardless of
// CHAOS-5818's own verdict -- exactly the "two citations at the same
// path" collision this package's shapes exist to tell apart). This lets
// the tests below assert CHAOS-5818's shape in isolation; the real
// explainParity wiring is what TestExplainParity_RealThroughputAvgVsSumCaptureUnchanged
// and TestExplainAggregateFloats_WiringPin exercise end to end.
func explainCHAOS5818Options() Options {
	return Options{
		OrderInsensitiveLists: []OrderInsensitiveList{
			{Path: "data.drivers", KeyFields: []string{"id"}, Reason: "test fixture", Ticket: "CHAOS-TEST-5818"},
		},
		BaselineDefects: []BaselineDefect{{
			Ticket: "CHAOS-TEST-5818", Reason: "test fixture",
			Paths:        []string{"data.drivers.value"},
			Intermittent: true, IntermittentReason: "test fixture",
			KeyedDirectionShape: &KeyedDirectionShape{
				ListPath:               "data.drivers",
				ValueField:             "value",
				ValuePath:              "data.drivers.value",
				KeyFields:              []string{"id"},
				CandidateMustBeGreater: true,
			},
		}},
	}
}

func explainDriver(id string, value float64) string {
	return `{"id":"` + id + `","label":"` + id + `","value":` + jsonFloat(value) + `,"delta_pct":0,"evidence_link":"","display_name":null}`
}

// CHAOS-5818's own direction pin: candidate (Go sum) strictly greater
// than baseline (Python avg) at the same driver id is admitted.
func TestExplainCHAOS5818_CandidateGreaterIsAdmitted(t *testing.T) {
	baseline := explainRESTSnapshot(t, explainDriversBody(explainDriver("repo-a", 0.42)))
	candidate := explainRESTSnapshot(t, explainDriversBody(explainDriver("repo-a", 57)))

	result := Compare(baseline, candidate, explainCHAOS5818Options())
	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("outside = %d, want 0 -- candidate greater than baseline at the same id is the declared avg-vs-sum direction: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
}

// Blind-spot pin (direction): the REVERSE direction -- baseline greater
// than candidate at the same id -- is the one shape CandidateMustBeGreater
// cannot produce, and must never be admitted.
func TestExplainCHAOS5818_BaselineGreaterIsNotAdmitted(t *testing.T) {
	baseline := explainRESTSnapshot(t, explainDriversBody(explainDriver("repo-a", 57)))
	candidate := explainRESTSnapshot(t, explainDriversBody(explainDriver("repo-a", 0.42)))

	result := Compare(baseline, candidate, explainCHAOS5818Options())
	if result.DifferencesOutsideBaselineDefect != 1 {
		t.Fatalf("outside = %d, want 1 -- a baseline-greater difference at this key must never be admitted by a CandidateMustBeGreater shape: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
}

// Blind-spot pin (order): explainDriverRankOrderInsensitive's own stated
// cost -- the SAME set of ids, the SAME value at each id, in a DIFFERENT
// order, produces NO finding at all. A Go-side ranking regression that
// computes the right values in the wrong order is therefore invisible on
// this route while this declaration stands, exactly as its own doc
// comment states.
func TestExplainCHAOS5818_SameSetDifferentOrderIsInvisible(t *testing.T) {
	baseline := explainRESTSnapshot(t, explainDriversBody(explainDriver("repo-a", 10)+","+explainDriver("repo-b", 20)))
	candidate := explainRESTSnapshot(t, explainDriversBody(explainDriver("repo-b", 20)+","+explainDriver("repo-a", 10)))

	result := Compare(baseline, candidate, explainCHAOS5818Options())
	if result.TerminalState != TerminalStateMatch {
		t.Fatalf("terminal = %q, want match -- a pure reorder of the same id/value pairs must produce no finding under the order-insensitive declaration: findings %+v", result.TerminalState, result.Findings)
	}
}

// data.drivers.delta_pct is deliberately NOT in either of CHAOS-5818's
// declared entries' own Paths (no provable direction, verified against a
// real capture where its own sign disagreed with data.drivers.value's --
// see the entries' own doc comment). This is a wiring pin, not a
// Compare-behaviour test: data.drivers.delta_pct also sits inside
// CHAOS-5813's OWN, unrelated, still-blanket Paths (the argMax NULL-skip
// mechanism), so a Compare-level test through the real explainParity
// would stay covered regardless of what CHAOS-5818 does -- checking
// CHAOS-5818's own declared Paths directly is the only way to pin that
// IT, specifically, dropped the field.
func TestExplainCHAOS5818_DriverDeltaPctDroppedFromPaths(t *testing.T) {
	found5818 := false
	for _, defect := range explainParity.BaselineDefects {
		if defect.Ticket != "CHAOS-5818" || defect.KeyedDirectionShape == nil {
			continue
		}
		found5818 = true
		for _, p := range defect.Paths {
			if p == "data.drivers.delta_pct" || p == "data.contributors.delta_pct" {
				t.Fatalf("CHAOS-5818 entry (ValuePath %s) still declares %q -- delta_pct has no provable direction and must stay dropped", defect.KeyedDirectionShape.ValuePath, p)
			}
		}
	}
	if !found5818 {
		t.Fatal("no CHAOS-5818 KeyedDirectionShape entry found in explainParity.BaselineDefects")
	}
}
