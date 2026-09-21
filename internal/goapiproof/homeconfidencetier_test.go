package goapiproof

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"testing"
)

// This file exercises HomeConfidenceTierShape (homeconfidencetier.go):
// small synthetic bodies for the individual rules, plus the real captured
// GET /api/v1/home home_default_org pair (a production deployed-vs-
// deployed prove run) through the actual registered homeConfidenceTierParity.

func homeTierTestShape() *HomeConfidenceTierShape {
	return &HomeConfidenceTierShape{
		CoveragePctPath:              "data.data_confidence.coverage_pct",
		MissingSourcesPath:           "data.data_confidence.missing_sources",
		ConnectedSourcesPath:         "data.data_confidence.connected_sources",
		LevelPath:                    "data.data_confidence.level",
		LimitingFactorConfidencePath: "data.limiting_factor.confidence",
		SignalsListPath:              "data.signals",
	}
}

// homeTierSignal builds one signals[] element.
func homeTierSignal(id string, evidenceCount int, confidence string) map[string]any {
	return map[string]any{"id": id, "confidence": confidence, "evidence_count": float64(evidenceCount)}
}

// homeTierBody builds a minimal home response body carrying exactly the
// leaves HomeConfidenceTierShape reads.
func homeTierBody(coveragePct float64, missing, connected []string, level string, limitingFactorConfidence string, signals []map[string]any) map[string]any {
	missingList := make([]any, len(missing))
	for i, s := range missing {
		missingList[i] = s
	}
	connectedList := make([]any, len(connected))
	for i, s := range connected {
		connectedList[i] = s
	}
	signalsList := make([]any, len(signals))
	for i, s := range signals {
		signalsList[i] = s
	}
	return map[string]any{
		"data_confidence": map[string]any{
			"coverage_pct":      coveragePct,
			"missing_sources":   missingList,
			"connected_sources": connectedList,
			"level":             level,
		},
		"limiting_factor": map[string]any{"confidence": limitingFactorConfidence},
		"signals":         signalsList,
	}
}

// homeTierRoundTrip re-decodes body through JSON, matching what Compare
// actually receives (json.Number, not Go float64/int) -- the plan's own
// asFloat/type-assertion logic must work against the REAL decoded shape,
// not a hand-built Go literal.
func homeTierRoundTrip(t *testing.T, body map[string]any) any {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return value
}

// TestHomeConfidenceTierShape_AdmitsAgreeingTierAndSignalConfidence pins
// rule 3-5's own positive admission: a coverage_pct straddling 75 flips
// level and every high-evidence signal's own confidence, and the shape
// admits all three leaf kinds because each leg's own observed value
// equals its own recomputed value.
func TestHomeConfidenceTierShape_AdmitsAgreeingTierAndSignalConfidence(t *testing.T) {
	baseSignals := []map[string]any{
		homeTierSignal("metric:a", 14, "medium"),
		homeTierSignal("metric:b", 3, "medium"),
	}
	candSignals := []map[string]any{
		homeTierSignal("metric:a", 14, "high"),
		homeTierSignal("metric:b", 3, "medium"),
	}
	baseBody := homeTierRoundTrip(t, homeTierBody(74.0, nil, []string{"ci"}, "medium", "medium", baseSignals))
	candBody := homeTierRoundTrip(t, homeTierBody(76.0, nil, []string{"ci"}, "high", "high", candSignals))

	plan := buildHomeConfidenceTierPlan(homeTierTestShape(), baseBody, candBody)
	if !plan.valid {
		t.Fatal("plan is not valid")
	}
	if !plan.levelAdmits {
		t.Error("level should be admitted: medium (74<75) vs high (76>=75) both recompute correctly")
	}
	if !plan.limitingFactorAdmits {
		t.Error("limiting_factor.confidence should be admitted: equals signals[0].confidence on both legs, same top id")
	}
	if !plan.signalConfidenceAdmits[0] {
		t.Error("signals[0].confidence should be admitted: evidence_count=14 identical, medium/high both recompute from own coverage_pct")
	}
	// signals[1] (deploy_freq-shaped, evidence_count=3) never actually
	// differs between legs (medium both sides, evidence below the
	// high-tier floor), so admits() is never even asked about it in a
	// real comparison -- whether the plan's own internal map happens to
	// mark it is not a behaviour this shape's contract promises either
	// way.
}

// TestHomeConfidenceTierShape_RefusesWhenObservedLevelDisagreesWithRecomputedTier
// is the guard-removal pin the team lead asked for: a candidate whose own
// observed level does not match ITS OWN coverage_pct under the documented
// formula must never be admitted, even though baseline and candidate
// still differ (a live finding exists). This is the regression this
// shape must never hide: if the recompute-and-compare guard were
// deleted (replaced by "the two legs merely differ"), this case would
// wrongly pass.
func TestHomeConfidenceTierShape_RefusesWhenObservedLevelDisagreesWithRecomputedTier(t *testing.T) {
	baseSignals := []map[string]any{homeTierSignal("metric:a", 14, "medium")}
	candSignals := []map[string]any{homeTierSignal("metric:a", 14, "medium")}
	baseBody := homeTierRoundTrip(t, homeTierBody(74.0, nil, []string{"ci"}, "medium", "medium", baseSignals))
	// candidate's own coverage_pct (76, >=75) recomputes to "high", but the
	// wire claims "low" -- broken, must refuse.
	candBody := homeTierRoundTrip(t, homeTierBody(76.0, nil, []string{"ci"}, "low", "low", candSignals))

	plan := buildHomeConfidenceTierPlan(homeTierTestShape(), baseBody, candBody)
	if !plan.valid {
		t.Fatal("plan should still be valid (missing/connected agree) -- only the tier recompute should fail")
	}
	if plan.levelAdmits {
		t.Error("level must NOT be admitted: candidate's own observed level disagrees with its own recomputed tier")
	}
}

// TestHomeConfidenceTierShape_RefusesWhenEvidenceCountDiffersBetweenLegs
// pins rule 4's own precondition: evidence_count is never part of the
// declared mechanism, so a difference there (of ANY kind) must refuse
// that signal's own confidence admission even if coverage_pct also
// differs.
func TestHomeConfidenceTierShape_RefusesWhenEvidenceCountDiffersBetweenLegs(t *testing.T) {
	baseSignals := []map[string]any{homeTierSignal("metric:a", 14, "medium")}
	candSignals := []map[string]any{homeTierSignal("metric:a", 15, "high")}
	baseBody := homeTierRoundTrip(t, homeTierBody(74.0, nil, []string{"ci"}, "medium", "medium", baseSignals))
	candBody := homeTierRoundTrip(t, homeTierBody(76.0, nil, []string{"ci"}, "high", "high", candSignals))

	plan := buildHomeConfidenceTierPlan(homeTierTestShape(), baseBody, candBody)
	if plan.signalConfidenceAdmits[0] {
		t.Error("signals[0].confidence must NOT be admitted: evidence_count differs between legs (14 vs 15), never part of the declared mechanism")
	}
	if plan.limitingFactorAdmits {
		t.Error("limiting_factor.confidence must NOT be admitted: it depends on signals[0], which is itself refused")
	}
}

// TestHomeConfidenceTierShape_RefusesWhenMissingSourcesDiffer pins rule 2:
// a source-status difference is a SEPARATE, real defect this shape must
// never paper over by admitting the tier leaf anyway.
func TestHomeConfidenceTierShape_RefusesWhenMissingSourcesDiffer(t *testing.T) {
	baseSignals := []map[string]any{homeTierSignal("metric:a", 14, "medium")}
	candSignals := []map[string]any{homeTierSignal("metric:a", 14, "high")}
	baseBody := homeTierRoundTrip(t, homeTierBody(76.0, nil, []string{"ci"}, "high", "high", baseSignals))
	candBody := homeTierRoundTrip(t, homeTierBody(76.0, []string{"github"}, []string{"ci"}, "medium", "medium", candSignals))

	plan := buildHomeConfidenceTierPlan(homeTierTestShape(), baseBody, candBody)
	if plan.valid {
		t.Error("plan must refuse entirely: missing_sources differs between legs (rule 2)")
	}
}

// TestHomeConfidenceTierShape_RefusesWhenConnectedSourcesDiffer pins rule
// 2's OTHER half separately from missing_sources: a connected_sources
// difference is its own, independent source-status defect this shape
// must never paper over either, even when missing_sources itself agrees.
func TestHomeConfidenceTierShape_RefusesWhenConnectedSourcesDiffer(t *testing.T) {
	baseSignals := []map[string]any{homeTierSignal("metric:a", 14, "medium")}
	candSignals := []map[string]any{homeTierSignal("metric:a", 14, "high")}
	baseBody := homeTierRoundTrip(t, homeTierBody(74.0, nil, []string{"ci"}, "medium", "medium", baseSignals))
	candBody := homeTierRoundTrip(t, homeTierBody(76.0, nil, []string{"ci", "github"}, "high", "high", candSignals))

	plan := buildHomeConfidenceTierPlan(homeTierTestShape(), baseBody, candBody)
	if plan.valid {
		t.Error("plan must refuse entirely: connected_sources differs between legs (rule 2), even though missing_sources agrees")
	}
}

// TestHomeConfidenceTierShape_RefusesLimitingFactorWhenTopSignalIDDiffers
// pins rule 5's own top-signal identity check: even if signals[0]'s own
// confidence recomputes cleanly on each leg, limiting_factor.confidence
// is refused when the two legs' own top signal is not the SAME signal.
func TestHomeConfidenceTierShape_RefusesLimitingFactorWhenTopSignalIDDiffers(t *testing.T) {
	baseSignals := []map[string]any{homeTierSignal("metric:a", 14, "medium")}
	candSignals := []map[string]any{homeTierSignal("metric:b", 14, "high")}
	baseBody := homeTierRoundTrip(t, homeTierBody(74.0, nil, []string{"ci"}, "medium", "medium", baseSignals))
	candBody := homeTierRoundTrip(t, homeTierBody(76.0, nil, []string{"ci"}, "high", "high", candSignals))

	plan := buildHomeConfidenceTierPlan(homeTierTestShape(), baseBody, candBody)
	if plan.limitingFactorAdmits {
		t.Error("limiting_factor.confidence must NOT be admitted: the two legs' own top signal carries a different id")
	}
}

// homeTierOptions wraps one HomeConfidenceTierShape declaration in an
// Options value suitable for Compare, the same pattern
// heatmapBoundaryOptions (heatmapcellboundary_test.go) uses for its own
// shape.
func homeTierOptions(ticket string) Options {
	return Options{
		BaselineDefects: []BaselineDefect{{
			Ticket: ticket, Reason: "test fixture",
			Paths:                   []string{"data.data_confidence.level", "data.limiting_factor.confidence", "data.signals.confidence"},
			Intermittent:            true,
			IntermittentReason:      "test fixture",
			HomeConfidenceTierShape: homeTierTestShape(),
		}},
	}
}

func homeTierSnapshot(t *testing.T, body map[string]any) Snapshot {
	t.Helper()
	return Snapshot{Data: homeTierRoundTrip(t, body), DataPresent: true}
}

// TestHomeConfidenceTierShape_ASignalWhoseObservedConfidenceDisagreesWithItsOwnRecomputedTierStaysOutside
// pins rule 4's own recompute-and-compare guard through a real Compare
// run: signals[0] (top-ranked) is a CORRECT pair -- evidence_count
// identical, each leg's own observed confidence equals its own
// recomputed tier -- and is admitted, carrying limiting_factor.confidence
// with it (rule 5). signals[1]'s own evidence_count is ALSO identical
// between legs, but its candidate-side OBSERVED confidence ("low") does
// not match what its own coverage_pct (76, >=75) and evidence_count (14,
// >=7) recompute to ("high") -- a genuinely broken pair this shape must
// never paper over just because it shares a path with a real, admissible
// mechanism. That ONE finding stays outside; every other admissible leaf
// (level, limiting_factor.confidence, signals[0].confidence) is still
// admitted.
func TestHomeConfidenceTierShape_ASignalWhoseObservedConfidenceDisagreesWithItsOwnRecomputedTierStaysOutside(t *testing.T) {
	baseSignals := []map[string]any{
		homeTierSignal("metric:a", 14, "medium"),
		homeTierSignal("metric:b", 14, "medium"),
	}
	candSignals := []map[string]any{
		homeTierSignal("metric:a", 14, "high"),
		homeTierSignal("metric:b", 14, "low"),
	}
	baseline := homeTierSnapshot(t, homeTierBody(74.0, nil, []string{"ci"}, "medium", "medium", baseSignals))
	candidate := homeTierSnapshot(t, homeTierBody(76.0, nil, []string{"ci"}, "high", "high", candSignals))

	ticket := "CHAOS-TEST-TIER"
	result := Compare(baseline, candidate, homeTierOptions(ticket))

	if len(result.Findings) != 5 {
		t.Fatalf("findings = %d, want 5 (coverage_pct, level, limiting_factor.confidence, signals[0].confidence, signals[1].confidence): %+v", len(result.Findings), result.Findings)
	}
	if result.DifferencesOutsideBaselineDefect != 2 {
		t.Fatalf("outside = %d, want 2 (coverage_pct, never in this declaration's own Paths, plus signals[1].confidence) -- covered %v outside %v findings %+v",
			result.DifferencesOutsideBaselineDefect, result.CoveredByShape, result.OutsideByShape, result.Findings)
	}
	sawSignal1Outside := false
	for _, f := range result.Findings {
		if f.Path != "$.data.signals[1].confidence" {
			continue
		}
		sawSignal1Outside = true
	}
	if !sawSignal1Outside {
		t.Fatalf("no finding at $.data.signals[1].confidence: %+v", result.Findings)
	}
	matched := false
	for _, ticketMatched := range result.BaselineDefectsMatched {
		if ticketMatched == ticket {
			matched = true
		}
	}
	if !matched {
		t.Fatalf("matched = %v, want %s among them (signals[0]/level are still admitted): live-unexplained %v",
			result.BaselineDefectsMatched, ticket, result.LiveBaselineDefectsUnexplained)
	}
}

// TestHomeConfidenceTierShape_RefusesLimitingFactorWhenItDisagreesWithItsOwnTopSignalConfidence
// pins rule 5's own wiring guard (baseLF == baseTopConfidence && candLF ==
// candTopConfidence) separately from the top-id equality
// TestHomeConfidenceTierShape_RefusesLimitingFactorWhenTopSignalIDDiffers
// already pins: the two legs' own top signal shares ONE id and its own
// confidence recomputes cleanly on both legs (so
// signalConfidenceAdmits[0] is true), but limiting_factor.confidence
// itself is wired to a DIFFERENT value than that same top signal's own
// confidence -- broken wiring this shape must never admit merely because
// the top signal itself checks out.
func TestHomeConfidenceTierShape_RefusesLimitingFactorWhenItDisagreesWithItsOwnTopSignalConfidence(t *testing.T) {
	baseSignals := []map[string]any{homeTierSignal("metric:a", 14, "medium")}
	candSignals := []map[string]any{homeTierSignal("metric:a", 14, "high")}
	baseBody := homeTierRoundTrip(t, homeTierBody(74.0, nil, []string{"ci"}, "medium", "medium", baseSignals))
	// candidate's own top signal (metric:a) recomputes to "high" and its
	// own observed confidence agrees -- but limiting_factor.confidence
	// itself claims "medium", disagreeing with that SAME signal.
	candBody := homeTierRoundTrip(t, homeTierBody(76.0, nil, []string{"ci"}, "high", "medium", candSignals))

	plan := buildHomeConfidenceTierPlan(homeTierTestShape(), baseBody, candBody)
	if !plan.signalConfidenceAdmits[0] {
		t.Fatal("signals[0].confidence should be admitted here -- it is not the rule under test")
	}
	if plan.limitingFactorAdmits {
		t.Error("limiting_factor.confidence must NOT be admitted: it disagrees with its own top signal's own confidence on the candidate leg")
	}
}

func homeTierSnapshotFromFile(t *testing.T, path string) Snapshot {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture %s: %v", path, err)
	}
	snapshot, err := DecodeRESTSnapshot(body)
	if err != nil {
		t.Fatalf("decode REST fixture %s: %v", path, err)
	}
	return snapshot
}

const (
	homeConfidenceTierBaselinePath  = "testdata/home_confidence_tier_baseline_609456e1.json"
	homeConfidenceTierCandidatePath = "testdata/home_confidence_tier_candidate_3968a6a4.json"
)

// homeConfidenceTierTicket reads homeConfidenceTierParity's own declared
// ticket, rather than a literal copy that could drift out of sync with
// home_corpus.go.
func homeConfidenceTierTicket(t *testing.T) string {
	t.Helper()
	for _, d := range homeConfidenceTierParity.BaselineDefects {
		if d.HomeConfidenceTierShape != nil {
			return d.Ticket
		}
	}
	t.Fatal("homeConfidenceTierParity declares no HomeConfidenceTierShape entry")
	return ""
}

// TestHomeConfidenceTierParity_RealCapturedBodyAdmitsConsequenceAndBaseCoverageLeaves
// runs the real captured GET /api/v1/home home_default_org pair (STEP 97,
// prod rev 102) through the actual registered homeConfidenceTierParity:
// coverage_pct straddles 75 (74.9893 baseline, 75.0224 candidate),
// flipping data_confidence.level, limiting_factor.confidence and 9 of 11
// signals' own confidence (deploy_freq's own evidence_count=3 stays below
// the high-tier's own >=7 floor on both legs; blocked_work's own
// evidence_count=0 stays "low" on both legs) -- 11 leaves admitted by the
// tier shape. The two BASE ratio leaves (coverage_pct itself, and
// freshness.coverage.issues_with_cycle_states_pct) are admitted by the
// separate coverage citation, never by the tier shape: its magnitude and
// direction are not re-derived.
func TestHomeConfidenceTierParity_RealCapturedBodyAdmitsConsequenceAndBaseCoverageLeaves(t *testing.T) {
	baseline := homeTierSnapshotFromFile(t, homeConfidenceTierBaselinePath)
	candidate := homeTierSnapshotFromFile(t, homeConfidenceTierCandidatePath)

	result := Compare(baseline, candidate, homeConfidenceTierParity)

	if result.TerminalState != TerminalStateMismatch {
		t.Fatalf("terminal = %q, want mismatch", result.TerminalState)
	}
	if len(result.Findings) != 13 {
		t.Fatalf("findings = %d, want 13: %+v", len(result.Findings), result.Findings)
	}
	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("outside = %d, want 0 -- covered %v outside %v findings %+v",
			result.DifferencesOutsideBaselineDefect, result.CoveredByShape, result.OutsideByShape, result.Findings)
	}
	if result.CoveredByShape["value"] != 13 {
		t.Fatalf("coveredByShape = %v, want {value: 13}", result.CoveredByShape)
	}
	wantTicket := homeConfidenceTierTicket(t)
	found := false
	for _, ticket := range result.BaselineDefectsMatched {
		if ticket == wantTicket {
			found = true
		}
	}
	if !found {
		t.Fatalf("matched = %v, want %s among them: idle %v stale %v unexplained %v",
			result.BaselineDefectsMatched, wantTicket, result.IdleIntermittentBaselineDefects, result.StaleBaselineDefects, result.LiveBaselineDefectsUnexplained)
	}
}

// homeCoverageBody is the slice of a home response the coverage citation
// touches: the three ratios, their mean, and the tier leaf derived from it.
func homeCoverageBody(t *testing.T, repos, prs, issues, mean float64) Snapshot {
	t.Helper()
	return gapDoc(t, fmt.Sprintf(`{"data_confidence":{"coverage_pct":%v,"level":"high","connected_sources":["ci"],"missing_sources":[]},`+
		`"freshness":{"coverage":{"repos_covered_pct":%v,"prs_linked_to_issues_pct":%v,"issues_with_cycle_states_pct":%v}}}`, mean, repos, prs, issues))
}

// The production pair from the run that first showed the difference (three
// superseded in-window versions: 431/580 raw vs 428/577 FINAL) is admitted
// as exactly the coverage citation's leaves; a difference in the repos
// ratio, which no cycle-time read feeds, stays outside it.
func TestHomeConfidenceTierParity_CoverageSupersessionAdmitsOnlyItsLeaves(t *testing.T) {
	repos, prs := 54.54545454545454, 100.0
	base := homeCoverageBody(t, repos, prs, 74.3103448275862, 76.28526645768025)
	cand := homeCoverageBody(t, repos, prs, 74.17677642980935, 76.2407436584213)

	got := Compare(base, cand, homeConfidenceTierParity)
	if got.DifferencesOutsideBaselineDefect != 0 || len(got.Findings) != 2 {
		t.Fatalf("outside=%d findings=%d, want 0 and 2: %+v", got.DifferencesOutsideBaselineDefect, len(got.Findings), got.Findings)
	}

	skewed := Compare(homeCoverageBody(t, 50.0, prs, 74.3103448275862, 76.2), cand, homeConfidenceTierParity)
	if skewed.DifferencesOutsideBaselineDefect == 0 {
		t.Fatalf("a repos_covered_pct difference was admitted: %+v", skewed.Findings)
	}
}
