package goapiproof

import (
	"encoding/json"
	"os"
	"testing"
)

// This file exercises LimitDisplacementShape (limitdisplacement.go)
// directly through its registered declaration on
// investmentSunburstParityWithLimit (restcorpus.go), against a real
// captured GET /api/v1/investment/sunburst?limit=50 response pair. The
// pair carries the sibling repos-join fan-out entry's own mechanism
// (full-chaos/dev-health-go) on TWO of its slices: one
// (feature_delivery.enablement) stays inside both planes' own limit-50
// cutoff and shows as an ordinary covered value difference; the other
// (quality.reliability) crosses the cutoff once doubled, entering
// baseline's list at the cost of whichever slice then ranks 51st on that
// plane -- the displaced pair this shape exists to admit.

const (
	sunburstDisplacementBaselinePath  = "testdata/investmentsunburst_explicitlimit_baseline_bd829115.json"
	sunburstDisplacementCandidatePath = "testdata/investmentsunburst_explicitlimit_candidate_869f2156.json"
	// sunburstDisplacementFixtureLimit is the ACTUAL limit=50 the
	// captured pair's own request carried (restcorpus.go's own
	// "explicit_limit" corpus entry) -- every test in this file that uses
	// the unmodified real fixture's own row count must declare the SAME
	// effective limit, or LimitDisplacementShape's own rule 1 refuses the
	// whole plan by design.
	sunburstDisplacementFixtureLimit = 50
)

// limitDisplacementTicket reads the ticket the registered
// LimitDisplacementShape declaration actually carries, rather than a
// literal copy that could drift out of sync with restcorpus.go.
func limitDisplacementTicket(t *testing.T) string {
	t.Helper()
	for _, defect := range investmentSunburstBaselineDefects {
		if defect.LimitDisplacementShape != nil {
			return defect.Ticket
		}
	}
	t.Fatal("investmentSunburstBaselineDefects carries no LimitDisplacementShape entry")
	return ""
}

func readSunburstFixture(t *testing.T, path string) []any {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var rows []any
	if err := json.Unmarshal(raw, &rows); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	return rows
}

func sunburstSnapshotFromRows(t *testing.T, rows []any) Snapshot {
	t.Helper()
	raw, err := json.Marshal(rows)
	if err != nil {
		t.Fatalf("marshal fixture: %v", err)
	}
	snapshot, err := DecodeRESTSnapshot(raw)
	if err != nil {
		t.Fatalf("decode REST fixture: %v", err)
	}
	return snapshot
}

// TestLimitDisplacementShape_RealCapturedCaseIsFullyAdmitted pins a real
// production case: the presence pair (quality.reliability/full-chaos/
// dev-health-go entering baseline's list, quality.bugfix/full-chaos/
// dev-health-web leaving it) is admitted alongside the ordinary covered
// value difference on the SAME repo's other slice, and nothing stays
// outside.
func TestLimitDisplacementShape_RealCapturedCaseIsFullyAdmitted(t *testing.T) {
	wantTicket := limitDisplacementTicket(t)
	baseline := sunburstSnapshotFromRows(t, readSunburstFixture(t, sunburstDisplacementBaselinePath))
	candidate := sunburstSnapshotFromRows(t, readSunburstFixture(t, sunburstDisplacementCandidatePath))

	result := Compare(baseline, candidate, investmentSunburstParityWithLimit(sunburstDisplacementFixtureLimit))

	if result.TerminalState != TerminalStateMismatch {
		t.Fatalf("terminal = %q, want mismatch -- a declared defect never converts one", result.TerminalState)
	}
	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("outside = %d, want 0: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
	sawDisplacement := false
	for _, ticket := range result.BaselineDefectsMatched {
		if ticket == wantTicket {
			sawDisplacement = true
		}
	}
	if !sawDisplacement {
		t.Fatalf("matched = %v, want %q present -- the displaced presence pair must be admitted, not merely absent from this capture", result.BaselineDefectsMatched, wantTicket)
	}
}

// sunburstRowMap builds a sunburst row literal as a decoded map,
// mirroring sunburstRow's own string-building sibling in
// investmentsunburst_direction_test.go but as JSON-native values, since
// these tests mutate a real decoded fixture rather than composing a body
// by hand.
func sunburstRowMap(theme, subcategory, scope string, value float64) map[string]any {
	return map[string]any{"theme": theme, "subcategory": subcategory, "scope": scope, "value": value}
}

// removeSunburstRow deletes the first row matching theme/subcategory/
// scope from rows, failing the test if none matches -- used to swap an
// unrelated row IN PLACE OF an existing one so a mutated fixture's own
// list length stays at the effective limit.
func removeSunburstRow(t *testing.T, rows []any, theme, subcategory, scope string) []any {
	t.Helper()
	for i, row := range rows {
		object := row.(map[string]any)
		if object["theme"] == theme && object["subcategory"] == subcategory && object["scope"] == scope {
			return append(append([]any(nil), rows[:i]...), rows[i+1:]...)
		}
	}
	t.Fatalf("fixture carries no %s/%s/%s row to remove", theme, subcategory, scope)
	return nil
}

// TestLimitDisplacementShape_UnrelatedRepoPresenceDifferenceStaysOutside
// is RED: a SECOND, unrelated presence pair replaces one ordinary shared
// row on each plane (a row unrelated to either the fan-out or the
// displacement, so BOTH lists stay at exactly the effective limit) --
// counts still balance (2 baseline-only, 2 candidate-only) and both
// lists are still exactly at the limit, but the added pair has no
// covered value leaf to derive a multiplier from, so it must stay
// outside even though the genuine pair right beside it is still
// admitted.
func TestLimitDisplacementShape_UnrelatedRepoPresenceDifferenceStaysOutside(t *testing.T) {
	baseline := readSunburstFixture(t, sunburstDisplacementBaselinePath)
	candidate := readSunburstFixture(t, sunburstDisplacementCandidatePath)

	// full-chaos/ask-dev's feature_delivery.roadmap row is shared,
	// stable, and untouched by the fan-out on either plane -- removing
	// it from both and replacing it with two DIFFERENT unrelated
	// repositories manufactures a second presence swap without changing
	// either list's own length.
	baseline = removeSunburstRow(t, baseline, "feature_delivery", "feature_delivery.roadmap", "full-chaos/ask-dev")
	candidate = removeSunburstRow(t, candidate, "feature_delivery", "feature_delivery.roadmap", "full-chaos/ask-dev")
	baseline = append(baseline, sunburstRowMap("risk", "risk.security", "full-chaos/unrelated-repo", 900))
	// A value comfortably ABOVE the genuine displaced row's own value
	// (~1320): rule 3 admits the LOWEST-valued candidate-only row first
	// (Limit-displacement's own one-entrant-one-slot rule), so this must
	// never be able to win that slot ahead of the genuine one.
	candidate = append(candidate, sunburstRowMap("risk", "risk.security", "full-chaos/another-unrelated-repo", 999999))

	if len(baseline) != sunburstDisplacementFixtureLimit || len(candidate) != sunburstDisplacementFixtureLimit {
		t.Fatalf("fixture lengths = %d/%d, want both exactly %d", len(baseline), len(candidate), sunburstDisplacementFixtureLimit)
	}

	result := Compare(sunburstSnapshotFromRows(t, baseline), sunburstSnapshotFromRows(t, candidate), investmentSunburstParityWithLimit(sunburstDisplacementFixtureLimit))

	if result.DifferencesOutsideBaselineDefect != 2 {
		t.Fatalf("outside = %d, want 2 (the unrelated pair's own two presence findings): findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
	sawUnrelatedBaselineOnly, sawUnrelatedCandidateOnly := false, false
	for _, f := range result.Findings {
		if f.Kind != FindingMismatch || f.Shape != ShapePresence {
			continue
		}
		key, ok := parsePresenceDetailKey(f.Detail)
		if !ok {
			continue
		}
		switch key {
		case "risk\x1frisk.security\x1ffull-chaos/unrelated-repo":
			sawUnrelatedBaselineOnly = true
		case "risk\x1frisk.security\x1ffull-chaos/another-unrelated-repo":
			sawUnrelatedCandidateOnly = true
		}
	}
	if !sawUnrelatedBaselineOnly || !sawUnrelatedCandidateOnly {
		t.Fatalf("expected both unrelated-repo presence findings in the outside set: findings %+v", result.Findings)
	}
}

// TestLimitDisplacementShape_ListBelowTheLimitStaysOutside is RED:
// dropping one row from the baseline list (unrelated to either the
// fan-out or the displacement) makes the two lists different lengths --
// rule 1's own structural precondition -- and the whole plan must refuse
// every presence finding it would otherwise explain, not just the
// mismatched-length one.
func TestLimitDisplacementShape_ListBelowTheLimitStaysOutside(t *testing.T) {
	wantTicket := limitDisplacementTicket(t)
	baseline := readSunburstFixture(t, sunburstDisplacementBaselinePath)
	candidate := readSunburstFixture(t, sunburstDisplacementCandidatePath)

	trimmed := removeSunburstRow(t, baseline, "feature_delivery", "feature_delivery.roadmap", "full-chaos/ask-dev")

	result := Compare(sunburstSnapshotFromRows(t, trimmed), sunburstSnapshotFromRows(t, candidate), investmentSunburstParityWithLimit(sunburstDisplacementFixtureLimit))

	if result.DifferencesOutsideBaselineDefect == 0 {
		t.Fatalf("outside = 0, want > 0 -- mismatched list lengths must refuse the whole plan: findings %+v", result.Findings)
	}
	for _, ticket := range result.BaselineDefectsMatched {
		if ticket == wantTicket {
			t.Fatalf("matched = %v, must not include %q -- the two lists are no longer the same length", result.BaselineDefectsMatched, wantTicket)
		}
	}
}

// TestLimitDisplacementShape_ListBelowTheEffectiveLimitStaysOutside is
// RED: both lists are the SAME length as each other (the real captured
// pair, untouched) but that length sits BELOW the effective limit this
// comparison declares -- a boundary the request never actually reached.
// The otherwise-admissible displaced pair must stay outside: equal
// lengths alone prove nothing about a limit boundary existing at all.
func TestLimitDisplacementShape_ListBelowTheEffectiveLimitStaysOutside(t *testing.T) {
	wantTicket := limitDisplacementTicket(t)
	baseline := sunburstSnapshotFromRows(t, readSunburstFixture(t, sunburstDisplacementBaselinePath))
	candidate := sunburstSnapshotFromRows(t, readSunburstFixture(t, sunburstDisplacementCandidatePath))

	// A limit well above the fixture's own 50 rows: both lists are equal
	// length, but neither sits at THIS comparison's declared boundary.
	result := Compare(baseline, candidate, investmentSunburstParityWithLimit(sunburstDisplacementFixtureLimit*2))

	if result.DifferencesOutsideBaselineDefect == 0 {
		t.Fatalf("outside = 0, want > 0 -- equal-length lists below the declared limit must never be treated as a boundary: findings %+v", result.Findings)
	}
	for _, ticket := range result.BaselineDefectsMatched {
		if ticket == wantTicket {
			t.Fatalf("matched = %v, must not include %q -- the lists never reached the declared limit", result.BaselineDefectsMatched, wantTicket)
		}
	}
}

// TestLimitDisplacementShape_ReducedValueStillRanksInsideStaysOutside is
// RED: the baseline-only entrant's own value is inflated well past what
// the SAME repository's own verified multiplier can explain -- dividing
// by that multiplier still leaves it ABOVE candidate's minimum, meaning
// it would have ranked inside candidate's list even without any fan-out,
// which is not a limit-boundary artifact and must never be admitted.
// With the entrant unadmitted, the genuine candidate-only row must ALSO
// stay outside: one entrant displaces at most one row, and here there is
// no admitted entrant to displace anything at all.
func TestLimitDisplacementShape_ReducedValueStillRanksInsideStaysOutside(t *testing.T) {
	baseline := readSunburstFixture(t, sunburstDisplacementBaselinePath)
	candidate := readSunburstFixture(t, sunburstDisplacementCandidatePath)

	mutated := false
	for _, row := range baseline {
		object := row.(map[string]any)
		if object["theme"] == "quality" && object["subcategory"] == "quality.reliability" && object["scope"] == "full-chaos/dev-health-go" {
			object["value"] = 6000.0
			mutated = true
		}
	}
	if !mutated {
		t.Fatal("fixture carries no quality.reliability/full-chaos/dev-health-go row to mutate")
	}

	result := Compare(sunburstSnapshotFromRows(t, baseline), sunburstSnapshotFromRows(t, candidate), investmentSunburstParityWithLimit(sunburstDisplacementFixtureLimit))

	if result.DifferencesOutsideBaselineDefect == 0 {
		t.Fatalf("outside = 0, want > 0 -- a reduced value that still ranks inside candidate's list must never be admitted: findings %+v", result.Findings)
	}
	sawUnadmittedEntrant, sawUnadmittedGenuineCandidateOnly := false, false
	for _, f := range result.Findings {
		if f.Kind != FindingMismatch || f.Shape != ShapePresence {
			continue
		}
		key, ok := parsePresenceDetailKey(f.Detail)
		if !ok {
			continue
		}
		switch key {
		case "quality\x1fquality.reliability\x1ffull-chaos/dev-health-go":
			sawUnadmittedEntrant = true
		case "quality\x1fquality.bugfix\x1ffull-chaos/dev-health-web":
			sawUnadmittedGenuineCandidateOnly = true
		}
	}
	if !sawUnadmittedEntrant {
		t.Fatalf("expected the mutated entrant's own presence finding in the outside set: findings %+v", result.Findings)
	}
	if !sawUnadmittedGenuineCandidateOnly {
		t.Fatalf("expected the genuine candidate-only row to ALSO stay outside -- it has no admitted entrant to displace it: findings %+v", result.Findings)
	}
}
