package providersync

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

// The multi-day differential pair for CHAOS-3494 (#1541 review M8).
//
// The Go side here runs the REAL production loop -- GitHubWorkItemDeriver.Derive
// -- rather than calling the builder n times itself. That is the whole point: a
// test that re-implemented the loop would agree with Python about multiplicity
// while the shipped deriver did something else entirely.

// githubDerivedMultiDayOracleCases carries `Days` instead of `Day`. The window
// is deliberately three days: two would not distinguish "repeats once" from
// "repeats per day", and the fixture keeps one item with a team-bearing repo so
// the compute has a real candidate to emit rather than only the unassigned
// fallback.
func githubDerivedMultiDayOracleCases() []oracleCase {
	return []oracleCase{
		{
			ID: "three_day_backfill_repeats_attributions",
			Input: map[string]any{
				"OrgID": githubDerivedOracleOrg,
				"Days":  []any{"2026-08-03", "2026-08-04", "2026-08-05"},
				// Whole-second ComputedAt: the stamp-quantization divergence is
				// pinned by its own test, and letting it vary here would mix two
				// independent properties into one comparison.
				"ComputedAt": "2026-08-05T00:30:00Z", "AsOf": "2026-08-05T00:30:00Z",
				"Facts": githubDerivedOracleEmptyFacts(),
				"WorkItems": []any{
					githubDerivedOracleItem("acme/api#1", map[string]any{"story_points": 3}),
					githubDerivedOracleItem("acme/api#2", nil),
				},
				"Transitions": []any{},
			},
		},
	}
}

// githubMultiDayOracleSource feeds loadGitHubWorkItemDerivationContext the same
// facts the Python pair reads, so neither side sees a fact set the other did
// not.
type githubMultiDayOracleSource struct {
	facts githubWorkItemDerivationFacts
	loads int
}

func (source *githubMultiDayOracleSource) Load(
	context.Context, Claim, githubWorkItemDerivationLoadRequest,
) (githubWorkItemDerivationFacts, error) {
	source.loads++
	return source.facts, nil
}

// TestGitHubWorkItemTeamAttributionsMatchLivePythonProductionAcrossDays is the
// multiplicity pin. Python emits n copies because its no-day compute sits inside
// the day loop; the Go deriver must emit the same n copies in the same order.
func TestGitHubWorkItemTeamAttributionsMatchLivePythonProductionAcrossDays(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t,
		"github/work-items/team-attributions-multiday",
		githubDerivedMultiDayOracleCases(),
		func(t *testing.T, input map[string]any) githubTeamAttributionColumns {
			t.Helper()
			return newGitHubTeamAttributionColumns(
				githubMultiDayOracleAttributions(t, input),
			)
		},
		nil,
	)
}

// githubMultiDayOracleAttributions runs the production deriver over the case's
// window and decodes the destination back into typed rows.
func githubMultiDayOracleAttributions(
	t *testing.T, input map[string]any,
) []githubWorkItemTeamAttributionRow {
	t.Helper()
	claim := githubWorkItemOracleClaim()
	claim.OrgID = input["OrgID"].(string)

	days := input["Days"].([]any)
	if len(days) < 2 {
		t.Fatalf("multi-day case needs at least two days, got %d", len(days))
	}
	first, err := time.Parse("2006-01-02", days[0].(string))
	if err != nil {
		t.Fatal(err)
	}
	last, err := time.Parse("2006-01-02", days[len(days)-1].(string))
	if err != nil {
		t.Fatal(err)
	}
	// BeforeAt is EXCLUSIVE, so it is the day AFTER the last included day --
	// the same mapping resolve_date_range applies to its `--before` flag.
	since := first
	before := last.AddDate(0, 0, 1)
	claim.SinceAt, claim.BeforeAt = &since, &before

	computedAt, err := time.Parse(time.RFC3339Nano, input["ComputedAt"].(string))
	if err != nil {
		t.Fatal(err)
	}

	rows := githubWorkItemRows{}
	for _, raw := range input["WorkItems"].([]any) {
		rows.WorkItems = append(rows.WorkItems, githubDerivedOracleGoItem(t, raw.(map[string]any)))
	}
	for _, raw := range input["Transitions"].([]any) {
		rows.StatusTransitions = append(
			rows.StatusTransitions, githubDerivedOracleGoTransition(t, raw.(map[string]any)),
		)
	}
	for _, raw := range githubDerivedOracleList(input, "Dependencies") {
		rows.Dependencies = append(
			rows.Dependencies, githubDerivedOracleGoDependency(t, raw.(map[string]any)),
		)
	}

	encodedFacts, err := json.Marshal(input["Facts"])
	if err != nil {
		t.Fatal(err)
	}
	var facts githubWorkItemDerivationFacts
	if err := json.Unmarshal(encodedFacts, &facts); err != nil {
		t.Fatal(err)
	}
	for _, raw := range githubDerivedOracleList(input, "Donors") {
		facts.DonorItems = append(facts.DonorItems, githubWorkItemDerivationSubjectFromRow(
			githubDerivedOracleGoItem(t, raw.(map[string]any)),
		))
	}

	source := &githubMultiDayOracleSource{facts: facts}
	deriver := GitHubWorkItemDeriver{Source: source, engine: githubWorkItemStubEngine{}}
	derived, err := deriver.Derive(context.Background(), claim, rows, computedAt)
	if err != nil {
		t.Fatal(err)
	}
	// The context load stays outside the loop on the Go side too; the Python
	// pair builds its resolver cascade exactly once for the same reason.
	if source.loads != 1 {
		t.Fatalf("derivation context loads=%d want=1", source.loads)
	}

	encoded := derived[githubTeamAttributionsDestination]
	// Non-vacuity, asserted against the case's own day count rather than a
	// literal: a window that silently collapsed to one day would otherwise
	// compare clean against a Python side that had also been handed one day.
	if len(encoded) == 0 || len(encoded)%len(days) != 0 {
		t.Fatalf("attribution rows=%d over %d days; expected a whole multiple",
			len(encoded), len(days))
	}
	attributions := make([]githubWorkItemTeamAttributionRow, 0, len(encoded))
	for _, raw := range encoded {
		var row githubWorkItemTeamAttributionRow
		if err := json.Unmarshal(raw, &row); err != nil {
			t.Fatal(err)
		}
		attributions = append(attributions, row)
	}
	return attributions
}
