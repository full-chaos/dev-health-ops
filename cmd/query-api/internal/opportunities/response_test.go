// Golden parity tests for FromHomeResponse -- each fixture is captured
// by monkeypatching dev_health_ops.api.services.opportunities.
// build_home_response (the exact name build_opportunities_response
// calls) to return a HomeResponse built from the given input, then
// calling the real build_opportunities_response and dumping its JSON.
// This is the correct boundary for THIS route's own added logic:
// internal/home's own golden/integration tests already pin every
// ClickHouse/Postgres read and dedup fix this route inherits; re-deriving
// that fidelity here would just duplicate it.
//
// CAPTURE COMMAND (verbatim, from the ops repo root, this worktree's
// venv), org_default.json:
//
//	.venv/bin/python <<'PYEOF'
//	import asyncio, json
//	from datetime import date
//	import dev_health_ops.api.services.opportunities as opportunities_mod
//	from dev_health_ops.api.models.schemas import HomeResponse
//	from dev_health_ops.api.models.filters import MetricFilter, TimeFilter, ScopeFilter
//	with open("cmd/query-api/internal/home/testdata/org_default.json") as f:
//	    home_response = HomeResponse.model_validate(json.load(f))
//	async def fake_build_home_response(*, db_url, filters, cache, org_id="", semantic_session=None):
//	    return home_response
//	opportunities_mod.build_home_response = fake_build_home_response
//	filters = MetricFilter(
//	    time=TimeFilter(range_days=7, compare_days=7, end_date=date(2024, 1, 8)),
//	    scope=ScopeFilter(level="org", ids=[]),
//	)
//	async def main():
//	    result = await opportunities_mod.build_opportunities_response(
//	        db_url="unused", filters=filters, cache=None, org_id="org-1",
//	    )
//	    print(result.model_dump_json(indent=2))
//	asyncio.run(main())
//	PYEOF
//
// team_scoped.json/repo_scoped.json: the same script, reading
// cmd/query-api/internal/home/testdata/{team,repo}_scoped.json and
// scope=ScopeFilter(level="team", ids=["team-1"]) /
// ScopeFilter(level="repo", ids=["checkout-service"]) respectively.
//
// pr_rework_ratio_default_experiments.json/no_cards_fallback.json: the
// same monkeypatch, but home_response is built directly from a minimal
// hand-constructed HomeResponse (freshness/rework_theme_allocation/
// summary/tiles/constraint/events all empty-shaped) whose deltas are
// chosen to exercise, respectively: the ONE _METRICS entry
// (pr_rework_ratio) absent from _METRIC_SUGGESTED_EXPERIMENTS, falling
// through to _DEFAULT_SUGGESTED_EXPERIMENTS; and every delta_pct <= 0,
// producing the "Maintain steady flow" fallback card.
package opportunities

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/home"
)

func loadHomeGolden(t *testing.T, path string) *home.Response {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var resp home.Response
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&resp); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	return &resp
}

func loadOpportunitiesGolden(t *testing.T, name string) Response {
	t.Helper()
	data, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatalf("read golden %s: %v", name, err)
	}
	var resp Response
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&resp); err != nil {
		t.Fatalf("decode golden %s: %v", name, err)
	}
	return resp
}

func assertOpportunitiesEqual(t *testing.T, got, want Response) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		gotJSON, _ := json.MarshalIndent(got, "", "  ")
		wantJSON, _ := json.MarshalIndent(want, "", "  ")
		t.Fatalf("response mismatch\n got:  %s\n want: %s", gotJSON, wantJSON)
	}
}

func TestFromHomeResponseOrgScopeDefault(t *testing.T) {
	h := loadHomeGolden(t, "../home/testdata/org_default.json")
	f := home.Filters{
		Time:  home.TimeFilter{RangeDays: 7, CompareDays: 7},
		Scope: home.ScopeFilter{Level: "org"},
	}
	got := FromHomeResponse(h, f)
	want := loadOpportunitiesGolden(t, "org_default.json")
	assertOpportunitiesEqual(t, *got, want)
}

func TestFromHomeResponseTeamScoped(t *testing.T) {
	h := loadHomeGolden(t, "../home/testdata/team_scoped.json")
	f := home.Filters{
		Time:  home.TimeFilter{RangeDays: 7, CompareDays: 7},
		Scope: home.ScopeFilter{Level: "team", IDs: []string{"team-1"}},
	}
	got := FromHomeResponse(h, f)
	want := loadOpportunitiesGolden(t, "team_scoped.json")
	assertOpportunitiesEqual(t, *got, want)
}

func TestFromHomeResponseRepoScoped(t *testing.T) {
	h := loadHomeGolden(t, "../home/testdata/repo_scoped.json")
	f := home.Filters{
		Time:  home.TimeFilter{RangeDays: 7, CompareDays: 7},
		Scope: home.ScopeFilter{Level: "repo", IDs: []string{"checkout-service"}},
	}
	got := FromHomeResponse(h, f)
	want := loadOpportunitiesGolden(t, "repo_scoped.json")
	assertOpportunitiesEqual(t, *got, want)
}

// TestFromHomeResponsePRReworkRatioDefaultExperiments exercises the ONE
// _METRICS entry (pr_rework_ratio) with no dedicated
// _METRIC_SUGGESTED_EXPERIMENTS entry, falling through to
// _DEFAULT_SUGGESTED_EXPERIMENTS.
func TestFromHomeResponsePRReworkRatioDefaultExperiments(t *testing.T) {
	h := &home.Response{
		Deltas: []home.MetricDelta{
			{Metric: "pr_rework_ratio", Label: "PR Rework Ratio", DeltaPct: 15.0},
			{Metric: "cycle_time", Label: "Cycle Time", DeltaPct: -5.0},
			{Metric: "deploy_freq", Label: "Deploy Frequency", DeltaPct: 0.0},
		},
	}
	f := home.Filters{
		Time:  home.TimeFilter{RangeDays: 14, CompareDays: 14},
		Scope: home.ScopeFilter{Level: "team", IDs: []string{"team-9"}},
	}
	got := FromHomeResponse(h, f)
	want := loadOpportunitiesGolden(t, "pr_rework_ratio_default_experiments.json")
	assertOpportunitiesEqual(t, *got, want)
}

// TestFromHomeResponseNoCardsFallback exercises every delta_pct <= 0,
// producing the "Maintain steady flow" fallback card (opp-0).
func TestFromHomeResponseNoCardsFallback(t *testing.T) {
	h := &home.Response{
		Deltas: []home.MetricDelta{
			{Metric: "cycle_time", Label: "Cycle Time", DeltaPct: -5.0},
			{Metric: "deploy_freq", Label: "Deploy Frequency", DeltaPct: 0.0},
		},
	}
	f := home.Filters{
		Time:  home.TimeFilter{RangeDays: 14, CompareDays: 14},
		Scope: home.ScopeFilter{Level: "repo", IDs: []string{"checkout-service"}},
	}
	got := FromHomeResponse(h, f)
	want := loadOpportunitiesGolden(t, "no_cards_fallback.json")
	assertOpportunitiesEqual(t, *got, want)
}

// TestFromHomeResponseMoreThanFourPositiveDeltasKeepsTopFour pins the
// idx-based "opp-N" ids and the "first four after ranking" cutoff
// (services/opportunities.py:75: ranked[:4]) against a synthetic input
// with more than four positive deltas -- a shape the checked-in home
// fixtures do not exercise on their own (they all resolve to exactly
// four).
func TestFromHomeResponseMoreThanFourPositiveDeltasKeepsTopFour(t *testing.T) {
	h := &home.Response{
		Deltas: []home.MetricDelta{
			{Metric: "a", Label: "A", DeltaPct: 10},
			{Metric: "b", Label: "B", DeltaPct: 50},
			{Metric: "c", Label: "C", DeltaPct: 40},
			{Metric: "d", Label: "D", DeltaPct: 30},
			{Metric: "e", Label: "E", DeltaPct: 20},
			{Metric: "f", Label: "F", DeltaPct: -1},
		},
	}
	f := home.Filters{Time: home.TimeFilter{RangeDays: 14, CompareDays: 14}, Scope: home.ScopeFilter{Level: "org"}}
	got := FromHomeResponse(h, f)
	if len(got.Items) != 4 {
		t.Fatalf("want 4 items, got %d: %+v", len(got.Items), got.Items)
	}
	wantOrder := []string{"b", "c", "d", "e"}
	for i, metric := range wantOrder {
		gotLink := got.Items[i].EvidenceLinks[0]
		if !bytes.Contains([]byte(gotLink), []byte("metric="+metric+"&")) {
			t.Fatalf("item %d: want metric %s, evidence link %s", i, metric, gotLink)
		}
		wantID := fmt.Sprintf("opp-%d", i+1)
		if got.Items[i].ID != wantID {
			t.Fatalf("item %d: want id %s, got %s", i, wantID, got.Items[i].ID)
		}
	}
}
