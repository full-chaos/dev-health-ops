// Tests for FromHomeResponse against five files under testdata/. ALL FIVE are
// Go snapshots (kind go-generated in testdata.manifest.tsv): regression
// snapshots, Go against Go. They are NOT parity with Python and they prove
// nothing about the Python port.
//
// Since CHAOS-8109 every card carries four more keys that the Python
// reference never had: change_percent, direction, range_days and
// compare_days. So no Python answer is the expected answer of this route any
// more, for any input. The difference in each of the five files (old file ->
// Go snapshot): each card gets the four keys, and nothing else changes
// (titles, rationales, links, experiments and their order are the same):
//
//   - org_default.json, team_scoped.json, repo_scoped.json: four cards each,
//     with the metric's delta_pct of the home golden as change_percent,
//     "up" as direction (all four metrics climbed), and 7 / 7 as the window.
//   - pr_rework_ratio_default_experiments.json: one card, change_percent 15,
//     direction "up", window 14 / 14.
//   - no_cards_fallback.json: the fallback card, change_percent null and
//     direction null (no metric moved the wrong way: that is not a move of
//     0), window 14 / 14.
//
// The meaning of the four keys is pinned by the tests at the end of this
// file, which build their input by hand. The text below is the older history
// of the five files and is kept for that.
//
// BEFORE CHAOS-8109 the files were of TWO kinds (three Go snapshots since
// CHAOS-7776, two Python captures):
//
// GO SNAPSHOTS (kind go-generated): org_default.json, team_scoped.json and
// repo_scoped.json. They hold what THIS package's Go code returns for the home
// goldens internal/queryapi/home/testdata/{org_default,team_scoped,repo_scoped}.json
// with the filters of the three tests below. The three tests that read them
// are regression snapshots, Go against Go: they are NOT parity with Python
// and they prove nothing about the Python port.
//
// Until CHAOS-7776 these three files were Python captures. That ticket
// decided a divergence from the Python port: Python titled every card
// "Reduce <metric>" and picked cards by the sign of the move alone; Go names
// and picks a card by the polarity of its metric. So the Python answer for
// these inputs is no longer the expected answer. The difference, the same in
// each of the three files (old Python capture -> Go snapshot):
//
//   - the card "Reduce Throughput" is gone: throughput is higher-is-better
//     and it rose in the input, which is an improvement, not an opportunity;
//   - "Reduce Code Churn" takes the free place in the top four;
//   - the cards are, in order: Reduce Rework Ratio, Reduce Change Failure
//     Rate, Reduce Review Latency, Reduce Code Churn (they were: Reduce
//     Rework Ratio, Reduce Throughput, Reduce Change Failure Rate, Reduce
//     Review Latency).
//
// The behaviour itself (polarity, "Recover", improvements are not
// opportunities, ranking by the size of the move) is pinned by the tests at
// the end of this file, which build their input by hand. The snapshots only
// keep the whole response of three real inputs from changing unseen.
//
// PYTHON CAPTURES UNTIL CHAOS-8109: pr_rework_ratio_default_experiments.json
// and no_cards_fallback.json. Their inputs gave the same answer in Python and
// in Go (a lower-is-better metric that rose; no opportunity at all) until the
// four keys above were added. Each was captured by monkeypatching
// dev_health_ops.api.services.opportunities.build_home_response (the exact
// name build_opportunities_response calls) to return a HomeResponse, then
// calling the real build_opportunities_response and dumping its JSON. The
// HomeResponse was a minimal hand-constructed one (freshness/
// rework_theme_allocation/summary/tiles/constraint/events all empty-shaped)
// whose deltas exercise, respectively: the ONE _METRICS entry
// (pr_rework_ratio) absent from _METRIC_SUGGESTED_EXPERIMENTS, falling
// through to _DEFAULT_SUGGESTED_EXPERIMENTS; and every delta_pct <= 0,
// producing the "Maintain steady flow" fallback card. This is the correct
// boundary for THIS route's own added logic: internal/home's own tests pin
// every ClickHouse/Postgres read and dedup fix this route inherits.
//
// THE CAPTURE METHOD (the monkeypatch; it is also how the three snapshots
// were first made, before CHAOS-7776), from the ops repo root with a venv,
// shown for org_default.json:
//
//	.venv/bin/python <<'PYEOF'
//	import asyncio, json
//	from datetime import date
//	import dev_health_ops.api.services.opportunities as opportunities_mod
//	from dev_health_ops.api.models.schemas import HomeResponse
//	from dev_health_ops.api.models.filters import MetricFilter, TimeFilter, ScopeFilter
//	with open("internal/queryapi/home/testdata/org_default.json") as f:
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
// The recordings are stopped: nothing here is captured from Python again.
package opportunities

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/home"
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
	// Five lower-is-better metrics that climbed (worsened) and one that fell.
	h := &home.Response{
		Deltas: []home.MetricDelta{
			{Metric: "cycle_time", Label: "A", DeltaPct: 10},
			{Metric: "review_latency", Label: "B", DeltaPct: 50},
			{Metric: "churn", Label: "C", DeltaPct: 40},
			{Metric: "wip_saturation", Label: "D", DeltaPct: 30},
			{Metric: "blocked_work", Label: "E", DeltaPct: 20},
			{Metric: "change_failure_rate", Label: "F", DeltaPct: -1},
		},
	}
	f := home.Filters{Time: home.TimeFilter{RangeDays: 14, CompareDays: 14}, Scope: home.ScopeFilter{Level: "org"}}
	got := FromHomeResponse(h, f)
	if len(got.Items) != 4 {
		t.Fatalf("want 4 items, got %d: %+v", len(got.Items), got.Items)
	}
	wantOrder := []string{"review_latency", "churn", "wip_saturation", "blocked_work"}
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

// CHAOS-7776: an opportunity is a metric that moved the WRONG way for its
// polarity. The title verb and the rationale follow it, and an improvement is
// never an opportunity.
func polarityFilters() home.Filters {
	return home.Filters{
		Time:  home.TimeFilter{RangeDays: 14, CompareDays: 14},
		Scope: home.ScopeFilter{Level: "org"},
	}
}

func TestLowerIsBetterMetricThatClimbedIsReduce(t *testing.T) {
	h := &home.Response{Deltas: []home.MetricDelta{
		{Metric: "cycle_time", Label: "Cycle Time", DeltaPct: 19.4},
	}}
	got := FromHomeResponse(h, polarityFilters())
	if len(got.Items) != 1 {
		t.Fatalf("want 1 item, got %+v", got.Items)
	}
	if got.Items[0].Title != "Reduce Cycle Time" {
		t.Fatalf("title = %q", got.Items[0].Title)
	}
	if got.Items[0].Rationale != "Cycle Time climbed 19% in the last 14 days." {
		t.Fatalf("rationale = %q", got.Items[0].Rationale)
	}
}

func TestHigherIsBetterMetricThatFellIsRecover(t *testing.T) {
	for _, metric := range []string{"throughput", "deploy_freq", "ci_success"} {
		h := &home.Response{Deltas: []home.MetricDelta{
			{Metric: metric, Label: "Label " + metric, DeltaPct: -18.6},
		}}
		got := FromHomeResponse(h, polarityFilters())
		if len(got.Items) != 1 {
			t.Fatalf("%s: want 1 item, got %+v", metric, got.Items)
		}
		if got.Items[0].Title != "Recover Label "+metric {
			t.Fatalf("%s: title = %q", metric, got.Items[0].Title)
		}
		// The size of the move is positive in words: "fell 19%", not "fell -19%".
		want := "Label " + metric + " fell 19% in the last 14 days."
		if got.Items[0].Rationale != want {
			t.Fatalf("%s: rationale = %q, want %q", metric, got.Items[0].Rationale, want)
		}
	}
}

func TestImprovementsAreNotOpportunities(t *testing.T) {
	// Throughput up and cycle time down are good news: no card, the fallback.
	h := &home.Response{Deltas: []home.MetricDelta{
		{Metric: "throughput", Label: "Throughput", DeltaPct: 33},
		{Metric: "cycle_time", Label: "Cycle Time", DeltaPct: -20},
		{Metric: "ci_success", Label: "CI Success Rate", DeltaPct: 5},
	}}
	got := FromHomeResponse(h, polarityFilters())
	if len(got.Items) != 1 || got.Items[0].ID != "opp-0" || got.Items[0].Title != "Maintain steady flow" {
		t.Fatalf("want only the steady-flow fallback, got %+v", got.Items)
	}
}

func TestOpportunitiesRankByTheSizeOfTheMoveAcrossPolarities(t *testing.T) {
	h := &home.Response{Deltas: []home.MetricDelta{
		{Metric: "cycle_time", Label: "Cycle Time", DeltaPct: 10},
		{Metric: "throughput", Label: "Throughput", DeltaPct: -40},
		{Metric: "churn", Label: "Code Churn", DeltaPct: 25},
	}}
	got := FromHomeResponse(h, polarityFilters())
	titles := make([]string, 0, len(got.Items))
	for _, item := range got.Items {
		titles = append(titles, item.Title)
	}
	want := []string{"Recover Throughput", "Reduce Code Churn", "Reduce Cycle Time"}
	if !reflect.DeepEqual(titles, want) {
		t.Fatalf("titles = %v, want %v", titles, want)
	}
}

// CHAOS-8109: a card serves the move it is about as values. Before, the size
// and the direction of the move were only inside the rationale sentence.

func cardByTitle(t *testing.T, resp *Response, title string) Card {
	t.Helper()
	for _, card := range resp.Items {
		if card.Title == title {
			return card
		}
	}
	t.Fatalf("no card %q in %+v", title, resp.Items)
	return Card{}
}

func TestCardServesTheChangeOfItsMetric(t *testing.T) {
	h := &home.Response{
		Deltas: []home.MetricDelta{
			// lower is better, climbed: a card, direction up.
			{Metric: "cycle_time", Label: "Cycle Time", DeltaPct: 33.33333333333333},
			// higher is better, fell: a card, direction down.
			{Metric: "throughput", Label: "Throughput", DeltaPct: -20.5},
			// an improvement: no card.
			{Metric: "review_latency", Label: "Review Latency", DeltaPct: -40},
		},
	}
	// Two different numbers, so each key is shown to hold its own window.
	f := home.Filters{
		Time:  home.TimeFilter{RangeDays: 30, CompareDays: 14},
		Scope: home.ScopeFilter{Level: "org"},
	}
	got := FromHomeResponse(h, f)
	if len(got.Items) != 2 {
		t.Fatalf("cards = %d, want 2", len(got.Items))
	}

	reduce := cardByTitle(t, got, "Reduce Cycle Time")
	// Unrounded: the number Home serves for the metric, not the "33%" of the
	// rationale.
	if reduce.ChangePercent == nil || *reduce.ChangePercent != 33.33333333333333 {
		t.Errorf("change_percent = %v, want the metric's delta_pct 33.33333333333333", reduce.ChangePercent)
	}
	if reduce.Direction == nil || *reduce.Direction != "up" {
		t.Errorf("direction = %v, want up", reduce.Direction)
	}

	recover := cardByTitle(t, got, "Recover Throughput")
	if recover.ChangePercent == nil || *recover.ChangePercent != -20.5 {
		t.Errorf("change_percent = %v, want the signed delta_pct -20.5", recover.ChangePercent)
	}
	if recover.Direction == nil || *recover.Direction != "down" {
		t.Errorf("direction = %v, want down", recover.Direction)
	}

	for _, card := range got.Items {
		if card.RangeDays != 30 || card.CompareDays != 14 {
			t.Errorf("%s: window = %d / %d, want the request's 30 / 14", card.Title, card.RangeDays, card.CompareDays)
		}
	}
}

// The fallback card is about no metric. It has no change and no direction:
// null, never 0 and never "up".
func TestFallbackCardHasNoChangeAndNoDirection(t *testing.T) {
	h := &home.Response{Deltas: []home.MetricDelta{{Metric: "cycle_time", Label: "Cycle Time", DeltaPct: -5}}}
	f := home.Filters{Time: home.TimeFilter{RangeDays: 30, CompareDays: 14}, Scope: home.ScopeFilter{Level: "org"}}
	got := FromHomeResponse(h, f)
	if len(got.Items) != 1 || got.Items[0].ID != "opp-0" {
		t.Fatalf("items = %+v, want the fallback card only", got.Items)
	}
	card := got.Items[0]
	if card.ChangePercent != nil || card.Direction != nil {
		t.Errorf("change_percent = %v, direction = %v, want null and null", card.ChangePercent, card.Direction)
	}
	if card.RangeDays != 30 || card.CompareDays != 14 {
		t.Errorf("window = %d / %d, want the request's 30 / 14", card.RangeDays, card.CompareDays)
	}

	// On the wire the two keys are present and null, not left out: a caller
	// can tell "no move" from "an older server".
	encoded, err := json.Marshal(card)
	if err != nil {
		t.Fatal(err)
	}
	var keys map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &keys); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"change_percent", "direction"} {
		if raw, ok := keys[key]; !ok || string(raw) != "null" {
			t.Errorf("%s on the wire = %s (present %v), want null", key, raw, ok)
		}
	}
	for _, key := range []string{"range_days", "compare_days"} {
		if _, ok := keys[key]; !ok {
			t.Errorf("%s is not on the wire", key)
		}
	}
}

// The direction is the sign of the move, for every card of a mixed answer.
func TestDirectionIsTheSignOfTheChange(t *testing.T) {
	h := &home.Response{
		Deltas: []home.MetricDelta{
			{Metric: "cycle_time", Label: "Cycle Time", DeltaPct: 10},
			{Metric: "throughput", Label: "Throughput", DeltaPct: -30},
			{Metric: "churn", Label: "Code Churn", DeltaPct: 5},
			{Metric: "deploy_freq", Label: "Deploy Frequency", DeltaPct: -2},
		},
	}
	got := FromHomeResponse(h, home.Filters{Time: home.TimeFilter{RangeDays: 7, CompareDays: 7}, Scope: home.ScopeFilter{Level: "org"}})
	if len(got.Items) == 0 {
		t.Fatal("no cards")
	}
	for _, card := range got.Items {
		if card.ChangePercent == nil || card.Direction == nil {
			t.Fatalf("%s: change_percent or direction is null on a metric card", card.Title)
		}
		want := "up"
		if *card.ChangePercent < 0 {
			want = "down"
		}
		if *card.Direction != want {
			t.Errorf("%s: direction = %q for a change of %v, want %q", card.Title, *card.Direction, *card.ChangePercent, want)
		}
	}
}
