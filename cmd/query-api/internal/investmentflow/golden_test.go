package investmentflow

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/sankey"
)

// Golden fixtures under testdata/ were captured from a live run of the
// real Python build_investment_flow_response/build_investment_repo_team_
// flow_response (ClickHouse readers monkeypatched to fixed rows via
// unittest.mock.patch.object, _tables_present/_columns_present forced
// true, the same convention cmd/query-api/internal/sankey/golden_test.go's
// own doc comment establishes) -- run once for all seven goldens via:
//
//	GOLDEN_OUT_DIR=cmd/query-api/internal/investmentflow/testdata \
//	  .venv/bin/python capture_investment_flow_goldens.py
//
// where capture_investment_flow_goldens.py is an uncommitted, one-off
// script (never checked in) that, for each golden:
//   - patches m._tables_present/_columns_present to always-true,
//     m.clickhouse_client to an async contextmanager yielding a fake
//     backend_type="clickhouse" sink, and the specific fetch_investment_*
//     function(s) that scenario exercises to an async function returning
//     a fixed row list (or, for fetch_investment_unassigned_counts, a
//     fixed {"missing_team": 1, "missing_repo": 2} dict);
//   - calls build_investment_flow_response(db_url="clickhouse://fake",
//     filters=MetricFilter(), org_id="org-1", **scenario kwargs) or
//     build_investment_repo_team_flow_response(...);
//   - writes json.dumps(resp.model_dump(mode="json"), indent=2,
//     sort_keys=True) verbatim to testdata/<name>.json.
//
// DisallowUnknownFields makes a field-name mismatch (Python emitted a key
// this Go type does not declare) a hard test failure rather than a silent
// drop -- reusing sankey.Response, the same type GET+POST /api/v1/sankey
// already decodes its own goldens into.
func loadFlowGolden(t *testing.T, name string) sankey.Response {
	t.Helper()
	data, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatalf("read golden %s: %v", name, err)
	}
	var resp sankey.Response
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&resp); err != nil {
		t.Fatalf("decode golden %s: %v", name, err)
	}
	return resp
}

func assertFlowGoldenMatch(t *testing.T, got, want sankey.Response) {
	t.Helper()
	gotJSON, err := json.MarshalIndent(got, "", "  ")
	if err != nil {
		t.Fatalf("marshal got: %v", err)
	}
	wantJSON, err := json.MarshalIndent(want, "", "  ")
	if err != nil {
		t.Fatalf("marshal want: %v", err)
	}
	if !bytes.Equal(gotJSON, wantJSON) {
		t.Fatalf("response mismatch:\n--- got ---\n%s\n--- want (golden) ---\n%s", gotJSON, wantJSON)
	}
}

func day(y int, m time.Month, d int) time.Time {
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}

// tcrRows/tsrRows/repoTeamRows/unassignedRow are the fixture rows the
// capture script's own TCR_ROWS/TSR_ROWS/REPOTEAM_ROWS/{"missing_team":1,
// "missing_repo":2} fed the mocked fetchers -- kept byte-identical here
// (same values, same order) so the Go fetchers replaying them reproduce
// the same goldens.
var (
	tcrRows = [][]any{
		{"teamA", "feature_delivery", "repoA", 10.0},
		{"teamA", "feature_delivery", "repoB", 5.0},
		{"teamB", "maintenance", "repoA", 3.0},
		{"unassigned", "feature_delivery", "unassigned", 2.0},
	}
	tsrRows = [][]any{
		{"teamA", "feature_delivery.customer", "repoA", 8.0},
		{"teamA", "feature_delivery.roadmap", "repoB", 4.0},
		{"unassigned", "maintenance.debt", "unassigned", 6.0},
	}
	repoTeamRows = [][]any{
		{"feature_delivery.customer", "unassigned", "teamA", 4.0},
		{"maintenance.debt", "repoA", "teamB", 7.0},
		{"risk.security", "repoB", "", 3.0},
	}
	unassignedRow = [][]any{{int64(2), int64(1)}} // missing_repo, missing_team
)

// flowFixtureHandler dispatches on the GROUP BY (and unit_team presence)
// shape unique to each of the six fetchers -- same "dispatch on query
// text" convention as sankey/golden_test.go.
func flowFixtureHandler(t *testing.T, query string, _ []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	switch {
	case strings.Contains(query, "GROUP BY subcategory, repo, team"):
		return &fixtureRowScanner{rows: repoTeamRows}, nil
	case strings.Contains(query, "GROUP BY team, category, repo"):
		return &fixtureRowScanner{rows: tcrRows}, nil
	case strings.Contains(query, "GROUP BY team, subcategory, repo"):
		return &fixtureRowScanner{rows: tsrRows}, nil
	case strings.Contains(query, "missing_repo"):
		return &fixtureRowScanner{rows: unassignedRow}, nil
	default:
		t.Fatalf("unexpected query for flow-mode golden:\n%s", query)
		return nil, nil
	}
}

func TestGoldenTeamCategoryRepo(t *testing.T) {
	want := loadFlowGolden(t, "team_category_repo.json")
	client := dispatchClient(t, flowFixtureHandler)
	got, err := BuildFlowResponse(context.Background(), client, Params{
		OrgID: "org-1", StartTS: day(2024, 1, 1), EndTS: day(2024, 1, 31),
		ScopeLevel: "org", FlowMode: strPtr("team_category_repo"), TopNRepos: 1,
	})
	if err != nil {
		t.Fatalf("BuildFlowResponse: %v", err)
	}
	assertFlowGoldenMatch(t, *got, want)
}

func TestGoldenTeamCategorySubcategoryRepo(t *testing.T) {
	want := loadFlowGolden(t, "team_category_subcategory_repo.json")
	client := dispatchClient(t, flowFixtureHandler)
	got, err := BuildFlowResponse(context.Background(), client, Params{
		OrgID: "org-1", StartTS: day(2024, 1, 1), EndTS: day(2024, 1, 31),
		ScopeLevel: "org", FlowMode: strPtr("team_category_subcategory_repo"), TopNRepos: 12,
	})
	if err != nil {
		t.Fatalf("BuildFlowResponse: %v", err)
	}
	assertFlowGoldenMatch(t, *got, want)
}

func TestGoldenTeamSubcategoryRepo(t *testing.T) {
	want := loadFlowGolden(t, "team_subcategory_repo.json")
	client := dispatchClient(t, flowFixtureHandler)
	got, err := BuildFlowResponse(context.Background(), client, Params{
		OrgID: "org-1", StartTS: day(2024, 1, 1), EndTS: day(2024, 1, 31),
		ScopeLevel: "org", FlowMode: strPtr("team_subcategory_repo"),
		DrillCategory: strPtr("feature_delivery"), TopNRepos: 12,
	})
	if err != nil {
		t.Fatalf("BuildFlowResponse: %v", err)
	}
	assertFlowGoldenMatch(t, *got, want)
}

func dynamicFixtureHandler(teamRows, repoRows [][]any) func(t *testing.T, query string, _ []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	return func(t *testing.T, query string, _ []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		switch {
		case strings.Contains(query, "GROUP BY source, target") && strings.Contains(query, "AS unit_team"):
			return &fixtureRowScanner{rows: teamRows}, nil
		case strings.Contains(query, "GROUP BY source, target"):
			return &fixtureRowScanner{rows: repoRows}, nil
		default:
			t.Fatalf("unexpected query for dynamic-mode golden:\n%s", query)
			return nil, nil
		}
	}
}

func TestGoldenDynamicTeam(t *testing.T) {
	want := loadFlowGolden(t, "dynamic_team.json")
	teamRows := [][]any{
		{"feature_delivery.customer", "teamA", 10.0},
		{"maintenance.debt", "teamB", 5.0},
		{"risk.security", "unassigned", 2.0},
	}
	repoRows := [][]any{
		{"feature_delivery.customer", "repoA", 6.0},
		{"maintenance.debt", "unassigned", 9.0},
	}
	client := dispatchClient(t, dynamicFixtureHandler(teamRows, repoRows))
	got, err := BuildFlowResponse(context.Background(), client, Params{
		OrgID: "org-1", StartTS: day(2024, 1, 1), EndTS: day(2024, 1, 31), ScopeLevel: "org",
	})
	if err != nil {
		t.Fatalf("BuildFlowResponse: %v", err)
	}
	assertFlowGoldenMatch(t, *got, want)
}

func TestGoldenDynamicRepoScope(t *testing.T) {
	want := loadFlowGolden(t, "dynamic_repo_scope.json")
	teamRows := [][]any{
		{"feature_delivery.customer", "unassigned", 10.0},
		{"maintenance.debt", "teamA", 2.0},
	}
	repoRows := [][]any{
		{"feature_delivery.customer", "repoA", 9.0},
		{"maintenance.debt", "repoB", 3.0},
	}
	client := dispatchClient(t, dynamicFixtureHandler(teamRows, repoRows))
	got, err := BuildFlowResponse(context.Background(), client, Params{
		OrgID: "org-1", StartTS: day(2024, 1, 1), EndTS: day(2024, 1, 31), ScopeLevel: "org",
	})
	if err != nil {
		t.Fatalf("BuildFlowResponse: %v", err)
	}
	assertFlowGoldenMatch(t, *got, want)
}

func TestGoldenDynamicFallback(t *testing.T) {
	want := loadFlowGolden(t, "dynamic_fallback.json")
	rows := [][]any{{"feature_delivery.customer", "unassigned", 5.0}}
	client := dispatchClient(t, dynamicFixtureHandler(rows, rows))
	got, err := BuildFlowResponse(context.Background(), client, Params{
		OrgID: "org-1", StartTS: day(2024, 1, 1), EndTS: day(2024, 1, 31), ScopeLevel: "org",
	})
	if err != nil {
		t.Fatalf("BuildFlowResponse: %v", err)
	}
	assertFlowGoldenMatch(t, *got, want)
}

func TestGoldenRepoTeam(t *testing.T) {
	want := loadFlowGolden(t, "repo_team.json")
	client := dispatchClient(t, func(t *testing.T, query string, _ []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		if strings.Contains(query, "GROUP BY subcategory, repo, team") {
			return &fixtureRowScanner{rows: repoTeamRows}, nil
		}
		t.Fatalf("unexpected query for repo-team golden:\n%s", query)
		return nil, nil
	})
	got, err := BuildRepoTeamFlowResponse(context.Background(), client, RepoTeamParams{
		OrgID: "org-1", StartTS: day(2024, 1, 1), EndTS: day(2024, 1, 31), ScopeLevel: "org",
	})
	if err != nil {
		t.Fatalf("BuildRepoTeamFlowResponse: %v", err)
	}
	assertFlowGoldenMatch(t, *got, want)
}
