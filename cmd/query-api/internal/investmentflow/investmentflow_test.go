package investmentflow

import (
	"context"
	"errors"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

func TestBuildFlowResponseNilClient(t *testing.T) {
	_, err := BuildFlowResponse(context.Background(), nil, Params{})
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("want ErrUnavailable, got %v", err)
	}
}

func TestBuildRepoTeamFlowResponseNilClient(t *testing.T) {
	_, err := BuildRepoTeamFlowResponse(context.Background(), nil, RepoTeamParams{})
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("want ErrUnavailable, got %v", err)
	}
}

// TestTeamSubcategoryRepoRequiresDrillCategory ports the one ValueError
// build_investment_flow_response's own body can raise (investment_flow.py:
// 232-233): flow_mode="team_subcategory_repo" with no drill_category is a
// 400, not a 503 -- the route layer's own RequestError->400 mapping
// depends on this.
func TestTeamSubcategoryRepoRequiresDrillCategory(t *testing.T) {
	client := dispatchClient(t, func(t *testing.T, query string, _ []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		t.Fatalf("no query should run before the drill_category check:\n%s", query)
		return nil, nil
	})
	_, err := BuildFlowResponse(context.Background(), client, Params{
		OrgID: "org-1", ScopeLevel: "org", FlowMode: strPtr("team_subcategory_repo"),
	})
	reqErr, ok := AsRequestError(err)
	if !ok {
		t.Fatalf("want a *RequestError, got %v", err)
	}
	if reqErr.Status != 400 {
		t.Fatalf("want status 400, got %d", reqErr.Status)
	}
	if reqErr.Message != "drill_category is required for team_subcategory_repo" {
		t.Fatalf("unexpected message: %q", reqErr.Message)
	}
}

// TestFlowModeWithDrillCategoryNeverErrors confirms the other two
// flow_mode values never require drill_category -- team_category_repo/
// team_category_subcategory_repo both proceed even with none set.
func TestFlowModeWithoutDrillCategoryOtherModesOK(t *testing.T) {
	for _, mode := range []string{"team_category_repo", "team_category_subcategory_repo"} {
		t.Run(mode, func(t *testing.T) {
			client := dispatchClient(t, func(t *testing.T, query string, _ []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
				return &fixtureRowScanner{}, nil
			})
			_, err := BuildFlowResponse(context.Background(), client, Params{
				OrgID: "org-1", ScopeLevel: "org", FlowMode: strPtr(mode), TopNRepos: 12,
				StartTS: time.Now().UTC(), EndTS: time.Now().UTC(),
			})
			if err != nil {
				t.Fatalf("BuildFlowResponse(%s): %v", mode, err)
			}
		})
	}
}

// TestFlowModeSchemaDriftDegradesToEmpty ports the _tables_present/
// _columns_present schema-drift guard both build_investment_flow_
// response branches run: a missing table/column degrades to an empty
// SankeyResponse, never an error.
func TestFlowModeSchemaDriftDegradesToEmpty(t *testing.T) {
	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		for _, b := range bindings {
			if b.Name == "tables" {
				return &fixtureRowScanner{}, nil // no tables present
			}
		}
		t.Fatalf("unexpected query reached past the missing-table guard:\n%s", query)
		return nil, nil
	}}
	got, err := BuildFlowResponse(context.Background(), client, Params{OrgID: "org-1", ScopeLevel: "org", FlowMode: strPtr("team_category_repo")})
	if err != nil {
		t.Fatalf("BuildFlowResponse: %v", err)
	}
	if got.Mode != "investment" || len(got.Nodes) != 0 || len(got.Links) != 0 || got.Label != nil {
		t.Fatalf("expected the empty-response shape, got %+v", got)
	}
}

func TestDynamicModeSchemaDriftDegradesToEmpty(t *testing.T) {
	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		for _, b := range bindings {
			if b.Name == "tables" {
				return &fixtureRowScanner{}, nil
			}
		}
		t.Fatalf("unexpected query reached past the missing-table guard:\n%s", query)
		return nil, nil
	}}
	got, err := BuildFlowResponse(context.Background(), client, Params{OrgID: "org-1", ScopeLevel: "org"})
	if err != nil {
		t.Fatalf("BuildFlowResponse: %v", err)
	}
	if got.Mode != "investment" || len(got.Nodes) != 0 || len(got.Links) != 0 {
		t.Fatalf("expected the empty-response shape, got %+v", got)
	}
}

func TestRepoTeamSchemaDriftDegradesToEmpty(t *testing.T) {
	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		for _, b := range bindings {
			if b.Name == "tables" {
				return &fixtureRowScanner{rows: [][]any{{"work_unit_investments"}}}, nil
			}
			if b.Name == "columns" {
				return &fixtureRowScanner{}, nil // no columns present
			}
		}
		t.Fatalf("unexpected query reached past the missing-column guard:\n%s", query)
		return nil, nil
	}}
	got, err := BuildRepoTeamFlowResponse(context.Background(), client, RepoTeamParams{OrgID: "org-1", ScopeLevel: "org"})
	if err != nil {
		t.Fatalf("BuildRepoTeamFlowResponse: %v", err)
	}
	if got.Mode != "investment" || len(got.Nodes) != 0 || len(got.Links) != 0 {
		t.Fatalf("expected the empty-response shape, got %+v", got)
	}
}

// TestThemeOverridesWorkCategory ports `if theme: theme_filters = [theme]`
// (investment_flow.py:227-228 and :516-517, both builders): an explicit
// theme parameter replaces whatever filters.why.work_category derived,
// rather than merging with it.
func TestThemeOverridesWorkCategory(t *testing.T) {
	var capturedThemes []string
	client := dispatchClient(t, func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		for _, b := range bindings {
			if b.Name == "themes" {
				capturedThemes, _ = b.Value.([]string)
			}
		}
		return &fixtureRowScanner{}, nil
	})
	_, err := BuildFlowResponse(context.Background(), client, Params{
		OrgID: "org-1", ScopeLevel: "org", FlowMode: strPtr("team_category_repo"), TopNRepos: 12,
		WorkCategory: []string{"maintenance"}, Theme: strPtr("risk"),
	})
	if err != nil {
		t.Fatalf("BuildFlowResponse: %v", err)
	}
	if len(capturedThemes) != 1 || capturedThemes[0] != "risk" {
		t.Fatalf("expected theme override to replace work_category-derived themes, got %v", capturedThemes)
	}
}
