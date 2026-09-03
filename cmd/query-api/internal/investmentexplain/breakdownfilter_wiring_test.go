package investmentexplain

import (
	"context"
	"strings"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

// capturingBreakdownClient wraps explainFixtureClient's row data (embedded,
// unchanged) but records the bindings of the ONE breakdown-fetch query so a
// test can assert which filters actually reached FetchInvestmentBreakdown --
// the exact wiring codex round 1 (P1) found broken: RepoIDs applied
// unconditionally regardless of scope level (build_investment_response only
// scopes the breakdown for team/repo, investment.py:175) and
// why.work_category never threaded into BreakdownFilters at all
// (investment.py:187-188 does pass it).
type capturingBreakdownClient struct {
	explainFixtureClient
	lastBreakdownBindings []dhclickhouse.Binding
}

func (c *capturingBreakdownClient) Query(ctx context.Context, statement string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	if strings.Contains(statement, "ARRAY JOIN CAST(subcategory_distribution_json") && strings.Contains(statement, "subcategory_kv.1 AS subcategory") {
		c.lastBreakdownBindings = bindings
	}
	return c.explainFixtureClient.Query(ctx, statement, bindings)
}

func hasBinding(bindings []dhclickhouse.Binding, name string) bool {
	for _, b := range bindings {
		if b.Name == name {
			return true
		}
	}
	return false
}

func runExplainForWiring(t *testing.T, client *capturingBreakdownClient, opts ExplainInvestmentMixOptions) {
	t.Helper()
	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	opts.LLMProvider = "mock"
	opts.ForceRefresh = true
	opts.Now = explainFixtureNow
	if opts.StartTS.IsZero() {
		opts.StartTS = time.Date(2025, 12, 1, 0, 0, 0, 0, time.UTC)
	}
	if opts.EndTS.IsZero() {
		opts.EndTS = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	}
	if opts.OrgID == "" {
		opts.OrgID = "org-golden-4977"
	}
	if _, err := reader.ExplainInvestmentMix(context.Background(), nil, CompleteInvestmentMixExplanation, opts); err != nil {
		t.Fatalf("ExplainInvestmentMix: %v", err)
	}
}

// TestBreakdownFilterScopeAppliedOnlyForTeamOrRepoLevel regresses the
// unconditional-RepoIDs bug: an "org"-scoped request (with RepoIDs already
// resolved, e.g. from filters.what.repos) must NOT scope the breakdown
// query by repo -- only "team"/"repo" scope does (investment.py:175).
func TestBreakdownFilterScopeAppliedOnlyForTeamOrRepoLevel(t *testing.T) {
	t.Run("org scope: no scope_ids binding even with RepoIDs set", func(t *testing.T) {
		client := &capturingBreakdownClient{}
		runExplainForWiring(t, client, ExplainInvestmentMixOptions{
			ScopeLevel: "org",
			RepoIDs:    []string{"repo-1"},
		})
		if hasBinding(client.lastBreakdownBindings, "scope_ids") {
			t.Fatalf("breakdown query bound scope_ids for org scope; bindings=%+v", client.lastBreakdownBindings)
		}
	})

	t.Run("repo scope: scope_ids binding present", func(t *testing.T) {
		client := &capturingBreakdownClient{}
		runExplainForWiring(t, client, ExplainInvestmentMixOptions{
			ScopeLevel: "repo",
			RepoIDs:    []string{"repo-1"},
		})
		if !hasBinding(client.lastBreakdownBindings, "scope_ids") {
			t.Fatalf("breakdown query did not bind scope_ids for repo scope; bindings=%+v", client.lastBreakdownBindings)
		}
	})

	t.Run("team scope: scope_ids binding present", func(t *testing.T) {
		client := &capturingBreakdownClient{}
		runExplainForWiring(t, client, ExplainInvestmentMixOptions{
			ScopeLevel: "team",
			RepoIDs:    []string{"repo-1"},
		})
		if !hasBinding(client.lastBreakdownBindings, "scope_ids") {
			t.Fatalf("breakdown query did not bind scope_ids for team scope; bindings=%+v", client.lastBreakdownBindings)
		}
	})
}

// TestBreakdownFilterAppliesWorkCategory regresses the missing
// why.work_category wiring: WorkCategory must reach BreakdownFilters'
// Themes/Subcategories (investment.py:187-188's themes=/subcategories=),
// not just the work-unit-level filtering.
func TestBreakdownFilterAppliesWorkCategory(t *testing.T) {
	client := &capturingBreakdownClient{}
	runExplainForWiring(t, client, ExplainInvestmentMixOptions{
		ScopeLevel:   "org",
		WorkCategory: []string{"velocity"},
	})
	if !hasBinding(client.lastBreakdownBindings, "themes") {
		t.Fatalf("breakdown query did not bind themes for why.work_category=[velocity]; bindings=%+v", client.lastBreakdownBindings)
	}
}
