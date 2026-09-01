package analytics

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/full-chaos/dev-health-go/clickhouse"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
)

// recordingClient captures every statement issued, then fails it. Failing
// is deliberate: this test asserts on WHICH query was built, not on any
// result, and every caller in resolveSankey degrades gracefully on a query
// error -- so the statements are observable without scripting responses.
type recordingClient struct {
	mu         sync.Mutex
	statements []string
}

func (c *recordingClient) Query(_ context.Context, statement string, _ []clickhouse.Binding) (clickhouse.RowScanner, error) {
	c.mu.Lock()
	c.statements = append(c.statements, statement)
	c.mu.Unlock()
	return nil, errors.New("recordingClient: no result scripted")
}

func (c *recordingClient) coverageStatement(t *testing.T) string {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, s := range c.statements {
		if strings.Contains(s, "AS assigned_team") {
			return s
		}
	}
	t.Fatalf("no coverage query was issued; got %d statement(s)", len(c.statements))
	return ""
}

// TestResolveSankey_CoverageUsesRawThreeStateFlagNotAutoRoute pins the
// asymmetry this port has to reproduce, and is the regression test for a
// real divergence found in review.
//
// compile_sankey's nodes/edges go through _get_context_params, which
// AUTO-ROUTES a nil use_investment to the investment path for any of
// {THEME, SUBCATEGORY, WORK_TYPE}. The coverage computation does NOT: it
// reads `request.use_investment` directly (analytics.py:665-677 --
// `bool(request.use_investment)` for the columns, `if
// request.use_investment` for the table), and `bool(None)` is False.
//
// So for `TEAM -> THEME` with BOTH sankey.useInvestment and
// batch.useInvestment omitted, Python computes nodes/edges from
// latest_work_unit_investments while computing coverage from
// investment_metrics_daily. Passing the auto-routed boolean into coverage
// instead makes Go read latest_work_unit_investments, and a daily row with
// no overlapping current work-unit row then yields a real coverage value
// on Python and 0/0 on Go.
//
// This test fails on that wiring: it asserts the coverage query targets
// the DAILY table for the omitted-flag case.
func TestResolveSankey_CoverageUsesRawThreeStateFlagNotAutoRoute(t *testing.T) {
	autoRoutingPath := []model.DimensionInput{model.DimensionInputTeam, model.DimensionInputTheme}

	// Guard the premise: if THEME ever stops auto-routing, this test would
	// pass for the wrong reason, so assert the auto-route is actually live.
	if !pathAutoRoutesToInvestment([]Dimension{DimensionTeam, DimensionTheme}) {
		t.Fatal("premise broken: TEAM->THEME no longer auto-routes to the investment path, so this test no longer exercises the asymmetry")
	}

	input := model.SankeyRequestInput{
		Path:    autoRoutingPath,
		Measure: model.MeasureInputCount,
		DateRange: &model.DateRangeInput{
			StartDate: mustGraphQLDate("2026-01-01"),
			EndDate:   mustGraphQLDate("2026-01-08"),
		},
		MaxNodes: 16,
		MaxEdges: 100,
		// UseInvestment deliberately omitted (nil).
	}

	t.Run("both flags omitted -> coverage reads the DAILY table", func(t *testing.T) {
		client := &recordingClient{}
		if _, err := resolveSankey(context.Background(), client, "org-1", input, nil, nil); err != nil {
			t.Fatalf("resolveSankey: %v", err)
		}
		got := client.coverageStatement(t)
		if !strings.Contains(got, "investment_metrics_daily") {
			t.Errorf("coverage query does not read investment_metrics_daily for an omitted use_investment flag;\nthis is the auto-route leaking into coverage. statement:\n%s", got)
		}
		if strings.Contains(got, "latest_work_unit_investments") || strings.Contains(got, "work_unit_membership_runs") {
			t.Errorf("coverage query took the INVESTMENT path for an omitted use_investment flag; Python's bool(None) is False. statement:\n%s", got)
		}
	})

	t.Run("explicit true -> coverage reads the investment source", func(t *testing.T) {
		client := &recordingClient{}
		explicit := true
		withFlag := input
		withFlag.UseInvestment = &explicit
		if _, err := resolveSankey(context.Background(), client, "org-1", withFlag, nil, nil); err != nil {
			t.Fatalf("resolveSankey: %v", err)
		}
		got := client.coverageStatement(t)
		if strings.Contains(got, "FROM investment_metrics_daily") {
			t.Errorf("coverage query read the daily table despite useInvestment=true. statement:\n%s", got)
		}
		if !strings.Contains(got, "work_unit_investments") {
			t.Errorf("coverage query did not read the investment source for useInvestment=true. statement:\n%s", got)
		}
	})

	t.Run("batch-level flag is honoured when the sankey flag is omitted", func(t *testing.T) {
		client := &recordingClient{}
		batchTrue := true
		if _, err := resolveSankey(context.Background(), client, "org-1", input, &batchTrue, nil); err != nil {
			t.Fatalf("resolveSankey: %v", err)
		}
		got := client.coverageStatement(t)
		if !strings.Contains(got, "work_unit_investments") {
			t.Errorf("batch.useInvestment=true did not reach coverage (Python: sk_req.use_investment ?? batch.use_investment). statement:\n%s", got)
		}
	})

	t.Run("explicit false -> coverage reads the DAILY table", func(t *testing.T) {
		client := &recordingClient{}
		explicitFalse := false
		withFlag := input
		withFlag.UseInvestment = &explicitFalse
		if _, err := resolveSankey(context.Background(), client, "org-1", withFlag, nil, nil); err != nil {
			t.Fatalf("resolveSankey: %v", err)
		}
		got := client.coverageStatement(t)
		if !strings.Contains(got, "investment_metrics_daily") {
			t.Errorf("explicit useInvestment=false did not read the daily table. statement:\n%s", got)
		}
	})
}
