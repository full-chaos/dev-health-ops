package analytics

import (
	"context"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

func flowSelectionInput(path ...model.DimensionInput) model.SankeyRequestInput {
	return model.SankeyRequestInput{
		Path:      path,
		Measure:   model.MeasureInputCount,
		DateRange: &model.DateRangeInput{StartDate: mustGraphQLDate("2026-01-01"), EndDate: mustGraphQLDate("2026-01-08")},
		MaxNodes:  16,
		MaxEdges:  100,
	}
}

func (c *recordingClient) countContaining(marker string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, s := range c.statements {
		if strings.Contains(s, marker) {
			n++
		}
	}
	return n
}

func (c *recordingClient) total() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.statements)
}

func TestResolveSankey_FlowSelection(t *testing.T) {
	// An investment path also runs the coverage's own telemetry probes (once per process per org), so the total
	// statement count is pinned on the plain path only.
	investmentSource := func(path []model.DimensionInput) bool { return len(path) != 2 || path[1] != model.DimensionInputRepo }
	paths := map[string][]model.DimensionInput{
		"investment path": {model.DimensionInputTeam, model.DimensionInputWorkType},
		"repo join path":  {model.DimensionInputTeam, model.DimensionInputTheme, model.DimensionInputRepo},
		"plain path":      {model.DimensionInputTeam, model.DimensionInputRepo},
	}
	for name, path := range paths {
		t.Run(name, func(t *testing.T) {
			input := flowSelectionInput(path...)
			run := func(coverage, flow bool) *recordingClient {
				c := &recordingClient{}
				if _, err := resolveSankey(context.Background(), c, "org-1", input, nil, nil, coverage, flow); err != nil {
					t.Fatalf("resolveSankey(coverage=%v, flow=%v): %v", coverage, flow, err)
				}
				return c
			}

			c := run(true, false)
			if got := c.countContaining("AS grouping_set,"); got != 0 {
				t.Errorf("coverage only: %d grouped statements, want 0", got)
			}
			if got := c.countContaining("AS assigned_team"); got != 1 {
				t.Errorf("coverage only: %d coverage statements, want 1", got)
			}
			if !investmentSource(path) {
				if got := c.total(); got != 1 {
					t.Errorf("coverage only: %d statements in all, want 1", got)
				}
			}

			c = run(true, true)
			if got := c.countContaining("AS grouping_set,"); got != 1 {
				t.Errorf("coverage+flow: %d grouped statements, want 1", got)
			}
			if got := c.countContaining("AS assigned_team"); got != 1 {
				t.Errorf("coverage+flow: %d coverage statements, want 1", got)
			}

			c = run(false, true)
			if got := c.countContaining("AS grouping_set,"); got != 1 {
				t.Errorf("flow only: %d grouped statements, want 1", got)
			}
			if got := c.countContaining("AS assigned_team"); got != 0 {
				t.Errorf("flow only: %d coverage statements, want 0", got)
			}

			if got := run(false, false).total(); got != 0 {
				t.Errorf("neither selected: %d statements, want 0", got)
			}
		})
	}
}

func TestResolveSankey_FlowNotSelectedStillRejectsABadRequest(t *testing.T) {
	input := flowSelectionInput(model.DimensionInputTeam)
	c := &recordingClient{}
	if _, err := resolveSankey(context.Background(), c, "org-1", input, nil, nil, true, false); err == nil {
		t.Fatal("a one-dimension path must still be rejected when only coverage is selected")
	}
}

func TestResolveSankey_FlowNotSelectedLeavesNodesAndEdgesEmptyAndKeepsUnit(t *testing.T) {
	c := &recordingClient{}
	res, err := resolveSankey(context.Background(), c, "org-1", flowSelectionInput(model.DimensionInputTeam, model.DimensionInputWorkType), nil, nil, true, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Nodes) != 0 || len(res.Edges) != 0 {
		t.Fatalf("nodes/edges = %d/%d, want 0/0", len(res.Nodes), len(res.Edges))
	}
	if res.Unit != model.SankeyValueUnitWorkUnits {
		t.Fatalf("unit = %v", res.Unit)
	}
}

func TestResolveSelected_EverythingSelectedStillReadsFlowAndCoverage(t *testing.T) {
	if !EverythingSelected().SankeyFlow {
		t.Fatal("EverythingSelected must select the sankey flow")
	}
	if (Selection{SankeyCoverage: true}).SankeyFlow {
		t.Fatal("a coverage-only selection must not select the flow")
	}
}

// The grouped-path probes (argMax transition guard, repo join dedup) belong to the flow. Each run uses a fresh org
// because both probes keep a per-org cooldown.
func TestResolveSankey_GroupedPathProbesFollowTheFlowSelection(t *testing.T) {
	useInvestment := true
	input := flowSelectionInput(model.DimensionInputTeam, model.DimensionInputTheme, model.DimensionInputRepo)
	input.UseInvestment = &useInvestment
	orgs := 0
	run := func(coverage, flow bool) *recordingClient {
		orgs++
		c := &recordingClient{}
		org := "org-probe-" + t.Name() + "-" + string(rune('a'+orgs))
		if _, err := resolveSankey(context.Background(), c, org, input, nil, nil, coverage, flow); err != nil {
			t.Fatalf("resolveSankey(coverage=%v, flow=%v): %v", coverage, flow, err)
		}
		return c
	}

	flowOnly := run(false, true)
	if got, want := flowOnly.total(), 3; got != want {
		t.Fatalf("flow only: %d statements, want %d (grouped sankey + argMax guard + repo join dedup)", got, want)
	}
	if flowOnly.countContaining("excess_repo_versions") != 1 {
		t.Fatalf("flow only: the repo join dedup probe was not sent")
	}

	req, err := SankeyRequestFromInput(input)
	if err != nil {
		t.Fatal(err)
	}
	orgs++
	alone := &recordingClient{}
	resolveSankeyCoverage(context.Background(), alone, "org-probe-direct-"+string(rune('a'+orgs)), req, queryTimeoutSecs, true, nil)

	coverageOnly := run(true, false)
	if coverageOnly.countContaining("AS grouping_set,") != 0 {
		t.Fatal("coverage only: the grouped sankey statement was sent")
	}
	if got, want := coverageOnly.total(), alone.total(); got != want {
		t.Fatalf("coverage only: %d statements, but the coverage alone sends %d; the flow's probes leaked", got, want)
	}

	if got := run(false, false).total(); got != 0 {
		t.Fatalf("nothing selected: %d statements, want 0", got)
	}
}
