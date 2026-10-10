//go:build integration

package analytics

import (
	"context"
	"reflect"
	"sort"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/retractionseed"
)

// TestInvestmentMetricsDailyReadersGiveRetractionRowsNoWeight reads the
// default analytics source (investment_metrics_daily) as a breakdown by team
// and as the sankey coverage, for two organizations that hold the same
// measurements. One of them also holds the old rows of the retired team ids
// and the retraction row over each (package retractionseed). The answers must
// be the same: a retired team id is not a bucket with a value of 0, and a
// retraction row is not a row of the coverage count.
func TestInvestmentMetricsDailyReadersGiveRetractionRowsNoWeight(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	store := retractionseed.Start(ctx, t)
	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: store.URI})
	if err != nil {
		t.Fatalf("construct query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	first, last := store.Days[1], store.Days[len(store.Days)-1]
	// One measured row with no team for each day, in both organizations, so
	// the team coverage is a real share (4 of 5 rows) and not 1.
	for _, org := range []string{retractionseed.ControlOrg, retractionseed.RetractedOrg} {
		for _, day := range store.Recomputed() {
			if err := store.Conn.Exec(ctx, `INSERT INTO investment_metrics_daily
(org_id, day, repo_id, team_id, investment_area, project_stream, delivery_units, work_items_completed,
 prs_merged, churn_loc, cycle_p50_hours, computed_at)
VALUES (?, ?, ?, NULL, 'feature_delivery', 'roadmap', 1, 1, 0, 10, 4, ?)`,
				org, day, retractionseed.Teams[0].RepoID, store.NewComputedAt); err != nil {
				t.Fatalf("insert the row with no team: %v", err)
			}
		}
	}

	type answers struct {
		Teams    map[string]float64
		Coverage model.SankeyCoverage
	}
	read := func(org string) answers {
		t.Helper()
		request := BreakdownRequest{
			Dimension: DimensionTeam, Measure: MeasureCount, TopN: 50,
			StartDate: mustGraphQLDate(first.Format("2006-01-02")), EndDate: mustGraphQLDate(last.Format("2006-01-02")),
		}
		query, err := CompileBreakdown(request, org, queryTimeoutSecs, false, nil)
		if err != nil {
			t.Fatalf("%s CompileBreakdown: %v", org, err)
		}
		result, err := ExecuteBreakdown(ctx, client, org, query, "team", "count")
		if err != nil {
			t.Fatalf("%s ExecuteBreakdown: %v", org, err)
		}
		out := answers{Teams: map[string]float64{}}
		for _, item := range result.Items {
			if item.Value == nil {
				t.Fatalf("%s team bucket %q has no value", org, item.Key)
			}
			out.Teams[item.Key] = *item.Value
		}
		sankey, err := SankeyRequestFromInput(model.SankeyRequestInput{
			Path:    []model.DimensionInput{model.DimensionInputTeam, model.DimensionInputTheme},
			Measure: model.MeasureInputCount,
			DateRange: &model.DateRangeInput{
				StartDate: mustGraphQLDate(first.Format("2006-01-02")), EndDate: mustGraphQLDate(last.Format("2006-01-02")),
			},
			MaxNodes: 16, MaxEdges: 100,
		})
		if err != nil {
			t.Fatalf("%s SankeyRequestFromInput: %v", org, err)
		}
		coverage := resolveSankeyCoverage(ctx, client, org, sankey, 60, false, nil)
		if coverage == nil {
			t.Fatalf("%s sankey coverage is absent", org)
		}
		out.Coverage = *coverage
		return out
	}

	control := read(retractionseed.ControlOrg)

	// The control answers, from the seed. The teams complete 2, 3, 4 and 5
	// items on each of the six days; the row with no team completes 1. Of
	// the five rows of a day, four have a team: team coverage 0.8.
	var buckets []string
	for key := range control.Teams {
		buckets = append(buckets, key)
	}
	sort.Strings(buckets)
	if len(buckets) != 5 {
		t.Fatalf("control team buckets = %v, want the four keyed teams and the bucket of no team", buckets)
	}
	for team, want := range map[string]float64{"jira:ENG": 12, "github:platform": 18, "gitlab:ops": 24, "linear:core": 30} {
		if control.Teams[team] != want {
			t.Fatalf("control count of %s = %v, want %v (buckets %v)", team, control.Teams[team], want, control.Teams)
		}
	}
	if control.Coverage.TeamCoverage != 0.8 {
		t.Fatalf("control team coverage = %v, want 0.8", control.Coverage.TeamCoverage)
	}

	retracted := read(retractionseed.RetractedOrg)
	if !reflect.DeepEqual(control, retracted) {
		t.Fatalf("the retraction rows changed an answer:\n control   %+v\n retracted %+v", control, retracted)
	}
}
