//go:build integration

package aianalytics

import (
	"context"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graphqldate"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/retractionseed"
)

// aiAnswers is what the AI analytics loaders of the team-keyed daily tables
// give for one organization.
type aiAnswers struct {
	TeamFlow []flowRow
	Coverage []model.AIGovernanceCoverageRow
}

func readAIAnswers(ctx context.Context, t *testing.T, client QueryClient, org string, start, end time.Time) aiAnswers {
	t.Helper()
	var answers aiAnswers
	var err error
	answers.TeamFlow, err = loadTeamFlowRows(ctx, client, []clickhouse.Binding{
		{Name: "org_id", Value: org}, {Name: "window_days", Value: uint32(30)},
	}, "")
	if err != nil {
		t.Fatalf("%s team flow: %v", org, err)
	}
	sort.Slice(answers.TeamFlow, func(i, j int) bool { return answers.TeamFlow[i].EntityID < answers.TeamFlow[j].EntityID })
	startDate, err := graphqldate.Parse(start.Format("2006-01-02"))
	if err != nil {
		t.Fatal(err)
	}
	endDate, err := graphqldate.Parse(end.Format("2006-01-02"))
	if err != nil {
		t.Fatal(err)
	}
	answers.Coverage, err = loadCoverage(ctx, client, org, model.AIDateRangeInput{StartDate: startDate, EndDate: endDate}, scope{})
	if err != nil {
		t.Fatalf("%s coverage: %v", org, err)
	}
	return answers
}

// TestAIAnalyticsLoadersGiveRetractionRowsNoWeight reads the team flow rows
// and the governance coverage rows of two organizations that hold the same
// measurements. One of them also holds the old rows of the retired team ids
// and the retraction row over each (package retractionseed). The rows must be
// the same: a retired team id is not a flow entity with five days of data,
// and a retraction row is not a coverage row.
//
// The daily impact rows (loadDaily) are not read here. ai_impact_metrics_daily
// stores a measured row with 0 in every measure (the bucket 'unknown' of a
// group with no pull request), so a retraction row cannot be told from a
// measurement there and both are served as a row of zeros.
func TestAIAnalyticsLoadersGiveRetractionRowsNoWeight(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	store := retractionseed.Start(ctx, t)
	client, err := clickhouse.NewClickHouseQueryClientWithOptions(clickhouse.Options{DSN: store.URI})
	if err != nil {
		t.Fatalf("construct query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	// Inclusive day window: the days computed again.
	start, end := store.Days[1], store.Days[len(store.Days)-1]
	control := readAIAnswers(ctx, t, client, retractionseed.ControlOrg, start, end)

	// The control rows, from the seed. Team flow keeps a team with at least 5
	// days of data: the four keyed ids (6 days); a retired id has one day.
	var entities []string
	for _, row := range control.TeamFlow {
		entities = append(entities, row.EntityID)
		if row.WipCongestion == nil || row.CycleP50 == nil {
			t.Fatalf("control team flow row %s has no value: %+v", row.EntityID, row)
		}
	}
	if want := []string{"github:platform", "gitlab:ops", "jira:ENG", "linear:core"}; !reflect.DeepEqual(entities, want) {
		t.Fatalf("control team flow entities = %v, want %v", entities, want)
	}
	// jira:ENG is the first seeded team: wip_congestion_ratio 0.5 each day.
	if got := *control.TeamFlow[2].WipCongestion; got != 0.5 {
		t.Fatalf("control wip congestion of jira:ENG = %v, want 0.5", got)
	}
	if len(control.Coverage) != 6*4 {
		t.Fatalf("control coverage rows = %d, want 24 (four teams, six days)", len(control.Coverage))
	}

	retracted := readAIAnswers(ctx, t, client, retractionseed.RetractedOrg, start, end)
	if !reflect.DeepEqual(control, retracted) {
		t.Fatalf("the retraction rows changed an answer:\n control   %+v\n retracted %+v", control, retracted)
	}
}
