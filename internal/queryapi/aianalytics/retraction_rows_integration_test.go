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
	TeamFlow     []flowRow
	NamedRetired []flowRow
	Coverage     []model.AIGovernanceCoverageRow
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
	// The same read for a team the caller names: a retired id. The active-team
	// rule does not apply to a named team, so only the live-row rule keeps its
	// retraction rows from counting as days of data.
	answers.NamedRetired, err = loadTeamFlowRows(ctx, client, []clickhouse.Binding{
		{Name: "org_id", Value: org}, {Name: "window_days", Value: uint32(30)},
	}, retractionseed.Teams[0].RetiredID)
	if err != nil {
		t.Fatalf("%s team flow of the retired id: %v", org, err)
	}
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
	// The retired id of the first team holds one measured day (the day that was
	// not computed again): fewer than the 5 days a flow entity needs.
	if len(control.NamedRetired) != 0 {
		t.Fatalf("control team flow of the named retired id = %+v, want no row (one day of data)", control.NamedRetired)
	}
	if len(control.Coverage) != 6*4 {
		t.Fatalf("control coverage rows = %d, want 24 (four teams, six days)", len(control.Coverage))
	}

	retracted := readAIAnswers(ctx, t, client, retractionseed.RetractedOrg, start, end)
	if !reflect.DeepEqual(control, retracted) {
		t.Fatalf("the retraction rows changed an answer:\n control   %+v\n retracted %+v", control, retracted)
	}
}

// TestTeamFlowEntitiesAreTheActiveTeams reads the team flow rows of an
// organization whose days hold measured rows under a keyed id and under the
// retired id it replaced (the state of a day computed again before the
// writers stored retraction rows). No row there is a retraction row, and the
// retired ids have six days of data. With no team named, the entities are the
// active teams: a retired id is not a team. A read that names the retired id
// still reads it.
func TestTeamFlowEntitiesAreTheActiveTeams(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	store := retractionseed.Start(ctx, t)
	retractionseed.Double(ctx, t, store.Conn, store.Seed)
	client, err := clickhouse.NewClickHouseQueryClientWithOptions(clickhouse.Options{DSN: store.URI})
	if err != nil {
		t.Fatalf("construct query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	base := []clickhouse.Binding{{Name: "org_id", Value: retractionseed.DoubledOrg}, {Name: "window_days", Value: uint32(30)}}
	rows, err := loadTeamFlowRows(ctx, client, base, "")
	if err != nil {
		t.Fatalf("team flow: %v", err)
	}
	var entities []string
	for _, row := range rows {
		entities = append(entities, row.EntityID)
	}
	sort.Strings(entities)
	if want := []string{"github:platform", "gitlab:ops", "jira:ENG", "linear:core"}; !reflect.DeepEqual(entities, want) {
		t.Fatalf("team flow entities = %v, want the active teams %v", entities, want)
	}

	retired := retractionseed.Teams[0].RetiredID
	named, err := loadTeamFlowRows(ctx, client, base, retired)
	if err != nil {
		t.Fatalf("team flow of %s: %v", retired, err)
	}
	if len(named) != 1 || named[0].EntityID != retired {
		t.Fatalf("team flow of the named retired id = %+v, want its one row", named)
	}
}

// TestAIImpactAnswersGiveRetractionRowsNoWeight reads the AI impact summary
// and the AI opportunities of two organizations that hold the same
// measurements. One of them also holds the old rows of the retired team ids
// and the retraction row over each (package retractionseed).
//
// ai_impact_metrics_daily has no column that tells a retraction row from a
// measured row with no pull request, so the rule of the other tables does not
// apply. What holds: no number moves (the reads sum, or weight by a count),
// and a row with no measure is not a row of the daily list when its team id
// is a retired one. A measured row with no pull request of an ACTIVE team
// stays in the list.
func TestAIImpactAnswersGiveRetractionRowsNoWeight(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	store := retractionseed.Start(ctx, t)
	client, err := clickhouse.NewClickHouseQueryClientWithOptions(clickhouse.Options{DSN: store.URI})
	if err != nil {
		t.Fatalf("construct query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	first, last := store.Days[1], store.Days[len(store.Days)-1]
	// A measured row with no pull request (the bucket 'unknown' of a group)
	// under an active team, in both organizations.
	for _, org := range []string{retractionseed.ControlOrg, retractionseed.RetractedOrg} {
		if err := store.Conn.Exec(ctx, `INSERT INTO ai_impact_metrics_daily
(org_id, team_id, repo_id, work_type, day, attribution_bucket, computed_at)
VALUES (?, ?, ?, 'feature', ?, 'unknown', ?)`,
			org, retractionseed.Teams[0].KeyedID, retractionseed.Teams[0].RepoID, last, store.NewComputedAt); err != nil {
			t.Fatalf("insert the measured row with no pull request: %v", err)
		}
	}
	startDate, err := graphqldate.Parse(first.Format("2006-01-02"))
	if err != nil {
		t.Fatal(err)
	}
	endDate, err := graphqldate.Parse(last.Format("2006-01-02"))
	if err != nil {
		t.Fatal(err)
	}
	read := func(org string) (*model.AIImpactSummary, *model.AIOpportunitiesResult) {
		t.Helper()
		summary, err := ImpactSummary(ctx, client, org, model.AIDateRangeInput{StartDate: startDate, EndDate: endDate}, nil)
		if err != nil {
			t.Fatalf("%s impact summary: %v", org, err)
		}
		opportunities, err := AiOpportunities(ctx, client, org, nil, 25)
		if err != nil {
			t.Fatalf("%s opportunities: %v", org, err)
		}
		summary.OrgID, opportunities.OrgID = "", ""
		return summary, opportunities
	}

	control, controlOpportunities := read(retractionseed.ControlOrg)

	// The control answer, from the seed: the teams have 4, 5, 6 and 7 pull
	// requests on each of six days; the daily list holds a row for each team
	// and day and the one measured row with no pull request.
	if control.TotalPrs != 6*(4+5+6+7) || len(control.Daily) != 6*4+1 || !control.DataAvailable {
		t.Fatalf("control summary: %d pull requests, %d daily rows, data %v; want 132, 25, true",
			control.TotalPrs, len(control.Daily), control.DataAvailable)
	}

	retracted, retractedOpportunities := read(retractionseed.RetractedOrg)
	if !reflect.DeepEqual(control, retracted) {
		t.Errorf("the retraction rows changed the impact summary:\n control   %+v\n retracted %+v", control, retracted)
	}
	if !reflect.DeepEqual(controlOpportunities, retractedOpportunities) {
		t.Errorf("the retraction rows changed the opportunities:\n control   %+v\n retracted %+v", controlOpportunities, retractedOpportunities)
	}
}
