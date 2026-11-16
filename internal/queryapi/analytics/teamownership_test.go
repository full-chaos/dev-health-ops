package analytics

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

const ownedCountMatch = "owned_repositories"

func teamScopeFilter(ids ...string) *model.FilterInput {
	return &model.FilterInput{Scope: &model.ScopeFilterInput{Level: model.ScopeLevelInputTeam, Ids: ids}}
}

func teamBatch(filters *model.FilterInput, measures ...model.MeasureInput) model.AnalyticsRequestInput {
	var batch model.AnalyticsRequestInput
	for _, m := range measures {
		batch.Timeseries = append(batch.Timeseries, tsInput(model.DimensionInputTeam, m))
	}
	batch.Filters = filters
	return batch
}

func ownedCount(n uint64) *fakeRowScanner { return &fakeRowScanner{rows: [][]any{{n}}} }

func reasonText(r *model.AnalyticsResult) string {
	if r.FilterEmptyReason == nil {
		return "null"
	}
	return *r.FilterEmptyReason
}

func countCalls(c *routingFakeClient, match string) int {
	n := 0
	for _, call := range c.calls {
		if call == match {
			n++
		}
	}
	return n
}

var reasonSelected = Selection{FilterEmptyReason: true}

// A selected team that owns no repository: the reason, and no series at all (never a zero-filled bucket).
func TestResolveSelected_TeamThatOwnsNoRepositoryGetsTheReasonAndNoSeries(t *testing.T) {
	client := (&routingFakeClient{}).
		on(ownedCountMatch, ownedCount(0)).
		on("AS dimension_value", &fakeRowScanner{})
	got, err := ResolveSelected(context.Background(), client, "org-1", teamBatch(teamScopeFilter("t0"), model.MeasureInputTestPassRate), reasonSelected)
	if err != nil {
		t.Fatal(err)
	}
	if reasonText(got) != "team_owns_no_repository" {
		t.Fatalf("reason = %s, want team_owns_no_repository", reasonText(got))
	}
	if len(got.Timeseries) != 0 {
		t.Fatalf("series = %+v, want none", got.Timeseries)
	}
}

// A team with one or more owned repositories and no rows in the window: the empty-window state, no reason.
func TestResolveSelected_TeamWithOwnedRepositoriesAndNoRowsHasNoReason(t *testing.T) {
	client := (&routingFakeClient{}).
		on(ownedCountMatch, ownedCount(2)).
		on("AS dimension_value", &fakeRowScanner{})
	got, err := ResolveSelected(context.Background(), client, "org-1", teamBatch(teamScopeFilter("t1"), model.MeasureInputTestPassRate), reasonSelected)
	if err != nil {
		t.Fatal(err)
	}
	if got.FilterEmptyReason != nil || len(got.Timeseries) != 0 {
		t.Fatalf("reason = %s, series = %+v; want null and no series", reasonText(got), got.Timeseries)
	}
}

func TestResolveSelected_TeamWithOwnedRepositoriesAndRowsServesTheSeries(t *testing.T) {
	client := (&routingFakeClient{}).
		on(ownedCountMatch, ownedCount(1)).
		on("AS dimension_value", &fakeRowScanner{rows: [][]any{{mustTime("2026-01-01"), "t1", 90.0}}})
	got, err := ResolveSelected(context.Background(), client, "org-1", teamBatch(teamScopeFilter("t1"), model.MeasureInputTestPassRate), reasonSelected)
	if err != nil {
		t.Fatal(err)
	}
	if got.FilterEmptyReason != nil || len(got.Timeseries) != 1 || got.Timeseries[0].DimensionValue != "t1" {
		t.Fatalf("reason = %s, series = %+v", reasonText(got), got.Timeseries)
	}
}

// The ORG series still holds the org's repositories under a team scope that owns nothing: the reason is about the
// team, and the org series is read as before.
func TestResolveSelected_OrgSeriesIsUnchangedByTheReason(t *testing.T) {
	client := (&routingFakeClient{}).
		on(ownedCountMatch, ownedCount(0)).
		on("AS numerator", &fakeRowScanner{rows: [][]any{{mustTime("2026-01-01"), 2.0, 10.0}}}).
		on("AS dimension_value", &fakeRowScanner{})
	got, err := ResolveSelected(context.Background(), client, "org-1", teamBatch(teamScopeFilter("t0"), model.MeasureInputTestFlakeRate),
		Selection{FilterEmptyReason: true, TimeseriesScope: true})
	if err != nil {
		t.Fatal(err)
	}
	if reasonText(got) != "team_owns_no_repository" {
		t.Fatalf("reason = %s", reasonText(got))
	}
	if len(got.Timeseries) != 1 || got.Timeseries[0].Scope != model.TimeseriesScopeOrg || got.Timeseries[0].Buckets[0].Value == nil || *got.Timeseries[0].Buckets[0].Value != 20 {
		t.Fatalf("series = %+v, want the one ORG series with value 20 (2/10)", got.Timeseries)
	}
}

// No team scope: no reason and no ownership statement.
func TestResolveSelected_NoTeamScopeHasNoReasonAndNoStatement(t *testing.T) {
	for name, filters := range map[string]*model.FilterInput{
		"no filters":         nil,
		"empty team ids":     teamScopeFilter(),
		"blank team id":      teamScopeFilter(""),
		"a repository scope": {Scope: &model.ScopeFilterInput{Level: model.ScopeLevelInputRepo, Ids: []string{"00000000-0000-0000-0000-000000000001"}}},
	} {
		client := (&routingFakeClient{}).
			on(ownedCountMatch, ownedCount(0)).
			on("AS dimension_value", &fakeRowScanner{})
		got, err := ResolveSelected(context.Background(), client, "org-1", teamBatch(filters, model.MeasureInputTestPassRate), reasonSelected)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if got.FilterEmptyReason != nil || countCalls(client, ownedCountMatch) != 0 {
			t.Errorf("%s: reason = %s, ownership statements = %d; want null, 0", name, reasonText(got), countCalls(client, ownedCountMatch))
		}
	}
}

// The field not selected: no extra statement at all (the recording client sees none).
func TestResolveSelected_FieldNotSelectedSendsNoOwnershipStatement(t *testing.T) {
	client := (&routingFakeClient{}).
		on(ownedCountMatch, ownedCount(0)).
		on("AS dimension_value", &fakeRowScanner{})
	got, err := ResolveSelected(context.Background(), client, "org-1", teamBatch(teamScopeFilter("t0"), model.MeasureInputTestPassRate), Selection{})
	if err != nil {
		t.Fatal(err)
	}
	if got.FilterEmptyReason != nil || countCalls(client, ownedCountMatch) != 0 {
		t.Fatalf("reason = %s, ownership statements = %d; want null, 0", reasonText(got), countCalls(client, ownedCountMatch))
	}
	if len(client.calls) != 1 {
		t.Fatalf("statements sent = %v, want only the series read", client.calls)
	}
}

// The statement is sent exactly once per request, however many sub-requests it holds.
func TestResolveSelected_OneOwnershipStatementPerRequest(t *testing.T) {
	client := (&routingFakeClient{}).
		on(ownedCountMatch, ownedCount(0)).
		on("AS dimension_value", &fakeRowScanner{}).
		on("AS dimension_value", &fakeRowScanner{})
	batch := teamBatch(teamScopeFilter("t0"), model.MeasureInputTestPassRate, model.MeasureInputTestFailureRate, model.MeasureInputPipelineSuccessRate)
	if _, err := ResolveSelected(context.Background(), client, "org-1", batch, reasonSelected); err != nil {
		t.Fatal(err)
	}
	if n := countCalls(client, ownedCountMatch); n != 1 {
		t.Fatalf("ownership statements = %d, want 1", n)
	}
}

// A batch that holds a read of the stored team column cannot say "owns no repository" about that part: no reason.
func TestResolveSelected_MixedBatchWithAStoredTeamColumnReadHasNoReason(t *testing.T) {
	client := (&routingFakeClient{}).
		on(ownedCountMatch, ownedCount(0)).
		on("AS dimension_value", &fakeRowScanner{})
	batch := teamBatch(teamScopeFilter("t0"), model.MeasureInputTestPassRate, model.MeasureInputCycleTimeHours)
	got, err := ResolveSelected(context.Background(), client, "org-1", batch, reasonSelected)
	if err != nil {
		t.Fatal(err)
	}
	if got.FilterEmptyReason != nil || countCalls(client, ownedCountMatch) != 0 {
		t.Fatalf("reason = %s, ownership statements = %d; want null, 0", reasonText(got), countCalls(client, ownedCountMatch))
	}
}

func TestResolveSelected_OwnershipReadFailureIsAnErrorNotAReason(t *testing.T) {
	boom := errors.New("boom")
	client := (&routingFakeClient{}).
		onErr(ownedCountMatch, boom).
		on("AS dimension_value", &fakeRowScanner{})
	got, err := ResolveSelected(context.Background(), client, "org-1", teamBatch(teamScopeFilter("t0"), model.MeasureInputTestPassRate), reasonSelected)
	if err == nil || !errors.Is(err, boom) || got != nil {
		t.Fatalf("got %v, %v; want the read error and no answer", got, err)
	}
}

func TestBatchTeamScopeReadsOwnership(t *testing.T) {
	ts := func(m model.MeasureInput, d model.DimensionInput) model.TimeseriesRequestInput { return tsInput(d, m) }
	bd := func(m model.MeasureInput, d model.DimensionInput) model.BreakdownRequestInput { return bdInput(d, m) }
	cases := []struct {
		name          string
		batch         model.AnalyticsRequestInput
		useInvestment bool
		want          bool
	}{
		{"TestOps timeseries", model.AnalyticsRequestInput{Timeseries: []model.TimeseriesRequestInput{ts(model.MeasureInputTestPassRate, model.DimensionInputTeam)}}, false, true},
		{"TestOps timeseries and breakdown", model.AnalyticsRequestInput{
			Timeseries: []model.TimeseriesRequestInput{ts(model.MeasureInputTestPassRate, model.DimensionInputTeam)},
			Breakdowns: []model.BreakdownRequestInput{bd(model.MeasureInputPipelineFailureRate, model.DimensionInputTeam)},
		}, false, true},
		{"stored column timeseries", model.AnalyticsRequestInput{Timeseries: []model.TimeseriesRequestInput{ts(model.MeasureInputCycleTimeHours, model.DimensionInputTeam)}}, false, false},
		{"stored column breakdown beside TestOps", model.AnalyticsRequestInput{
			Timeseries: []model.TimeseriesRequestInput{ts(model.MeasureInputTestPassRate, model.DimensionInputTeam)},
			Breakdowns: []model.BreakdownRequestInput{bd(model.MeasureInputCycleTimeHours, model.DimensionInputTeam)},
		}, false, false},
		{"investment path, stored-column measure", model.AnalyticsRequestInput{Timeseries: []model.TimeseriesRequestInput{ts(model.MeasureInputCount, model.DimensionInputTeam)}}, true, true},
		{"canonical-only dimension off the investment flag", model.AnalyticsRequestInput{Timeseries: []model.TimeseriesRequestInput{ts(model.MeasureInputCount, model.DimensionInputTheme)}}, false, true},
		{"sankey present", model.AnalyticsRequestInput{
			Timeseries: []model.TimeseriesRequestInput{ts(model.MeasureInputTestPassRate, model.DimensionInputTeam)},
			Sankey:     &model.SankeyRequestInput{},
		}, false, false},
		{"flow matrix present", model.AnalyticsRequestInput{
			Timeseries: []model.TimeseriesRequestInput{ts(model.MeasureInputTestPassRate, model.DimensionInputTeam)},
			FlowMatrix: &model.FlowMatrixRequestInput{},
		}, false, false},
		{"nothing to read", model.AnalyticsRequestInput{}, false, false},
	}
	for _, c := range cases {
		got, err := batchTeamScopeReadsOwnership(c.batch, c.useInvestment)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if got != c.want {
			t.Errorf("%s = %v, want %v", c.name, got, c.want)
		}
	}
}

// The statement of the reason is the ownership statement of the team scope, never the stored team column.
func TestOwnershipStatementReadsTeamRepoOwnership(t *testing.T) {
	var seen string
	client := &statementCapture{onQuery: func(s string) { seen = s }}
	if _, err := resolveFilterEmptyReason(context.Background(), client, "org-1", teamBatch(teamScopeFilter("t0"), model.MeasureInputTestPassRate), false, true); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(seen, "team_repo_ownership") || strings.Contains(seen, "team_id = ") && !strings.Contains(seen, "o.team_id") {
		t.Fatalf("statement = %s", seen)
	}
}

type statementCapture struct{ onQuery func(string) }

func (c *statementCapture) Query(_ context.Context, statement string, _ []clickhouse.Binding) (clickhouse.RowScanner, error) {
	c.onQuery(statement)
	return &fakeRowScanner{rows: [][]any{{uint64(0)}}}, nil
}
