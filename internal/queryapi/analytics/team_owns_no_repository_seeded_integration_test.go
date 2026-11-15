//go:build integration

// CHAOS-13868: a team scope that owns no repository answers filterEmptyReason = team_owns_no_repository, on a real
// ClickHouse with the migrated schema. Cells: no ownership row for the team (stored rows only), ownership that ended or
// names a repository outside the catalog or an inactive team, a team with owned repositories and no rows in the window,
// a team with rows, two teams, no team scope, and the field not selected (no ownership statement is sent).
package analytics

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graphqldate"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/testopsjobfailures"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/testopswindow"
)

type ownershipRecorder struct {
	inner QueryClient
	owned atomic.Int32
}

func (r *ownershipRecorder) Query(ctx context.Context, statement string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	if strings.Contains(statement, "owned_repositories") {
		r.owned.Add(1)
	}
	return r.inner.Query(ctx, statement, bindings)
}

// The six measures the ticket names, and four more: ten is the sub-request limit of a batch.
var reasonMeasures = []model.MeasureInput{
	model.MeasureInputTestPassRate, model.MeasureInputTestFailureRate, model.MeasureInputTestFlakeRate,
	model.MeasureInputTestSuiteDurationP95, model.MeasureInputPipelineDurationP95, model.MeasureInputCoverageBranchPct,
	model.MeasureInputPipelineSuccessRate, model.MeasureInputPipelineFailureRate, model.MeasureInputPipelineQueueTime,
	model.MeasureInputCoverageLinePct,
}

func allMeasuresBatch(filters *model.FilterInput) model.AnalyticsRequestInput {
	return teamBatch(filters, reasonMeasures...)
}

func TestTeamOwnsNoRepositoryOnARealClickHouse(t *testing.T) {
	e := startOwnersEnv(t)
	const org = "org-13868"
	const live, orphanRepo, endedRepo, inactiveRepo = "13868000-0000-4000-8000-0000000000a1", "13868000-0000-4000-8000-0000000000a2", "13868000-0000-4000-8000-0000000000a3", "13868000-0000-4000-8000-0000000000a4"
	for id, name := range map[string]string{live: "acme/live", orphanRepo: "acme/orphan", endedRepo: "acme/ended", inactiveRepo: "acme/inactive"} {
		e.repo(org, id, name)
	}
	e.team(org, "team-owner", 1)
	e.team(org, "team-gone", 0)
	past := openFrom().AddDate(0, 6, 0)
	e.owner(org, "team-owner", live, "acme/live", 1, openFrom(), nil)
	e.owner(org, "team-ended", endedRepo, "acme/ended", 1, openFrom(), &past)
	e.owner(org, "team-gone", inactiveRepo, "acme/inactive", 1, openFrom(), nil)
	e.owner(org, "team-orphan", "00000000-0000-4000-8000-0000000000fe", "acme/not-in-catalog", 1, openFrom(), nil)
	// Stored rows only for team-x (no ownership row names it), and rows for the repository team-owner owns, in Jan 2-4.
	e.daily(org, repoDay{repo: live, store: "team-owner", day: day1, runs: 100, ok: 90, passed: 90, bad: 10, flakeCases: 5, distinct: 50, linesTotal: 1000, linesCov: 900})
	e.daily(org, repoDay{repo: orphanRepo, store: "team-x", day: day1, runs: 100, ok: 10, passed: 10, bad: 90, flakeCases: 5, distinct: 50, linesTotal: 1000, linesCov: 100})

	run := func(name string, filters *model.FilterInput, selected bool) (*model.AnalyticsResult, *ownershipRecorder) {
		t.Helper()
		rec := &ownershipRecorder{inner: e.client}
		got, err := ResolveSelected(e.ctx, rec, org, allMeasuresBatch(filters), Selection{FilterEmptyReason: selected, TimeseriesScope: true})
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		return got, rec
	}

	t.Run("no ownership row: the reason, and no value in any series", func(t *testing.T) {
		for _, team := range []string{"team-x", "team-ended", "team-gone", "team-orphan", "team-nobody"} {
			got, _ := run(team, teamScopeFilter(team), true)
			if reasonText(got) != "team_owns_no_repository" {
				t.Fatalf("%s: reason = %s, want team_owns_no_repository", team, reasonText(got))
			}
			for _, s := range got.Timeseries {
				if s.Scope != model.TimeseriesScopeOrg {
					t.Fatalf("%s: a team series %s/%s of %d buckets, want none (the ORG series only)", team, s.Measure, s.DimensionValue, len(s.Buckets))
				}
			}
		}
		// The ORG series reads the request's own team filter (CompileOrgTimeseries): under a team scope that owns nothing it is the
		// empty set of repositories, never a value, and never the whole org. The reason does not change it.
		got, _ := run("org under scope", teamScopeFilter("team-x"), true)
		for _, s := range got.Timeseries {
			for _, b := range s.Buckets {
				if b.Value != nil {
					t.Fatalf("%s/%s under a team scope that owns nothing served a value %v", s.Measure, s.Scope, *b.Value)
				}
			}
		}
	})

	t.Run("owned repository: no reason, the series is served", func(t *testing.T) {
		got, _ := run("owner", teamScopeFilter("team-owner"), true)
		if got.FilterEmptyReason != nil {
			t.Fatalf("reason = %s, want null", reasonText(got))
		}
		served := 0
		for _, s := range got.Timeseries {
			if s.Scope != model.TimeseriesScopeOrg && s.DimensionValue == "team-owner" {
				served++
			}
		}
		if served == 0 {
			t.Fatal("team-owner has no series")
		}
	})

	t.Run("owned repository and no rows in the window: no reason", func(t *testing.T) {
		batch := allMeasuresBatch(teamScopeFilter("team-owner"))
		for i := range batch.Timeseries {
			batch.Timeseries[i].DateRange = &model.DateRangeInput{StartDate: mustGraphQLDate("2025-03-01"), EndDate: mustGraphQLDate("2025-03-07")}
		}
		got, err := ResolveSelected(e.ctx, e.client, org, batch, Selection{FilterEmptyReason: true})
		if err != nil {
			t.Fatal(err)
		}
		if got.FilterEmptyReason != nil {
			t.Fatalf("reason = %s, want null", reasonText(got))
		}
		for _, s := range got.Timeseries {
			for _, b := range s.Buckets {
				if b.Value != nil && *b.Value != 0 {
					t.Fatalf("a window with no rows served a value: %+v", s)
				}
			}
		}
	})

	t.Run("two teams: the union decides", func(t *testing.T) {
		if got, _ := run("one owns", teamScopeFilter("team-x", "team-owner"), true); got.FilterEmptyReason != nil {
			t.Fatalf("reason = %s, want null when one of two teams owns a repository", reasonText(got))
		}
		if got, _ := run("none owns", teamScopeFilter("team-x", "team-ended"), true); reasonText(got) != "team_owns_no_repository" {
			t.Fatalf("reason = %s, want team_owns_no_repository when neither owns", reasonText(got))
		}
	})

	t.Run("no team scope: no reason, no ownership statement, and the org series holds every repository", func(t *testing.T) {
		got, rec := run("org view", nil, true)
		if got.FilterEmptyReason != nil || rec.owned.Load() != 0 {
			t.Fatalf("reason = %s, ownership statements = %d; want null, 0", reasonText(got), rec.owned.Load())
		}
		orgSeries := 0
		for _, s := range got.Timeseries {
			if s.Scope == model.TimeseriesScopeOrg && s.Measure == string(model.MeasureInputPipelineSuccessRate) {
				orgSeries++
				if len(s.Buckets) != 1 || s.Buckets[0].Value == nil || *s.Buckets[0].Value != 50 {
					t.Fatalf("org success rate = %+v, want 50 over both repositories", s.Buckets)
				}
			}
			if s.Scope != model.TimeseriesScopeOrg && s.DimensionValue == "team-x" {
				t.Fatalf("team-x owns no repository and has a team series: %+v", s)
			}
		}
		if orgSeries != 1 {
			t.Fatalf("org series count = %d, want 1", orgSeries)
		}
	})

	t.Run("field not selected: the ownership statement is not sent", func(t *testing.T) {
		got, rec := run("not selected", teamScopeFilter("team-x"), false)
		if got.FilterEmptyReason != nil || rec.owned.Load() != 0 {
			t.Fatalf("reason = %s, ownership statements = %d; want null, 0", reasonText(got), rec.owned.Load())
		}
		_, rec = run("selected", teamScopeFilter("team-x"), true)
		if rec.owned.Load() != 1 {
			t.Fatalf("ownership statements = %d, want exactly 1 per request", rec.owned.Load())
		}
	})

	t.Run("the test window and the job failures say it too", func(t *testing.T) {
		since, until := graphqldate.New(day1), graphqldate.New(day3)
		for team, want := range map[string]bool{"team-x": true, "team-ended": true, "team-owner": false} {
			window, err := testopswindow.Resolve(e.ctx, e.client, org, since, until, testopswindow.Scope{TeamIDs: []string{team}, FilterEmptyReason: true})
			if err != nil {
				t.Fatal(err)
			}
			if (window.FilterEmptyReason != nil) != want || (want && *window.FilterEmptyReason != "team_owns_no_repository") {
				t.Errorf("window %s: reason = %v, want set = %v", team, window.FilterEmptyReason, want)
			}
			jobs, err := testopsjobfailures.Resolve(e.ctx, e.client, org, since, until, testopsjobfailures.Scope{TeamIDs: []string{team}, FilterEmptyReason: true}, 20)
			if err != nil {
				t.Fatal(err)
			}
			if (jobs.FilterEmptyReason != nil) != want {
				t.Errorf("job failures %s: reason = %v, want set = %v", team, jobs.FilterEmptyReason, want)
			}
		}
	})
}
