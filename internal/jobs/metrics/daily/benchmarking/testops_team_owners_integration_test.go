//go:build integration

package benchmarking

import (
	"context"
	"math"
	"sort"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestTestopsTeamSeriesTakeTheOwnersAtTheRun reads the team and organization
// series of a team metric of each TestOps table. The team of a repository is
// its owner in team_repo_ownership at the run instant, not the team stored on
// the row (the main owner of the day it was computed):
//
//   - repo-r is co-owned by team-a and team-b (share 0.5 each): its value goes
//     in full, unscaled, to both. team-a also owns repo-s: its value is the
//     mean of repo-r and repo-s.
//   - repo-t is stored under team-c, which owns no repository; team-d owns it.
//     team-c has no series.
//   - repo-u is stored under team-e, its owner until a date after the target
//     day; team-f owns it at the run. Every day of the window is team-f's.
//   - the organization series takes each repository once, repo-r included.
func TestTestopsTeamSeriesTakeTheOwnersAtTheRun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	ch, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	defer func() { _ = ch.Close(context.Background()) }()
	chschema.Apply(ctx, t, ch)
	options, err := stdclickhouse.ParseDSN(ch.URI)
	if err != nil {
		t.Fatal(err)
	}
	admin, err := stdclickhouse.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = admin.Close() }()

	const org = "org-13857"
	repoR := "00000000-0000-4000-8000-000000013801"
	repoS := "00000000-0000-4000-8000-000000013802"
	repoT := "00000000-0000-4000-8000-000000013803"
	repoU := "00000000-0000-4000-8000-000000013804"
	first := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	target := first.AddDate(0, 0, 2)
	claimed := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	handover := time.Date(2026, 8, 5, 0, 0, 0, 0, time.UTC)
	run := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)

	for _, team := range []string{"team-a", "team-b", "team-c", "team-d", "team-e", "team-f"} {
		if err := admin.Exec(ctx, `INSERT INTO teams (id, team_uuid, name, org_id, provider, is_active, updated_at, last_synced)
			VALUES (?, generateUUIDv4(), ?, ?, 'linear', 1, ?, ?)`, team, team, org, claimed, claimed); err != nil {
			t.Fatalf("insert team %s: %v", team, err)
		}
	}
	for name, repo := range map[string]string{"acme/r": repoR, "acme/s": repoS, "acme/t": repoT, "acme/u": repoU} {
		if err := admin.Exec(ctx, `INSERT INTO repos (id, repo, provider, last_synced, org_id) VALUES (toUUID(?), ?, 'github', ?, ?)`,
			repo, name, claimed, org); err != nil {
			t.Fatalf("insert repo %s: %v", name, err)
		}
	}
	for _, own := range []struct {
		team, repo, name string
		share            float64
		from             time.Time
		to               *time.Time
	}{
		{"team-a", repoR, "acme/r", 0.5, claimed, nil},
		{"team-b", repoR, "acme/r", 0.5, claimed, nil},
		{"team-a", repoS, "acme/s", 1, claimed, nil},
		{"team-d", repoT, "acme/t", 1, claimed, nil},
		{"team-e", repoU, "acme/u", 1, claimed, &handover},
		{"team-f", repoU, "acme/u", 1, handover, nil},
	} {
		if err := admin.Exec(ctx, `INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type,
    source, is_primary, specificity, priority, share, valid_from, valid_to, updated_at)
VALUES (?, 'github', ?, toUUID(?), ?, 'exact', 'inferred', 1, 10, 0, ?, ?, ?, ?)`,
			org, own.team, own.repo, own.name, own.share, own.from, own.to, claimed); err != nil {
			t.Fatalf("insert ownership %s %s: %v", own.team, own.name, err)
		}
	}

	stored := []struct {
		repo, team string
		value      float64
	}{{repoR, "team-a", 0.2}, {repoS, "team-a", 0.6}, {repoT, "team-c", 0.5}, {repoU, "team-e", 0.9}}
	days := []time.Time{first, first.AddDate(0, 0, 1), target}

	for _, source := range []struct{ table, column, metric string }{
		{"testops_pipeline_metrics_daily", "rerun_rate", "rerun_rate"},
		{"testops_test_metrics_daily", "flake_rate", "flake_rate"},
		{"testops_coverage_metrics_daily", "line_coverage_pct", "line_coverage_pct"},
	} {
		t.Run(source.table, func(t *testing.T) {
			// rerun_rate is read as reruns over runs: every repo-day has the same 10 runs, so the
			// weights are equal and the expected values are those of the stored rates. The test table gets 10 distinct
			// cases and the rate's share of them as flake cases, for the same reason.
			insert := "INSERT INTO " + source.table + " (repo_id, day, " + source.column +
				", team_id, org_id, computed_at, pipelines_count) VALUES (toUUID(?), ?, ?, ?, ?, ?, 10)"
			if source.table != "testops_pipeline_metrics_daily" {
				insert = "INSERT INTO " + source.table + " (repo_id, day, " + source.column +
					", team_id, org_id, computed_at) VALUES (toUUID(?), ?, ?, ?, ?, ?)"
			}
			if source.table == "testops_test_metrics_daily" {
				insert = "INSERT INTO " + source.table + " (repo_id, day, " + source.column +
					", team_id, org_id, computed_at, flake_cases, distinct_cases) VALUES (toUUID(?), ?, ?, ?, ?, ?, toUInt32(round(?*10)), 10)"
			}
			for _, day := range days {
				for _, row := range stored {
					args := []any{row.repo, day, row.value, row.team, org, day.Add(time.Hour)}
					if source.table == "testops_test_metrics_daily" {
						args = append(args, row.value)
					}
					if err := admin.Exec(ctx, insert, args...); err != nil {
						t.Fatalf("insert %s: %v", row.repo, err)
					}
				}
			}
			loader, err := NewClickHouseLoader(admin, org)
			if err != nil {
				t.Fatal(err)
			}
			loader.SetOwnersAsOf(run)

			teamSeries, err := loader.FetchMetricSeriesByScope(ctx, source.metric, first, target, ScopeTeam)
			if err != nil {
				t.Fatalf("team series: %v", err)
			}
			want := map[string]float64{"team-a": 0.4, "team-b": 0.2, "team-d": 0.5, "team-f": 0.9}
			assertSeries(t, "team", teamSeries, want, days)

			globalSeries, err := loader.FetchMetricSeriesByScope(ctx, source.metric, first, target, ScopeGlobal)
			if err != nil {
				t.Fatalf("global series: %v", err)
			}
			assertSeries(t, "organization", globalSeries, map[string]float64{"global": (0.2 + 0.6 + 0.5 + 0.9) / 4}, days)
		})
	}
}

// assertSeries checks that series holds exactly the keys of want, each with one
// point of its value on every day.
func assertSeries(t *testing.T, label string, series map[string][]MetricPoint, want map[string]float64, days []time.Time) {
	t.Helper()
	var got, wantKeys []string
	for key := range series {
		got = append(got, key)
	}
	for key := range want {
		wantKeys = append(wantKeys, key)
	}
	sort.Strings(got)
	sort.Strings(wantKeys)
	if len(got) != len(wantKeys) {
		t.Fatalf("%s series keys = %v, want %v (series %+v)", label, got, wantKeys, series)
	}
	for i := range got {
		if got[i] != wantKeys[i] {
			t.Fatalf("%s series keys = %v, want %v (series %+v)", label, got, wantKeys, series)
		}
	}
	for key, value := range want {
		points := series[key]
		if len(points) != len(days) {
			t.Fatalf("%s series %s = %+v, want one point on each of %d days", label, key, points, len(days))
		}
		for i, point := range points {
			if !point.Day.Equal(days[i]) || math.Abs(point.Value-value) > 1e-9 {
				t.Fatalf("%s series %s point %d = %+v, want %v on %s", label, key, i, point, value, days[i].Format("2006-01-02"))
			}
		}
	}
}
