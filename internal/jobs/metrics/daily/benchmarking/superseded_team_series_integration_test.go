//go:build integration

package benchmarking

import (
	"context"
	"math"
	"reflect"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestTestopsTeamSeriesHoldNoSupersededTeam reads the team and organization
// series of a metric of each testops source table (pipeline, test, coverage),
// whose key is (org, repo, day). The team of a key is the owner of its
// repository at the run (team_repo_ownership), never the team stored on a row:
//
//   - repo-a, the day: an older row under team-old and the newer row of the
//     recompute under team-new, both unmerged; team-new owns repo-a. The key is
//     ONE repository-day: its value is the newer row's, under team-new, and
//     team-old has no series from it.
//   - repo-c, the day: an older row under team-old and the newer row with no
//     team; no team owns repo-c. It gives no team a series, and its newer value
//     is the organization's sample.
//   - repo-b, the day: one row under team-retired, which owns repo-b and is
//     inactive now. It is a sample of the organization, and no team series: an
//     inactive team owns nothing.
func TestTestopsTeamSeriesHoldNoSupersededTeam(t *testing.T) {
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
		t.Fatalf("parse ClickHouse DSN: %v", err)
	}
	admin, err := stdclickhouse.Open(options)
	if err != nil {
		t.Fatalf("open ClickHouse admin connection: %v", err)
	}
	defer func() { _ = admin.Close() }()

	const org = "org-9055"
	day := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	older := time.Date(2026, 8, 1, 1, 0, 0, 0, time.UTC)
	newer := time.Date(2026, 8, 2, 1, 0, 0, 0, time.UTC)
	repoA := "00000000-0000-0000-0000-00000000000a"
	repoB := "00000000-0000-0000-0000-00000000000b"
	repoC := "00000000-0000-0000-0000-00000000000c"

	teams := `INSERT INTO teams (id, team_uuid, name, org_id, provider, is_active, updated_at, last_synced)
		VALUES (?, generateUUIDv4(), ?, ?, 'linear', ?, ?, ?)`
	for _, team := range []struct {
		id     string
		active uint8
	}{{"team-old", 1}, {"team-new", 1}, {"team-retired", 0}} {
		if err := admin.Exec(ctx, teams, team.id, team.id, org, team.active, newer, newer); err != nil {
			t.Fatalf("insert team %s: %v", team.id, err)
		}
	}

	for name, repo := range map[string]string{"acme/a": repoA, "acme/b": repoB, "acme/c": repoC} {
		if err := admin.Exec(ctx, `INSERT INTO repos (id, repo, provider, last_synced, org_id) VALUES (toUUID(?), ?, 'github', ?, ?)`,
			repo, name, older, org); err != nil {
			t.Fatalf("insert repo %s: %v", name, err)
		}
	}
	for _, own := range []struct{ team, repo, name string }{{"team-new", repoA, "acme/a"}, {"team-retired", repoB, "acme/b"}} {
		if err := admin.Exec(ctx, `INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type,
    source, is_primary, specificity, priority, valid_from, updated_at)
VALUES (?, 'github', ?, toUUID(?), ?, 'exact', 'native', 1, 10, 0, ?, ?)`,
			org, own.team, own.repo, own.name, older, older); err != nil {
			t.Fatalf("insert ownership %s: %v", own.name, err)
		}
	}

	// The same rows in each of the three testops source tables, read through
	// one metric of each.
	for _, source := range []struct{ table, column, metric string }{
		{"testops_pipeline_metrics_daily", "rerun_rate", "rerun_rate"},
		{"testops_test_metrics_daily", "pass_rate", "pass_rate"},
		{"testops_coverage_metrics_daily", "line_coverage_pct", "line_coverage_pct"},
	} {
		t.Run(source.table, func(t *testing.T) {
			// The two rows of repo-a stay two rows: a background merge would
			// make the read pass without the key rule.
			if err := admin.Exec(ctx, "SYSTEM STOP MERGES "+source.table); err != nil {
				t.Fatalf("stop merges: %v", err)
			}
			insert := "INSERT INTO " + source.table + " (repo_id, day, " + source.column +
				", team_id, org_id, computed_at) VALUES (?, ?, ?, ?, ?, ?)"
			for _, row := range []struct {
				repo string
				team *string
				rate float64
				at   time.Time
			}{
				{repoA, ptr("team-old"), 0.2, older},
				{repoA, ptr("team-new"), 0.8, newer},
				{repoB, ptr("team-retired"), 0.5, older},
				{repoC, ptr("team-old"), 0.3, older},
				{repoC, nil, 0.4, newer},
			} {
				if err := admin.Exec(ctx, insert, row.repo, day, row.rate, row.team, org, row.at); err != nil {
					t.Fatalf("insert %s: %v", row.repo, err)
				}
			}
			var stored uint64
			if err := admin.QueryRow(ctx,
				"SELECT count() FROM "+source.table+" WHERE org_id = ? AND repo_id = ?", org, repoA,
			).Scan(&stored); err != nil || stored != 2 {
				t.Fatalf("repo-a holds %d stored rows (err %v), want the 2 unmerged rows the test is about", stored, err)
			}

			loader, err := NewClickHouseLoader(admin, org)
			if err != nil {
				t.Fatalf("NewClickHouseLoader: %v", err)
			}
			loader.SetOwnersAsOf(newer)
			teamSeries, err := loader.FetchMetricSeriesByScope(ctx, source.metric, day, day, ScopeTeam)
			if err != nil {
				t.Fatalf("team series: %v", err)
			}
			wantTeams := map[string][]MetricPoint{"team-new": {{Day: day, Value: 0.8}}}
			if !reflect.DeepEqual(teamSeries, wantTeams) {
				t.Fatalf("team series = %+v, want %+v (team-new owns repo-a, whose newest row is 0.8; no team owns repo-c; team-retired is inactive)",
					teamSeries, wantTeams)
			}
			if dropped := loader.DroppedInactiveTeams(); len(dropped) != 0 {
				t.Fatalf("dropped inactive teams = %v, want none (an inactive team is never an owner, so no row names it)", dropped)
			}

			globalSeries, err := loader.FetchMetricSeriesByScope(ctx, source.metric, day, day, ScopeGlobal)
			if err != nil {
				t.Fatalf("global series: %v", err)
			}
			// One sample per repository-day, its newest row: 0.8, 0.5 and 0.4.
			points := globalSeries["global"]
			if len(globalSeries) != 1 || len(points) != 1 || math.Abs(points[0].Value-1.7/3) > 1e-9 {
				t.Fatalf("global series = %+v, want one point of %v (one sample per repository and day)", globalSeries, 1.7/3)
			}
		})
	}
}

func ptr(text string) *string { return &text }
