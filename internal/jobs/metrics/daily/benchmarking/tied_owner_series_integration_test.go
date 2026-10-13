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

// TestTiedMainOwnersEachHoldTheRepositoryWhole reads the three testops metrics
// tables for a repository with two main owners (a full ownership tie, D5990):
// the tables hold one row per owner (team_id is in the sorting key,
// migration 124), a team series gives each owner the repository's value, and
// the repository and global series count the repository once.
func TestTiedMainOwnersEachHoldTheRepositoryWhole(t *testing.T) {
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

	const org = "org-9468"
	day := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	older := time.Date(2026, 8, 1, 1, 0, 0, 0, time.UTC)
	newer := time.Date(2026, 8, 2, 1, 0, 0, 0, time.UTC)
	tiedRepo := "00000000-0000-0000-0000-0000000000d1"
	singleRepo := "00000000-0000-0000-0000-0000000000d2"

	for _, team := range []string{"team-a", "team-b", "team-gone", "team-solo"} {
		if err := admin.Exec(ctx, `INSERT INTO teams (id, team_uuid, name, org_id, provider, is_active, updated_at, last_synced)
			VALUES (?, generateUUIDv4(), ?, ?, 'linear', 1, ?, ?)`, team, team, org, newer, newer); err != nil {
			t.Fatalf("insert team %s: %v", team, err)
		}
	}

	for _, source := range []struct{ table, column, metric string }{
		{"testops_pipeline_metrics_daily", "success_rate", "success_rate"},
		{"testops_test_metrics_daily", "pass_rate", "pass_rate"},
		{"testops_coverage_metrics_daily", "line_coverage_pct", "line_coverage_pct"},
	} {
		t.Run(source.table, func(t *testing.T) {
			if err := admin.Exec(ctx, "SYSTEM STOP MERGES "+source.table); err != nil {
				t.Fatalf("stop merges: %v", err)
			}
			var sortingKey string
			if err := admin.QueryRow(ctx,
				"SELECT sorting_key FROM system.tables WHERE database = currentDatabase() AND name = ?", source.table,
			).Scan(&sortingKey); err != nil || sortingKey != "org_id, repo_id, day, team_id" {
				t.Fatalf("sorting key of %s = %q (err %v), want org_id, repo_id, day, team_id", source.table, sortingKey, err)
			}
			insert := "INSERT INTO " + source.table + " (repo_id, day, " + source.column +
				", team_id, org_id, computed_at) VALUES (?, ?, ?, ?, ?, ?)"
			for _, row := range []struct {
				repo string
				team string
				rate float64
				at   time.Time
			}{
				{tiedRepo, "team-gone", 0.1, older},
				{tiedRepo, "team-a", 0.6, newer},
				{tiedRepo, "team-b", 0.6, newer},
				{singleRepo, "team-solo", 0.2, newer},
			} {
				if err := admin.Exec(ctx, insert, row.repo, day, row.rate, row.team, org, row.at); err != nil {
					t.Fatalf("insert %s: %v", row.repo, err)
				}
			}
			var stored uint64
			if err := admin.QueryRow(ctx,
				"SELECT count() FROM "+source.table+" WHERE org_id = ? AND repo_id = ?", org, tiedRepo,
			).Scan(&stored); err != nil || stored != 3 {
				t.Fatalf("the tied repository holds %d stored rows (err %v), want 3: one per owner and one older run", stored, err)
			}

			loader, err := NewClickHouseLoader(admin, org)
			if err != nil {
				t.Fatalf("NewClickHouseLoader: %v", err)
			}
			teamSeries, err := loader.FetchMetricSeriesByScope(ctx, source.metric, day, day, ScopeTeam)
			if err != nil {
				t.Fatalf("team series: %v", err)
			}
			wantTeams := map[string][]MetricPoint{
				"team-a":    {{Day: day, Value: 0.6}},
				"team-b":    {{Day: day, Value: 0.6}},
				"team-solo": {{Day: day, Value: 0.2}},
			}
			if !reflect.DeepEqual(teamSeries, wantTeams) {
				t.Fatalf("team series = %+v, want %+v (both owners hold the tied repository whole; team-gone holds an older run only)", teamSeries, wantTeams)
			}

			globalSeries, err := loader.FetchMetricSeriesByScope(ctx, source.metric, day, day, ScopeGlobal)
			if err != nil {
				t.Fatalf("global series: %v", err)
			}
			points := globalSeries["global"]
			if len(points) != 1 || math.Abs(points[0].Value-0.4) > 1e-9 {
				t.Fatalf("global series = %+v, want one point of 0.4: the tied repository is one sample, not two", globalSeries)
			}
			repoSeries, err := loader.FetchMetricSeriesByScope(ctx, source.metric, day, day, ScopeRepo)
			if err != nil {
				t.Fatalf("repo series: %v", err)
			}
			if len(repoSeries) != 2 || math.Abs(repoSeries[tiedRepo][0].Value-0.6) > 1e-9 {
				t.Fatalf("repo series = %+v, want two repositories, the tied one at 0.6", repoSeries)
			}
		})
	}
}
