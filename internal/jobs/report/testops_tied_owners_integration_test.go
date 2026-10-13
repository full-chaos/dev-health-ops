//go:build integration

package report

import (
	"context"
	"reflect"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// A repository with two main owners (D5990) has one row per owner in each
// testops metrics table. A read that groups by team sees both owners of the
// newest run and none of an older run; any other read sees the repository once.
func TestTestopsTiedOwnersAreReadOncePerRepositoryAndWholePerTeam(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	ch, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse: %v", err)
	}
	defer func() { _ = ch.Close(context.Background()) }()
	chschema.Apply(ctx, t, ch)
	options, err := stdclickhouse.ParseDSN(ch.URI)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := stdclickhouse.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	day := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	older := day.Add(time.Hour)
	newer := day.Add(26 * time.Hour)
	const repo = "00000000-0000-0000-0000-0000000000e1"

	for _, tc := range []struct{ table, column string }{
		{"testops_pipeline_metrics_daily", "success_rate"},
		{"testops_test_metrics_daily", "pass_rate"},
		{"testops_coverage_metrics_daily", "line_coverage_pct"},
	} {
		t.Run(tc.table, func(t *testing.T) {
			table := tc.table
			if err := conn.Exec(ctx, "SYSTEM STOP MERGES "+table); err != nil {
				t.Fatal(err)
			}
			for _, row := range []struct {
				team string
				at   time.Time
			}{{"team-gone", older}, {"team-a", newer}, {"team-b", newer}} {
				if err := conn.Exec(ctx, "INSERT INTO "+table+" (repo_id, day, "+tc.column+", team_id, org_id, computed_at) VALUES (?, ?, 0.5, ?, 'org-9468', ?)",
					repo, day, row.team, row.at); err != nil {
					t.Fatal(err)
				}
			}
			read := func(byTeam bool) []string {
				src := dedupFromSourceFor(table, byTeam)
				rows, err := conn.Query(ctx, "SELECT team_id FROM "+src+" WHERE org_id = 'org-9468' ORDER BY team_id")
				if err != nil {
					t.Fatalf("read byTeam=%v: %v", byTeam, err)
				}
				defer func() { _ = rows.Close() }()
				var teams []string
				for rows.Next() {
					var team *string
					if err := rows.Scan(&team); err != nil {
						t.Fatal(err)
					}
					teams = append(teams, *team)
				}
				return teams
			}
			if got := read(true); !reflect.DeepEqual(got, []string{"team-a", "team-b"}) {
				t.Errorf("team read = %v, want both owners of the newest run", got)
			}
			if got := read(false); !reflect.DeepEqual(got, []string{"team-a"}) {
				t.Errorf("repository read = %v, want one row: the smallest team_id of the newest run", got)
			}
		})
	}
}
