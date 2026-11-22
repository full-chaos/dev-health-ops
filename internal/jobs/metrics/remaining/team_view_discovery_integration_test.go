//go:build integration

package remaining

import (
	"context"
	"reflect"
	"sort"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestTeamDiscoveryKeepsTodaysTeamSet pins the teams the capacity forecast job and the recommendations job work on
// (CHAOS-14340): the teams of the org view of work_item_metrics_daily, as before team rows. A team row of a co-owner
// team (row_view 'team') does not make that team a target: a co-owner-only team gets no capacity or recommendation
// run of its own.
func TestTeamDiscoveryKeepsTodaysTeamSet(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	inst, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	defer func() { _ = inst.Close(context.Background()) }()
	chschema.Apply(ctx, t, inst)
	opts, err := stdclickhouse.ParseDSN(inst.URI)
	if err != nil {
		t.Fatalf("parse ClickHouse DSN: %v", err)
	}
	conn, err := stdclickhouse.Open(opts)
	if err != nil {
		t.Fatalf("open ClickHouse: %v", err)
	}
	defer func() { _ = conn.Close() }()

	const org = "org-14340-discovery"
	now := time.Now().UTC()
	day := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, -1)
	at := now.Add(-time.Hour)
	for _, row := range []struct {
		team      string
		completed uint32
		view      string
	}{
		{"P", 3, "org"}, {"P", 3, "scope"}, {"P", 3, "team"}, {"C", 1, "team"},
	} {
		if err := conn.Exec(ctx, `INSERT INTO work_item_metrics_daily
			(day, provider, work_scope_id, team_id, team_name, items_completed, wip_count_end_of_day, computed_at, org_id, row_view)
			VALUES (?, 'jira', 'S', ?, ?, ?, 2, ?, ?, ?)`,
			day, row.team, row.team, row.completed, at, org, row.view); err != nil {
			t.Fatalf("insert %+v: %v", row, err)
		}
	}

	capacity := &CapacityExecutor{conn: conn}
	targets, err := capacity.resolveScopes(ctx, org, capacityScope{AllTeams: true})
	if err != nil {
		t.Fatalf("capacity scopes: %v", err)
	}
	var scopes []string
	for _, target := range targets {
		scopes = append(scopes, *target.TeamID+" @ "+*target.WorkScopeID)
	}
	if want := []string{"P @ S"}; !reflect.DeepEqual(scopes, want) {
		t.Errorf("capacity scopes = %v, want %v: a co-owner team row must not start a capacity run", scopes, want)
	}

	recommendations := &RecommendationsExecutor{conn: conn}
	teams, err := recommendations.DiscoverTeamIDs(ctx, org, now)
	if err != nil {
		t.Fatalf("recommendation teams: %v", err)
	}
	sort.Strings(teams)
	if want := []string{"P"}; !reflect.DeepEqual(teams, want) {
		t.Errorf("recommendation teams = %v, want %v: a co-owner team row must not start a recommendation run", teams, want)
	}
}
