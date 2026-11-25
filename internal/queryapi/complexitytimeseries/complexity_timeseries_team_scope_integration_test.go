//go:build integration

// CHAOS-14674: the complexityTimeseries team scope against a real ClickHouse engine, on the path the web takes (REPO
// level, no repoIds, the default top-N): the top N are picked among the repositories the team OWNS, a team of another
// org matches nothing here, and repoIds with teamIds is their intersection. A fake client never evaluates the SQL.
package complexitytimeseries

import (
	"context"
	"slices"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/full-chaos/dev-health-go/clickhouse"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

const (
	complexityTeamScopeOrg       = "org-14674-team-scope"
	complexityTeamScopeOtherOrg  = "org-14674-other"
	complexityTeamScopeTeamA     = "team-a"
	complexityTeamScopeTeamB     = "team-b"
	complexityTeamScopeElsewhere = "team-elsewhere"
	complexityTeamScopeRepoA     = "14674aaa-0000-4000-8000-00000000000a"
	complexityTeamScopeRepoB     = "14674bbb-0000-4000-8000-00000000000b"
	complexityTeamScopeRepoC     = "14674ccc-0000-4000-8000-00000000000c"
)

// newComplexityTeamScopeFixture seeds three repositories of one org with one complexity day each: team A owns repo A
// (the least complex), team B owns repo B, repo C (the most complex) has no owner, and a team of another org owns repo
// A there. The org's top 1 is therefore C; team A's top 1 is A.
func newComplexityTeamScopeFixture(ctx context.Context, t *testing.T) func(scope Scope, limit int) []string {
	t.Helper()
	inst, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	t.Cleanup(func() { _ = inst.Close(context.Background()) })
	chschema.Apply(ctx, t, inst)
	opts, err := stdclickhouse.ParseDSN(inst.URI)
	if err != nil {
		t.Fatalf("parse ClickHouse DSN: %v", err)
	}
	conn, err := stdclickhouse.Open(opts)
	if err != nil {
		t.Fatalf("open raw ClickHouse connection: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	synced := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	for _, r := range []struct {
		name, id string
		perKloc  float64
	}{
		{"acme/a", complexityTeamScopeRepoA, 10},
		{"acme/b", complexityTeamScopeRepoB, 20},
		{"acme/c", complexityTeamScopeRepoC, 30},
	} {
		if err := conn.Exec(ctx, `INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES (?, ?, ?, ?, ?, ?)`,
			uuid.MustParse(r.id), r.name, "github", complexityTeamScopeOrg, synced, synced); err != nil {
			t.Fatalf("seed repo %s: %v", r.name, err)
		}
		if err := conn.Exec(ctx, `INSERT INTO repo_complexity_daily (repo_id, day, loc_total, cyclomatic_total, cyclomatic_per_kloc, high_complexity_functions, very_high_complexity_functions, computed_at, org_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			uuid.MustParse(r.id), time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC), uint64(1000), uint64(r.perKloc*10), r.perKloc, uint64(1), uint64(0),
			time.Date(2026, 8, 10, 4, 0, 0, 0, time.UTC), complexityTeamScopeOrg); err != nil {
			t.Fatalf("seed complexity %s: %v", r.name, err)
		}
	}
	validFrom := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, o := range []struct{ org, team, repo, name string }{
		{complexityTeamScopeOrg, complexityTeamScopeTeamA, complexityTeamScopeRepoA, "acme/a"},
		{complexityTeamScopeOrg, complexityTeamScopeTeamB, complexityTeamScopeRepoB, "acme/b"},
		{complexityTeamScopeOtherOrg, complexityTeamScopeElsewhere, complexityTeamScopeRepoA, "acme/a"},
	} {
		if err := conn.Exec(ctx, `INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			o.org, "github", o.team, uuid.MustParse(o.repo), o.name, "exact", "inferred", uint8(0), uint16(0), int32(0), validFrom, nil, validFrom); err != nil {
			t.Fatalf("seed ownership %s/%s: %v", o.org, o.team, err)
		}
	}

	client, err := clickhouse.NewClickHouseQueryClientWithOptions(clickhouse.Options{DSN: inst.URI})
	if err != nil {
		t.Fatalf("construct ClickHouse query client: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	since := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	until := time.Date(2026, 8, 31, 0, 0, 0, 0, time.UTC)
	return func(scope Scope, limit int) []string {
		t.Helper()
		scope.AsOf = time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
		got, err := ResolveScoped(ctx, client, complexityTeamScopeOrg, since, until, model.TimeGranularityDay, model.ComplexityScopeRepo, scope, &limit)
		if err != nil {
			t.Fatalf("ResolveScoped(%+v, %d): %v", scope, limit, err)
		}
		var repos []string
		for _, p := range got.Points {
			if !slices.Contains(repos, p.ScopeID) {
				repos = append(repos, p.ScopeID)
			}
		}
		slices.Sort(repos)
		return repos
	}
}

func TestComplexityTimeseriesTeamScopeOnTheRealEngine(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	resolve := newComplexityTeamScopeFixture(ctx, t)

	for _, c := range []struct {
		label string
		scope Scope
		limit int
		want  []string
	}{
		{"no scope, top 3 (control)", Scope{}, 3, []string{complexityTeamScopeRepoA, complexityTeamScopeRepoB, complexityTeamScopeRepoC}},
		{"no scope, top 1 (control: the org's most complex)", Scope{}, 1, []string{complexityTeamScopeRepoC}},
		{"team A, top 1: picked among the team's repositories", Scope{TeamIDs: []string{complexityTeamScopeTeamA}}, 1, []string{complexityTeamScopeRepoA}},
		{"teams A and B, top 3", Scope{TeamIDs: []string{complexityTeamScopeTeamA, complexityTeamScopeTeamB}}, 3, []string{complexityTeamScopeRepoA, complexityTeamScopeRepoB}},
		{"team of another org: nothing here", Scope{TeamIDs: []string{complexityTeamScopeElsewhere}}, 3, nil},
		{"repoIds [A, B] with team A: the intersection", Scope{RepoIDs: []string{complexityTeamScopeRepoA, complexityTeamScopeRepoB}, TeamIDs: []string{complexityTeamScopeTeamA}}, 3, []string{complexityTeamScopeRepoA}},
		{"repoIds [A] with team B: empty", Scope{RepoIDs: []string{complexityTeamScopeRepoA}, TeamIDs: []string{complexityTeamScopeTeamB}}, 3, nil},
	} {
		if got := resolve(c.scope, c.limit); !slices.Equal(got, c.want) {
			t.Errorf("%s: repositories %v, want %v", c.label, got, c.want)
		} else {
			t.Logf("%s: %v", c.label, got)
		}
	}
}
