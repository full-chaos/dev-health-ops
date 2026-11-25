//go:build integration

// CHAOS-14642: the hotspots team scope against a real ClickHouse engine. teamIds narrows both reads (the file rows and
// the per-repository top files) to the repositories the teams OWN (teamscope.RepoCondition), a team that owns nothing
// here matches nothing, and repoIds with teamIds is their intersection. A fake client never evaluates the SQL.
package hotspots

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/clickhouse"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

const (
	teamScopeOrgID     = "org-14642-team-scope"
	teamScopeOtherOrg  = "org-14642-other"
	teamScopeTeamA     = "team-a"
	teamScopeTeamB     = "team-b"
	teamScopeElsewhere = "team-elsewhere"
	teamScopeRepoA     = "14642aaa-0000-4000-8000-00000000000a"
	teamScopeRepoB     = "14642bbb-0000-4000-8000-00000000000b"
	teamScopeRepoC     = "14642ccc-0000-4000-8000-00000000000c"
)

var teamScopeAsOf = time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)

type teamScopeResult struct{ files, repos []string }

// newTeamScopeFixture seeds three repositories with one hotspot file each: team A owns repo A, team B owns repo B,
// repo C has no owner, and a team of another org owns repo A there.
func newTeamScopeFixture(ctx context.Context, t *testing.T) func(scope Scope) teamScopeResult {
	t.Helper()
	inst, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	t.Cleanup(func() { _ = inst.Close(context.Background()) })
	chschema.Apply(ctx, t, inst)
	conn := openRawClickHouse(t, inst.URI)

	synced := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	repos := map[string]string{"acme/a": teamScopeRepoA, "acme/b": teamScopeRepoB, "acme/c": teamScopeRepoC}
	for name, id := range repos {
		if err := conn.Exec(ctx, `INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES (?, ?, ?, ?, ?, ?)`,
			uuid.MustParse(id), name, "github", teamScopeOrgID, synced, synced); err != nil {
			t.Fatalf("seed repo %s: %v", name, err)
		}
	}
	validFrom := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, o := range []struct{ org, team, repo, name string }{
		{teamScopeOrgID, teamScopeTeamA, teamScopeRepoA, "acme/a"},
		{teamScopeOrgID, teamScopeTeamB, teamScopeRepoB, "acme/b"},
		{teamScopeOtherOrg, teamScopeElsewhere, teamScopeRepoA, "acme/a"},
	} {
		if err := conn.Exec(ctx, `INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			o.org, "github", o.team, uuid.MustParse(o.repo), o.name, "exact", "inferred", uint8(0), uint16(0), int32(0), validFrom, nil, validFrom); err != nil {
			t.Fatalf("seed ownership %s/%s: %v", o.org, o.team, err)
		}
	}
	for _, f := range []struct {
		repo, file string
		risk       float64
	}{
		{teamScopeRepoA, "a/a1.go", 5},
		{teamScopeRepoB, "b/b1.go", 9},
		{teamScopeRepoC, "c/c1.go", 7},
	} {
		insert := fmt.Sprintf(
			"INSERT INTO file_hotspot_daily (repo_id, day, file_path, churn_loc_30d, churn_commits_30d, "+
				"cyclomatic_total, cyclomatic_avg, blame_concentration, risk_score, computed_at, org_id) VALUES "+
				"(toUUID('%s'), toDate('2026-09-07'), '%s', 1, 1, 1, 1, 0.5, %g, toDateTime('2026-09-07 04:00:00'), '%s')",
			f.repo, f.file, f.risk, teamScopeOrgID)
		if err := conn.Exec(ctx, insert); err != nil {
			t.Fatalf("seed %s: %v", f.file, err)
		}
	}

	client, err := clickhouse.NewClickHouseQueryClientWithOptions(clickhouse.Options{DSN: inst.URI})
	if err != nil {
		t.Fatalf("construct ClickHouse query client: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	since := time.Date(2026, 8, 9, 0, 0, 0, 0, time.UTC)
	until := time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC)
	return func(scope Scope) teamScopeResult {
		t.Helper()
		scope.AsOf = teamScopeAsOf
		got, err := ResolveScoped(ctx, client, teamScopeOrgID, since, until, scope, nil)
		if err != nil {
			t.Fatalf("ResolveScoped(%+v): %v", scope, err)
		}
		var out teamScopeResult
		for _, r := range got.Rows {
			out.files = append(out.files, r.FilePath)
		}
		for _, r := range got.Repos {
			out.repos = append(out.repos, r.RepoID)
		}
		slices.Sort(out.files)
		slices.Sort(out.repos)
		return out
	}
}

func assertTeamScopeResult(t *testing.T, label string, got teamScopeResult, files, repos []string) {
	t.Helper()
	if !slices.Equal(got.files, files) {
		t.Errorf("%s: files = %v, want %v", label, got.files, files)
	}
	if !slices.Equal(got.repos, repos) {
		t.Errorf("%s: repos = %v, want %v", label, got.repos, repos)
	}
}

func TestResolveScoped_TeamScopeKeepsTheOwnedRepositoriesOnTheRealEngine(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	resolve := newTeamScopeFixture(ctx, t)

	assertTeamScopeResult(t, "no scope (control)", resolve(Scope{}),
		[]string{"a/a1.go", "b/b1.go", "c/c1.go"}, []string{teamScopeRepoA, teamScopeRepoB, teamScopeRepoC})
	assertTeamScopeResult(t, "repoIds [B] (control)", resolve(Scope{RepoIDs: []string{teamScopeRepoB}}),
		[]string{"b/b1.go"}, []string{teamScopeRepoB})
	assertTeamScopeResult(t, "team A", resolve(Scope{TeamIDs: []string{teamScopeTeamA}}),
		[]string{"a/a1.go"}, []string{teamScopeRepoA})
	assertTeamScopeResult(t, "teams A and B", resolve(Scope{TeamIDs: []string{teamScopeTeamA, teamScopeTeamB}}),
		[]string{"a/a1.go", "b/b1.go"}, []string{teamScopeRepoA, teamScopeRepoB})
}

func TestResolveScoped_ATeamOwningNothingHereIsEmptyNotTheOrgOnTheRealEngine(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	resolve := newTeamScopeFixture(ctx, t)

	assertTeamScopeResult(t, "team of another org", resolve(Scope{TeamIDs: []string{teamScopeElsewhere}}), nil, nil)
	assertTeamScopeResult(t, "team with no ownership row", resolve(Scope{TeamIDs: []string{"team-none"}}), nil, nil)
}

func TestResolveScoped_RepoIDsAndTeamIDsIntersectOnTheRealEngine(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	resolve := newTeamScopeFixture(ctx, t)

	assertTeamScopeResult(t, "repo A with team B", resolve(Scope{RepoIDs: []string{teamScopeRepoA}, TeamIDs: []string{teamScopeTeamB}}), nil, nil)
	assertTeamScopeResult(t, "repos A and B with team A", resolve(Scope{RepoIDs: []string{teamScopeRepoA, teamScopeRepoB}, TeamIDs: []string{teamScopeTeamA}}),
		[]string{"a/a1.go"}, []string{teamScopeRepoA})
}
