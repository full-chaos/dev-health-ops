//go:build integration

// reviewEdges' team scope against a real ClickHouse engine (CHAOS-7785): the shared ownership
// condition really narrows the rows, a revoked ownership row admits nothing, the LIMIT applies
// after the narrowing, and a team id of another org matches nothing. A fake client cannot show
// any of this because it never evaluates the SQL.
package reviewedges

import (
	"context"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/chquery"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

const (
	teamScopeOrg   = "re-org"
	teamA          = "team-a"
	teamB          = "team-b"
	teamElsewhere  = "team-elsewhere"
	teamScopeOther = "re-other-org"
)

var teamScopeAsOf = time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)

type teamScopeFixture struct {
	client                                 QueryClient
	repoA, repoB, repoRevoked, repoUnowned uuid.UUID
}

func newTeamScopeFixture(ctx context.Context, t *testing.T) teamScopeFixture {
	t.Helper()
	ch, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	t.Cleanup(func() { _ = ch.Close(context.Background()) })
	chschema.Apply(ctx, t, ch)

	options, err := stdclickhouse.ParseDSN(ch.URI)
	if err != nil {
		t.Fatalf("parse ClickHouse DSN: %v", err)
	}
	admin, err := stdclickhouse.Open(options)
	if err != nil {
		t.Fatalf("open admin connection: %v", err)
	}
	t.Cleanup(func() { _ = admin.Close() })
	queryClient, err := chquery.NewProductionClient(ch.URI)
	if err != nil {
		t.Fatalf("construct query client: %v", err)
	}
	t.Cleanup(func() { _ = queryClient.Close() })

	f := teamScopeFixture{client: queryClient, repoA: uuid.New(), repoB: uuid.New(), repoRevoked: uuid.New(), repoUnowned: uuid.New()}
	synced := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	for name, id := range map[string]uuid.UUID{"acme/a": f.repoA, "acme/b": f.repoB, "acme/revoked": f.repoRevoked, "acme/unowned": f.repoUnowned} {
		if err := admin.Exec(ctx, `INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES (?, ?, ?, ?, ?, ?)`,
			id, name, "github", teamScopeOrg, synced, synced); err != nil {
			t.Fatalf("seed repo %s: %v", name, err)
		}
	}

	validFrom := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	revokedAt := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC) // before teamScopeAsOf
	ownership := []struct {
		org, team string
		repo      uuid.UUID
		name      string
		validTo   any
	}{
		{teamScopeOrg, teamA, f.repoA, "acme/a", nil},
		{teamScopeOrg, teamB, f.repoB, "acme/b", nil},
		{teamScopeOrg, teamA, f.repoRevoked, "acme/revoked", revokedAt},
		// A team of ANOTHER org that "owns" repo A there: must never match in this org.
		{teamScopeOther, teamElsewhere, f.repoA, "acme/a", nil},
	}
	for _, o := range ownership {
		if err := admin.Exec(ctx, `INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			o.org, "github", o.team, o.repo, o.name, "exact", "inferred", uint8(0), uint16(0), int32(0), validFrom, o.validTo, validFrom); err != nil {
			t.Fatalf("seed ownership %s/%s: %v", o.org, o.team, err)
		}
	}

	computed := time.Date(2026, 9, 1, 6, 0, 0, 0, time.UTC)
	edge := func(repo uuid.UUID, day int, reviewer, author string, count uint32) {
		if err := admin.Exec(ctx, `INSERT INTO review_edges_daily (repo_id, day, reviewer, author, reviews_count, computed_at, org_id) VALUES (?, ?, ?, ?, ?, ?, ?)`,
			repo, time.Date(2026, 8, day, 0, 0, 0, 0, time.UTC), reviewer, author, count, computed, teamScopeOrg); err != nil {
			t.Fatalf("seed edge: %v", err)
		}
	}
	// Team A's repo: two small rows. Team B's repo: five big rows. A revoked repo and an
	// unowned repo carry big rows too.
	edge(f.repoA, 1, "rev-1", "auth-1", 3)
	edge(f.repoA, 2, "rev-1", "auth-1", 2)
	for d := 1; d <= 5; d++ {
		edge(f.repoB, d, "rev-2", "auth-2", uint32(50-d))
	}
	edge(f.repoRevoked, 3, "rev-3", "auth-3", 99)
	edge(f.repoUnowned, 4, "rev-4", "auth-4", 77)
	return f
}

func (f teamScopeFixture) resolve(ctx context.Context, t *testing.T, scope Scope, limit int) []string {
	t.Helper()
	scope.AsOf = teamScopeAsOf
	got, err := ResolveScoped(ctx, f.client, teamScopeOrg, mustDate(t, "2026-08-01"), mustDate(t, "2026-08-31"), scope, limit)
	if err != nil {
		t.Fatalf("ResolveScoped: %v", err)
	}
	var out []string
	for _, e := range got.Edges {
		out = append(out, e.Reviewer+">"+e.Author)
	}
	return out
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestTeamScope_NarrowsToTheOwnedRepositories(t *testing.T) {
	ctx := context.Background()
	f := newTeamScopeFixture(ctx, t)

	all := f.resolve(ctx, t, Scope{}, 500)
	if len(all) != 9 {
		t.Fatalf("control: no team scope must see all 9 seeded rows, got %d", len(all))
	}
	// Team A owns repo A now; its repo-revoked ownership ended before the as-of instant.
	got := f.resolve(ctx, t, Scope{TeamIDs: []string{teamA}}, 500)
	if want := []string{"rev-1>auth-1", "rev-1>auth-1"}; !equalStrings(got, want) {
		t.Fatalf("team A edges = %v, want %v (repo A only: not B's, not the revoked repo's, not the unowned repo's)", got, want)
	}
}

func TestTeamScope_LimitAppliesAfterTheNarrowing(t *testing.T) {
	ctx := context.Background()
	f := newTeamScopeFixture(ctx, t)

	// Control: with no team scope, a cut of 3 is eaten by the biggest rows (team B's repo).
	control := f.resolve(ctx, t, Scope{}, 3)
	for _, pair := range control {
		if pair == "rev-1>auth-1" {
			t.Fatalf("control must be starved of team A's small rows, got %v", control)
		}
	}
	// With the team scope inside the WHERE, team A's own rows are what the cut counts.
	got := f.resolve(ctx, t, Scope{TeamIDs: []string{teamA}}, 3)
	if len(got) != 2 {
		t.Fatalf("team A with limit 3 returned %v, want its 2 rows (not starved by other teams' bigger rows)", got)
	}
}

func TestTeamScope_RepoIDsAndTeamIDsIntersect(t *testing.T) {
	ctx := context.Background()
	f := newTeamScopeFixture(ctx, t)

	got := f.resolve(ctx, t, Scope{RepoIDs: []string{f.repoA.String()}, TeamIDs: []string{teamB}}, 500)
	if len(got) != 0 {
		t.Fatalf("repo A with team B's ownership must be empty (intersection), got %v", got)
	}
	got = f.resolve(ctx, t, Scope{RepoIDs: []string{f.repoA.String()}, TeamIDs: []string{teamA}}, 500)
	if len(got) != 2 {
		t.Fatalf("repo A with team A must keep A's 2 rows, got %v", got)
	}
}

func TestTeamScope_ATeamOfAnotherOrgMatchesNothing(t *testing.T) {
	ctx := context.Background()
	f := newTeamScopeFixture(ctx, t)

	got := f.resolve(ctx, t, Scope{TeamIDs: []string{teamElsewhere}}, 500)
	if len(got) != 0 {
		t.Fatalf("a team owning a repo in another org must match nothing here, got %v", got)
	}
}
