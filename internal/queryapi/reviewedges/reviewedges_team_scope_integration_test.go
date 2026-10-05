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
	admin                                  stdclickhouse.Conn
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

	f := teamScopeFixture{client: queryClient, admin: admin, repoA: uuid.New(), repoB: uuid.New(), repoRevoked: uuid.New(), repoUnowned: uuid.New()}
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

func (f teamScopeFixture) resolveResult(ctx context.Context, t *testing.T, scope Scope, limit int) (total int, truncated bool, returned int) {
	t.Helper()
	scope.AsOf = teamScopeAsOf
	got, err := ResolveScoped(ctx, f.client, teamScopeOrg, mustDate(t, "2026-08-01"), mustDate(t, "2026-08-31"), scope, limit)
	if err != nil {
		t.Fatalf("ResolveScoped: %v", err)
	}
	return got.TotalCount, got.Truncated, len(got.Edges)
}

// CHAOS-7786: totalCount is the number of deduplicated (pair, day) rows the filters match, and
// truncated says the list was cut.
func TestTotalCount_CountsTheDedupedRowsAndFlagsTheCut(t *testing.T) {
	ctx := context.Background()
	f := newTeamScopeFixture(ctx, t)

	// A recompute wrote an OLDER copy of one key; the dedup (argMax) must count the key once.
	if err := f.admin.Exec(ctx, `INSERT INTO review_edges_daily (repo_id, day, reviewer, author, reviews_count, computed_at, org_id) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		f.repoA, time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC), "rev-1", "auth-1", uint32(1), time.Date(2026, 8, 2, 0, 0, 0, 0, time.UTC), teamScopeOrg); err != nil {
		t.Fatalf("seed older duplicate: %v", err)
	}

	total, truncated, returned := f.resolveResult(ctx, t, Scope{}, 3)
	if total != 9 || returned != 3 || !truncated {
		t.Fatalf("cut at 3: total=%d truncated=%v returned=%d, want 9/true/3 (9 deduped rows, the older copy not counted twice)", total, truncated, returned)
	}
	total, truncated, returned = f.resolveResult(ctx, t, Scope{}, 500)
	if total != 9 || returned != 9 || truncated {
		t.Fatalf("not cut: total=%d truncated=%v returned=%d, want 9/false/9", total, truncated, returned)
	}
	// Exactly at the cap is not a cut.
	total, truncated, returned = f.resolveResult(ctx, t, Scope{}, 9)
	if total != 9 || returned != 9 || truncated {
		t.Fatalf("at the cap: total=%d truncated=%v returned=%d, want 9/false/9", total, truncated, returned)
	}
}

func TestTotalCount_FollowsTheTeamScope(t *testing.T) {
	ctx := context.Background()
	f := newTeamScopeFixture(ctx, t)

	total, truncated, returned := f.resolveResult(ctx, t, Scope{TeamIDs: []string{teamA}}, 1)
	if total != 2 || returned != 1 || !truncated {
		t.Fatalf("team A cut at 1: total=%d truncated=%v returned=%d, want 2/true/1 (the count uses the same team filter as the rows)", total, truncated, returned)
	}
	total, truncated, _ = f.resolveResult(ctx, t, Scope{TeamIDs: []string{teamElsewhere}}, 500)
	if total != 0 || truncated {
		t.Fatalf("a team owning nothing here: total=%d truncated=%v, want 0/false", total, truncated)
	}
}

// ---- CHAOS-7787: bots and self pairs are excluded at read time ----------------------------

type edgeSeed struct {
	reviewer, author string
	count            uint32
}

// newExclusionFixture starts a ClickHouse and seeds review_edges_daily with GitHub-shaped rows
// (logins; a "[bot]" login is an App actor), GitLab-shaped rows (plain usernames) and self pairs.
func newExclusionFixture(ctx context.Context, t *testing.T) QueryClient {
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
	client, err := chquery.NewProductionClient(ch.URI)
	if err != nil {
		t.Fatalf("construct query client: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	repo := uuid.New()
	computed := time.Date(2026, 9, 1, 6, 0, 0, 0, time.UTC)
	seeds := []edgeSeed{
		// Kept: humans and gitlab-shaped usernames ("abbot" merely contains "bot"; a plain gitlab bot
		// username has no "[bot]" suffix, so by the ruling it stays).
		{"alice-reviewer", "bob-author", 5},
		{"gl-reviewer", "gl-author", 4},
		{"project_42_bot_abc", "gl-author", 20},
		{"abbot", "gl-author", 3},
		// Excluded: bot reviewers (case and padding), a bot author, self pairs (case-insensitive).
		{"dependabot[bot]", "bob-author", 90},
		{"Renovate[BOT]", "carol", 80},
		{"alice-reviewer", "github-actions[bot]", 70},
		{" Dependabot[bot] ", "bob-author", 60},
		{"Bob", "bob", 50},
		{"carol", "carol", 40},
		{"gl-user", "gl-user", 30},
		// The literal "unknown" on both sides is a self pair like any other (CHAOS-8499): left out.
		{"unknown", "unknown", 45},
	}
	for i, e := range seeds {
		if err := admin.Exec(ctx, `INSERT INTO review_edges_daily (repo_id, day, reviewer, author, reviews_count, computed_at, org_id) VALUES (?, ?, ?, ?, ?, ?, ?)`,
			repo, time.Date(2026, 8, 1+i, 0, 0, 0, 0, time.UTC), e.reviewer, e.author, e.count, computed, teamScopeOrg); err != nil {
			t.Fatalf("seed edge %d: %v", i, err)
		}
	}
	return client
}

func TestExclusion_DropsBotsAndSelfPairsAndKeepsEveryoneElse(t *testing.T) {
	ctx := context.Background()
	client := newExclusionFixture(ctx, t)

	got, err := ResolveScoped(ctx, client, teamScopeOrg, mustDate(t, "2026-08-01"), mustDate(t, "2026-08-31"), Scope{}, 500)
	if err != nil {
		t.Fatalf("ResolveScoped: %v", err)
	}
	var pairs []string
	for _, e := range got.Edges {
		pairs = append(pairs, e.Reviewer+">"+e.Author)
	}
	want := []string{"project_42_bot_abc>gl-author", "alice-reviewer>bob-author", "gl-reviewer>gl-author", "abbot>gl-author"}
	if !equalStrings(pairs, want) {
		t.Fatalf("edges = %v, want %v (bots by the [bot] suffix in either column, case and padding ignored; self pairs; nothing else)", pairs, want)
	}
	if got.TotalCount != 4 || got.Truncated {
		t.Fatalf("totalCount=%d truncated=%v, want 4/false (the count excludes the same rows)", got.TotalCount, got.Truncated)
	}
}

func TestExclusion_BotRowsNeverStarveTheCutOrInflateTheTotal(t *testing.T) {
	ctx := context.Background()
	client := newExclusionFixture(ctx, t)

	// The bot rows carry the biggest counts. A cut of 2 must still return two KEPT rows.
	got, err := ResolveScoped(ctx, client, teamScopeOrg, mustDate(t, "2026-08-01"), mustDate(t, "2026-08-31"), Scope{}, 2)
	if err != nil {
		t.Fatalf("ResolveScoped: %v", err)
	}
	if len(got.Edges) != 2 || got.Edges[0].Reviewer != "project_42_bot_abc" || got.Edges[1].Reviewer != "alice-reviewer" {
		t.Fatalf("cut at 2 = %+v, want the two biggest KEPT rows (the 90/80/70/60 bot rows must not take the cut)", got.Edges)
	}
	if got.TotalCount != 4 || !got.Truncated {
		t.Fatalf("totalCount=%d truncated=%v, want 4/true (four kept rows, two returned)", got.TotalCount, got.Truncated)
	}
}
