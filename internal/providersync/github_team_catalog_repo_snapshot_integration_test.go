//go:build integration

package providersync

import (
	"context"
	"errors"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

type repoOwnershipFact struct {
	TeamID, Repo, Source string
}

// openRepoOwnership reads what the readers see: the newest version of each row
// key whose valid_to is empty or in the future.
func openRepoOwnership(ctx context.Context, t *testing.T, conn driver.Conn, orgID, provider string) []repoOwnershipFact {
	t.Helper()
	result, err := conn.Query(ctx,
		`SELECT team_id, repo_full_name, toString(source) FROM team_repo_ownership FINAL `+
			`WHERE org_id = ? AND provider = ? AND (valid_to IS NULL OR valid_to > now64(3, 'UTC'))`,
		orgID, provider)
	if err != nil {
		t.Fatal(err)
	}
	defer result.Close()
	var out []repoOwnershipFact
	for result.Next() {
		var fact repoOwnershipFact
		if err := result.Scan(&fact.TeamID, &fact.Repo, &fact.Source); err != nil {
			t.Fatal(err)
		}
		out = append(out, fact)
	}
	if err := result.Err(); err != nil {
		t.Fatal(err)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].TeamID+out[i].Repo+out[i].Source < out[j].TeamID+out[j].Repo+out[j].Source
	})
	return out
}

func countRepoOwnershipRows(ctx context.Context, t *testing.T, conn driver.Conn, orgID string) uint64 {
	t.Helper()
	var count uint64
	if err := conn.QueryRow(ctx, `SELECT count() FROM team_repo_ownership FINAL WHERE org_id = ?`, orgID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func requireRepoFacts(t *testing.T, label string, got []repoOwnershipFact, want ...repoOwnershipFact) {
	t.Helper()
	sort.Slice(want, func(i, j int) bool {
		return want[i].TeamID+want[i].Repo+want[i].Source < want[j].TeamID+want[j].Repo+want[j].Source
	})
	if len(got) != len(want) {
		t.Fatalf("%s: open rows = %+v, want %+v", label, got, want)
	}
	for index := range got {
		if got[index] != want[index] {
			t.Fatalf("%s: open rows = %+v, want %+v", label, got, want)
		}
	}
}

func githubSnapshotRun(
	ctx context.Context, t *testing.T, conn driver.Conn, orgID string, paths map[string]string, statuses map[string]int,
	selections TeamCatalogSelections, at time.Time,
) (TeamCatalogResult, error) {
	t.Helper()
	doer := &githubTeamCatalogFixtureDoer{t: t, byPath: paths, statuses: statuses}
	adapter := GitHubTeamCatalogCollector{Sink: GitHubTeamCatalogClickHouseEffects{Conn: conn}}
	credential := providerfoundation.Credential{Provider: "github", Config: map[string]string{"org": "acme"}}
	client := githubTeamCatalogAdapterClient(t, doer)
	return adapter.CollectTeamCatalog(ctx, TeamCatalogReference{OrgID: orgID, SyncRunID: "run"}, credential, client, selections, at)
}

func TestGitHubTeamCatalogClosesProviderAccessRowsGitHubNoLongerReturns(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	sink := GitHubTeamCatalogClickHouseEffects{Conn: conn}
	t0 := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	teamsOnly := TeamCatalogSelections{Teams: true}
	platform := repoOwnershipFact{TeamID: "gh:platform", Repo: "acme/api", Source: "provider_access"}
	platformWeb := repoOwnershipFact{TeamID: "gh:platform", Repo: "acme/web", Source: "provider_access"}
	ops := repoOwnershipFact{TeamID: "gh:ops", Repo: "acme/infra", Source: "provider_access"}
	twoTeams := map[string]string{
		"/orgs/acme/teams":                `[{"slug":"platform","name":"Platform"},{"slug":"ops","name":"Ops"}]`,
		"/orgs/acme/teams/platform/repos": `[{"name":"api"},{"name":"web"}]`,
		"/orgs/acme/teams/ops/repos":      `[{"name":"infra"}]`,
	}

	t.Run("a repo dropped from the listing is closed and the rest stay open", func(t *testing.T) {
		org := "snap-dropped"
		if _, err := githubSnapshotRun(ctx, t, conn, org, twoTeams, nil, teamsOnly, t0); err != nil {
			t.Fatal(err)
		}
		requireRepoFacts(t, "after seed", openRepoOwnership(ctx, t, conn, org, "github"), platform, platformWeb, ops)
		next := map[string]string{
			"/orgs/acme/teams":                twoTeams["/orgs/acme/teams"],
			"/orgs/acme/teams/platform/repos": `[{"name":"api"}]`,
			"/orgs/acme/teams/ops/repos":      `[{"name":"infra"}]`,
		}
		result, err := githubSnapshotRun(ctx, t, conn, org, next, nil, teamsOnly, t0.Add(time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		if result.RepoOwnershipWritten != 2 {
			t.Fatalf("RepoOwnershipWritten = %d, want the 2 grants GitHub returned", result.RepoOwnershipWritten)
		}
		requireRepoFacts(t, "after drop", openRepoOwnership(ctx, t, conn, org, "github"), platform, ops)
	})

	t.Run("a listed team with no repo at all has its row closed", func(t *testing.T) {
		org := "snap-empty-team"
		if _, err := githubSnapshotRun(ctx, t, conn, org, twoTeams, nil, teamsOnly, t0); err != nil {
			t.Fatal(err)
		}
		next := map[string]string{
			"/orgs/acme/teams":                twoTeams["/orgs/acme/teams"],
			"/orgs/acme/teams/platform/repos": twoTeams["/orgs/acme/teams/platform/repos"],
			"/orgs/acme/teams/ops/repos":      `[]`,
		}
		if _, err := githubSnapshotRun(ctx, t, conn, org, next, nil, teamsOnly, t0.Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
		requireRepoFacts(t, "after empty listing", openRepoOwnership(ctx, t, conn, org, "github"), platform, platformWeb)
	})

	t.Run("a failed repo listing closes nothing", func(t *testing.T) {
		org := "snap-failed-listing"
		if _, err := githubSnapshotRun(ctx, t, conn, org, twoTeams, nil, teamsOnly, t0); err != nil {
			t.Fatal(err)
		}
		next := map[string]string{
			"/orgs/acme/teams":                twoTeams["/orgs/acme/teams"],
			"/orgs/acme/teams/platform/repos": `[{"name":"api"}]`,
			"/orgs/acme/teams/ops/repos":      `[]`,
		}
		statuses := map[string]int{"/orgs/acme/teams/ops/repos": 500}
		if _, err := githubSnapshotRun(ctx, t, conn, org, next, statuses, teamsOnly, t0.Add(time.Hour)); err == nil {
			t.Fatal("a failed repo listing must fail the run")
		}
		requireRepoFacts(t, "after failed listing", openRepoOwnership(ctx, t, conn, org, "github"), platform, platformWeb, ops)
	})

	t.Run("a run that listed no team closes nothing", func(t *testing.T) {
		org := "snap-no-teams"
		if _, err := githubSnapshotRun(ctx, t, conn, org, twoTeams, nil, teamsOnly, t0); err != nil {
			t.Fatal(err)
		}
		if _, err := githubSnapshotRun(ctx, t, conn, org, map[string]string{"/orgs/acme/teams": `[]`}, nil, teamsOnly, t0.Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
		requireRepoFacts(t, "after empty team list", openRepoOwnership(ctx, t, conn, org, "github"), platform, platformWeb, ops)
	})

	t.Run("the only listed team has no repo: its row is closed", func(t *testing.T) {
		org := "snap-only-team-empty"
		single := map[string]string{
			"/orgs/acme/teams":                `[{"slug":"ops-team","name":"Ops Team"}]`,
			"/orgs/acme/teams/ops-team/repos": `[{"name":"acr"}]`,
		}
		if _, err := githubSnapshotRun(ctx, t, conn, org, single, nil, teamsOnly, t0); err != nil {
			t.Fatal(err)
		}
		requireRepoFacts(t, "after seed", openRepoOwnership(ctx, t, conn, org, "github"),
			repoOwnershipFact{TeamID: "gh:ops-team", Repo: "acme/acr", Source: "provider_access"})
		single["/orgs/acme/teams/ops-team/repos"] = `[]`
		result, err := githubSnapshotRun(ctx, t, conn, org, single, nil, teamsOnly, t0.Add(time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		if result.RepoOwnershipWritten != 0 {
			t.Fatalf("RepoOwnershipWritten = %d, want 0", result.RepoOwnershipWritten)
		}
		requireRepoFacts(t, "after GitHub returned no repo", openRepoOwnership(ctx, t, conn, org, "github"))
	})

	t.Run("a team the run did not list keeps its rows", func(t *testing.T) {
		org := "snap-unlisted-team"
		if _, err := githubSnapshotRun(ctx, t, conn, org, twoTeams, nil, teamsOnly, t0); err != nil {
			t.Fatal(err)
		}
		paths := map[string]string{
			"/orgs/acme/teams":                `[{"slug":"platform","name":"Platform"}]`,
			"/orgs/acme/teams/platform/repos": `[{"name":"api"}]`,
		}
		if _, err := githubSnapshotRun(ctx, t, conn, org, paths, nil, teamsOnly, t0.Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
		requireRepoFacts(t, "after a run that listed only platform", openRepoOwnership(ctx, t, conn, org, "github"), platform, ops)
	})

	t.Run("a failed read of the open rows fails the run and writes nothing", func(t *testing.T) {
		org := "snap-read-fails"
		if _, err := githubSnapshotRun(ctx, t, conn, org, twoTeams, nil, teamsOnly, t0); err != nil {
			t.Fatal(err)
		}
		before := countRepoOwnershipRows(ctx, t, conn, org)
		failing := GitHubTeamCatalogCollector{Sink: GitHubTeamCatalogClickHouseEffects{Conn: openRowsReadFailingConn{Conn: conn}}}
		next := map[string]string{
			"/orgs/acme/teams":                twoTeams["/orgs/acme/teams"],
			"/orgs/acme/teams/platform/repos": `[{"name":"api"}]`,
			"/orgs/acme/teams/ops/repos":      `[]`,
		}
		doer := &githubTeamCatalogFixtureDoer{t: t, byPath: next}
		_, err := failing.CollectTeamCatalog(ctx, TeamCatalogReference{OrgID: org, SyncRunID: "run"},
			providerfoundation.Credential{Provider: "github", Config: map[string]string{"org": "acme"}},
			githubTeamCatalogAdapterClient(t, doer), teamsOnly, t0.Add(time.Hour))
		if err == nil {
			t.Fatal("a failed read of the open rows must fail the run")
		}
		requireRepoFacts(t, "after failed read", openRepoOwnership(ctx, t, conn, org, "github"), platform, platformWeb, ops)
		if after := countRepoOwnershipRows(ctx, t, conn, org); after != before {
			t.Fatalf("rows written despite a failed read: %d -> %d", before, after)
		}
	})

	t.Run("a members-only run lists no repo and closes nothing", func(t *testing.T) {
		org := "snap-members-only"
		if _, err := githubSnapshotRun(ctx, t, conn, org, twoTeams, nil, teamsOnly, t0); err != nil {
			t.Fatal(err)
		}
		paths := map[string]string{
			"/orgs/acme/teams":                  twoTeams["/orgs/acme/teams"],
			"/orgs/acme/teams/platform/members": `[]`,
			"/orgs/acme/teams/ops/members":      `[]`,
		}
		if _, err := githubSnapshotRun(ctx, t, conn, org, paths, nil, TeamCatalogSelections{Members: true}, t0.Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
		requireRepoFacts(t, "after members-only run", openRepoOwnership(ctx, t, conn, org, "github"), platform, platformWeb, ops)
	})

	t.Run("another org, another source and another provider are untouched", func(t *testing.T) {
		org, other := "snap-scope-a", "snap-scope-b"
		// The other org holds the same grants under an OLDER valid_from: a read
		// that leaks across orgs would hand that stamp to this org's rows.
		if _, err := githubSnapshotRun(ctx, t, conn, other, twoTeams, nil, teamsOnly, t0.Add(-24*time.Hour)); err != nil {
			t.Fatal(err)
		}
		if _, err := githubSnapshotRun(ctx, t, conn, org, twoTeams, nil, teamsOnly, t0); err != nil {
			t.Fatal(err)
		}
		// A row of a still-returned grant (same team and repo) under every
		// other source, and one of another provider, all under an OLDER
		// valid_from and written by the derivation's own insert: a read that
		// takes them in would hand that stamp to the provider_access row.
		batch, err := conn.PrepareBatch(ctx, teamRepoOwnershipInsert)
		if err != nil {
			t.Fatal(err)
		}
		repoID := uuid.New()
		for _, row := range []struct{ provider, source string }{
			{"github", "inferred"}, {"github", "manual"}, {"github", "native"}, {"gitlab", "provider_access"},
		} {
			if err := batch.Append(org, row.provider, "gh:platform", &repoID, "acme/api", "exact", row.source,
				uint8(0), uint16(100), int32(300), t0.Add(-24*time.Hour), nil, t0.Add(-24*time.Hour)); err != nil {
				t.Fatal(err)
			}
		}
		if err := batch.Send(); err != nil {
			t.Fatal(err)
		}
		next := map[string]string{
			"/orgs/acme/teams":                twoTeams["/orgs/acme/teams"],
			"/orgs/acme/teams/platform/repos": `[{"name":"api"}]`,
			"/orgs/acme/teams/ops/repos":      `[{"name":"infra"}]`,
		}
		if _, err := githubSnapshotRun(ctx, t, conn, org, next, nil, teamsOnly, t0.Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
		requireRepoFacts(t, "github rows of the run's org", openRepoOwnership(ctx, t, conn, org, "github"),
			platform, ops,
			repoOwnershipFact{TeamID: "gh:platform", Repo: "acme/api", Source: "inferred"},
			repoOwnershipFact{TeamID: "gh:platform", Repo: "acme/api", Source: "manual"},
			repoOwnershipFact{TeamID: "gh:platform", Repo: "acme/api", Source: "native"})
		requireRepoFacts(t, "gitlab row", openRepoOwnership(ctx, t, conn, org, "gitlab"),
			repoOwnershipFact{TeamID: "gh:platform", Repo: "acme/api", Source: "provider_access"})
		requireRepoFacts(t, "other org", openRepoOwnership(ctx, t, conn, other, "github"), platform, platformWeb, ops)
		var stamp time.Time
		if err := conn.QueryRow(ctx,
			`SELECT min(valid_from) FROM team_repo_ownership FINAL WHERE org_id = ? AND provider = 'github' AND source = 'provider_access' AND team_id = 'gh:platform' AND repo_full_name = 'acme/api'`,
			org).Scan(&stamp); err != nil {
			t.Fatal(err)
		}
		if !stamp.Equal(t0) {
			t.Fatalf("this org's grant took valid_from %s from another org, source or provider, want %s", stamp, t0)
		}
	})

	t.Run("a repeat run adds no row and keeps every first valid_from", func(t *testing.T) {
		org := "snap-idempotent"
		if _, err := githubSnapshotRun(ctx, t, conn, org, twoTeams, nil, teamsOnly, t0); err != nil {
			t.Fatal(err)
		}
		before := countRepoOwnershipRows(ctx, t, conn, org)
		for run := 1; run <= 2; run++ {
			if _, err := githubSnapshotRun(ctx, t, conn, org, twoTeams, nil, teamsOnly, t0.Add(time.Duration(run)*time.Hour)); err != nil {
				t.Fatal(err)
			}
		}
		requireRepoFacts(t, "after repeats", openRepoOwnership(ctx, t, conn, org, "github"), platform, platformWeb, ops)
		var earliest, latest time.Time
		if err := conn.QueryRow(ctx,
			`SELECT min(valid_from), max(valid_from) FROM team_repo_ownership FINAL WHERE org_id = ?`, org,
		).Scan(&earliest, &latest); err != nil {
			t.Fatal(err)
		}
		if !earliest.Equal(t0) || !latest.Equal(t0) {
			t.Fatalf("valid_from moved on a repeat run: min=%s max=%s want %s", earliest, latest, t0)
		}
		if after := countRepoOwnershipRows(ctx, t, conn, org); after != before {
			t.Fatalf("row count moved on a repeat run: %d -> %d", before, after)
		}
	})

	t.Run("an older duplicate open row of a held grant is closed", func(t *testing.T) {
		org := "snap-duplicate"
		// The writer of before this change stamped each run's own valid_from.
		for _, at := range []time.Time{t0, t0.Add(time.Hour)} {
			row, err := normalizeGitHubTeamRepoOwnership(org, "platform", "acme/api", at)
			if err != nil {
				t.Fatal(err)
			}
			if err := sink.WriteTeamRepoOwnership(ctx, org, []githubTeamRepoOwnershipRow{row}); err != nil {
				t.Fatal(err)
			}
		}
		paths := map[string]string{
			"/orgs/acme/teams":                `[{"slug":"platform","name":"Platform"}]`,
			"/orgs/acme/teams/platform/repos": `[{"name":"api"}]`,
		}
		if _, err := githubSnapshotRun(ctx, t, conn, org, paths, nil, teamsOnly, t0.Add(2*time.Hour)); err != nil {
			t.Fatal(err)
		}
		var open uint64
		if err := conn.QueryRow(ctx,
			`SELECT count() FROM team_repo_ownership FINAL WHERE org_id = ? AND (valid_to IS NULL OR valid_to > now64(3, 'UTC'))`, org,
		).Scan(&open); err != nil {
			t.Fatal(err)
		}
		if open != 1 {
			t.Fatalf("open rows for one held grant = %d, want 1", open)
		}
	})
}

// openRowsReadFailingConn fails only the read of open team_repo_ownership
// rows; every write and every other read goes to the real connection.
type openRowsReadFailingConn struct {
	driver.Conn
}

func (conn openRowsReadFailingConn) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	if strings.Contains(query, "FROM team_repo_ownership FINAL") {
		return nil, errors.New("injected team_repo_ownership read failure")
	}
	return conn.Conn.Query(ctx, query, args...)
}
