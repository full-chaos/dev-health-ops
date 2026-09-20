//go:build integration

package busfactor

import (
	"context"
	"fmt"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

var ddls = []string{
	`CREATE TABLE repos (id UUID, repo String, provider String DEFAULT 'unknown', org_id String DEFAULT 'default',
      created_at DateTime64(3, 'UTC'), last_synced DateTime64(3, 'UTC')) ENGINE = ReplacingMergeTree(last_synced) ORDER BY id`,
	`CREATE TABLE git_commits (repo_id UUID, hash String, author_name Nullable(String), author_email Nullable(String),
      committer_when DateTime64(3, 'UTC'), org_id String DEFAULT 'default', last_synced DateTime64(3, 'UTC'))
      ENGINE = ReplacingMergeTree(last_synced) ORDER BY (repo_id, hash)`,
	`CREATE TABLE git_commit_stats (repo_id UUID, commit_hash String, file_path String, additions Int32, deletions Int32,
      org_id String DEFAULT 'default', last_synced DateTime64(3, 'UTC'))
      ENGINE = ReplacingMergeTree(last_synced) ORDER BY (repo_id, commit_hash, file_path)`,
	`CREATE TABLE team_repo_ownership (org_id String, provider String, team_id String, repo_id Nullable(UUID),
      repo_full_name String, match_type Enum8('exact' = 1, 'pattern' = 2),
      source Enum8('native' = 1, 'jira_legacy' = 2, 'provider_access' = 3, 'manual' = 4, 'inferred' = 5),
      is_primary UInt8 DEFAULT 0, specificity UInt16 DEFAULT 0, priority Int32 DEFAULT 0,
      valid_from DateTime64(3, 'UTC'), valid_to Nullable(DateTime64(3, 'UTC')), updated_at DateTime64(3, 'UTC'))
      ENGINE = ReplacingMergeTree(updated_at) ORDER BY (org_id, provider, repo_full_name, team_id, source, valid_from)`,
}

const (
	rA = "11111111-1111-1111-1111-111111111111"
	rB = "22222222-2222-2222-2222-222222222222"
	rC = "33333333-3333-3333-3333-333333333333" // no repos row
	rX = "44444444-4444-4444-4444-444444444444" // org-2
)

func startStore(t *testing.T) (context.Context, stdclickhouse.Conn, QueryClient) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	t.Cleanup(cancel)
	inst, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse: %v", err)
	}
	t.Cleanup(func() { _ = inst.Close(context.Background()) })
	opts, err := stdclickhouse.ParseDSN(inst.URI)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := stdclickhouse.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	for _, d := range ddls {
		if err := conn.Exec(ctx, d); err != nil {
			t.Fatalf("ddl: %v", err)
		}
	}
	client, err := clickhouse.NewClickHouseQueryClientWithOptions(clickhouse.Options{DSN: inst.URI})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return ctx, conn, client
}

func exec(t *testing.T, ctx context.Context, conn stdclickhouse.Conn, q string, a ...any) {
	t.Helper()
	if err := conn.Exec(ctx, fmt.Sprintf(q, a...)); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
}

func repo(t *testing.T, ctx context.Context, conn stdclickhouse.Conn, org, id, name string) {
	exec(t, ctx, conn, `INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) SELECT '%s', '%s', 'github', '%s', now64(3), now64(3)`, id, name, org)
}

// commit seeds one commit with one stat row.
func commit(t *testing.T, ctx context.Context, conn stdclickhouse.Conn, org, repoID, hash, email, name string, add, del int) {
	q := func(s string) string {
		if s == "" {
			return "NULL"
		}
		return "'" + s + "'"
	}
	exec(t, ctx, conn, `INSERT INTO git_commits (repo_id, hash, author_name, author_email, committer_when, org_id, last_synced) SELECT '%s', '%s', %s, %s, now64(3), '%s', now64(3)`, repoID, hash, q(name), q(email), org)
	exec(t, ctx, conn, `INSERT INTO git_commit_stats (repo_id, commit_hash, file_path, additions, deletions, org_id, last_synced) SELECT '%s', '%s', 'f.go', %d, %d, '%s', now64(3)`, repoID, hash, add, del, org)
}

func own(t *testing.T, ctx context.Context, conn stdclickhouse.Conn, org, team, repoID, fullName string) {
	exec(t, ctx, conn, `INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, valid_from, updated_at)
      SELECT '%s', 'github', '%s', %s, '%s', 'exact', 'native', toDateTime64('2020-01-01 00:00:00', 3, 'UTC'), now64(3)`, org, team, repoID, fullName)
}

func seed(t *testing.T, ctx context.Context, conn stdclickhouse.Conn) {
	repo(t, ctx, conn, "org-1", rA, "acme/web")
	repo(t, ctx, conn, "org-1", rB, "acme/api")
	repo(t, ctx, conn, "org-2", rX, "other/secret")
	// rA: alice 80, bob 20 -> value 1. rB: even 3 authors -> value 3.
	commit(t, ctx, conn, "org-1", rA, "a1", "alice@x", "", 60, 20)
	commit(t, ctx, conn, "org-1", rA, "a2", "bob@x", "", 10, 10)
	commit(t, ctx, conn, "org-1", rB, "b1", "alice@x", "", 10, 0)
	commit(t, ctx, conn, "org-1", rB, "b2", "", "Carol", 5, 5)
	commit(t, ctx, conn, "org-1", rB, "b3", "", "", 10, 0)
	// rC has commits but no repos row: name falls back to the id.
	commit(t, ctx, conn, "org-1", rC, "c1", "dave@x", "", 4, 0)
	// org-2 must never appear for org-1.
	commit(t, ctx, conn, "org-2", rX, "x1", "eve@x", "", 999, 0)
	// team-1 owns only rA; alice (a member by activity) also authored in rB.
	own(t, ctx, conn, "org-1", "team-1", "'"+rA+"'", "acme/web")
	own(t, ctx, conn, "org-2", "team-1", "'"+rX+"'", "other/secret")
}

func TestRealClickHouse_BusFactor(t *testing.T) {
	ctx, conn, client := startStore(t)
	seed(t, ctx, conn)
	now := time.Now().UTC()
	names := func(b *model.BusFactor) []string {
		var out []string
		for _, r := range b.Repos {
			out = append(out, fmt.Sprintf("%s=%d/%d", r.RepoName, r.Value, r.EvidenceSampleCount))
		}
		return out
	}
	eq := func(name string, got, want []string) {
		t.Helper()
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Errorf("%s: got %v want %v", name, got, want)
		}
	}
	sp := func(s string) *string { return &s }

	all, err := Resolve(ctx, client, "org-1", nil, now)
	if err != nil {
		t.Fatal(err)
	}
	// value ascending then name: <rC id>(1), acme/web(1), acme/api(3). rC has no repos row: name is its id.
	// (unset join columns read as '' on the driver defaults, so the id fallback applies)
	eq("unscoped repos", names(all), []string{rC + "=1/1", "acme/web=1/2", "acme/api=3/3"})
	// org total: alice 80+10=90 of 130... compute: alice 90, bob 20, Carol 10, unknown 10, dave 4 => total 134; 80% = 107.2; alice 90 + bob 20 = 110 -> 2
	if all.Value != 2 || all.EvidenceSampleCount != 6 || all.Scope.RepoID != nil || all.Scope.TeamID != nil || all.OrgID != "org-1" {
		t.Errorf("unscoped: %#v", all)
	}
	if len(all.TopMaintainers) != 5 || all.TopMaintainers[0].Author != "alice@x" {
		t.Errorf("top: %#v", all.TopMaintainers)
	}

	byRepo, err := Resolve(ctx, client, "org-1", &model.BusFactorScopeInput{RepoID: sp(rB)}, now)
	if err != nil {
		t.Fatal(err)
	}
	eq("repo scope", names(byRepo), []string{"acme/api=3/3"})
	if byRepo.Value != 3 || *byRepo.Scope.RepoID != rB {
		t.Errorf("repo scope: %#v", byRepo)
	}

	team, err := Resolve(ctx, client, "org-1", &model.BusFactorScopeInput{TeamID: sp("team-1")}, now)
	if err != nil {
		t.Fatal(err)
	}
	eq("team scope = owned repos only", names(team), []string{"acme/web=1/2"})

	both, err := Resolve(ctx, client, "org-1", &model.BusFactorScopeInput{TeamID: sp("team-1"), RepoID: sp(rA)}, now)
	if err != nil {
		t.Fatal(err)
	}
	eq("team and owned repo", names(both), []string{"acme/web=1/2"})
	notOwned, err := Resolve(ctx, client, "org-1", &model.BusFactorScopeInput{TeamID: sp("team-1"), RepoID: sp(rB)}, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(notOwned.Repos) != 0 || notOwned.Value != 0 || notOwned.EvidenceSampleCount != 0 {
		t.Errorf("repo outside the team: %#v", notOwned)
	}
	unknownTeam, err := Resolve(ctx, client, "org-1", &model.BusFactorScopeInput{TeamID: sp("no-such-team")}, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(unknownTeam.Repos) != 0 || unknownTeam.Value != 0 {
		t.Errorf("unknown team: %#v", unknownTeam)
	}
	// The same team id in another org owns another org's repo only.
	otherOrg, err := Resolve(ctx, client, "org-2", &model.BusFactorScopeInput{TeamID: sp("team-1")}, now)
	if err != nil {
		t.Fatal(err)
	}
	eq("org-2 team", names(otherOrg), []string{"other/secret=1/1"})
	// A repo id from another org is invisible.
	foreign, err := Resolve(ctx, client, "org-1", &model.BusFactorScopeInput{RepoID: sp(rX)}, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(foreign.Repos) != 0 {
		t.Errorf("foreign repo: %#v", foreign)
	}
	// Malformed repo id serves unscoped.
	bad, err := Resolve(ctx, client, "org-1", &model.BusFactorScopeInput{RepoID: sp("nope")}, now)
	if err != nil {
		t.Fatal(err)
	}
	if bad.EvidenceSampleCount != 6 || bad.Scope.RepoID != nil {
		t.Errorf("malformed repo id: %#v", bad)
	}
	// Ownership that has expired before the request instant owns nothing.
	past, err := Resolve(ctx, client, "org-1", &model.BusFactorScopeInput{TeamID: sp("team-1")}, time.Date(2019, 1, 1, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if len(past.Repos) != 0 {
		t.Errorf("before valid_from: %#v", past)
	}
}

// The same repository id exists in two orgs with different names and
// different commit evidence: each org sees only its own.
func TestRealClickHouse_SameRepoIDInTwoOrgs(t *testing.T) {
	ctx, conn, client := startStore(t)
	repo(t, ctx, conn, "org-1", rA, "acme/web")
	repo(t, ctx, conn, "org-2", rA, "other/web")
	commit(t, ctx, conn, "org-1", rA, "h1", "alice@x", "", 10, 0)
	commit(t, ctx, conn, "org-2", rA, "h1", "eve@x", "", 90, 0)
	commit(t, ctx, conn, "org-2", rA, "h2", "mallory@x", "", 80, 0)
	own(t, ctx, conn, "org-2", "team-x", "'"+rA+"'", "other/web")
	now := time.Now().UTC()
	for _, c := range []struct {
		org, name, top string
		evidence       int
	}{{"org-1", "acme/web", "alice@x", 1}, {"org-2", "other/web", "eve@x", 2}} {
		got, err := Resolve(ctx, client, c.org, nil, now)
		if err != nil {
			t.Fatal(err)
		}
		if len(got.Repos) != 1 || got.Repos[0].RepoName != c.name || got.EvidenceSampleCount != c.evidence || got.TopMaintainers[0].Author != c.top {
			t.Errorf("%s: %#v", c.org, got)
		}
	}
	team := "team-x"
	other, err := Resolve(ctx, client, "org-1", &model.BusFactorScopeInput{TeamID: &team}, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(other.Repos) != 0 {
		t.Errorf("org-1 resolved org-2's team ownership: %#v", other.Repos)
	}
}
