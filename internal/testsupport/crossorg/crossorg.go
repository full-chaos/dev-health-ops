// Package crossorg seeds two organisations that share one repository id, on a
// real migrated ClickHouse, for the org-binding regression tests (CHAOS-7239).
//
// WHY TWO ORGS SHARE A REPO ID. Provider sync mints a repository id from the
// repository's full name alone (sha256 of lower(trim(full_name)) in
// internal/providersync/github_repository_route.go), not from (org, name).
// Two organisations that sync the same repository therefore write the same
// repo_id. Every raw and daily table keys its rows on (org_id, repo_id, ...)
// since migration 027, so each org keeps its own rows under that shared id,
// and the repos table holds one row per org for it.
//
// A reader that binds org_id on only ONE side of a repo_id join or filter
// then reads the other org's rows: `fact.repo_id IN (SELECT id FROM repos
// WHERE org_id = A)` admits org B's fact rows, and `JOIN repos ON repos.id =
// fact.repo_id` with no repos.org_id joins org B's repos row too. The fixture
// here is the smallest data that makes either shape visible: one shared repo
// id, one row per org on each side, and distinct values per org so a leaked
// row changes the answer.
package crossorg

import (
	"context"
	"testing"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/quadrant"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chquery"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// Fixture names the two orgs, the repository id they share, and one person
// who contributes to both orgs' copy of that repository.
type Fixture struct {
	OrgA     string
	OrgB     string
	RepoID   string
	RepoName string
	// Identity is the same author email in both orgs, as a real contributor
	// to a shared open-source repository would be.
	Identity string
}

// Default is the fixture every CHAOS-7239 test uses.
func Default() Fixture {
	return Fixture{
		OrgA:     "org-a-chaos-7239",
		OrgB:     "org-b-chaos-7239",
		RepoID:   "72390000-0000-4000-8000-000000000001",
		RepoName: "acme/shared",
		Identity: "dev@shared.example",
	}
}

// PersonID is the people/heatmap API's person id for f.Identity, derived by
// the production helper rather than a second copy of its hash.
func (f Fixture) PersonID() string {
	return quadrant.PersonIDForIdentity(f.Identity)
}

// Start runs a real ClickHouse, migrates it to the chain's head, and returns
// an admin connection for seeding and a query client built with production
// defaults.
func Start(ctx context.Context, t *testing.T) (stdclickhouse.Conn, *dhclickhouse.Client) {
	t.Helper()
	ch, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("crossorg: start ClickHouse: %v", err)
	}
	t.Cleanup(func() { _ = ch.Close(context.Background()) })

	chschema.Apply(ctx, t, ch)

	options, err := stdclickhouse.ParseDSN(ch.URI)
	if err != nil {
		t.Fatalf("crossorg: parse ClickHouse DSN: %v", err)
	}
	admin, err := stdclickhouse.Open(options)
	if err != nil {
		t.Fatalf("crossorg: open admin connection: %v", err)
	}
	t.Cleanup(func() { _ = admin.Close() })

	client, err := chquery.NewProductionClient(ch.URI)
	if err != nil {
		t.Fatalf("crossorg: construct query client: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return admin, client
}

// SeedRepos writes the shared repository once per org, under the same id and
// name -- what two orgs syncing the same repository produce.
func SeedRepos(ctx context.Context, t *testing.T, admin stdclickhouse.Conn, f Fixture) {
	t.Helper()
	for _, org := range []string{f.OrgA, f.OrgB} {
		Exec(ctx, t, admin, `
            INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced)
            VALUES (?, ?, 'github', ?, now64(3), now64(3))`,
			f.RepoID, f.RepoName, org)
	}
}

// Exec runs one seeding statement and fails the test on error.
func Exec(ctx context.Context, t *testing.T, admin stdclickhouse.Conn, statement string, args ...any) {
	t.Helper()
	if err := admin.Exec(ctx, statement, args...); err != nil {
		t.Fatalf("crossorg: seed: %v", err)
	}
}
