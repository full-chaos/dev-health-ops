//go:build integration

package admin

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
)

// narrowedPurge is a purge on the API role as the memberships REVOKE leaves it:
// SELECT on memberships, no INSERT, UPDATE or DELETE.
type narrowedPurge struct {
	admin *pgxpool.Pool
	h     *handlers
}

func newNarrowedPurge(t *testing.T, ctx context.Context) *narrowedPurge {
	t.Helper()
	pg, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pg.Close(context.Background()) })
	admin, err := pgxpool.New(ctx, pg.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	pgschema.Apply(ctx, t, admin)
	role, err := containers.RoleName("purge_api", pg)
	if err != nil {
		t.Fatal(err)
	}
	const password = "purge-test-only-not-a-credential"
	for _, statement := range []string{
		fmt.Sprintf(`CREATE ROLE %q LOGIN PASSWORD '%s'`, role, password),
		fmt.Sprintf(`GRANT USAGE ON SCHEMA public TO %q`, role),
		fmt.Sprintf(`GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO %q`, role),
		fmt.Sprintf(`REVOKE INSERT, UPDATE, DELETE ON memberships FROM %q`, role),
	} {
		if _, err := admin.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	t.Cleanup(func() { containers.DropRole(admin, role, t.Logf) })
	cfg := admin.Config().Copy()
	cfg.ConnConfig.User, cfg.ConnConfig.Password = role, password
	narrowed, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(narrowed.Close)
	return &narrowedPurge{admin: admin, h: &handlers{store: pgStore{Pool: narrowed, Now: time.Now}}}
}

func (p *narrowedPurge) seed(t *testing.T, ctx context.Context) uuid.UUID {
	t.Helper()
	org, user := uuid.New(), uuid.New()
	pgseed.User(ctx, t, p.admin, user.String())
	pgseed.Org(ctx, t, p.admin, org.String(), "community")
	pgseed.Membership(ctx, t, p.admin, uuid.NewString(), user.String(), org.String(), "member")
	return org
}

func (p *narrowedPurge) wantGone(t *testing.T, ctx context.Context, org uuid.UUID) {
	t.Helper()
	var left int
	if err := p.admin.QueryRow(ctx, `SELECT (SELECT count(*) FROM organizations WHERE id = $1) + (SELECT count(*) FROM memberships WHERE org_id = $1)`,
		org).Scan(&left); err != nil {
		t.Fatal(err)
	}
	if left != 0 {
		t.Fatalf("%d rows left after the purge, want none", left)
	}
}

func TestOrgPurgeTakesNoMembershipLockWhileOwned(t *testing.T) {
	t.Setenv("AUTH_ORGS_OWNED", "true")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	p := newNarrowedPurge(t, ctx)
	org := p.seed(t, ctx)
	result := &orgDeletionResult{postgresTableCount: map[string]int64{"memberships": 1, "organizations": 1}}
	if err := p.h.orgDeletionPurgePostgres(ctx, org, org.String(), result); err != nil {
		t.Fatalf("purge on the narrowed role: %v", err)
	}
	p.wantGone(t, ctx, org)
}

// The membership appeared after the count: the org row's cascade takes it.
func TestOrgPurgeCascadeTakesALateMembership(t *testing.T) {
	t.Setenv("AUTH_ORGS_OWNED", "true")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	p := newNarrowedPurge(t, ctx)
	org := p.seed(t, ctx)
	result := &orgDeletionResult{postgresTableCount: map[string]int64{"organizations": 1}}
	if err := p.h.orgDeletionPurgePostgres(ctx, org, org.String(), result); err != nil {
		t.Fatalf("purge on the narrowed role: %v", err)
	}
	p.wantGone(t, ctx, org)
}
