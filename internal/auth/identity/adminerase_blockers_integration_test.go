//go:build integration

package identity

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/audit"
)

// The four auth keys that name a user principal with ON DELETE RESTRICT each refuse the force erase with
// ErrReferenceBlocked and write nothing. The refusal is the accepted known-stricter row of ci/auth_exception_ledger.tsv.
func TestAdminForceDeleteIsRefusedByEachRestrictKeyIntoThePrincipal(t *testing.T) {
	for _, c := range []struct {
		name string
		seed func(t *testing.T, ctx context.Context, e *env, tx seedTx, user, other, org uuid.UUID)
	}{
		{"platform role granted by the user", func(t *testing.T, ctx context.Context, e *env, tx seedTx, user, other, org uuid.UUID) {
			tx.exec(t, ctx, `INSERT INTO auth.platform_role_assignments (principal_id, role_key, granted_by) VALUES ($1, 'platform_admin', $2)`, other, user)
		}},
		{"active grant held by the user", func(t *testing.T, ctx context.Context, e *env, tx seedTx, user, other, org uuid.UUID) {
			tx.exec(t, ctx, `INSERT INTO auth.resource_grants (principal_id, organization_id, role_key, resource_kind, resource_id)
VALUES ($1, $2, 'viewer', 'repository', 'acme/repo')`, user, org)
		}},
		{"revoked grant held by the user", func(t *testing.T, ctx context.Context, e *env, tx seedTx, user, other, org uuid.UUID) {
			tx.exec(t, ctx, `INSERT INTO auth.resource_grants (principal_id, organization_id, role_key, resource_kind, resource_id, revoked_at, revoked_by)
VALUES ($1, $2, 'viewer', 'repository', 'acme/repo', now(), $3)`, user, org, other)
		}},
		{"grant revoked by the user", func(t *testing.T, ctx context.Context, e *env, tx seedTx, user, other, org uuid.UUID) {
			tx.exec(t, ctx, `INSERT INTO auth.resource_grants (principal_id, organization_id, role_key, resource_kind, resource_id, revoked_at, revoked_by)
VALUES ($1, $2, 'viewer', 'repository', 'acme/repo', now(), $3)`, other, org, user)
		}},
		{"service account owned by the user", func(t *testing.T, ctx context.Context, e *env, tx seedTx, user, other, org uuid.UUID) {
			sa := uuid.New()
			tx.exec(t, ctx, `INSERT INTO auth.principals (id, kind) VALUES ($1, 'service_account')`, sa)
			tx.exec(t, ctx, `INSERT INTO auth.service_accounts (principal_id, organization_id, name, owner_principal_id) VALUES ($1, $2, $3, $4)`, sa, org, "sa-"+sa.String(), user)
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			e := getEnv(t)
			ctx := context.Background()
			none := func(context.Context, audit.TxOps) error { return nil }
			actor := e.seedUser(t, ctx, uniqueEmail("blk-actor"), true, true)
			e.project(t, ctx, actor)
			created, err := e.owned.AdminCreateUser(ctx, AdminCreateRequest{Email: uniqueEmail("blk"), Actor: actor, Authorize: none})
			if err != nil {
				t.Fatal(err)
			}
			org := uuid.New()
			tx := beginSeed(t, ctx, e)
			tx.exec(t, ctx, `INSERT INTO auth.organizations (id, slug, name) VALUES ($1, $2, 'blk')`, org, "blk-"+org.String())
			c.seed(t, ctx, e, tx, created.ID, actor, org)
			tx.commit(t, ctx)
			var before int
			if err := e.admin.QueryRow(ctx, `SELECT count(*) FROM auth.security_audit_events WHERE subject_principal_id = $1`, created.ID).Scan(&before); err != nil {
				t.Fatal(err)
			}
			_, err = e.owned.AdminForceDeleteUser(ctx, AdminForceDeleteRequest{Principal: created.ID, Actor: actor, Authorize: none, RevokeRoles: none})
			if !errors.Is(err, ErrReferenceBlocked) {
				t.Fatalf("erase: %v, want ErrReferenceBlocked", err)
			}
			var principals, publicUsers, after, erased int
			if err := e.admin.QueryRow(ctx, `SELECT (SELECT count(*) FROM auth.principals WHERE id = $1), (SELECT count(*) FROM public.users WHERE id = $1),
(SELECT count(*) FROM auth.security_audit_events WHERE subject_principal_id = $1),
(SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.user.erased' AND attributes->>'target_id' = $1::text)`, created.ID).Scan(&principals, &publicUsers, &after, &erased); err != nil {
				t.Fatal(err)
			}
			if principals != 1 || publicUsers != 1 || after != before || erased != 0 {
				t.Errorf("after the refusal: principals %d, public.users %d, audit rows %d -> %d, erase rows %d; want 1, 1, unchanged, 0", principals, publicUsers, before, after, erased)
			}
		})
	}
}

// A user with none of the four keys (a membership-free user with a public impersonation row) is erased: the refusal
// above is the four keys, nothing wider.
func TestAdminForceDeleteWithNoRestrictKeyErases(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	none := func(context.Context, audit.TxOps) error { return nil }
	actor := e.seedUser(t, ctx, uniqueEmail("blk-ctl-actor"), true, true)
	e.project(t, ctx, actor)
	created, err := e.owned.AdminCreateUser(ctx, AdminCreateRequest{Email: uniqueEmail("blk-ctl"), Actor: actor, Authorize: none})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.owned.AdminForceDeleteUser(ctx, AdminForceDeleteRequest{Principal: created.ID, Actor: actor, Authorize: none, RevokeRoles: none}); err != nil {
		t.Fatalf("erase: %v", err)
	}
	var n int
	if err := e.admin.QueryRow(ctx, `SELECT count(*) FROM auth.principals WHERE id = $1`, created.ID).Scan(&n); err != nil || n != 0 {
		t.Errorf("principal rows %d (err %v), want 0", n, err)
	}
}

// seedTx seeds blocking rows as the table owner with the triggers off for this transaction only.
type seedTx struct{ tx pgx.Tx }

func beginSeed(t *testing.T, ctx context.Context, e *env) seedTx {
	t.Helper()
	tx, err := e.admin.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `SET LOCAL session_replication_role = replica`); err != nil {
		t.Fatal(err)
	}
	return seedTx{tx}
}

func (s seedTx) exec(t *testing.T, ctx context.Context, sql string, args ...any) {
	t.Helper()
	if _, err := s.tx.Exec(ctx, sql, args...); err != nil {
		t.Fatal(err)
	}
}

func (s seedTx) commit(t *testing.T, ctx context.Context) {
	t.Helper()
	if err := s.tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}
