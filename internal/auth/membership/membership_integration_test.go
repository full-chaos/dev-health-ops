//go:build integration

package membership

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/usersbackfill"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

type env struct {
	admin    *pgxpool.Pool
	runtime  *pgxpool.Pool
	store    *Store
	notOwned *Store
	identity *identity.Store
}

var (
	shared     *env
	sharedErr  error
	sharedOnce sync.Once
	closers    []func()
)

func TestMain(m *testing.M) {
	code := m.Run()
	for i := len(closers) - 1; i >= 0; i-- {
		closers[i]()
	}
	os.Exit(code)
}

func getEnv(t *testing.T) *env {
	t.Helper()
	sharedOnce.Do(func() { shared, sharedErr = newEnv(t) })
	if sharedErr != nil {
		t.Fatal(sharedErr)
	}
	return shared
}

func newEnv(t *testing.T) (*env, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		return nil, err
	}
	closers = append(closers, func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = instance.Close(closeCtx)
	})
	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		return nil, fmt.Errorf("admin pool: %w", err)
	}
	closers = append(closers, admin.Close)
	pgschema.Apply(ctx, t, admin)
	role, err := containers.RoleName("membership_runtime", instance)
	if err != nil {
		return nil, fmt.Errorf("derive role: %w", err)
	}
	const password = "membership-test-only-not-a-credential"
	if _, err := admin.Exec(ctx, fmt.Sprintf(`CREATE ROLE %q LOGIN PASSWORD '%s'`, role, password)); err != nil {
		return nil, fmt.Errorf("create role: %w", err)
	}
	closers = append(closers, func() { containers.DropRole(admin, role, func(string, ...any) {}) })
	if _, err := authschema.Apply(ctx, admin, authschema.Options{Schema: "auth", RuntimeRole: role, UsersOwned: true}); err != nil {
		return nil, fmt.Errorf("authschema.Apply: %w", err)
	}
	cfg := admin.Config().Copy()
	cfg.ConnConfig.User = role
	cfg.ConnConfig.Password = password
	runtime, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("runtime pool: %w", err)
	}
	closers = append(closers, runtime.Close)
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		return nil, err
	}
	store, err := New(Config{Pool: runtime, Schema: schema, OwnsUsers: true})
	if err != nil {
		return nil, err
	}
	notOwned, err := New(Config{Pool: runtime, Schema: schema})
	if err != nil {
		return nil, err
	}
	id, err := identity.New(identity.Config{Pool: runtime, Schema: schema, UsersOwnedByAuth: true})
	if err != nil {
		return nil, err
	}
	return &env{admin: admin, runtime: runtime, store: store, notOwned: notOwned, identity: id}, nil
}

func allowAll(context.Context, audit.TxOps, Locked) error { return nil }

func (e *env) seedUser(t *testing.T, ctx context.Context) uuid.UUID {
	t.Helper()
	id := uuid.New()
	if _, err := e.admin.Exec(ctx, `INSERT INTO public.users
	(id, email, password_hash, auth_provider, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, NULL, 'local', true, true, false, 0, now(), now())`, id, "member-"+id.String()[:8]+"@example.test"); err != nil {
		t.Fatalf("seed public.users: %v", err)
	}
	if err := e.identity.Project(ctx, id); err != nil {
		t.Fatalf("Project: %v", err)
	}
	return id
}

func (e *env) seedOrg(t *testing.T, ctx context.Context) uuid.UUID {
	t.Helper()
	id := uuid.New()
	if _, err := e.admin.Exec(ctx, `INSERT INTO public.organizations (id, slug, name) VALUES ($1, $2, 'membership-test')`,
		id, "org-"+id.String()[:8]); err != nil {
		t.Fatalf("seed public.organizations: %v", err)
	}
	if err := e.identity.ProjectOrganization(ctx, id); err != nil {
		t.Fatalf("ProjectOrganization: %v", err)
	}
	return id
}

func (e *env) add(t *testing.T, ctx context.Context, org, user uuid.UUID, role string) *Membership {
	t.Helper()
	m, err := e.store.Add(ctx, Change{OrgID: org, PrincipalID: user, Role: role, Authorize: allowAll})
	if err != nil {
		t.Fatalf("Add %s: %v", role, err)
	}
	return m
}

// rows is the pair in both tables: the public role ("" = no row) and the auth
// status and role ("" = no row or NULL).
type rows struct {
	publicRole, authStatus, authRole string
}

func (e *env) rows(t *testing.T, ctx context.Context, org, user uuid.UUID) rows {
	t.Helper()
	var r rows
	if err := e.admin.QueryRow(ctx, `SELECT
  COALESCE((SELECT role FROM public.memberships WHERE org_id = $1 AND user_id = $2), ''),
  COALESCE((SELECT status FROM auth.memberships WHERE organization_id = $1 AND principal_id = $2), ''),
  COALESCE((SELECT role FROM auth.memberships WHERE organization_id = $1 AND principal_id = $2), '')`, org, user).
		Scan(&r.publicRole, &r.authStatus, &r.authRole); err != nil {
		t.Fatalf("read rows: %v", err)
	}
	return r
}

func (e *env) count(t *testing.T, ctx context.Context, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := e.admin.QueryRow(ctx, sql, args...).Scan(&n); err != nil {
		t.Fatalf("count %q: %v", sql, err)
	}
	return n
}

func (e *env) auditRows(t *testing.T, ctx context.Context, eventType string, org uuid.UUID) int {
	return e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = $1 AND organization_id = $2`, eventType, org.String())
}

func (e *env) outboxRows(t *testing.T, ctx context.Context, eventType string, membershipID uuid.UUID) int {
	return e.count(t, ctx, `SELECT count(*) FROM auth.auth_outbox_events WHERE event_type = $1 AND aggregate_id = $2`, eventType, "mbr_"+membershipID.String())
}

func (e *env) revision(t *testing.T, ctx context.Context, membershipID uuid.UUID) int64 {
	t.Helper()
	var rev int64
	if err := e.admin.QueryRow(ctx, `SELECT COALESCE((SELECT revision FROM auth.aggregate_revisions
WHERE aggregate_type = 'membership' AND aggregate_id = $1), 0)`, "mbr_"+membershipID.String()).Scan(&rev); err != nil {
		t.Fatal(err)
	}
	return rev
}

// requireReconciled reads the users-reconcile membership counts (CHAOS-10783)
// over the whole database: every pair the store wrote agrees in both tables.
// membership_role_diff is not read: a test plays go-api's own role writer.
func (e *env) requireReconciled(t *testing.T, ctx context.Context) {
	t.Helper()
	tx, err := usersbackfill.OpenSnapshot(ctx, e.admin)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	d, err := usersbackfill.TxTarget{Tx: tx, Schema: schema}.MembershipDiffs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if d.PublicMissing != 0 || d.InactiveInPublic != 0 || d.AuthMissing != 0 {
		t.Fatalf("reconcile: public_missing=%d inactive_in_public=%d auth_missing=%d, want 0 0 0",
			d.PublicMissing, d.InactiveInPublic, d.AuthMissing)
	}
}

// Remove marks the auth row removed and deletes go-api's row, with one audit
// row, one outbox event and one revision, in one commit.
func TestRemoveWritesBothTablesOneAuditOneOutbox(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	org, user, owner := e.seedOrg(t, ctx), e.seedUser(t, ctx), e.seedUser(t, ctx)
	e.add(t, ctx, org, owner, "owner")
	m := e.add(t, ctx, org, user, "member")
	before := e.revision(t, ctx, m.ID)
	if _, err := e.store.Remove(ctx, Change{OrgID: org, PrincipalID: user, Authorize: allowAll}); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if got := e.rows(t, ctx, org, user); got != (rows{"", "removed", "member"}) {
		t.Fatalf("rows after remove = %+v, want no public row and auth removed/member", got)
	}
	if n := e.auditRows(t, ctx, "auth.membership.removed", org); n != 1 {
		t.Fatalf("audit rows = %d, want 1", n)
	}
	if n := e.outboxRows(t, ctx, "membership.removed", m.ID); n != 1 {
		t.Fatalf("outbox rows = %d, want 1", n)
	}
	if after := e.revision(t, ctx, m.ID); after != before+1 {
		t.Fatalf("revision %d -> %d, want +1", before, after)
	}
	if _, err := e.store.Remove(ctx, Change{OrgID: org, PrincipalID: user, Authorize: allowAll}); !errors.Is(err, ErrNotMember) {
		t.Fatalf("second Remove = %v, want ErrNotMember", err)
	}
	e.requireReconciled(t, ctx)
	// go-api's member routes write no public audit_logs row; the aggregate writes none either.
	if n := e.count(t, ctx, `SELECT count(*) FROM public.audit_logs WHERE org_id = $1`, org); n != 0 {
		t.Fatalf("public audit_logs rows = %d, want 0", n)
	}
}

// A removed member added again gets the same auth row back, active, and a new
// public row; no second auth row exists.
func TestReAddAfterRemoveReactivatesTheSameAuthRow(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	org, user, owner := e.seedOrg(t, ctx), e.seedUser(t, ctx), e.seedUser(t, ctx)
	e.add(t, ctx, org, owner, "owner")
	first := e.add(t, ctx, org, user, "member")
	if _, err := e.store.Remove(ctx, Change{OrgID: org, PrincipalID: user, Authorize: allowAll}); err != nil {
		t.Fatal(err)
	}
	again := e.add(t, ctx, org, user, "viewer")
	if again.ID != first.ID {
		t.Fatalf("re-add id %v, want the removed row's %v", again.ID, first.ID)
	}
	if got := e.rows(t, ctx, org, user); got != (rows{"viewer", "active", "viewer"}) {
		t.Fatalf("rows after re-add = %+v", got)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.memberships WHERE organization_id = $1 AND principal_id = $2`, org, user); n != 1 {
		t.Fatalf("auth rows = %d, want 1", n)
	}
	var pubID uuid.UUID
	if err := e.admin.QueryRow(ctx, `SELECT id FROM public.memberships WHERE org_id = $1 AND user_id = $2`, org, user).Scan(&pubID); err != nil || pubID != first.ID {
		t.Fatalf("public id = %v (%v), want %v", pubID, err, first.ID)
	}
	e.requireReconciled(t, ctx)
}

func TestChangeRoleSuspendReactivateProject(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	org, user, owner := e.seedOrg(t, ctx), e.seedUser(t, ctx), e.seedUser(t, ctx)
	e.add(t, ctx, org, owner, "owner")
	e.add(t, ctx, org, user, "member")
	if _, err := e.store.ChangeRole(ctx, Change{OrgID: org, PrincipalID: user, Role: "admin", Authorize: allowAll}); err != nil {
		t.Fatal(err)
	}
	if got := e.rows(t, ctx, org, user); got != (rows{"admin", "active", "admin"}) {
		t.Fatalf("after role change = %+v", got)
	}
	if _, err := e.store.Suspend(ctx, Change{OrgID: org, PrincipalID: user, Authorize: allowAll}); err != nil {
		t.Fatal(err)
	}
	if got := e.rows(t, ctx, org, user); got != (rows{"", "suspended", "admin"}) {
		t.Fatalf("after suspend = %+v", got)
	}
	if _, err := e.store.Reactivate(ctx, Change{OrgID: org, PrincipalID: user, Authorize: allowAll}); err != nil {
		t.Fatal(err)
	}
	if got := e.rows(t, ctx, org, user); got != (rows{"admin", "active", "admin"}) {
		t.Fatalf("after reactivate = %+v", got)
	}
	e.requireReconciled(t, ctx)
}

func TestTransferOwnershipMovesBothRolesInOneCommit(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	org, from, to := e.seedOrg(t, ctx), e.seedUser(t, ctx), e.seedUser(t, ctx)
	e.add(t, ctx, org, from, "owner")
	e.add(t, ctx, org, to, "member")
	if err := e.store.TransferOwnership(ctx, Transfer{OrgID: org, From: from, To: to, Authorize: allowAll}); err != nil {
		t.Fatal(err)
	}
	if got := e.rows(t, ctx, org, from); got != (rows{"admin", "active", "admin"}) {
		t.Fatalf("source after transfer = %+v", got)
	}
	if got := e.rows(t, ctx, org, to); got != (rows{"owner", "active", "owner"}) {
		t.Fatalf("target after transfer = %+v", got)
	}
	if n := e.auditRows(t, ctx, "auth.membership.ownership_transferred", org); n != 1 {
		t.Fatalf("transfer audit rows = %d, want 1", n)
	}
	e.requireReconciled(t, ctx)
}

func TestRemoveSuspendOrDemoteTheOnlyOwnerIsRefused(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	org, owner := e.seedOrg(t, ctx), e.seedUser(t, ctx)
	e.add(t, ctx, org, owner, "owner")
	for name, verb := range map[string]func(context.Context, Change) (*Membership, error){"remove": e.store.Remove, "suspend": e.store.Suspend, "role change": e.store.ChangeRole} {
		if _, err := verb(ctx, Change{OrgID: org, PrincipalID: owner, Role: "admin", Authorize: allowAll}); !errors.Is(err, ErrLastOwner) {
			t.Fatalf("%s of the only owner = %v, want ErrLastOwner", name, err)
		}
	}
	if got := e.rows(t, ctx, org, owner); got != (rows{"owner", "active", "owner"}) {
		t.Fatalf("rows after refusals = %+v", got)
	}
}

// Two removals of the last two owners at once: exactly one wins.
func TestConcurrentRemovalOfTheLastTwoOwnersLeavesOne(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	org, a, b := e.seedOrg(t, ctx), e.seedUser(t, ctx), e.seedUser(t, ctx)
	e.add(t, ctx, org, a, "owner")
	e.add(t, ctx, org, b, "owner")
	errs := make(chan error, 2)
	for _, user := range []uuid.UUID{a, b} {
		go func(user uuid.UUID) {
			_, err := e.store.Remove(ctx, Change{OrgID: org, PrincipalID: user, Authorize: allowAll})
			errs <- err
		}(user)
	}
	var ok, lastOwner int
	for range 2 {
		switch err := <-errs; {
		case err == nil:
			ok++
		case errors.Is(err, ErrLastOwner):
			lastOwner++
		default:
			t.Fatalf("Remove: %v", err)
		}
	}
	if ok != 1 || lastOwner != 1 {
		t.Fatalf("ok=%d last_owner=%d, want 1 and 1", ok, lastOwner)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM public.memberships WHERE org_id = $1 AND role = 'owner'`, org); n != 1 {
		t.Fatalf("owners left = %d, want 1", n)
	}
}

// go-api's role writer (users row FOR UPDATE, then the membership row) and a
// removal of the same pair run one after the other: no deadlock.
func TestRemovalAndGoAPIRoleWriterDoNotDeadlock(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	org, user, owner := e.seedOrg(t, ctx), e.seedUser(t, ctx), e.seedUser(t, ctx)
	e.add(t, ctx, org, owner, "owner")
	e.add(t, ctx, org, user, "member")
	for i := range 20 {
		errs := make(chan error, 2)
		go func() {
			_, err := e.store.Remove(ctx, Change{OrgID: org, PrincipalID: user, Authorize: allowAll})
			if errors.Is(err, ErrNotMember) {
				err = nil
			}
			errs <- err
		}()
		go func() {
			tx, err := e.admin.Begin(ctx)
			if err != nil {
				errs <- err
				return
			}
			defer func() { _ = tx.Rollback(ctx) }()
			if _, err := tx.Exec(ctx, `SELECT 1 FROM public.users WHERE id = $1 FOR UPDATE`, user); err != nil {
				errs <- err
				return
			}
			if _, err := tx.Exec(ctx, `UPDATE public.memberships SET role = 'viewer' WHERE org_id = $1 AND user_id = $2`, org, user); err != nil {
				errs <- err
				return
			}
			errs <- tx.Commit(ctx)
		}()
		for range 2 {
			if err := <-errs; err != nil {
				t.Fatalf("round %d: %v", i, err)
			}
		}
		e.add(t, ctx, org, user, "member")
	}
}

func TestRevisionConflictWritesNothing(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	org, user, owner := e.seedOrg(t, ctx), e.seedUser(t, ctx), e.seedUser(t, ctx)
	e.add(t, ctx, org, owner, "owner")
	m := e.add(t, ctx, org, user, "member")
	stale := e.revision(t, ctx, m.ID) - 1
	if _, err := e.store.ChangeRole(ctx, Change{OrgID: org, PrincipalID: user, Role: "admin", ExpectedRevision: &stale, Authorize: allowAll}); !errors.Is(err, ErrRevisionConflict) {
		t.Fatalf("stale revision = %v, want ErrRevisionConflict", err)
	}
	if got := e.rows(t, ctx, org, user); got != (rows{"member", "active", "member"}) {
		t.Fatalf("rows after conflict = %+v", got)
	}
}

func TestEveryVerbRefusesWhenUsersAreNotOwned(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	org, user := e.seedOrg(t, ctx), e.seedUser(t, ctx)
	if _, err := e.notOwned.Add(ctx, Change{OrgID: org, PrincipalID: user, Role: "member", Authorize: allowAll}); !errors.Is(err, ErrNotOwned) {
		t.Fatalf("Add with users not owned = %v, want ErrNotOwned", err)
	}
	if got := e.rows(t, ctx, org, user); got != (rows{}) {
		t.Fatalf("rows = %+v, want none", got)
	}
}

func TestAuthorizeRefusalAndNilAuthorizeWriteNothing(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	org, user := e.seedOrg(t, ctx), e.seedUser(t, ctx)
	refuse := func(context.Context, audit.TxOps, Locked) error { return ErrDenied }
	for name, auth := range map[string]Authorize{"refusing": refuse, "nil": nil} {
		if _, err := e.store.Add(ctx, Change{OrgID: org, PrincipalID: user, Role: "member", Authorize: auth}); !errors.Is(err, ErrDenied) {
			t.Fatalf("%s Authorize = %v, want ErrDenied", name, err)
		}
	}
	if got := e.rows(t, ctx, org, user); got != (rows{}) {
		t.Fatalf("rows = %+v, want none", got)
	}
}

// plantFailure makes the next write to table by the runtime role fail inside
// the commit; drop removes it.
func (e *env) plantFailure(t *testing.T, ctx context.Context, table string) {
	t.Helper()
	fn := "membership_test_fail_" + uuid.NewString()[:8]
	stmts := []string{
		fmt.Sprintf(`CREATE FUNCTION public.%s() RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN RAISE EXCEPTION 'planted failure' USING ERRCODE = 'P0001'; END $$`, fn),
		fmt.Sprintf(`CREATE TRIGGER %s BEFORE INSERT OR UPDATE OR DELETE ON %s FOR EACH ROW EXECUTE FUNCTION public.%s()`, fn, table, fn),
	}
	for _, s := range stmts {
		if _, err := e.admin.Exec(ctx, s); err != nil {
			t.Fatalf("plant: %v", err)
		}
	}
	t.Cleanup(func() {
		_, _ = e.admin.Exec(context.Background(), fmt.Sprintf(`DROP TRIGGER %s ON %s`, fn, table))
		_, _ = e.admin.Exec(context.Background(), fmt.Sprintf(`DROP FUNCTION public.%s()`, fn))
	})
}

// A failure at either write rolls back both rows, the audit row, the outbox
// event and the revision.
func TestForcedFailureAtEitherWriteLeavesEverythingUnchanged(t *testing.T) {
	for _, table := range []string{"public.memberships", "auth.memberships"} {
		t.Run(table, func(t *testing.T) {
			e := getEnv(t)
			ctx := context.Background()
			org, user, owner := e.seedOrg(t, ctx), e.seedUser(t, ctx), e.seedUser(t, ctx)
			e.add(t, ctx, org, owner, "owner")
			m := e.add(t, ctx, org, user, "member")
			rev := e.revision(t, ctx, m.ID)
			e.plantFailure(t, ctx, table)
			if _, err := e.store.Remove(ctx, Change{OrgID: org, PrincipalID: user, Authorize: allowAll}); err == nil {
				t.Fatal("Remove with a planted failure succeeded")
			}
			if got := e.rows(t, ctx, org, user); got != (rows{"member", "active", "member"}) {
				t.Fatalf("rows = %+v, want unchanged", got)
			}
			if n := e.auditRows(t, ctx, "auth.membership.removed", org); n != 0 {
				t.Fatalf("audit rows = %d, want 0", n)
			}
			if n := e.outboxRows(t, ctx, "membership.removed", m.ID); n != 0 {
				t.Fatalf("outbox rows = %d, want 0", n)
			}
			if after := e.revision(t, ctx, m.ID); after != rev {
				t.Fatalf("revision %d -> %d, want unchanged", rev, after)
			}
		})
	}
}

// An add with an inviter writes the inviter to go-api's row and the auth row
// (go-api's admin member list returns invited_by_id; acp ruling 10-29 01:2xZ).
func TestAddWithInviterWritesItToBothRows(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	org, user, inviter := e.seedOrg(t, ctx), e.seedUser(t, ctx), e.seedUser(t, ctx)
	if _, err := e.store.Add(ctx, Change{OrgID: org, PrincipalID: user, Role: "member", InvitedBy: &inviter, Authorize: allowAll}); err != nil {
		t.Fatalf("Add with an inviter: %v", err)
	}
	var public, auth uuid.UUID
	if err := e.admin.QueryRow(ctx, `SELECT p.invited_by_id, a.invited_by FROM public.memberships p
JOIN auth.memberships a ON a.organization_id = p.org_id AND a.principal_id = p.user_id
WHERE p.org_id = $1 AND p.user_id = $2`, org, user).Scan(&public, &auth); err != nil {
		t.Fatal(err)
	}
	if public != inviter || auth != inviter {
		t.Fatalf("inviter public=%v auth=%v, want %v in both", public, auth, inviter)
	}
}

// An inviter that is not in public.users, or not in the auth schema, is
// refused before any write: no row, no audit row, no foreign-key error.
func TestAddWithUnknownInviterIsRefusedBeforeAnyWrite(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	org := e.seedOrg(t, ctx)
	publicOnly := uuid.New()
	if _, err := e.admin.Exec(ctx, `INSERT INTO public.users
	(id, email, password_hash, auth_provider, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, NULL, 'local', true, true, false, 0, now(), now())`, publicOnly, "inviter-"+publicOnly.String()[:8]+"@example.test"); err != nil {
		t.Fatal(err)
	}
	for name, inviter := range map[string]uuid.UUID{"nowhere": uuid.New(), "public.users only": publicOnly} {
		t.Run(name, func(t *testing.T) {
			user := e.seedUser(t, ctx)
			_, err := e.store.Add(ctx, Change{OrgID: org, PrincipalID: user, Role: "member", InvitedBy: &inviter, Authorize: allowAll})
			if !errors.Is(err, ErrInviterNotFound) {
				t.Fatalf("Add with an unknown inviter = %v, want ErrInviterNotFound", err)
			}
			if got := e.rows(t, ctx, org, user); got != (rows{}) {
				t.Fatalf("rows = %+v, want none", got)
			}
			if n := e.auditRows(t, ctx, "auth.membership.added", org); n != 0 {
				t.Fatalf("audit rows = %d, want 0", n)
			}
		})
	}
}

// both is the inviter and join time of a pair in each table.
type both struct {
	publicInviter, authInviter *uuid.UUID
	publicJoined, authJoined   time.Time
}

func (e *env) joins(t *testing.T, ctx context.Context, org, user uuid.UUID) both {
	t.Helper()
	var b both
	if err := e.admin.QueryRow(ctx, `SELECT p.invited_by_id, a.invited_by, p.joined_at, a.joined_at FROM public.memberships p
JOIN auth.memberships a ON a.organization_id = p.org_id AND a.principal_id = p.user_id
WHERE p.org_id = $1 AND p.user_id = $2`, org, user).Scan(&b.publicInviter, &b.authInviter, &b.publicJoined, &b.authJoined); err != nil {
		t.Fatalf("read both rows: %v", err)
	}
	return b
}

func sameInviter(a, b *uuid.UUID) bool {
	return (a == nil && b == nil) || (a != nil && b != nil && *a == *b)
}

// A re-add is a new join: both rows get the new inviter and one new joined_at.
// A reactivate is the same membership: both rows keep its inviter and joined_at.
func TestReAddAndReactivateKeepTheInviterAndJoinEqualInBothRows(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	org, user, owner, first, second := e.seedOrg(t, ctx), e.seedUser(t, ctx), e.seedUser(t, ctx), e.seedUser(t, ctx), e.seedUser(t, ctx)
	e.add(t, ctx, org, owner, "owner")
	if _, err := e.store.Add(ctx, Change{OrgID: org, PrincipalID: user, Role: "member", InvitedBy: &first, Authorize: allowAll}); err != nil {
		t.Fatal(err)
	}
	before := e.joins(t, ctx, org, user)
	if _, err := e.store.Remove(ctx, Change{OrgID: org, PrincipalID: user, Authorize: allowAll}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.store.Add(ctx, Change{OrgID: org, PrincipalID: user, Role: "viewer", InvitedBy: &second, Authorize: allowAll}); err != nil {
		t.Fatal(err)
	}
	readded := e.joins(t, ctx, org, user)
	if !sameInviter(readded.publicInviter, &second) || !sameInviter(readded.authInviter, &second) {
		t.Fatalf("re-add inviter public=%v auth=%v, want %v in both", readded.publicInviter, readded.authInviter, second)
	}
	if !readded.publicJoined.Equal(readded.authJoined) || !readded.authJoined.After(before.authJoined) {
		t.Fatalf("re-add joined_at public=%v auth=%v (first join %v), want one new instant in both", readded.publicJoined, readded.authJoined, before.authJoined)
	}
	if _, err := e.store.Suspend(ctx, Change{OrgID: org, PrincipalID: user, Authorize: allowAll}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.store.Reactivate(ctx, Change{OrgID: org, PrincipalID: user, Authorize: allowAll}); err != nil {
		t.Fatal(err)
	}
	back := e.joins(t, ctx, org, user)
	if !sameInviter(back.publicInviter, &second) || !sameInviter(back.authInviter, &second) {
		t.Fatalf("reactivate inviter public=%v auth=%v, want %v in both", back.publicInviter, back.authInviter, second)
	}
	if !back.publicJoined.Equal(readded.authJoined) || !back.authJoined.Equal(readded.authJoined) {
		t.Fatalf("reactivate joined_at public=%v auth=%v, want the membership's %v in both", back.publicJoined, back.authJoined, readded.authJoined)
	}
}

// A missing principal, inviter or organization is named only to an actor
// Authorize accepts; a refused actor gets ErrDenied for each, and nothing is
// written.
func TestLookupRefusalsAreDeniedToARefusedActor(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	org, user := e.seedOrg(t, ctx), e.seedUser(t, ctx)
	refuse := func(context.Context, audit.TxOps, Locked) error { return ErrDenied }
	missing, missingOrg := uuid.New(), uuid.New()
	cases := []struct {
		name string
		c    Change
		want error
	}{
		{"missing principal", Change{OrgID: org, PrincipalID: missing, Role: "member"}, ErrPrincipalNotFound},
		{"missing inviter", Change{OrgID: org, PrincipalID: user, Role: "member", InvitedBy: &missing}, ErrInviterNotFound},
		{"missing organization", Change{OrgID: missingOrg, PrincipalID: user, Role: "member"}, ErrOrgNotFound},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			denied := tc.c
			denied.Authorize = refuse
			if _, err := e.store.Add(ctx, denied); !errors.Is(err, ErrDenied) || errors.Is(err, tc.want) {
				t.Fatalf("refused actor: err = %v, want ErrDenied and not %v", err, tc.want)
			}
			allowed := tc.c
			allowed.Authorize = allowAll
			if _, err := e.store.Add(ctx, allowed); !errors.Is(err, tc.want) {
				t.Fatalf("accepted actor: err = %v, want %v", err, tc.want)
			}
		})
	}
	if got := e.rows(t, ctx, org, user); got != (rows{}) {
		t.Fatalf("rows = %+v, want none", got)
	}
}

// Authorize's superuser refusal is returned as it is, never as a store
// failure, and nothing is written.
func TestAuthorizeSuperuserRefusalIsNamed(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	org, owner, user := e.seedOrg(t, ctx), e.seedUser(t, ctx), e.seedUser(t, ctx)
	e.add(t, ctx, org, owner, "owner")
	superuserOnly := func(context.Context, audit.TxOps, Locked) error { return ErrSuperuserRequired }
	_, err := e.store.Add(ctx, Change{OrgID: org, PrincipalID: user, Role: "member", Authorize: superuserOnly})
	if !errors.Is(err, ErrSuperuserRequired) || errors.Is(err, ErrStore) {
		t.Fatalf("Add = %v, want ErrSuperuserRequired and no store failure", err)
	}
	err = e.store.TransferOwnership(ctx, Transfer{OrgID: org, From: owner, To: user, Authorize: superuserOnly})
	if !errors.Is(err, ErrSuperuserRequired) || errors.Is(err, ErrStore) {
		t.Fatalf("TransferOwnership = %v, want ErrSuperuserRequired", err)
	}
	if got := e.rows(t, ctx, org, user); got != (rows{}) {
		t.Fatalf("a refused add wrote %+v", got)
	}
}

// A change the lock-free read refuses answers its own refusal, never a store
// failure, and writes nothing: no state row, no audit row, no event (its
// planned audit row and event are placeholders that must never commit). A
// role change of a non-member, a transfer to a non-member, a removal of the
// last owner, and a role change in a missing organization for a refused actor.
func TestRefusedPlansAnswerTheirRefusal(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	org, owner, stranger := e.seedOrg(t, ctx), e.seedUser(t, ctx), e.seedUser(t, ctx)
	e.add(t, ctx, org, owner, "owner")
	missing := uuid.New()
	// writes is everything a refused change could write for org: both
	// membership tables (every column that changes on a write), the audit rows
	// and the outbox events naming the organization.
	writes := func(org uuid.UUID) string {
		t.Helper()
		var out string
		if err := e.admin.QueryRow(ctx, `SELECT concat_ws('|',
  (SELECT string_agg(concat_ws(',', id, user_id, role, updated_at), ';' ORDER BY id) FROM public.memberships WHERE org_id = $1),
  (SELECT string_agg(concat_ws(',', id, principal_id, status, role, joined_at, updated_at), ';' ORDER BY id) FROM auth.memberships WHERE organization_id = $1),
  (SELECT count(*) FROM auth.security_audit_events WHERE organization_id = $1),
  (SELECT count(*) FROM auth.auth_outbox_events WHERE payload->>'organization_id' = 'org_' || $1::text))`, org).Scan(&out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	deny := func(context.Context, audit.TxOps, Locked) error { return ErrDenied }
	for _, c := range []struct {
		name string
		org  uuid.UUID
		run  func() error
		want error
	}{
		{"role change of a non-member", org, func() error {
			_, err := e.store.ChangeRole(ctx, Change{OrgID: org, PrincipalID: stranger, Role: "admin", Actor: Actor{PrincipalID: &owner}, Authorize: allowAll})
			return err
		}, ErrNotMember},
		{"transfer to a non-member", org, func() error {
			return e.store.TransferOwnership(ctx, Transfer{OrgID: org, From: owner, To: stranger, Actor: Actor{PrincipalID: &owner}, Authorize: allowAll})
		}, ErrTargetNotMember},
		{"removal of the last owner", org, func() error {
			_, err := e.store.Remove(ctx, Change{OrgID: org, PrincipalID: owner, Actor: Actor{PrincipalID: &owner}, Authorize: allowAll})
			return err
		}, ErrLastOwner},
		{"a refused actor in a missing organization", missing, func() error {
			_, err := e.store.ChangeRole(ctx, Change{OrgID: missing, PrincipalID: stranger, Role: "admin", Authorize: deny})
			return err
		}, ErrDenied},
	} {
		before := writes(c.org)
		if err := c.run(); !errors.Is(err, c.want) || errors.Is(err, ErrStore) {
			t.Fatalf("%s = %v, want %v", c.name, err, c.want)
		}
		if after := writes(c.org); after != before {
			t.Fatalf("%s wrote: before %q after %q", c.name, before, after)
		}
	}
}

// A verb an actor makes and a verb on the actor, started together, take one
// lock order (the actor's principal is locked at step 1 with the targets): no
// deadlock, 20 rounds.
func TestActorAndTargetVerbsShareOneLockOrder(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	org, owner := e.seedOrg(t, ctx), e.seedUser(t, ctx)
	e.add(t, ctx, org, owner, "owner")
	for round := range 20 {
		admin, target := e.seedUser(t, ctx), e.seedUser(t, ctx)
		e.add(t, ctx, org, admin, "admin")
		errs := make(chan error, 2)
		go func() {
			_, err := e.store.Add(ctx, Change{OrgID: org, PrincipalID: target, Role: "member", Actor: Actor{PrincipalID: &admin}, Authorize: allowAll})
			errs <- err
		}()
		go func() {
			_, err := e.store.ChangeRole(ctx, Change{OrgID: org, PrincipalID: admin, Role: "viewer", Actor: Actor{PrincipalID: &owner}, Authorize: allowAll})
			errs <- err
		}()
		for range 2 {
			if err := <-errs; err != nil {
				t.Fatalf("round %d: %v", round, err)
			}
		}
	}
}
