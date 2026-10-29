//go:build integration

package platformrole

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

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

type env struct {
	admin    *pgxpool.Pool
	runtime  *pgxpool.Pool
	schema   authschema.ValidatedIdentifier
	store    *Store
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

// getEnv is the database the tests share. They seed fresh principals and
// delete nothing; a test that counts every platform administrator uses
// isolatedEnv.
func getEnv(t *testing.T) *env {
	t.Helper()
	sharedOnce.Do(func() { shared, sharedErr = newEnv(t) })
	if sharedErr != nil {
		t.Fatal(sharedErr)
	}
	return shared
}

// isolatedEnv is a database of its own, for a test whose rule counts every
// active platform administrator.
func isolatedEnv(t *testing.T) *env {
	t.Helper()
	e, err := newEnv(t)
	if err != nil {
		t.Fatal(err)
	}
	return e
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
	role, err := containers.RoleName("platformrole_runtime", instance)
	if err != nil {
		return nil, fmt.Errorf("derive role: %w", err)
	}
	const password = "platformrole-test-only-not-a-credential"
	if _, err := admin.Exec(ctx, fmt.Sprintf(`CREATE ROLE %q LOGIN PASSWORD '%s'`, role, password)); err != nil {
		return nil, fmt.Errorf("create role: %w", err)
	}
	closers = append(closers, func() { containers.DropRole(admin, role, func(string, ...any) {}) })
	if _, err := authschema.Apply(ctx, admin, authschema.Options{Schema: "auth", RuntimeRole: role}); err != nil {
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
	store, err := New(Config{Pool: runtime, Schema: schema})
	if err != nil {
		return nil, err
	}
	id, err := identity.New(identity.Config{Pool: runtime, Schema: schema})
	if err != nil {
		return nil, err
	}
	return &env{admin: admin, runtime: runtime, schema: schema, store: store, identity: id}, nil
}

// user projects one user principal; superuser gives it a backfilled
// platform_admin assignment (no granter), the bootstrap state.
func (e *env) user(t *testing.T, ctx context.Context, active, superuser bool) uuid.UUID {
	t.Helper()
	id := uuid.New()
	if _, err := e.identity.BackfillUser(ctx, identity.BackfillTarget{ID: id, Email: "pr-" + id.String()[:8] + "@example.test",
		DisplayName: "pr", Active: active, Verified: true, Superuser: superuser}, false); err != nil {
		t.Fatalf("BackfillUser: %v", err)
	}
	return id
}

func (e *env) count(t *testing.T, ctx context.Context, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := e.admin.QueryRow(ctx, sql, args...).Scan(&n); err != nil {
		t.Fatalf("count %q: %v", sql, err)
	}
	return n
}

func (e *env) revision(t *testing.T, ctx context.Context, assignment uuid.UUID) int64 {
	t.Helper()
	var rev int64
	if err := e.admin.QueryRow(ctx, `SELECT COALESCE((SELECT revision FROM auth.aggregate_revisions
WHERE aggregate_type = 'platform_role_assignment' AND aggregate_id = $1), 0)`, "pra_"+assignment.String()).Scan(&rev); err != nil {
		t.Fatal(err)
	}
	return rev
}

func ctxT(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	return ctx
}

func TestGrantRevokeCommitAuditOutboxAndRevision(t *testing.T) {
	e := getEnv(t)
	ctx := ctxT(t)
	admin := e.user(t, ctx, true, true)
	target := e.user(t, ctx, true, false)

	id, err := e.store.Grant(ctx, GrantInput{Actor: admin, Principal: target, Role: PlatformAdmin})
	if err != nil {
		t.Fatalf("Grant: %v", err)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.platform_role_assignments WHERE id = $1 AND principal_id = $2 AND granted_by = $3`, id, target, admin); n != 1 {
		t.Fatalf("assignment rows with the granter = %d", n)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.platform_role.granted'
		AND actor_principal_id = $1 AND subject_principal_id = $2`, admin.String(), target.String()); n != 1 {
		t.Fatalf("grant audit rows = %d", n)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.auth_outbox_events WHERE event_type = 'platform_role_assignment.granted' AND aggregate_id = $1`, "pra_"+id.String()); n != 1 {
		t.Fatalf("grant outbox rows = %d", n)
	}
	if rev := e.revision(t, ctx, id); rev != 1 {
		t.Fatalf("revision after grant = %d", rev)
	}
	held, err := e.store.Resolve(ctx, target)
	if err != nil || !held.Allows(GrantAction) {
		t.Fatalf("Resolve after grant: %v %v", held.Roles(), err)
	}

	if err := e.store.Revoke(ctx, RevokeInput{Actor: admin, Principal: target, Role: PlatformAdmin}); err != nil {
		t.Fatalf("Revoke: %v", err)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.platform_role_assignments WHERE principal_id = $1`, target); n != 0 {
		t.Fatalf("rows after revoke = %d", n)
	}
	if rev := e.revision(t, ctx, id); rev != 2 {
		t.Fatalf("revision after revoke = %d, want 2 (the revision survives the row)", rev)
	}
	if held, err := e.store.Resolve(ctx, target); err != nil || held.Any() {
		t.Fatalf("Resolve after revoke: %v %v", held.Roles(), err)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.platform_role.revoked'
		AND actor_principal_id = $1 AND subject_principal_id = $2`, admin.String(), target.String()); n != 1 {
		t.Fatalf("revoke audit rows = %d", n)
	}

	again, err := e.store.Grant(ctx, GrantInput{Actor: admin, Principal: target, Role: PlatformAdmin})
	if err != nil {
		t.Fatalf("re-grant: %v", err)
	}
	if again == id {
		t.Fatal("a re-grant reused the revoked assignment id")
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.platform_role.granted' AND subject_principal_id = $1`, target.String()); n != 2 {
		t.Fatalf("grant audit rows after revoke + grant = %d, want 2", n)
	}
}

func TestDuplicateGrantIsRefusedAndWritesNothing(t *testing.T) {
	e := getEnv(t)
	ctx := ctxT(t)
	admin := e.user(t, ctx, true, true)
	audits := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events`)
	if _, err := e.store.Grant(ctx, GrantInput{Actor: admin, Principal: admin, Role: PlatformAdmin}); !errors.Is(err, ErrAlreadyHeld) {
		t.Fatalf("a second grant: %v, want ErrAlreadyHeld", err)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events`); n != audits {
		t.Fatalf("a refused grant wrote %d audit rows", n-audits)
	}
}

func TestGrantRequiresAGranterThatHoldsTheRoleNow(t *testing.T) {
	e := getEnv(t)
	ctx := ctxT(t)
	e.user(t, ctx, true, true)
	plain := e.user(t, ctx, true, false)
	target := e.user(t, ctx, true, false)
	disabledAdmin := e.user(t, ctx, false, true)
	for name, actor := range map[string]uuid.UUID{"no role": plain, "disabled": disabledAdmin, "unknown": uuid.New()} {
		if _, err := e.store.Grant(ctx, GrantInput{Actor: actor, Principal: target, Role: PlatformAdmin}); !errors.Is(err, ErrGranterNotHeld) {
			t.Errorf("granter %s: %v, want ErrGranterNotHeld", name, err)
		}
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.platform_role_assignments WHERE principal_id = $1`, target); n != 0 {
		t.Fatalf("a refused grant wrote %d rows", n)
	}
}

func TestExpiredAssignmentGrantsNothing(t *testing.T) {
	e := getEnv(t)
	ctx := ctxT(t)
	admin := e.user(t, ctx, true, true)
	target := e.user(t, ctx, true, false)
	soon := time.Now().Add(2 * time.Second)
	id, err := e.store.Grant(ctx, GrantInput{Actor: admin, Principal: target, Role: PlatformAdmin, ExpiresAt: &soon})
	if err != nil {
		t.Fatalf("Grant with expiry: %v", err)
	}
	if held, err := e.store.Resolve(ctx, target); err != nil || !held.Any() {
		t.Fatalf("before expiry: %v %v", held.Roles(), err)
	}
	time.Sleep(3 * time.Second)
	if held, err := e.store.Resolve(ctx, target); err != nil || held.Any() {
		t.Fatalf("after expiry: %v %v", held.Roles(), err)
	}
	other := e.user(t, ctx, true, false)
	if _, err := e.store.Grant(ctx, GrantInput{Actor: target, Principal: other, Role: PlatformAdmin}); !errors.Is(err, ErrGranterNotHeld) {
		t.Fatalf("an expired granter: %v, want ErrGranterNotHeld", err)
	}
	list, err := e.store.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range list {
		if a.ID == id && a.Active {
			t.Fatal("List reports an expired assignment as active")
		}
	}
}

func TestGrantRefusesAPastExpiry(t *testing.T) {
	e := getEnv(t)
	ctx := ctxT(t)
	admin := e.user(t, ctx, true, true)
	target := e.user(t, ctx, true, false)
	past := time.Now().Add(-time.Minute)
	if _, err := e.store.Grant(ctx, GrantInput{Actor: admin, Principal: target, Role: PlatformAdmin, ExpiresAt: &past}); !errors.Is(err, ErrExpiryNotFuture) {
		t.Fatalf("a past expiry: %v, want ErrExpiryNotFuture", err)
	}
}

func TestDisabledPrincipalHoldsNoPlatformAuthority(t *testing.T) {
	e := getEnv(t)
	ctx := ctxT(t)
	e.user(t, ctx, true, true)
	disabled := e.user(t, ctx, false, true)
	if held, err := e.store.Resolve(ctx, disabled); err != nil || held.Any() {
		t.Fatalf("a disabled principal with an assignment: %v %v", held.Roles(), err)
	}
}

func TestGrantRefusesAMissingOrDisabledTarget(t *testing.T) {
	e := getEnv(t)
	ctx := ctxT(t)
	admin := e.user(t, ctx, true, true)
	disabled := e.user(t, ctx, false, false)
	if _, err := e.store.Grant(ctx, GrantInput{Actor: admin, Principal: uuid.New(), Role: PlatformAdmin}); !errors.Is(err, ErrPrincipalMissing) {
		t.Errorf("a missing target: %v", err)
	}
	if _, err := e.store.Grant(ctx, GrantInput{Actor: admin, Principal: disabled, Role: PlatformAdmin}); !errors.Is(err, ErrPrincipalOff) {
		t.Errorf("a disabled target: %v", err)
	}
}

func TestRevokeNeverLeavesNoActivePlatformAdmin(t *testing.T) {
	e := isolatedEnv(t)
	ctx := ctxT(t)
	only := e.user(t, ctx, true, true)
	if err := e.store.Revoke(ctx, RevokeInput{Actor: only, Principal: only, Role: PlatformAdmin}); !errors.Is(err, ErrLastPlatformAdmin) {
		t.Fatalf("revoking the only admin: %v, want ErrLastPlatformAdmin", err)
	}
	second := e.user(t, ctx, true, false)
	soon := time.Now().Add(2 * time.Second)
	if _, err := e.store.Grant(ctx, GrantInput{Actor: only, Principal: second, Role: PlatformAdmin, ExpiresAt: &soon}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(3 * time.Second)
	if err := e.store.Revoke(ctx, RevokeInput{Actor: only, Principal: only, Role: PlatformAdmin}); !errors.Is(err, ErrLastPlatformAdmin) {
		t.Fatalf("an expired second admin does not count: %v, want ErrLastPlatformAdmin", err)
	}
	third := e.user(t, ctx, true, false)
	if _, err := e.store.Grant(ctx, GrantInput{Actor: only, Principal: third, Role: PlatformAdmin}); err != nil {
		t.Fatal(err)
	}
	if err := e.store.Revoke(ctx, RevokeInput{Actor: only, Principal: only, Role: PlatformAdmin}); err != nil {
		t.Fatalf("revoking oneself with another active admin: %v", err)
	}
	if err := e.store.Revoke(ctx, RevokeInput{Actor: only, Principal: third, Role: PlatformAdmin}); !errors.Is(err, ErrGranterNotHeld) {
		t.Fatalf("a revoker without the role: %v, want ErrGranterNotHeld", err)
	}
}

// Two admins revoking each other at once: exactly one revoke commits.
func TestConcurrentRevokesKeepOneActivePlatformAdmin(t *testing.T) {
	e := isolatedEnv(t)
	ctx := ctxT(t)
	a := e.user(t, ctx, true, true)
	b := e.user(t, ctx, true, true)
	errs := make(chan error, 2)
	var start sync.WaitGroup
	start.Add(1)
	for _, pair := range [][2]uuid.UUID{{a, b}, {b, a}} {
		go func(actor, target uuid.UUID) {
			start.Wait()
			errs <- e.store.Revoke(ctx, RevokeInput{Actor: actor, Principal: target, Role: PlatformAdmin})
		}(pair[0], pair[1])
	}
	start.Done()
	var ok, refused int
	for range 2 {
		switch err := <-errs; {
		case err == nil:
			ok++
		case errors.Is(err, ErrLastPlatformAdmin), errors.Is(err, ErrGranterNotHeld), errors.Is(err, ErrNotHeld):
			refused++
		default:
			t.Fatalf("unexpected revoke error: %v", err)
		}
	}
	if ok != 1 || refused != 1 {
		t.Fatalf("revokes ok=%d refused=%d, want 1 and 1", ok, refused)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.platform_role_assignments WHERE role_key = 'platform_admin'`); n != 1 {
		t.Fatalf("platform_admin rows = %d, want 1", n)
	}
}

func TestRevokeOfARoleNotHeldIsRefused(t *testing.T) {
	e := getEnv(t)
	ctx := ctxT(t)
	admin := e.user(t, ctx, true, true)
	plain := e.user(t, ctx, true, false)
	if err := e.store.Revoke(ctx, RevokeInput{Actor: admin, Principal: plain, Role: PlatformAdmin}); !errors.Is(err, ErrNotHeld) {
		t.Fatalf("revoking a role not held: %v, want ErrNotHeld", err)
	}
}

// holdWriteLock takes the store's write lock in an open admin transaction and
// returns its release.
func (e *env) holdWriteLock(t *testing.T, ctx context.Context) func() {
	t.Helper()
	tx, err := e.admin.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, "auth.platform_role_assignments:write"); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	return func() { _ = tx.Rollback(ctx) }
}

// waitsForTheWriteLock runs write while another transaction holds the write
// lock: write must be seen waiting on the advisory lock before it returns, and
// must finish once the lock is released.
func (e *env) waitsForTheWriteLock(t *testing.T, ctx context.Context, write func() error) error {
	t.Helper()
	var role string
	if err := e.runtime.QueryRow(ctx, `SELECT current_user`).Scan(&role); err != nil {
		t.Fatal(err)
	}
	release := e.holdWriteLock(t, ctx)
	done := make(chan error, 1)
	go func() { done <- write() }()
	for {
		select {
		case err := <-done:
			release()
			t.Fatalf("the write returned (%v) while another transaction held the write lock", err)
		case <-ctx.Done():
			release()
			t.Fatal("no waiter on the write lock was seen")
		default:
		}
		var waiting int
		if err := e.admin.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity
			WHERE usename = $1 AND wait_event_type = 'Lock' AND wait_event = 'advisory'`, role).Scan(&waiting); err != nil {
			release()
			t.Fatal(err)
		}
		if waiting > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	release()
	return <-done
}

func TestGrantWaitsForTheWriteLock(t *testing.T) {
	e := getEnv(t)
	ctx := ctxT(t)
	admin := e.user(t, ctx, true, true)
	target := e.user(t, ctx, true, false)
	err := e.waitsForTheWriteLock(t, ctx, func() error {
		_, err := e.store.Grant(ctx, GrantInput{Actor: admin, Principal: target, Role: PlatformAdmin})
		return err
	})
	if err != nil {
		t.Fatalf("Grant after the lock was released: %v", err)
	}
}

func TestRevokeWaitsForTheWriteLock(t *testing.T) {
	e := getEnv(t)
	ctx := ctxT(t)
	a := e.user(t, ctx, true, true)
	b := e.user(t, ctx, true, true)
	err := e.waitsForTheWriteLock(t, ctx, func() error {
		return e.store.Revoke(ctx, RevokeInput{Actor: a, Principal: b, Role: PlatformAdmin})
	})
	if err != nil {
		t.Fatalf("Revoke after the lock was released: %v", err)
	}
}
