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

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/provisioning"
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
// must finish once the lock is released. whileBlocked, when set, runs after
// the wait is seen and before the release.
func (e *env) waitsForTheWriteLock(t *testing.T, ctx context.Context, write func() error, whileBlocked func()) error {
	t.Helper()
	return e.waitsBehind(t, ctx, func() func() { return e.holdWriteLock(t, ctx) },
		`wait_event_type = 'Lock' AND wait_event = 'advisory'`, "the write lock", write, whileBlocked)
}

// waitsBehind runs write while hold's transaction holds a lock: write must be
// seen waiting (waitCondition on pg_stat_activity, for the runtime role)
// before it returns, and must finish once the lock is released. whileBlocked,
// when set, runs after the wait is seen and before the release.
func (e *env) waitsBehind(t *testing.T, ctx context.Context, hold func() func(), waitCondition, what string, write func() error, whileBlocked func()) error {
	t.Helper()
	var role string
	if err := e.runtime.QueryRow(ctx, `SELECT current_user`).Scan(&role); err != nil {
		t.Fatal(err)
	}
	release := hold()
	done := make(chan error, 1)
	go func() { done <- write() }()
	for {
		select {
		case err := <-done:
			release()
			t.Fatalf("the write returned (%v) while another transaction held %s", err, what)
		case <-ctx.Done():
			release()
			t.Fatalf("no waiter on %s was seen", what)
		default:
		}
		var waiting int
		if err := e.admin.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE usename = $1 AND `+waitCondition, role).Scan(&waiting); err != nil {
			release()
			t.Fatal(err)
		}
		if waiting > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if whileBlocked != nil {
		whileBlocked()
	}
	release()
	return <-done
}

// holdPrincipalRow locks one principal row FOR UPDATE in an open admin
// transaction, as a concurrent disable would, and returns its release.
func (e *env) holdPrincipalRow(t *testing.T, ctx context.Context, principal uuid.UUID) func() {
	t.Helper()
	tx, err := e.admin.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var id uuid.UUID
	if err := tx.QueryRow(ctx, `SELECT id FROM auth.principals WHERE id = $1 FOR UPDATE`, principal).Scan(&id); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	return func() { _ = tx.Rollback(ctx) }
}

// rowLockWait is a wait on a row lock, not on the advisory write lock.
const rowLockWait = `wait_event_type = 'Lock' AND wait_event <> 'advisory'`

func TestGrantWaitsForTheWriteLock(t *testing.T) {
	e := getEnv(t)
	ctx := ctxT(t)
	admin := e.user(t, ctx, true, true)
	target := e.user(t, ctx, true, false)
	err := e.waitsForTheWriteLock(t, ctx, func() error {
		_, err := e.store.Grant(ctx, GrantInput{Actor: admin, Principal: target, Role: PlatformAdmin})
		return err
	}, nil)
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
	}, nil)
	if err != nil {
		t.Fatalf("Revoke after the lock was released: %v", err)
	}
}

// sleepPast waits until the database clock is past at.
func (e *env) sleepPast(t *testing.T, ctx context.Context, at time.Time) {
	t.Helper()
	for {
		var past bool
		if err := e.admin.QueryRow(ctx, `SELECT statement_timestamp() > $1::timestamptz`, at).Scan(&past); err != nil {
			t.Fatal(err)
		}
		if past {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// The granter is checked after the write lock is taken: a granter whose role
// expires while the grant waits for the lock is refused.
func TestGrantChecksTheGranterAfterTheWriteLock(t *testing.T) {
	e := getEnv(t)
	ctx := ctxT(t)
	admin := e.user(t, ctx, true, true)
	granter := e.user(t, ctx, true, false)
	target := e.user(t, ctx, true, false)
	until := time.Now().Add(3 * time.Second)
	if _, err := e.store.Grant(ctx, GrantInput{Actor: admin, Principal: granter, Role: PlatformAdmin, ExpiresAt: &until}); err != nil {
		t.Fatal(err)
	}
	err := e.waitsForTheWriteLock(t, ctx, func() error {
		_, err := e.store.Grant(ctx, GrantInput{Actor: granter, Principal: target, Role: PlatformAdmin})
		return err
	}, func() { e.sleepPast(t, ctx, until) })
	if !errors.Is(err, ErrGranterNotHeld) {
		t.Fatalf("a granter that expired while the grant waited: %v, want ErrGranterNotHeld", err)
	}
}

// The requested expiry is checked after the write lock is taken: an expiry
// that passes while the grant waits for the lock is refused.
func TestGrantChecksTheExpiryAfterTheWriteLock(t *testing.T) {
	e := getEnv(t)
	ctx := ctxT(t)
	admin := e.user(t, ctx, true, true)
	target := e.user(t, ctx, true, false)
	until := time.Now().Add(3 * time.Second)
	err := e.waitsForTheWriteLock(t, ctx, func() error {
		_, err := e.store.Grant(ctx, GrantInput{Actor: admin, Principal: target, Role: PlatformAdmin, ExpiresAt: &until})
		return err
	}, func() { e.sleepPast(t, ctx, until) })
	if !errors.Is(err, ErrExpiryNotFuture) {
		t.Fatalf("an expiry that passed while the grant waited: %v, want ErrExpiryNotFuture", err)
	}
}

// A granter whose role expires while the grant waits on a principal row lock
// is refused: the guard reads after every lock wait.
func TestGrantChecksTheGranterAfterAPrincipalRowWait(t *testing.T) {
	e := getEnv(t)
	ctx := ctxT(t)
	admin := e.user(t, ctx, true, true)
	granter := e.user(t, ctx, true, false)
	target := e.user(t, ctx, true, false)
	until := time.Now().Add(3 * time.Second)
	if _, err := e.store.Grant(ctx, GrantInput{Actor: admin, Principal: granter, Role: PlatformAdmin, ExpiresAt: &until}); err != nil {
		t.Fatal(err)
	}
	err := e.waitsBehind(t, ctx, func() func() { return e.holdPrincipalRow(t, ctx, target) }, rowLockWait, "the target's principal row",
		func() error {
			_, err := e.store.Grant(ctx, GrantInput{Actor: granter, Principal: target, Role: PlatformAdmin})
			return err
		}, func() { e.sleepPast(t, ctx, until) })
	if !errors.Is(err, ErrGranterNotHeld) {
		t.Fatalf("a granter that expired while the grant waited on a principal row: %v, want ErrGranterNotHeld", err)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.platform_role_assignments WHERE principal_id = $1`, target); n != 0 {
		t.Fatalf("the refused grant wrote %d rows", n)
	}
}

// A revoker whose role expires while the revoke waits on a principal row lock
// is refused, and the target keeps its role.
func TestRevokeChecksTheRevokerAfterAPrincipalRowWait(t *testing.T) {
	e := getEnv(t)
	ctx := ctxT(t)
	admin := e.user(t, ctx, true, true)
	revoker := e.user(t, ctx, true, false)
	target := e.user(t, ctx, true, true)
	until := time.Now().Add(3 * time.Second)
	if _, err := e.store.Grant(ctx, GrantInput{Actor: admin, Principal: revoker, Role: PlatformAdmin, ExpiresAt: &until}); err != nil {
		t.Fatal(err)
	}
	err := e.waitsBehind(t, ctx, func() func() { return e.holdPrincipalRow(t, ctx, target) }, rowLockWait, "the target's principal row",
		func() error {
			return e.store.Revoke(ctx, RevokeInput{Actor: revoker, Principal: target, Role: PlatformAdmin})
		}, func() { e.sleepPast(t, ctx, until) })
	if !errors.Is(err, ErrGranterNotHeld) {
		t.Fatalf("a revoker that expired while the revoke waited on a principal row: %v, want ErrGranterNotHeld", err)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.platform_role_assignments WHERE principal_id = $1`, target); n != 1 {
		t.Fatalf("the refused revoke left %d rows of the target, want 1", n)
	}
}

// bootstrapStore is the store as the migration role uses it (the admin pool).
func (e *env) bootstrapStore(t *testing.T) *Store {
	t.Helper()
	s, err := New(Config{Pool: e.admin, Schema: e.schema})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func (e *env) bootstrapAudits(t *testing.T, ctx context.Context, principal uuid.UUID) int {
	t.Helper()
	return e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.platform_role.granted'
		AND subject_principal_id = $1 AND attributes->>'reason_code' = 'restore_fence_bootstrap'`, principal)
}

func TestBootstrapRefusesWhileAnAdminIsActive(t *testing.T) {
	e := isolatedEnv(t)
	ctx := ctxT(t)
	e.user(t, ctx, true, true)
	target := e.user(t, ctx, true, false)
	if _, err := e.bootstrapStore(t).Bootstrap(ctx, target); !errors.Is(err, ErrBootstrapAdminActive) {
		t.Fatalf("bootstrap with an active admin: %v, want ErrBootstrapAdminActive", err)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.platform_role_assignments WHERE principal_id = $1`, target); n != 0 {
		t.Fatalf("the refused bootstrap wrote %d rows", n)
	}
}

func TestBootstrapGrantsWhenNoAdminIsActive(t *testing.T) {
	e := isolatedEnv(t)
	ctx := ctxT(t)
	target := e.user(t, ctx, true, false)
	id, err := e.bootstrapStore(t).Bootstrap(ctx, target)
	if err != nil {
		t.Fatalf("bootstrap with no active admin: %v", err)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.platform_role_assignments WHERE id = $1 AND principal_id = $2
		AND role_key = 'platform_admin' AND granted_by = $2 AND expires_at IS NULL`, id, target); n != 1 {
		t.Fatal("the bootstrap row is not a permanent self-granted platform_admin")
	}
	if n := e.bootstrapAudits(t, ctx, target); n != 1 {
		t.Fatalf("bootstrap audit rows = %d, want 1", n)
	}
	if held, err := e.store.Resolve(ctx, target); err != nil || !held.Allows(GrantAction) {
		t.Fatalf("Resolve after the bootstrap: %v %v", held.Roles(), err)
	}
	other := e.user(t, ctx, true, false)
	if _, err := e.bootstrapStore(t).Bootstrap(ctx, other); !errors.Is(err, ErrBootstrapAdminActive) {
		t.Fatalf("a second bootstrap: %v, want ErrBootstrapAdminActive", err)
	}
}

func TestBootstrapRefusesADisabledOrMissingPrincipal(t *testing.T) {
	e := isolatedEnv(t)
	ctx := ctxT(t)
	disabled := e.user(t, ctx, false, false)
	if _, err := e.bootstrapStore(t).Bootstrap(ctx, disabled); !errors.Is(err, ErrPrincipalOff) {
		t.Fatalf("bootstrap of a disabled principal: %v, want ErrPrincipalOff", err)
	}
	if _, err := e.bootstrapStore(t).Bootstrap(ctx, uuid.New()); !errors.Is(err, ErrPrincipalMissing) {
		t.Fatalf("bootstrap of a missing principal: %v, want ErrPrincipalMissing", err)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.platform_role_assignments`); n != 0 {
		t.Fatalf("refused bootstraps wrote %d rows", n)
	}
}

// The principal's own expired assignment blocks a new row: the bootstrap
// replaces it in the same commit, with the revoked event of the old id.
func TestBootstrapReplacesThePrincipalsExpiredAssignment(t *testing.T) {
	e := isolatedEnv(t)
	ctx := ctxT(t)
	target := e.user(t, ctx, true, false)
	expired := uuid.New()
	subject := target.String()
	if err := audit.Commit(ctx, e.admin, e.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			_, err := tx.Exec(ctx, `INSERT INTO auth.platform_role_assignments (id, principal_id, role_key, granted_by, expires_at)
VALUES ($1, $2, 'platform_admin', $2, statement_timestamp() - interval '1 minute')`, expired, target)
			return err
		},
		Audit: audit.AuditEvent{EventType: "auth.platform_role.granted", Outcome: audit.OutcomeAllowed, SubjectPrincipalID: &subject,
			Attributes: map[string]any{"reason_code": "role_granted", "target_kind": "principal", "target_id": subject}},
		Events: []audit.OutboxEvent{{AggregateType: "platform_role_assignment",
			AggregateID: provisioning.WireID("platform_role_assignment", expired.String()), EventType: "platform_role_assignment.granted",
			Change: map[string]any{"status": "active"}}},
	}); err != nil {
		t.Fatalf("plant an expired assignment: %v", err)
	}
	id, err := e.bootstrapStore(t).Bootstrap(ctx, target)
	if err != nil {
		t.Fatalf("bootstrap over the principal's expired assignment: %v", err)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.platform_role_assignments WHERE id = $1`, expired); n != 0 {
		t.Fatal("the expired assignment is still there")
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.platform_role_assignments WHERE id = $1 AND expires_at IS NULL AND granted_by = $2`, id, target); n != 1 {
		t.Fatal("the bootstrap row is missing")
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.auth_outbox_events WHERE event_type = 'platform_role_assignment.revoked' AND aggregate_id = $1`,
		"pra_"+expired.String()); n != 1 {
		t.Fatalf("revoked events for the expired assignment = %d, want 1", n)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.auth_outbox_events WHERE event_type = 'platform_role_assignment.granted' AND aggregate_id = $1`,
		"pra_"+id.String()); n != 1 {
		t.Fatalf("granted events for the bootstrap row = %d, want 1", n)
	}
	if n := e.bootstrapAudits(t, ctx, target); n != 1 {
		t.Fatalf("bootstrap audit rows (reason restore_fence_bootstrap) = %d, want 1", n)
	}
}

// plantAssignment writes one platform_admin row of principal through
// audit.Commit (grantedBy nil = no granter; expired = a minute in the past).
func (e *env) plantAssignment(t *testing.T, ctx context.Context, principal uuid.UUID, grantedBy *uuid.UUID, expired bool) uuid.UUID {
	t.Helper()
	id := uuid.New()
	expiry := "NULL"
	if expired {
		expiry = "statement_timestamp() - interval '1 minute'"
	}
	subject := principal.String()
	if err := audit.Commit(ctx, e.admin, e.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			_, err := tx.Exec(ctx, `INSERT INTO auth.platform_role_assignments (id, principal_id, role_key, granted_by, expires_at)
VALUES ($1, $2, 'platform_admin', $3, `+expiry+`)`, id, principal, grantedBy)
			return err
		},
		Audit: audit.AuditEvent{EventType: "auth.platform_role.granted", Outcome: audit.OutcomeAllowed, SubjectPrincipalID: &subject,
			Attributes: map[string]any{"reason_code": "role_granted", "target_kind": "principal", "target_id": subject}},
		Events: []audit.OutboxEvent{{AggregateType: "platform_role_assignment",
			AggregateID: provisioning.WireID("platform_role_assignment", id.String()), EventType: "platform_role_assignment.granted",
			Change: map[string]any{"status": "active"}}},
	}); err != nil {
		t.Fatalf("plant an assignment: %v", err)
	}
	return id
}

// unplantAssignment deletes one assignment through audit.Commit.
func (e *env) unplantAssignment(t *testing.T, ctx context.Context, id, principal uuid.UUID) {
	t.Helper()
	subject := principal.String()
	if err := audit.Commit(ctx, e.admin, e.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			_, err := tx.Exec(ctx, `DELETE FROM auth.platform_role_assignments WHERE id = $1`, id)
			return err
		},
		Audit: audit.AuditEvent{EventType: "auth.platform_role.revoked", Outcome: audit.OutcomeAllowed, SubjectPrincipalID: &subject,
			Attributes: map[string]any{"reason_code": "role_revoked", "target_kind": "principal", "target_id": subject}},
		Events: []audit.OutboxEvent{{AggregateType: "platform_role_assignment",
			AggregateID: provisioning.WireID("platform_role_assignment", id.String()), EventType: "platform_role_assignment.revoked",
			Change: map[string]any{"status": "revoked"}}},
	}); err != nil {
		t.Fatalf("remove an assignment: %v", err)
	}
}

// The target's own assignment that has NOT expired is an active admin: the
// bootstrap refuses and deletes nothing.
func TestBootstrapDeletesNoUnexpiredAssignment(t *testing.T) {
	e := isolatedEnv(t)
	ctx := ctxT(t)
	target := e.user(t, ctx, true, false)
	held := e.plantAssignment(t, ctx, target, nil, false)
	if _, err := e.bootstrapStore(t).Bootstrap(ctx, target); !errors.Is(err, ErrBootstrapAdminActive) {
		t.Fatalf("bootstrap over the target's unexpired assignment: %v, want ErrBootstrapAdminActive", err)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.platform_role_assignments WHERE id = $1`, held); n != 1 {
		t.Fatal("the refused bootstrap deleted the unexpired assignment")
	}
	if n := e.bootstrapAudits(t, ctx, target); n != 0 {
		t.Fatalf("the refused bootstrap wrote %d audit rows", n)
	}
}

// The replace deletes only the row the bootstrap read: when that row changed
// after the read (gone, or replaced by another expired row), the bootstrap is
// refused and writes nothing.
func TestBootstrapReplaceRefusesWhenTheRowChangedAfterTheRead(t *testing.T) {
	for name, replace := range map[string]bool{"removed": false, "replaced": true} {
		t.Run(name, func(t *testing.T) {
			e := isolatedEnv(t)
			ctx := ctxT(t)
			target := e.user(t, ctx, true, false)
			e.plantAssignment(t, ctx, target, &target, true)
			s := e.bootstrapStore(t)
			read, err := s.bootstrapTarget(ctx, target)
			if err != nil || read == nil {
				t.Fatalf("read: %v %v, want the expired row", read, err)
			}
			e.unplantAssignment(t, ctx, *read, target)
			var other uuid.UUID
			if replace {
				other = e.plantAssignment(t, ctx, target, &target, true)
			}
			if _, err := s.bootstrapApply(ctx, target, read); !errors.Is(err, ErrBootstrapRowChanged) {
				t.Fatalf("bootstrap after the row changed: %v, want ErrBootstrapRowChanged", err)
			}
			want := 0
			if replace {
				want = 1
				if n := e.count(t, ctx, `SELECT count(*) FROM auth.platform_role_assignments WHERE id = $1`, other); n != 1 {
					t.Fatal("the refused bootstrap deleted the row it did not read")
				}
			}
			if n := e.count(t, ctx, `SELECT count(*) FROM auth.platform_role_assignments WHERE principal_id = $1`, target); n != want {
				t.Fatalf("rows of the target after the refusal = %d, want %d", n, want)
			}
			if n := e.bootstrapAudits(t, ctx, target); n != 0 {
				t.Fatalf("the refused bootstrap wrote %d audit rows", n)
			}
		})
	}
}
