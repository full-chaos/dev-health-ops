//go:build integration

package org

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

type env struct {
	runtime *pgxpool.Pool
	store   *Store
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
	role, err := containers.RoleName("org_runtime", instance)
	if err != nil {
		return nil, fmt.Errorf("derive role: %w", err)
	}
	const password = "org-test-only-not-a-credential"
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
	return &env{runtime: runtime, store: store}, nil
}

func (e *env) count(t *testing.T, ctx context.Context, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := e.runtime.QueryRow(ctx, sql, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func slugFor() string { return "t-" + uuid.NewString()[:8] }

func TestCreateWritesRowAuditEventAndRevisionOneInOneTx(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	id, err := e.store.Create(ctx, CreateInput{Name: " Acme ", Slug: slugFor()}, Actor{})
	if err != nil {
		t.Fatal(err)
	}
	o, err := e.store.Get(ctx, id)
	if err != nil || o.Name != "Acme" || o.Status != StatusActive || o.Revision != 1 {
		t.Fatalf("Get = %+v, %v", o, err)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.organization.created' AND organization_id = $1`, id); n != 1 {
		t.Fatalf("audit rows = %d", n)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.auth_outbox_events WHERE aggregate_type = 'organization' AND aggregate_id = $1 AND event_type = 'organization.created'`, "org_"+id.String()); n != 1 {
		t.Fatalf("outbox rows = %d", n)
	}
}

func TestSlugCollisionAcrossOrgsDeniesAndWritesNothing(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	slug := slugFor()
	if _, err := e.store.Create(ctx, CreateInput{Name: "One", Slug: slug}, Actor{}); err != nil {
		t.Fatal(err)
	}
	before := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.organization.created'`)
	if _, err := e.store.Create(ctx, CreateInput{Name: "Two", Slug: slug}, Actor{}); !errors.Is(err, ErrSlugTaken) {
		t.Fatalf("second Create = %v, want ErrSlugTaken", err)
	}
	if after := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.organization.created'`); after != before {
		t.Fatalf("a refused create wrote audit rows: %d -> %d", before, after)
	}
}

func TestSuspendSetsDisabledAtAndReactivateClearsIt_RevisionIsContiguous(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	id, err := e.store.Create(ctx, CreateInput{Name: "Org", Slug: slugFor()}, Actor{})
	if err != nil {
		t.Fatal(err)
	}
	steps := []struct {
		run    func() error
		status Status
		rev    int64
	}{
		{func() error { return e.store.Suspend(ctx, id, nil, Actor{}) }, StatusSuspended, 2},
		{func() error { return e.store.Suspend(ctx, id, nil, Actor{}) }, StatusSuspended, 2},
		{func() error { return e.store.Reactivate(ctx, id, nil, Actor{}) }, StatusActive, 3},
		{func() error { return e.store.Rename(ctx, id, "Renamed", nil, Actor{}) }, StatusActive, 4},
	}
	for i, s := range steps {
		if err := s.run(); err != nil {
			t.Fatalf("step %d: %v", i, err)
		}
		o, err := e.store.Get(ctx, id)
		if err != nil || o.Status != s.status || o.Revision != s.rev || (o.Status == StatusSuspended) != (o.SuspendedAt != nil) {
			t.Fatalf("step %d: %+v, %v; want %s rev %d", i, o, err, s.status, s.rev)
		}
	}
	if err := e.store.Suspend(ctx, id, nil, Actor{}); err != nil {
		t.Fatal(err)
	}
	if err := e.store.Rename(ctx, id, "Late", nil, Actor{}); !errors.Is(err, ErrSuspended) {
		t.Fatalf("rename of a suspended org = %v", err)
	}
	if err := e.store.Suspend(ctx, uuid.New(), nil, Actor{}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown id = %v", err)
	}
}

func TestOutOfOrderSuspendReactivateWithStaleRevisionCannotRestoreActive(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	id, err := e.store.Create(ctx, CreateInput{Name: "Org", Slug: slugFor()}, Actor{})
	if err != nil {
		t.Fatal(err)
	}
	seen := int64(1)
	if err := e.store.Suspend(ctx, id, &seen, Actor{}); err != nil {
		t.Fatal(err)
	}
	if err := e.store.Reactivate(ctx, id, &seen, Actor{}); !errors.Is(err, ErrRevisionConflict) {
		t.Fatalf("stale reactivate = %v, want ErrRevisionConflict", err)
	}
	if o, err := e.store.Get(ctx, id); err != nil || o.Status != StatusSuspended || o.Revision != 2 {
		t.Fatalf("after the stale call: %+v, %v", o, err)
	}
}

func TestRuntimeRoleCannotDeleteOrganizationAndDirectUpdateIsRefusedOutsideCommit(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	id, err := e.store.Create(ctx, CreateInput{Name: "Org", Slug: slugFor()}, Actor{})
	if err != nil {
		t.Fatal(err)
	}
	var pgErr *pgconn.PgError
	if _, err := e.runtime.Exec(ctx, `DELETE FROM auth.organizations WHERE id = $1`, id); !errors.As(err, &pgErr) || pgErr.Code != "42501" {
		t.Fatalf("DELETE = %v, want 42501", err)
	}
	if _, err := e.runtime.Exec(ctx, `UPDATE auth.organizations SET disabled_at = now() WHERE id = $1`, id); !errors.As(err, &pgErr) || pgErr.Code != "42501" {
		t.Fatalf("direct UPDATE = %v, want 42501", err)
	}
}

func TestSuspendedOrgIsVisibleAsDeniedToCurrentStateRead(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	slug := slugFor()
	id, err := e.store.Create(ctx, CreateInput{Name: "Org", Slug: slug}, Actor{})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.store.Suspend(ctx, id, nil, Actor{}); err != nil {
		t.Fatal(err)
	}
	o, err := e.store.BySlug(ctx, slug)
	if err != nil || o.Status != StatusSuspended || o.SuspendedAt == nil {
		t.Fatalf("BySlug = %+v, %v", o, err)
	}
}

// Authorize runs under the row lock and before any write: a denial writes no
// row, no audit row, no event and no revision; an allowed call writes all four
// once; a repeat is a no-op with no second audit row.
func TestReactivateAuthorizedDeniedWritesNothingAllowedWritesAllOnce(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	id, err := e.store.Create(ctx, CreateInput{Name: "Org", Slug: slugFor()}, Actor{})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.store.Suspend(ctx, id, nil, Actor{}); err != nil {
		t.Fatal(err)
	}
	actor := uuid.New()
	if _, err := e.runtime.Exec(ctx, `INSERT INTO auth.principals (id, kind, display_name) VALUES ($1, 'user', '')`, actor); err != nil {
		t.Fatalf("seed the acting principal: %v", err)
	}
	auditSQL := `SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.organization.reactivated' AND organization_id = $1`
	outboxSQL := `SELECT count(*) FROM auth.auth_outbox_events WHERE aggregate_type = 'organization' AND aggregate_id = $1 AND event_type = 'organization.reactivated'`
	deny := func(context.Context, audit.TxOps, uuid.UUID) error { return ErrDenied }
	if changed, err := e.store.ReactivateAuthorized(ctx, id, nil, Actor{PrincipalID: &actor}, deny); !errors.Is(err, ErrDenied) || changed {
		t.Fatalf("denied call: changed=%v err=%v", changed, err)
	}
	if o, err := e.store.Get(ctx, id); err != nil || o.Status != StatusSuspended || o.Revision != 2 {
		t.Fatalf("after the denial: %+v, %v", o, err)
	}
	if e.count(t, ctx, auditSQL, id) != 0 || e.count(t, ctx, outboxSQL, "org_"+id.String()) != 0 {
		t.Fatal("a denied reactivate left an audit row or an event")
	}
	saw := false
	allow := func(_ context.Context, _ audit.TxOps, got uuid.UUID) error { saw = got == id; return nil }
	if changed, err := e.store.ReactivateAuthorized(ctx, id, nil, Actor{PrincipalID: &actor}, allow); err != nil || !changed || !saw {
		t.Fatalf("allowed call: changed=%v err=%v authorize saw org=%v", changed, err, saw)
	}
	if o, err := e.store.Get(ctx, id); err != nil || o.Status != StatusActive || o.SuspendedAt != nil || o.Revision != 3 {
		t.Fatalf("after the reactivate: %+v, %v", o, err)
	}
	if e.count(t, ctx, auditSQL, id) != 1 || e.count(t, ctx, outboxSQL, "org_"+id.String()) != 1 {
		t.Fatal("an allowed reactivate must leave one audit row and one event")
	}
	if changed, err := e.store.ReactivateAuthorized(ctx, id, nil, Actor{PrincipalID: &actor}, allow); err != nil || changed {
		t.Fatalf("repeat: changed=%v err=%v, want a no-op", changed, err)
	}
	if o, _ := e.store.Get(ctx, id); o.Revision != 3 || e.count(t, ctx, auditSQL, id) != 1 {
		t.Fatalf("a repeat bumped the revision or wrote a second audit row: %+v", o)
	}
	if _, err := e.store.ReactivateAuthorized(ctx, uuid.New(), nil, Actor{}, allow); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown org = %v", err)
	}
}

// Reactivate is last-writer and superuser only (the route passes no revision), but it is a real revision-bumping
// write: it moves the aggregate past the revision a suspend left, so a suspend that still carries that older
// revision is refused as a conflict and changes nothing; and a reactivate of an organization that is not
// suspended writes no row, no audit row, no event and no revision. Store level: the route has no revision input,
// so a suspend with one can only be made on the store.
func TestReactivateMovesTheRevisionPastASuspendAndAStaleSuspendIsRefused(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	id, err := e.store.Create(ctx, CreateInput{Name: "Org", Slug: slugFor()}, Actor{})
	if err != nil {
		t.Fatal(err)
	}
	actor := uuid.New()
	if _, err := e.runtime.Exec(ctx, `INSERT INTO auth.principals (id, kind, display_name) VALUES ($1, 'user', '')`, actor); err != nil {
		t.Fatalf("seed the acting principal: %v", err)
	}
	allow := func(context.Context, audit.TxOps, uuid.UUID) error { return nil }
	wire := "org_" + id.String()
	audits := func(eventType string) int {
		return e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = $1 AND organization_id = $2`, eventType, id)
	}
	events := func(eventType string) int {
		return e.count(t, ctx, `SELECT count(*) FROM auth.auth_outbox_events WHERE aggregate_type = 'organization' AND aggregate_id = $1 AND event_type = $2`, wire, eventType)
	}
	if err := e.store.Suspend(ctx, id, nil, Actor{}); err != nil {
		t.Fatal(err)
	}
	suspended, err := e.store.Get(ctx, id)
	if err != nil || suspended.Status != StatusSuspended {
		t.Fatalf("after the suspend: %+v, %v", suspended, err)
	}
	n := suspended.Revision

	// (1) the reactivate moves the revision past the suspend's.
	if changed, err := e.store.ReactivateAuthorized(ctx, id, nil, Actor{PrincipalID: &actor}, allow); err != nil || !changed {
		t.Fatalf("reactivate: changed=%v err=%v", changed, err)
	}
	reactivated, err := e.store.Get(ctx, id)
	if err != nil || reactivated.Status != StatusActive || reactivated.Revision <= n {
		t.Fatalf("after the reactivate: %+v (revision must be past %d), %v", reactivated, n, err)
	}

	// (2) a suspend that carries the revision it read before the reactivate is refused and changes nothing.
	suspendAudits, suspendEvents := audits("auth.organization.suspended"), events("organization.suspended")
	if err := e.store.Suspend(ctx, id, &n, Actor{}); !errors.Is(err, ErrRevisionConflict) {
		t.Fatalf("a suspend with the old revision %d = %v, want ErrRevisionConflict", n, err)
	}
	if after, err := e.store.Get(ctx, id); err != nil || after.Status != StatusActive || after.Revision != reactivated.Revision {
		t.Fatalf("after the refused suspend: %+v, %v; want the reactivated state unchanged", after, err)
	}
	if audits("auth.organization.suspended") != suspendAudits || events("organization.suspended") != suspendEvents {
		t.Fatal("a refused stale suspend wrote an audit row or an event")
	}

	// (3) a reactivate of an organization that is not suspended writes nothing.
	reAudits, reEvents := audits("auth.organization.reactivated"), events("organization.reactivated")
	if changed, err := e.store.ReactivateAuthorized(ctx, id, nil, Actor{PrincipalID: &actor}, allow); err != nil || changed {
		t.Fatalf("reactivate of an active organization: changed=%v err=%v, want a no-op", changed, err)
	}
	if again, err := e.store.Get(ctx, id); err != nil || again.Revision != reactivated.Revision {
		t.Fatalf("a no-op reactivate moved the revision: %+v, %v", again, err)
	}
	if audits("auth.organization.reactivated") != reAudits || events("organization.reactivated") != reEvents {
		t.Fatal("a no-op reactivate wrote an audit row or an event")
	}
}

// Authorize runs under the row lock and before any write: a denial writes no row, no audit row, no event and no
// revision; an allowed call writes all four once; a repeat is a no-op with no second audit row.
func TestSuspendAuthorizedDeniedWritesNothingAllowedWritesAllOnce(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	id, err := e.store.Create(ctx, CreateInput{Name: "Org", Slug: slugFor()}, Actor{})
	if err != nil {
		t.Fatal(err)
	}
	actor := uuid.New()
	if _, err := e.runtime.Exec(ctx, `INSERT INTO auth.principals (id, kind, display_name) VALUES ($1, 'user', '')`, actor); err != nil {
		t.Fatalf("seed the acting principal: %v", err)
	}
	auditSQL := `SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.organization.suspended' AND organization_id = $1`
	outboxSQL := `SELECT count(*) FROM auth.auth_outbox_events WHERE aggregate_type = 'organization' AND aggregate_id = $1 AND event_type = 'organization.suspended'`
	deny := func(context.Context, audit.TxOps, uuid.UUID) error { return ErrDenied }
	if changed, err := e.store.SuspendAuthorized(ctx, id, nil, Actor{PrincipalID: &actor}, deny); !errors.Is(err, ErrDenied) || changed {
		t.Fatalf("denied call: changed=%v err=%v", changed, err)
	}
	if o, err := e.store.Get(ctx, id); err != nil || o.Status != StatusActive || o.Revision != 1 {
		t.Fatalf("after the denial: %+v, %v", o, err)
	}
	if e.count(t, ctx, auditSQL, id) != 0 || e.count(t, ctx, outboxSQL, "org_"+id.String()) != 0 {
		t.Fatal("a denied suspend left an audit row or an event")
	}
	saw := false
	allow := func(_ context.Context, _ audit.TxOps, got uuid.UUID) error { saw = got == id; return nil }
	if changed, err := e.store.SuspendAuthorized(ctx, id, nil, Actor{PrincipalID: &actor}, allow); err != nil || !changed || !saw {
		t.Fatalf("allowed call: changed=%v err=%v authorize saw org=%v", changed, err, saw)
	}
	if o, err := e.store.Get(ctx, id); err != nil || o.Status != StatusSuspended || o.SuspendedAt == nil || o.Revision != 2 {
		t.Fatalf("after the suspend: %+v, %v", o, err)
	}
	if e.count(t, ctx, auditSQL, id) != 1 || e.count(t, ctx, outboxSQL, "org_"+id.String()) != 1 {
		t.Fatal("an allowed suspend must leave one audit row and one event")
	}
	if changed, err := e.store.SuspendAuthorized(ctx, id, nil, Actor{PrincipalID: &actor}, allow); err != nil || changed {
		t.Fatalf("repeat: changed=%v err=%v, want a no-op", changed, err)
	}
	if o, _ := e.store.Get(ctx, id); o.Revision != 2 || e.count(t, ctx, auditSQL, id) != 1 {
		t.Fatalf("a repeat bumped the revision or wrote a second audit row: %+v", o)
	}
	if _, err := e.store.SuspendAuthorized(ctx, uuid.New(), nil, Actor{}, allow); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown org = %v", err)
	}
}

// An acting principal with no auth.principals row fails the audit row's actor foreign key: the store answers ErrStore
// (the route's fail-closed 503) and writes nothing; the organization stays active.
func TestSuspendAuthorizedWithAnActorThatHasNoPrincipalRowIsAnErrStoreAndWritesNothing(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	id, err := e.store.Create(ctx, CreateInput{Name: "Org", Slug: slugFor()}, Actor{})
	if err != nil {
		t.Fatal(err)
	}
	ghost := uuid.New()
	allow := func(context.Context, audit.TxOps, uuid.UUID) error { return nil }
	changed, err := e.store.SuspendAuthorized(ctx, id, nil, Actor{PrincipalID: &ghost}, allow)
	if !errors.Is(err, ErrStore) || changed {
		t.Fatalf("an actor with no principal row: changed=%v err=%v, want ErrStore", changed, err)
	}
	if o, err := e.store.Get(ctx, id); err != nil || o.Status != StatusActive || o.Revision != 1 {
		t.Fatalf("after the failed suspend: %+v, %v; want the organization active at revision 1", o, err)
	}
}

// Suspend moves the revision like reactivate: a reactivate that still carries the revision read before the suspend is
// refused as a conflict and changes nothing.
func TestSuspendMovesTheRevisionPastAReactivateAndAStaleReactivateIsRefused(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	id, err := e.store.Create(ctx, CreateInput{Name: "Org", Slug: slugFor()}, Actor{})
	if err != nil {
		t.Fatal(err)
	}
	actor := uuid.New()
	if _, err := e.runtime.Exec(ctx, `INSERT INTO auth.principals (id, kind, display_name) VALUES ($1, 'user', '')`, actor); err != nil {
		t.Fatalf("seed the acting principal: %v", err)
	}
	allow := func(context.Context, audit.TxOps, uuid.UUID) error { return nil }
	if err := e.store.Suspend(ctx, id, nil, Actor{}); err != nil {
		t.Fatal(err)
	}
	if err := e.store.Reactivate(ctx, id, nil, Actor{}); err != nil {
		t.Fatal(err)
	}
	active, err := e.store.Get(ctx, id)
	if err != nil || active.Status != StatusActive {
		t.Fatalf("after the reactivate: %+v, %v", active, err)
	}
	n := active.Revision
	if changed, err := e.store.SuspendAuthorized(ctx, id, nil, Actor{PrincipalID: &actor}, allow); err != nil || !changed {
		t.Fatalf("suspend: changed=%v err=%v", changed, err)
	}
	suspended, err := e.store.Get(ctx, id)
	if err != nil || suspended.Status != StatusSuspended || suspended.Revision <= n {
		t.Fatalf("after the suspend: %+v (revision must be past %d), %v", suspended, n, err)
	}
	if err := e.store.Reactivate(ctx, id, &n, Actor{}); !errors.Is(err, ErrRevisionConflict) {
		t.Fatalf("a reactivate with the old revision %d = %v, want ErrRevisionConflict", n, err)
	}
	if after, err := e.store.Get(ctx, id); err != nil || after.Status != StatusSuspended || after.Revision != suspended.Revision {
		t.Fatalf("after the refused reactivate: %+v, %v; want the suspended state unchanged", after, err)
	}
}
