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
