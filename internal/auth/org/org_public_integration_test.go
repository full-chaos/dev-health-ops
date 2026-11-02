//go:build integration

package org

import (
	"context"
	"errors"
	"fmt"
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

// ownedEnv is a runtime role with the users-owned and orgs-owned grants, and two stores over it: owned (OwnsOrgs) and
// notOwned.
type ownedEnv struct {
	admin    *pgxpool.Pool
	runtime  *pgxpool.Pool
	owned    *Store
	notOwned *Store
}

var (
	ownedShared *ownedEnv
	ownedErr    error
	ownedOnce   sync.Once
)

func getOwnedEnv(t *testing.T) *ownedEnv {
	t.Helper()
	ownedOnce.Do(func() { ownedShared, ownedErr = newOwnedEnv(t) })
	if ownedErr != nil {
		t.Fatal(ownedErr)
	}
	return ownedShared
}

func newOwnedEnv(t *testing.T) (*ownedEnv, error) {
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
	role, err := containers.RoleName("org_owned_runtime", instance)
	if err != nil {
		return nil, fmt.Errorf("derive role: %w", err)
	}
	password := "org-owned-test-" + uuid.NewString()
	if _, err := admin.Exec(ctx, fmt.Sprintf(`CREATE ROLE %q LOGIN PASSWORD '%s'`, role, password)); err != nil {
		return nil, fmt.Errorf("create role: %w", err)
	}
	closers = append(closers, func() { containers.DropRole(admin, role, func(string, ...any) {}) })
	if _, err := authschema.Apply(ctx, admin, authschema.Options{Schema: "auth", RuntimeRole: role, UsersOwned: true, OrgsOwned: true}); err != nil {
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
	e := &ownedEnv{admin: admin, runtime: runtime}
	if e.owned, err = New(Config{Pool: runtime, Schema: schema, OwnsOrgs: true}); err != nil {
		return nil, err
	}
	if e.notOwned, err = New(Config{Pool: runtime, Schema: schema}); err != nil {
		return nil, err
	}
	return e, nil
}

// born creates the organization in auth and gives it go-api's row with the same id, name "Before", active, and an
// updated_at in the past.
func (e *ownedEnv) born(t *testing.T, ctx context.Context) uuid.UUID {
	t.Helper()
	slug := slugFor()
	id, err := e.owned.Create(ctx, CreateInput{Name: "Before", Slug: slug}, Actor{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.admin.Exec(ctx, `INSERT INTO public.organizations (id, slug, name, tier, is_active, created_at, updated_at)
VALUES ($1, $2, 'Before', 'community', true, now() - interval '1 day', now() - interval '1 day')`, id, slug); err != nil {
		t.Fatal(err)
	}
	return id
}

type publicOrg struct {
	name    string
	active  bool
	updated time.Time
}

func (e *ownedEnv) public(t *testing.T, ctx context.Context, id uuid.UUID) publicOrg {
	t.Helper()
	var p publicOrg
	if err := e.admin.QueryRow(ctx, `SELECT name, is_active, updated_at FROM public.organizations WHERE id = $1`, id).
		Scan(&p.name, &p.active, &p.updated); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestRenameWritesPublicAndAuthOneCommit(t *testing.T) {
	e := getOwnedEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	id := e.born(t, ctx)
	before := e.public(t, ctx, id)
	changed, err := e.owned.RenameAuthorized(ctx, id, "  After  ", nil, Actor{}, allowAll)
	if err != nil || !changed {
		t.Fatalf("rename: changed=%v err=%v", changed, err)
	}
	o, err := e.owned.Get(ctx, id)
	if err != nil || o.Name != "After" || o.Revision != 2 {
		t.Fatalf("auth row after the rename: %+v, %v; want name After at revision 2", o, err)
	}
	after := e.public(t, ctx, id)
	if after.name != "After" || !after.active || !after.updated.After(before.updated) {
		t.Fatalf("public row after the rename = %+v, want name After, active, updated_at moved past %v", after, before.updated)
	}
	changed, err = e.owned.RenameAuthorized(ctx, id, "After", nil, Actor{}, allowAll)
	if err != nil || changed {
		t.Fatalf("a rename to the current name: changed=%v err=%v, want a no-op", changed, err)
	}
	if again := e.public(t, ctx, id); again != after {
		t.Fatalf("a no-op rename wrote the public row: %+v, was %+v", again, after)
	}
}

func TestSuspendReactivateWritePublicIsActive(t *testing.T) {
	e := getOwnedEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	id := e.born(t, ctx)
	if err := e.owned.Suspend(ctx, id, nil, Actor{}); err != nil {
		t.Fatal(err)
	}
	if p := e.public(t, ctx, id); p.active || p.name != "Before" {
		t.Fatalf("public row after the suspend = %+v, want inactive, name kept", p)
	}
	if err := e.owned.Reactivate(ctx, id, nil, Actor{}); err != nil {
		t.Fatal(err)
	}
	if p := e.public(t, ctx, id); !p.active {
		t.Fatalf("public row after the reactivate = %+v, want active", p)
	}
	if _, err := e.owned.SuspendAuthorized(ctx, id, nil, Actor{}, allowAll); err != nil {
		t.Fatal(err)
	}
	if p := e.public(t, ctx, id); p.active {
		t.Fatalf("public row after the authorized suspend = %+v, want inactive", p)
	}
	if _, err := e.owned.ReactivateAuthorized(ctx, id, nil, Actor{}, allowAll); err != nil {
		t.Fatal(err)
	}
	if p := e.public(t, ctx, id); !p.active {
		t.Fatalf("public row after the authorized reactivate = %+v, want active", p)
	}
}

// ApplySuspend (the deprovision's suspend, inside its caller's commit) writes is_active false on go-api's row too.
func TestApplySuspendWritesPublicIsActive(t *testing.T) {
	e := getOwnedEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	id := e.born(t, ctx)
	err := e.owned.commit(ctx, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			if _, err := e.owned.LockForUpdate(ctx, tx, id); err != nil {
				return err
			}
			return e.owned.ApplySuspend(ctx, tx, id)
		},
		Audit:  auditFor(verbSuspend, id, Actor{}, StatusActive, StatusSuspended),
		Events: []audit.OutboxEvent{SuspendedEvent(id)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if p := e.public(t, ctx, id); p.active {
		t.Fatalf("public row after ApplySuspend = %+v, want inactive", p)
	}
}

// The public write is in the verb's commit: when the audit row fails (an actor with no principal row), neither row
// changes.
func TestPublicOrgWriteRollsBackWithAudit(t *testing.T) {
	e := getOwnedEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	id := e.born(t, ctx)
	before := e.public(t, ctx, id)
	ghost := uuid.New()
	if _, err := e.owned.RenameAuthorized(ctx, id, "Ghost", nil, Actor{PrincipalID: &ghost}, allowAll); !errors.Is(err, ErrStore) {
		t.Fatalf("rename by an actor with no principal row = %v, want ErrStore", err)
	}
	if _, err := e.owned.SuspendAuthorized(ctx, id, nil, Actor{PrincipalID: &ghost}, allowAll); !errors.Is(err, ErrStore) {
		t.Fatalf("suspend by an actor with no principal row = %v, want ErrStore", err)
	}
	if p := e.public(t, ctx, id); p != before {
		t.Fatalf("public row after the failed verbs = %+v, want %+v", p, before)
	}
	if o, err := e.owned.Get(ctx, id); err != nil || o.Name != "Before" || o.Status != StatusActive {
		t.Fatalf("auth row after the failed verbs = %+v, %v", o, err)
	}
}

// With the switch off the verbs write auth only, as before.
func TestPublicOrgWriteOffWhenSwitchOff(t *testing.T) {
	e := getOwnedEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	id := e.born(t, ctx)
	before := e.public(t, ctx, id)
	if _, err := e.notOwned.RenameAuthorized(ctx, id, "Auth Only", nil, Actor{}, allowAll); err != nil {
		t.Fatal(err)
	}
	if err := e.notOwned.Suspend(ctx, id, nil, Actor{}); err != nil {
		t.Fatal(err)
	}
	if p := e.public(t, ctx, id); p != before {
		t.Fatalf("public row with the switch off = %+v, want it untouched %+v", p, before)
	}
	if o, err := e.notOwned.Get(ctx, id); err != nil || o.Name != "Auth Only" || o.Status != StatusSuspended {
		t.Fatalf("auth row with the switch off = %+v, %v", o, err)
	}
}

// The runtime role still cannot write go-api's other organization columns.
func TestOwnedRuntimeRoleCannotWriteOtherPublicOrgColumns(t *testing.T) {
	e := getOwnedEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	id := e.born(t, ctx)
	var pgErr *pgconn.PgError
	for name, statement := range map[string]string{
		"tier": `UPDATE public.organizations SET tier = 'enterprise' WHERE id = $1`,
		"slug": `UPDATE public.organizations SET slug = 'other' WHERE id = $1`,
	} {
		if _, err := e.runtime.Exec(ctx, statement, id); !errors.As(err, &pgErr) || pgErr.Code != "42501" {
			t.Errorf("%s: %v, want 42501", name, err)
		}
	}
}
