//go:build integration

package repoidentity

import (
	"context"
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
	identity *identity.Store
	schema   authschema.ValidatedIdentifier
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
	role, err := containers.RoleName("repoidentity_runtime", instance)
	if err != nil {
		return nil, fmt.Errorf("derive role: %w", err)
	}
	const password = "repoidentity-test-only-not-a-credential"
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
	id, err := identity.New(identity.Config{Pool: runtime, Schema: schema, UsersOwnedByAuth: true})
	if err != nil {
		return nil, err
	}
	return &env{admin: admin, runtime: runtime, identity: id, schema: schema}, nil
}

func (e *env) seedOrg(t *testing.T, ctx context.Context) uuid.UUID {
	t.Helper()
	id := uuid.New()
	if _, err := e.admin.Exec(ctx, `INSERT INTO public.organizations (id, slug, name) VALUES ($1, $2, 'repoidentity-test')`,
		id, "org-"+id.String()[:8]); err != nil {
		t.Fatalf("seed public.organizations: %v", err)
	}
	if err := e.identity.ProjectOrganization(ctx, id); err != nil {
		t.Fatalf("ProjectOrganization: %v", err)
	}
	return id
}

func (e *env) syncAudits(t *testing.T, ctx context.Context, org uuid.UUID) int {
	t.Helper()
	var n int
	if err := e.admin.QueryRow(ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.repository_identity.synced' AND organization_id = $1`, org).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// A second Apply with the same input writes nothing: the guard counts a BEFORE INSERT even when ON CONFLICT then does
// nothing, so the insert must propose only the rows that are missing.
func TestApplyTwiceWithTheSameInputWritesNoAuditRowAndReportsNoChange(t *testing.T) {
	ctx := context.Background()
	e := getEnv(t)
	org := e.seedOrg(t, ctx)
	sink := PGSink{Pool: e.runtime, Schema: e.schema}
	repos := []Repo{{org.String(), "github", "1", "acme/a"}, {org.String(), "github", "2", "acme/b"}}
	first, err := sink.Apply(ctx, org, repos)
	if err != nil || !first.Changed {
		t.Fatalf("first apply: %+v %v", first, err)
	}
	before := e.syncAudits(t, ctx, org)
	second, err := sink.Apply(ctx, org, repos)
	if err != nil {
		t.Fatal(err)
	}
	if second.Changed {
		t.Error("a second apply with the same input reported Changed")
	}
	if after := e.syncAudits(t, ctx, org); after != before {
		t.Errorf("a second apply wrote %d audit row(s)", after-before)
	}
	moved, err := sink.Apply(ctx, org, []Repo{{org.String(), "github", "1", "acme/renamed"}, repos[1]})
	if err != nil || !moved.Changed {
		t.Fatalf("a rename must change: %+v %v", moved, err)
	}
	var path string
	if err := e.admin.QueryRow(ctx, `SELECT current_path FROM auth.repository_identities WHERE organization_id = $1 AND provider_repo_id = '1'`, org).Scan(&path); err != nil || path != "acme/renamed" {
		t.Errorf("path %q %v", path, err)
	}
}
