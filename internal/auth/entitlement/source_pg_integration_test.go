//go:build integration

package entitlement_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/auth/entitlement"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

var (
	itAdmin, itRuntime *pgxpool.Pool
	itCleanup          []func()
)

func TestMain(m *testing.M) {
	code := m.Run()
	for i := len(itCleanup) - 1; i >= 0; i-- {
		itCleanup[i]()
	}
	os.Exit(code)
}

// runtimePool is a pool logged in as the auth runtime role. The role gets its public read grants only from authschema.Apply
// (PublicReadColumns); the test adds none, so a grant PGSource needs and the manifest lacks fails the read.
func runtimePool(t *testing.T) (admin, runtime *pgxpool.Pool) {
	t.Helper()
	if itRuntime != nil {
		return itAdmin, itRuntime
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatalf("test database: %v", err)
	}
	itCleanup = append(itCleanup, func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = instance.Close(closeCtx)
	})
	adminPool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	itCleanup = append(itCleanup, adminPool.Close)
	pgschema.Apply(ctx, t, adminPool)
	role, err := containers.RoleName("entitlement_runtime", instance)
	if err != nil {
		t.Fatal(err)
	}
	ident, err := authschema.NewValidatedIdentifier(role)
	if err != nil {
		t.Fatal(err)
	}
	const password = "entitlement-test-only-not-a-credential"
	if _, err := adminPool.Exec(ctx, fmt.Sprintf("CREATE ROLE %s LOGIN PASSWORD '%s'", authschema.Quote(ident), password)); err != nil {
		t.Fatal(err)
	}
	itCleanup = append(itCleanup, func() { containers.DropRole(adminPool, role, func(string, ...any) {}) })
	if _, err := authschema.Apply(ctx, adminPool, authschema.Options{Schema: "auth", RuntimeRole: role}); err != nil {
		t.Fatalf("authschema.Apply: %v", err)
	}
	cfg := adminPool.Config().Copy()
	cfg.ConnConfig.User = role
	cfg.ConnConfig.Password = password
	rt, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	itCleanup = append(itCleanup, rt.Close)
	itAdmin, itRuntime = adminPool, rt
	return itAdmin, itRuntime
}

func TestPGSource_AsAuthRuntimeRole_ReadsLicenseAndOrganizationTier(t *testing.T) {
	ctx := context.Background()
	admin, runtime := runtimePool(t)
	source := entitlement.PGSource{Q: runtime}

	licensed, bare, none := uuid.New(), uuid.New(), uuid.New()
	for _, org := range []uuid.UUID{licensed, bare} {
		if _, err := admin.Exec(ctx, `INSERT INTO public.organizations (id, slug, name, tier) VALUES ($1, $2, 'entitlement-test', 'enterprise')`,
			org, "ent-"+org.String()); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := admin.Exec(ctx, `INSERT INTO public.org_licenses (id, org_id, tier, features_override, created_at, updated_at)
VALUES ($1, $2, 'team', '{"sso_saml": true}'::json, now(), now())`, uuid.New(), licensed); err != nil {
		t.Fatal(err)
	}

	got, err := source.Load(ctx, licensed)
	if err != nil {
		t.Fatalf("license read as the runtime role: %v", err)
	}
	if got.Tier != "team" || !got.Features["webhooks"] || !got.Features["sso_saml"] {
		t.Fatalf("license row: %+v", got)
	}

	got, err = source.Load(ctx, bare)
	if err != nil {
		t.Fatalf("organization tier read as the runtime role: %v", err)
	}
	if got.Tier != "enterprise" || !got.Features["sso_saml"] {
		t.Fatalf("organization tier: %+v", got)
	}

	got, err = source.Load(ctx, none)
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range got.Features {
		if v {
			t.Fatalf("unknown org holds %s", k)
		}
	}
}

func TestAdapter_AsAuthRuntimeRole_DeniesWhenGrantedFeatureMissing(t *testing.T) {
	ctx := context.Background()
	admin, runtime := runtimePool(t)
	org := uuid.New()
	if _, err := admin.Exec(ctx, `INSERT INTO public.organizations (id, slug, name, tier) VALUES ($1, $2, 'entitlement-test', 'community')`,
		org, "ent-"+org.String()); err != nil {
		t.Fatal(err)
	}
	a, err := entitlement.New(entitlement.Config{Source: entitlement.PGSource{Q: runtime}})
	if err != nil {
		t.Fatal(err)
	}
	if got := a.Check(ctx, org, "sso_saml"); got.Allowed || got.Reason != "entitlement_not_granted" {
		t.Fatalf("community org: %+v", got)
	}
}
