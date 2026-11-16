//go:build integration

package ssoprovider_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/auth/ssoprovider"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

// Every provider write of the store commits on a real PostgreSQL as auth-service's runtime role (a LOGIN role with the
// grants of the SSO providers switch, not the superuser): the row takes the new state, exactly one audit row of the
// write's event type is written, and no outbox row (provider config has no provisioning event).
func TestEveryProviderWriteCommitsAsTheRuntimeRoleWithOneAuditRowAndNoOutboxRow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = instance.Close(closeCtx)
	})
	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	pgschema.Apply(ctx, t, admin)
	role, err := containers.RoleName("ssoprovider_runtime", instance)
	if err != nil {
		t.Fatal(err)
	}
	const password = "ssoprovider-test-only-not-a-credential"
	if _, err := admin.Exec(ctx, fmt.Sprintf(`CREATE ROLE %q LOGIN PASSWORD '%s' NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS`, role, password)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { containers.DropRole(admin, role, t.Logf) })
	if _, err := authschema.Apply(ctx, admin, authschema.Options{Schema: "auth", RuntimeRole: role, UsersOwned: true, SSOProvidersOwned: true}); err != nil {
		t.Fatalf("authschema.Apply: %v", err)
	}
	cfg := admin.Config().Copy()
	cfg.ConnConfig.User, cfg.ConnConfig.Password = role, password
	runtime, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(runtime.Close)
	var super bool
	if err := runtime.QueryRow(ctx, `SELECT rolsuper FROM pg_roles WHERE rolname = current_user`).Scan(&super); err != nil || super {
		t.Fatalf("the store pool must be the runtime role (superuser=%v, err=%v)", super, err)
	}
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	store := ssoprovider.Store{Pool: runtime, Schema: schema}
	org, actor := uuid.New(), uuid.New()
	if _, err := admin.Exec(ctx, `INSERT INTO public.organizations (id, slug, name) VALUES ($1, $2, 'sso-store-test')`, org, "sso-"+org.String()[:8]); err != nil {
		t.Fatal(err)
	}
	who := ssoprovider.Actor{PrincipalID: &actor}

	counts := func(eventType string) (audit, outbox int) {
		t.Helper()
		if err := admin.QueryRow(ctx, `SELECT (SELECT count(*) FROM auth.security_audit_events WHERE event_type = $1),
			(SELECT count(*) FROM auth.auth_outbox_events)`, eventType).Scan(&audit, &outbox); err != nil {
			t.Fatal(err)
		}
		return
	}
	column := func(id uuid.UUID, col string) string {
		t.Helper()
		var v string
		if err := admin.QueryRow(ctx, `SELECT coalesce((SELECT `+col+` FROM public.sso_providers WHERE id = $1), '<no row>')`, id).Scan(&v); err != nil {
			t.Fatal(err)
		}
		return v
	}
	write := func(name, eventType string, do func() error, read func() string, want string) {
		t.Helper()
		a0, o0 := counts(eventType)
		err := do()
		a1, o1 := counts(eventType)
		if got := read(); err != nil || got != want || a1-a0 != 1 || o1-o0 != 0 {
			t.Errorf("%s: err=%v, state %q (want %q), %s audit rows +%d (want +1), outbox rows +%d (want +0)", name, err, got, want, eventType, a1-a0, o1-o0)
		}
	}

	config := pyjson.NewObject()
	config.Set("entity_id", "https://idp.example.test/saml/metadata")
	config.Set("sso_url", "https://idp.example.test/saml/sso")
	config.Set("certificate", "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n")
	var id uuid.UUID
	write("create", "auth.sso_provider.created", func() error {
		p, err := store.Create(ctx, org, ssoprovider.CreateInput{Name: "store-test", Protocol: ssoprovider.ProtocolSAML, DefaultRole: "member",
			Config: ssoprovider.Config{JSON: config}, AutoProvision: true, AllowedDomains: []string{}}, who)
		if p != nil {
			id = uuid.MustParse(p.ID)
		}
		return err
	}, func() string { return column(id, "status") }, ssoprovider.StatusPendingSetup)
	renamed := "store-test-renamed"
	write("update", "auth.sso_provider.updated", func() error {
		_, err := store.Update(ctx, org, id, ssoprovider.UpdateInput{Name: &renamed}, who)
		return err
	}, func() string { return column(id, "name") }, renamed)
	write("activate", "auth.sso_provider.activated", func() error {
		_, err := store.SetStatus(ctx, org, id, ssoprovider.StatusActive, who)
		return err
	}, func() string { return column(id, "status") }, ssoprovider.StatusActive)
	write("deactivate", "auth.sso_provider.deactivated", func() error {
		_, err := store.SetStatus(ctx, org, id, ssoprovider.StatusInactive, who)
		return err
	}, func() string { return column(id, "status") }, ssoprovider.StatusInactive)
	write("delete", "auth.sso_provider.deleted", func() error { return store.Delete(ctx, org, id, who) },
		func() string { return column(id, "status") }, "<no row>")
}
