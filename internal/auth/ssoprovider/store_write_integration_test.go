//go:build integration

package ssoprovider_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
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
	// The organization and the actor exist in the auth schema (the audit row references both), projected from public rows
	// by the identity store as auth-service does.
	if _, err := admin.Exec(ctx, `INSERT INTO public.organizations (id, slug, name) VALUES ($1, $2, 'sso-store-test')`, org, "sso-"+org.String()[:8]); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, `INSERT INTO public.users
	(id, email, password_hash, auth_provider, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, NULL, 'local', true, true, false, 0, now(), now())`, actor, "sso-actor-"+actor.String()[:8]+"@example.test"); err != nil {
		t.Fatal(err)
	}
	ids, err := identity.New(identity.Config{Pool: runtime, Schema: schema, UsersOwnedByAuth: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := ids.ProjectOrganization(ctx, org); err != nil {
		t.Fatalf("ProjectOrganization: %v", err)
	}
	if err := ids.Project(ctx, actor); err != nil {
		t.Fatalf("Project: %v", err)
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

	// A missing id is the expected refusal ErrNotFound (the legacy 404): nothing is written, and the refusal is a WARN line,
	// never an ERROR line.
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previous) })
	missing := uuid.New()
	a0, o0 := counts("auth.sso_provider.updated")
	for name, do := range map[string]func() error{
		"update": func() error {
			_, err := store.Update(ctx, org, missing, ssoprovider.UpdateInput{Name: &renamed}, who)
			return err
		},
		"activate": func() error { _, err := store.SetStatus(ctx, org, missing, ssoprovider.StatusActive, who); return err },
		"deactivate": func() error {
			_, err := store.SetStatus(ctx, org, missing, ssoprovider.StatusInactive, who)
			return err
		},
		"delete": func() error { return store.Delete(ctx, org, missing, who) },
	} {
		logs.Reset()
		if err := do(); !errors.Is(err, ssoprovider.ErrNotFound) {
			t.Errorf("%s of a missing id: err = %v, want ErrNotFound", name, err)
		}
		text := logs.String()
		if !strings.Contains(text, "level=WARN") || !strings.Contains(text, "CommitPublicProjection refused") || strings.Contains(text, "level=ERROR") {
			t.Errorf("%s of a missing id logged %q, want one WARN refusal line and no ERROR", name, text)
		}
	}
	if a1, o1 := counts("auth.sso_provider.updated"); a1 != a0 || o1 != o0 {
		t.Errorf("the refused writes wrote audit rows +%d, outbox rows +%d, want none", a1-a0, o1-o0)
	}

	// A create or a rename to a name another provider of the organization holds is ErrNameTaken (the unique key
	// uq_sso_provider_org_name), a WARN refusal: no row changes and no audit row. Its own name, and the same name in another
	// organization, are not refused.
	create := func(o uuid.UUID, name string) (uuid.UUID, error) {
		p, err := store.Create(ctx, o, ssoprovider.CreateInput{Name: name, Protocol: ssoprovider.ProtocolSAML, DefaultRole: "member",
			Config: ssoprovider.Config{JSON: config}, AutoProvision: true, AllowedDomains: []string{}}, who)
		if p == nil {
			return uuid.Nil, err
		}
		return uuid.MustParse(p.ID), err
	}
	snapshot := func() string {
		t.Helper()
		var v string
		if err := admin.QueryRow(ctx, `SELECT coalesce(string_agg(id || ':' || name || ':' || updated_at, ';' ORDER BY id), '') FROM public.sso_providers`).Scan(&v); err != nil {
			t.Fatal(err)
		}
		return v
	}
	taken, err := create(org, "taken")
	if err != nil {
		t.Fatalf("seed create: %v", err)
	}
	other, err := create(org, "other")
	if err != nil {
		t.Fatalf("seed create: %v", err)
	}
	before := snapshot()
	c0, _ := counts("auth.sso_provider.created")
	u0, _ := counts("auth.sso_provider.updated")
	takenName := "taken"
	for name, do := range map[string]func() error{
		"create": func() error { _, err := create(org, "taken"); return err },
		"rename": func() error {
			_, err := store.Update(ctx, org, other, ssoprovider.UpdateInput{Name: &takenName}, who)
			return err
		},
	} {
		logs.Reset()
		if err := do(); !errors.Is(err, ssoprovider.ErrNameTaken) || errors.Is(err, ssoprovider.ErrStore) {
			t.Errorf("%s to a taken name: err = %v, want ErrNameTaken and not ErrStore", name, err)
		}
		text := logs.String()
		if !strings.Contains(text, "level=WARN") || !strings.Contains(text, "CommitPublicProjection refused") || strings.Contains(text, "level=ERROR") {
			t.Errorf("%s to a taken name logged %q, want one WARN refusal line and no ERROR", name, text)
		}
	}
	c1, _ := counts("auth.sso_provider.created")
	u1, _ := counts("auth.sso_provider.updated")
	if after := snapshot(); after != before || c1 != c0 || u1 != u0 {
		t.Errorf("a refused taken-name write changed rows (%v) or wrote audit rows (created +%d, updated +%d)", after != before, c1-c0, u1-u0)
	}
	if _, err := store.Update(ctx, org, taken, ssoprovider.UpdateInput{Name: &takenName}, who); err != nil {
		t.Errorf("rename to its own name: %v, want nil", err)
	}
	second := uuid.New()
	if _, err := admin.Exec(ctx, `INSERT INTO public.organizations (id, slug, name) VALUES ($1, $2, 'sso-store-test-2')`, second, "sso-"+second.String()[:8]); err != nil {
		t.Fatal(err)
	}
	if err := ids.ProjectOrganization(ctx, second); err != nil {
		t.Fatalf("ProjectOrganization: %v", err)
	}
	if _, err := create(second, "taken"); err != nil {
		t.Errorf("the same name in another organization: %v, want nil", err)
	}
}
