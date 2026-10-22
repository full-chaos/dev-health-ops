//go:build integration

package identity

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

// raceMarker is what `check_go.sh auth race-integration` counts: a race test
// prints it only when it ran with -race against a live database.
const raceMarker = "AUTH_RACE_EXECUTED"

// projectedColumns is the read-only column grant on public.users that Q1
// gives auth-service until the users cutover.
const projectedColumns = "id, email, password_hash, is_active, is_verified, auth_provider, auth_provider_id"

type env struct {
	admin   *pgxpool.Pool
	runtime *pgxpool.Pool
	store   *Store
	// owned is a Store after the users cutover (UsersOwnedByAuth).
	owned *Store
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

// getEnv builds one PostgreSQL for the package: the real public schema
// (pgschema), the auth schema (authschema.Apply), a runtime role holding the
// declared posture plus the column grant above, and a Store that connects AS
// that role with its default search_path.
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
	role, err := containers.RoleName("identity_runtime", instance)
	if err != nil {
		return nil, fmt.Errorf("derive role: %w", err)
	}
	const password = "identity-test-only-not-a-credential"
	if _, err := admin.Exec(ctx, fmt.Sprintf(`CREATE ROLE %q LOGIN PASSWORD '%s'`, role, password)); err != nil {
		return nil, fmt.Errorf("create role: %w", err)
	}
	closers = append(closers, func() { containers.DropRole(admin, role, func(string, ...any) {}) })
	if _, err := authschema.Apply(ctx, admin, authschema.Options{Schema: "auth", RuntimeRole: role}); err != nil {
		return nil, fmt.Errorf("authschema.Apply: %w", err)
	}
	for _, grant := range []string{
		fmt.Sprintf(`GRANT SELECT (%s) ON public.users TO %q`, projectedColumns, role),
		fmt.Sprintf(`GRANT SELECT (user_id, org_id) ON public.memberships TO %q`, role),
		fmt.Sprintf(`GRANT SELECT (id, org_id) ON public.sso_providers TO %q`, role),
		fmt.Sprintf(`GRANT SELECT (id, slug, name, is_active) ON public.organizations TO %q`, role),
	} {
		if _, err := admin.Exec(ctx, grant); err != nil {
			return nil, fmt.Errorf("grant public columns: %w", err)
		}
	}
	cfg := admin.Config().Copy()
	cfg.ConnConfig.User = role
	cfg.ConnConfig.Password = password
	cfg.MaxConns = 24
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
	owned, err := New(Config{Pool: runtime, Schema: schema, UsersOwnedByAuth: true})
	if err != nil {
		return nil, err
	}
	return &env{admin: admin, runtime: runtime, store: store, owned: owned}, nil
}

// seedUser inserts a public.users row as go-api would (admin role).
func (e *env) seedUser(t *testing.T, ctx context.Context, email string, active, verified bool) uuid.UUID {
	t.Helper()
	id := uuid.New()
	if _, err := e.admin.Exec(ctx, `INSERT INTO public.users
	(id, email, password_hash, auth_provider, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, '$2b$12$abcdefghijklmnopqrstuuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0', 'local', $3, $4, false, 0, now(), now())`,
		id, email, active, verified); err != nil {
		t.Fatalf("seed public.users: %v", err)
	}
	return id
}

func uniqueEmail(prefix string) string {
	return fmt.Sprintf("%s-%s@example.test", prefix, uuid.NewString()[:8])
}

func (e *env) count(t *testing.T, ctx context.Context, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := e.admin.QueryRow(ctx, sql, args...).Scan(&n); err != nil {
		t.Fatalf("count %q: %v", sql, err)
	}
	return n
}

func (e *env) auditRows(t *testing.T, ctx context.Context, eventType, subject string) int {
	t.Helper()
	return e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = $1 AND subject_principal_id::text = $2`, eventType, subject)
}

func (e *env) outboxRows(t *testing.T, ctx context.Context, eventType, aggregate string) int {
	t.Helper()
	return e.count(t, ctx, `SELECT count(*) FROM auth.auth_outbox_events WHERE event_type = $1
AND aggregate_id IN ('prn_' || $2, 'idn_' || $2, 'org_' || $2)`, eventType, aggregate)
}

func (e *env) project(t *testing.T, ctx context.Context, id uuid.UUID) {
	t.Helper()
	if err := e.store.Project(ctx, id); err != nil {
		t.Fatalf("Project: %v", err)
	}
}

// lookup is IdentityBySubject with no legacy fallback, as (id, error):
// ErrNotFound on a miss.
func (e *env) lookup(ctx context.Context, key ExternalKey) (uuid.UUID, error) {
	p, found, err := e.store.IdentityBySubject(ctx, key.Issuer, key.Provider, key.Subject, nil)
	if err != nil {
		return uuid.Nil, err
	}
	if !found {
		return uuid.Nil, ErrNotFound
	}
	return p.ID, nil
}

func googleKey(subject string) ExternalKey {
	return ExternalKey{Issuer: "https://accounts.google.com", Provider: ProviderGoogle, Subject: subject}
}

func TestRuntimeRoleUsesItsDefaultSearchPath(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	var path string
	if err := e.runtime.QueryRow(ctx, `SHOW search_path`).Scan(&path); err != nil {
		t.Fatal(err)
	}
	if path != `"$user", public` {
		t.Fatalf("runtime search_path = %q, want the PostgreSQL default", path)
	}
}

func TestByEmailReadsPublicUsersLive(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	email := uniqueEmail("live")
	id := e.seedUser(t, ctx, email, true, false)

	got, err := e.store.ByEmail(ctx, email)
	if err != nil || got == nil {
		t.Fatalf("ByEmail = %v, %v", got, err)
	}
	if got.ID != id || !got.IsActive || got.IsVerified || got.PasswordHash == nil || got.AuthProvider == nil || *got.AuthProvider != "local" {
		t.Fatalf("ByEmail = %+v", got)
	}
	// go-api changes the password and state; the next read sees it at once.
	if _, err := e.admin.Exec(ctx, `UPDATE public.users SET password_hash = NULL, is_active = false, is_verified = true WHERE id = $1`, id); err != nil {
		t.Fatal(err)
	}
	got, err = e.store.ByEmail(ctx, email)
	if err != nil || got == nil || got.PasswordHash != nil || got.IsActive || !got.IsVerified {
		t.Fatalf("ByEmail after a go-api write = %+v, %v", got, err)
	}

	unknown, err := e.store.ByEmail(ctx, uniqueEmail("nobody"))
	if unknown != nil || err != nil {
		t.Fatalf("ByEmail(unknown) = %v, %v; want nil, nil", unknown, err)
	}
}

func TestByEmailMatchesLowerEmailAndRefusesTwoRows(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	lower := uniqueEmail("case")
	upper := "CASE" + lower[len("case"):]
	e.seedUser(t, ctx, upper, true, true)
	if got, err := e.store.ByEmail(ctx, lower); err != nil || got == nil {
		t.Fatalf("ByEmail(%q) over a stored %q = %v, %v", lower, upper, got, err)
	}
	e.seedUser(t, ctx, lower, true, true)
	if _, err := e.store.ByEmail(ctx, lower); !errors.Is(err, ErrMultipleUsers) {
		t.Fatalf("ByEmail over two case variants = %v, want ErrMultipleUsers", err)
	}
}

func TestProjectInsertsPrincipalAndUserWithTheSameID(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	email := "  Mixed.Case-" + uuid.NewString()[:8] + "@Example.COM"
	id := e.seedUser(t, ctx, email, true, true)
	e.project(t, ctx, id)

	var kind string
	var disabled, verified bool
	var storedEmail, emailLower string
	var hash *string
	if err := e.admin.QueryRow(ctx, `SELECT p.kind, p.disabled_at IS NOT NULL, u.email, u.email_lower, u.email_verified_at IS NOT NULL, u.password_hash
FROM auth.principals p JOIN auth.users u ON u.principal_id = p.id WHERE p.id = $1`, id).
		Scan(&kind, &disabled, &storedEmail, &emailLower, &verified, &hash); err != nil {
		t.Fatalf("read projection: %v", err)
	}
	want, _ := NormalizeEmail(email)
	if kind != "user" || disabled || !verified || storedEmail != email || emailLower != want {
		t.Fatalf("projection = kind %q disabled %v verified %v email %q lower %q (want lower %q)", kind, disabled, verified, storedEmail, emailLower, want)
	}
	if hash != nil {
		t.Fatalf("projection copied the password verifier; auth.users.password_hash must stay NULL until the users cutover")
	}
	if p, u := e.auditRows(t, ctx, "auth.principal.projected", id.String()), e.auditRows(t, ctx, "auth.principal.projection_updated", id.String()); p != 1 || u != 0 {
		t.Fatalf("a first projection wrote %d creation and %d update audit rows, want 1 and 0", p, u)
	}
}

func TestProjectUnchangedWritesNothing(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	id := e.seedUser(t, ctx, uniqueEmail("same"), true, false)
	e.project(t, ctx, id)
	var before time.Time
	if err := e.admin.QueryRow(ctx, `SELECT updated_at FROM auth.users WHERE principal_id = $1`, id).Scan(&before); err != nil {
		t.Fatal(err)
	}
	e.project(t, ctx, id)
	var after time.Time
	if err := e.admin.QueryRow(ctx, `SELECT updated_at FROM auth.users WHERE principal_id = $1`, id).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if !after.Equal(before) || e.auditRows(t, ctx, "auth.principal.projection_updated", id.String()) != 0 {
		t.Fatalf("an unchanged re-projection wrote a row or an audit event")
	}
}

func TestProjectDisableEnableAndVerifyAreAudited(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	id := e.seedUser(t, ctx, uniqueEmail("toggle"), true, false)
	e.project(t, ctx, id)

	if _, err := e.admin.Exec(ctx, `UPDATE public.users SET is_active = false, is_verified = true WHERE id = $1`, id); err != nil {
		t.Fatal(err)
	}
	e.project(t, ctx, id)
	var disabled, verified bool
	if err := e.admin.QueryRow(ctx, `SELECT p.disabled_at IS NOT NULL, u.email_verified_at IS NOT NULL
FROM auth.principals p JOIN auth.users u ON u.principal_id = p.id WHERE p.id = $1`, id).Scan(&disabled, &verified); err != nil {
		t.Fatal(err)
	}
	if !disabled || !verified {
		t.Fatalf("after disable+verify: disabled %v verified %v", disabled, verified)
	}
	var reason string
	var fields []string
	if err := e.admin.QueryRow(ctx, `SELECT attributes->>'reason_code', ARRAY(SELECT jsonb_array_elements_text(attributes->'changed_fields'))
FROM auth.security_audit_events WHERE event_type = 'auth.principal.projection_updated' AND subject_principal_id::text = $1`, id.String()).
		Scan(&reason, &fields); err != nil {
		t.Fatalf("read audit row: %v", err)
	}
	if reason != "principal_disabled" || len(fields) != 2 || fields[0] != "disabled_at" || fields[1] != "email_verified_at" {
		t.Fatalf("audit row reason %q changed_fields %v", reason, fields)
	}
	if d, v := e.outboxRows(t, ctx, "principal.disabled", id.String()), e.outboxRows(t, ctx, "user.email_verified", id.String()); d != 1 || v != 1 {
		t.Fatalf("principal.disabled rows %d, user.email_verified rows %d; want 1 and 1", d, v)
	}

	if _, err := e.admin.Exec(ctx, `UPDATE public.users SET is_active = true WHERE id = $1`, id); err != nil {
		t.Fatal(err)
	}
	e.project(t, ctx, id)
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events
WHERE event_type = 'auth.principal.projection_updated' AND subject_principal_id::text = $1 AND attributes->>'reason_code' = 'principal_enabled'`, id.String()); n != 1 {
		t.Fatalf("enable audit rows = %d, want 1", n)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.principals WHERE id = $1 AND disabled_at IS NULL`, id); n != 1 {
		t.Fatal("principal not re-enabled")
	}
}

func TestProjectEmailChangeIsAudited(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	id := e.seedUser(t, ctx, uniqueEmail("old"), true, true)
	e.project(t, ctx, id)
	next := uniqueEmail("new")
	if _, err := e.admin.Exec(ctx, `UPDATE public.users SET email = $2 WHERE id = $1`, id, next); err != nil {
		t.Fatal(err)
	}
	e.project(t, ctx, id)
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.users WHERE principal_id = $1 AND email = $2 AND email_lower = $2`, id, next); n != 1 {
		t.Fatal("email not re-projected")
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.principal.projection_updated'
AND subject_principal_id::text = $1 AND attributes->>'reason_code' = 'principal_projected' AND attributes->'changed_fields' = '["email"]'::jsonb`, id.String()); n != 1 {
		t.Fatalf("email change audit rows = %d, want 1", n)
	}
	if n := e.outboxRows(t, ctx, "user.email_changed", id.String()); n != 1 {
		t.Fatalf("user.email_changed rows = %d, want 1", n)
	}
}

func TestProjectRefusesAnEmailCollisionAndMergesNothing(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	lower := uniqueEmail("collide")
	first := e.seedUser(t, ctx, lower, true, true)
	second := e.seedUser(t, ctx, "COLLIDE"+lower[len("collide"):], true, true)
	e.project(t, ctx, first)
	if err := e.store.Project(ctx, second); !errors.Is(err, ErrEmailCollision) {
		t.Fatalf("Project of a case variant = %v, want ErrEmailCollision", err)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.principals WHERE id = $1`, second); n != 0 {
		t.Fatalf("the colliding user got a principal")
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.users WHERE principal_id = $1 AND email = $2`, first, lower); n != 1 {
		t.Fatalf("the first projection changed")
	}

	// The same refusal holds for an email change onto a held key.
	third := e.seedUser(t, ctx, uniqueEmail("third"), true, true)
	e.project(t, ctx, third)
	if _, err := e.admin.Exec(ctx, `UPDATE public.users SET email = $2 WHERE id = $1`, third, "Collide"+lower[len("collide"):]); err != nil {
		t.Fatal(err)
	}
	if err := e.store.Project(ctx, third); !errors.Is(err, ErrEmailCollision) {
		t.Fatalf("Project of an email change onto a held key = %v, want ErrEmailCollision", err)
	}
}

func TestProjectUnknownUser(t *testing.T) {
	e := getEnv(t)
	if err := e.store.Project(context.Background(), uuid.New()); !errors.Is(err, ErrUserNotFound) {
		t.Fatalf("Project(unknown) = %v, want ErrUserNotFound", err)
	}
}

func TestLinkIsAuditedAndLookupMatchesTheFullKeyOnly(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	id := e.seedUser(t, ctx, uniqueEmail("link"), true, true)
	e.project(t, ctx, id)
	key := ExternalKey{Issuer: "https://idp.example.com/realms/acme", Provider: Provider(uuid.NewString()), Subject: "sub-" + uuid.NewString()}
	actor := e.seedUser(t, ctx, uniqueEmail("admin"), true, true)
	e.project(t, ctx, actor)
	request := uuid.NewString()

	linkID, err := e.store.LinkIdentity(ctx, key, id, Actor{PrincipalID: &actor, RequestID: &request})
	if err != nil {
		t.Fatalf("LinkIdentity: %v", err)
	}
	if got, err := e.lookup(ctx, key); err != nil || got != id {
		t.Fatalf("lookup = %v, %v; want %v", got, err, id)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.identity.linked'
AND subject_principal_id::text = $1 AND actor_principal_id::text = $2 AND attributes->>'target_id' = $3
AND NOT attributes ? 'provider' AND attributes->>'reason_code' = 'identity_linked'`, id.String(), actor.String(), linkID.String()); n != 1 {
		t.Fatalf("link audit rows = %d, want 1", n)
	}
	if n := e.outboxRows(t, ctx, "external_identity.linked", linkID.String()); n != 1 {
		t.Fatalf("link outbox rows = %d, want 1", n)
	}

	for name, other := range map[string]ExternalKey{
		"other issuer":     {Issuer: "https://evil.example", Provider: key.Provider, Subject: key.Subject},
		"other provider":   {Issuer: key.Issuer, Provider: Provider(uuid.NewString()), Subject: key.Subject},
		"subject case":     {Issuer: key.Issuer, Provider: key.Provider, Subject: "SUB-" + key.Subject[len("sub-"):]},
		"issuer case":      {Issuer: "https://IDP.example.com/realms/acme", Provider: key.Provider, Subject: key.Subject},
		"issuer slash":     {Issuer: key.Issuer + "/", Provider: key.Provider, Subject: key.Subject},
		"social same name": {Issuer: "https://accounts.google.com", Provider: ProviderGoogle, Subject: key.Subject},
	} {
		if _, err := e.lookup(ctx, other); !errors.Is(err, ErrNotFound) {
			t.Errorf("%s: lookup = %v, want ErrNotFound", name, err)
		}
	}
	if _, err := e.store.LinkIdentity(ctx, key, id, Actor{}); !errors.Is(err, ErrAlreadyLinked) {
		t.Fatalf("relink by the holder = %v, want ErrAlreadyLinked", err)
	}
}

func TestLinkOfAHeldKeyIsAConflictAndNeverRepoints(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	holder := e.seedUser(t, ctx, uniqueEmail("holder"), true, true)
	other := e.seedUser(t, ctx, uniqueEmail("other"), true, true)
	e.project(t, ctx, holder)
	e.project(t, ctx, other)
	key := googleKey("held-" + uuid.NewString())
	if _, err := e.store.LinkIdentity(ctx, key, holder, Actor{}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.store.LinkIdentity(ctx, key, other, Actor{}); !errors.Is(err, ErrIdentityConflict) {
		t.Fatalf("link of a held key = %v, want ErrIdentityConflict", err)
	}
	if got, _ := e.lookup(ctx, key); got != holder {
		t.Fatalf("the key moved to %v", got)
	}
	if n := e.auditRows(t, ctx, "auth.identity.linked", other.String()); n != 0 {
		t.Fatalf("a refused link wrote %d audit rows", n)
	}
	if err := e.store.UnlinkIdentity(ctx, key, other, Actor{}); !errors.Is(err, ErrNotOwner) {
		t.Fatalf("unlink by a non-holder = %v, want ErrIdentityConflict", err)
	}
	if got, _ := e.lookup(ctx, key); got != holder {
		t.Fatal("a non-holder unlink removed the link")
	}
}

// TestAMatchingEmailNeverLinks: a principal whose email equals what a provider
// asserts is not found by, and not linked to, that provider's subject. Only
// LinkIdentity links, and it takes no email.
func TestAMatchingEmailNeverLinks(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	email := uniqueEmail("victim")
	victim := e.seedUser(t, ctx, email, true, true)
	e.project(t, ctx, victim)
	key := googleKey(email)
	if _, err := e.lookup(ctx, key); !errors.Is(err, ErrNotFound) {
		t.Fatalf("lookup with the email as subject = %v, want ErrNotFound", err)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.external_identities WHERE principal_id = $1`, victim); n != 0 {
		t.Fatalf("an external identity appeared without LinkIdentity")
	}
}

func TestLinkToAMissingOrDisabledPrincipalIsRefused(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	if _, err := e.store.LinkIdentity(ctx, googleKey("ghost-"+uuid.NewString()), uuid.New(), Actor{}); !errors.Is(err, ErrPrincipalUnavailable) {
		t.Fatalf("link to a missing principal = %v, want ErrPrincipalUnavailable", err)
	}
	id := e.seedUser(t, ctx, uniqueEmail("disabled"), false, true)
	e.project(t, ctx, id)
	if _, err := e.store.LinkIdentity(ctx, googleKey("off-"+uuid.NewString()), id, Actor{}); !errors.Is(err, ErrPrincipalUnavailable) {
		t.Fatalf("link to a disabled principal = %v, want ErrPrincipalUnavailable", err)
	}
}

func TestUnlinkIsAudited(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	id := e.seedUser(t, ctx, uniqueEmail("unlink"), true, true)
	e.project(t, ctx, id)
	key := ExternalKey{Issuer: "https://github.com", Provider: ProviderGitHub, Subject: uuid.NewString()}
	linkID, err := e.store.LinkIdentity(ctx, key, id, Actor{})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.store.UnlinkIdentity(ctx, key, id, Actor{}); err != nil {
		t.Fatalf("UnlinkIdentity: %v", err)
	}
	if _, err := e.lookup(ctx, key); !errors.Is(err, ErrNotFound) {
		t.Fatalf("lookup after unlink = %v", err)
	}
	if n := e.outboxRows(t, ctx, "external_identity.unlinked", linkID.String()); n != 1 {
		t.Fatalf("unlink outbox rows = %d", n)
	}
	if n := e.auditRows(t, ctx, "auth.identity.unlinked", id.String()); n != 1 {
		t.Fatalf("unlink audit rows = %d", n)
	}
	if err := e.store.UnlinkIdentity(ctx, key, id, Actor{}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("second unlink = %v, want ErrNotFound", err)
	}
}

// TestALegacyEmptyIssuerRowIsNotMatched: rows from before migration 0010 carry
// issuer ”. A lookup always names an issuer (Validate refuses ”), so such a
// row is never found by a provider assertion.
func TestALegacyEmptyIssuerRowIsNotMatched(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	id := e.seedUser(t, ctx, uniqueEmail("legacy"), true, true)
	e.project(t, ctx, id)
	subject := "legacy-" + uuid.NewString()
	// The row a pre-0010 writer left: no issuer column was written, so it
	// holds the migration default ''.
	legacy := uuid.NewString()
	principal := id.String()
	if err := audit.Commit(ctx, e.runtime, e.store.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			_, err := tx.Exec(ctx, `INSERT INTO auth.external_identities (id, principal_id, provider, subject) VALUES ($1, $2, 'google', $3)`,
				legacy, id, subject)
			return err
		},
		Audit: audit.AuditEvent{EventType: "auth.identity.linked", Outcome: audit.OutcomeAllowed, SubjectPrincipalID: &principal,
			Attributes: map[string]any{"reason_code": "identity_linked", "target_kind": "external_identity", "target_id": legacy}},
		Events: []audit.OutboxEvent{{AggregateType: "external_identity", AggregateID: "idn_" + legacy, EventType: "external_identity.linked",
			Change: map[string]any{"provider": "google", "principal_id": "prn_" + principal}}},
	}); err != nil {
		t.Fatalf("seed legacy row: %v", err)
	}
	if _, err := e.lookup(ctx, googleKey(subject)); !errors.Is(err, ErrNotFound) {
		t.Fatalf("lookup matched a legacy empty-issuer row: %v", err)
	}
	if _, err := e.lookup(ctx, ExternalKey{Provider: ProviderGoogle, Subject: subject}); !errors.Is(err, ErrInvalidExternalKey) {
		t.Fatalf("lookup with an empty issuer = %v, want ErrInvalidExternalKey", err)
	}
}

// TestADirectWriteOutsideAuditCommitIsRefused: the runtime role cannot write
// external_identities except through audit.Commit (the 0006 guard), so a link
// that skips the audit row is impossible for this package too.
func TestADirectWriteOutsideAuditCommitIsRefused(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	id := e.seedUser(t, ctx, uniqueEmail("direct"), true, true)
	e.project(t, ctx, id)
	_, err := e.runtime.Exec(ctx, `INSERT INTO auth.external_identities (principal_id, issuer, provider, subject) VALUES ($1, 'https://x', 'google', 'y')`, id)
	if err == nil {
		t.Fatal("the runtime role wrote external_identities outside audit.Commit")
	}
}

func TestRaceLinkOfOneKeyToManyPrincipalsHasOneWinner(t *testing.T) {
	if raceEnabled {
		t.Logf("%s %s", raceMarker, t.Name())
	}
	e := getEnv(t)
	ctx := context.Background()
	const n = 12
	principals := make([]uuid.UUID, n)
	for i := range principals {
		principals[i] = e.seedUser(t, ctx, uniqueEmail(fmt.Sprintf("race%d", i)), true, true)
		e.project(t, ctx, principals[i])
	}
	key := googleKey("race-" + uuid.NewString())
	errs := make([]error, n)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range principals {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, errs[i] = e.store.LinkIdentity(ctx, key, principals[i], Actor{})
		}(i)
	}
	close(start)
	wg.Wait()
	winners := 0
	var winner uuid.UUID
	for i, err := range errs {
		switch {
		case err == nil:
			winners++
			winner = principals[i]
		case errors.Is(err, ErrIdentityConflict):
		default:
			t.Errorf("principal %d: %v", i, err)
		}
	}
	if winners != 1 {
		t.Fatalf("%d links won, want exactly 1", winners)
	}
	if got, err := e.lookup(ctx, key); err != nil || got != winner {
		t.Fatalf("lookup = %v, %v; want the winner %v", got, err, winner)
	}
	if c := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.identity.linked' AND attributes->>'provider' = 'google'
AND subject_principal_id::text = ANY($1)`, uuidStrings(principals)); c != 1 {
		t.Fatalf("link audit rows across the racers = %d, want 1", c)
	}
}

func TestRaceConcurrentProjectionsOfOneUserConverge(t *testing.T) {
	if raceEnabled {
		t.Logf("%s %s", raceMarker, t.Name())
	}
	e := getEnv(t)
	ctx := context.Background()
	id := e.seedUser(t, ctx, uniqueEmail("conc"), true, false)
	e.project(t, ctx, id)
	if _, err := e.admin.Exec(ctx, `UPDATE public.users SET is_active = false WHERE id = $1`, id); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make([]error, 8)
	start := make(chan struct{})
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			errs[i] = e.store.Project(ctx, id)
		}(i)
	}
	close(start)
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Errorf("projection %d: %v", i, err)
		}
	}
	if n := e.auditRows(t, ctx, "auth.principal.projection_updated", id.String()); n != 1 {
		t.Fatalf("concurrent projections wrote %d audit rows, want 1", n)
	}
}

func uuidStrings(ids []uuid.UUID) []string {
	out := make([]string, len(ids))
	for i, id := range ids {
		out[i] = id.String()
	}
	return out
}

// seedLegacy inserts a public.users row as go-api social or SSO provisioning
// writes it: auth_provider and auth_provider_id set, password optional.
func (e *env) seedLegacy(t *testing.T, ctx context.Context, email, authProvider, authProviderID string, password bool) uuid.UUID {
	t.Helper()
	id := uuid.New()
	var hash *string
	if password {
		h := "$2b$12$abcdefghijklmnopqrstuuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0"
		hash = &h
	}
	if _, err := e.admin.Exec(ctx, `INSERT INTO public.users
	(id, email, password_hash, auth_provider, auth_provider_id, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, true, true, false, 0, now(), now())`, id, email, hash, authProvider, authProviderID); err != nil {
		t.Fatalf("seed legacy public.users: %v", err)
	}
	return id
}

func (e *env) seedOrg(t *testing.T, ctx context.Context) uuid.UUID {
	t.Helper()
	id := uuid.New()
	if _, err := e.admin.Exec(ctx, `INSERT INTO public.organizations (id, slug, name) VALUES ($1, $2, 'identity-test')`, id, "org-"+id.String()); err != nil {
		t.Fatalf("seed organization: %v", err)
	}
	return id
}

func (e *env) seedSSOProvider(t *testing.T, ctx context.Context, org uuid.UUID, protocol string) Provider {
	t.Helper()
	id := uuid.New()
	if _, err := e.admin.Exec(ctx, `INSERT INTO public.sso_providers (id, org_id, name, protocol, status) VALUES ($1, $2, $3, $4, 'active')`, id, org, "idp-"+id.String(), protocol); err != nil {
		t.Fatalf("seed sso provider: %v", err)
	}
	return Provider(id.String())
}

func (e *env) seedMembership(t *testing.T, ctx context.Context, user, org uuid.UUID) {
	t.Helper()
	if _, err := e.admin.Exec(ctx, `INSERT INTO public.memberships (id, user_id, org_id, role) VALUES ($1, $2, $3, 'member')`, uuid.New(), user, org); err != nil {
		t.Fatalf("seed membership: %v", err)
	}
}

func socialLegacy(p Provider) *LegacyMatch { return &LegacyMatch{AuthProvider: string(p)} }

func TestProjectKeepsARefusedAddressAsItsPythonLowerKey(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	email := " Someone-" + uuid.NewString()[:8] + "@Example.TEST"
	if _, err := NormalizeEmail(email); err == nil {
		t.Fatal("the fixture must be an address EmailStr refuses")
	}
	id := e.seedUser(t, ctx, email, true, true)
	e.project(t, ctx, id)
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.users WHERE principal_id = $1 AND email_lower = lower(btrim($2))`, id, email); n != 1 {
		t.Fatal("a refused address did not keep strip(lower()) as its key")
	}
}

func TestIdentityBySubjectProjectsALegacySocialUser(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	subject := "gh-" + uuid.NewString()
	user := e.seedLegacy(t, ctx, uniqueEmail("gh"), "github", subject, false)
	issuer, _ := SocialIssuer(ProviderGitHub)

	if _, found, err := e.store.IdentityBySubject(ctx, issuer, ProviderGitHub, subject, nil); err != nil || found {
		t.Fatalf("no legacy match: found %v, %v; want a miss", found, err)
	}
	p, found, err := e.store.IdentityBySubject(ctx, issuer, ProviderGitHub, subject, socialLegacy(ProviderGitHub))
	if err != nil || !found || p.ID != user || !p.IsActive || p.DisabledAt != nil {
		t.Fatalf("legacy social = %+v, %v, %v; want user %v", p, found, err, user)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.external_identities WHERE principal_id = $1 AND issuer = $2 AND provider = 'github' AND subject = $3`,
		user, issuer, subject); n != 1 {
		t.Fatal("the legacy identity was not linked")
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.identity.projected'
AND subject_principal_id::text = $1 AND attributes->>'reason_code' = 'identity_projected' AND attributes->>'provider' = 'github'`, user.String()); n != 1 {
		t.Fatalf("projection audit rows = %d, want 1", n)
	}
	// The second call hits auth.external_identities and writes nothing.
	if p, found, err := e.store.IdentityBySubject(ctx, issuer, ProviderGitHub, subject, socialLegacy(ProviderGitHub)); err != nil || !found || p.ID != user {
		t.Fatalf("second lookup = %+v, %v, %v", p, found, err)
	}
	if n := e.auditRows(t, ctx, "auth.identity.projected", user.String()); n != 1 {
		t.Fatalf("a hit wrote audit rows: %d", n)
	}
}

// A refused identity projection leaves the go-api account's auth principal
// and user rows, and their audit rows, as they were: the refusal comes before
// any projection of the account's changed go-api row.
func TestARefusedIdentityProjectionWritesNoPrincipalOrUserRow(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	issuer, _ := SocialIssuer(ProviderGitHub)
	held, wanted := "gh-held-"+uuid.NewString(), "gh-wanted-"+uuid.NewString()
	email := uniqueEmail("refused")
	user := e.seedLegacy(t, ctx, email, "github", wanted, false)
	e.project(t, ctx, user)
	if _, err := e.store.LinkIdentity(ctx, ExternalKey{Issuer: issuer, Provider: ProviderGitHub, Subject: held}, user, Actor{}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.admin.Exec(ctx, `UPDATE public.users SET email = $2, is_active = false, is_verified = false, updated_at = now() WHERE id = $1`,
		user, uniqueEmail("changed")); err != nil {
		t.Fatal(err)
	}
	snapshot := func() [8]int {
		return [8]int{
			e.count(t, ctx, `SELECT count(*) FROM auth.principals WHERE id = $1`, user),
			e.count(t, ctx, `SELECT count(*) FROM auth.principals WHERE id = $1 AND disabled_at IS NULL`, user),
			e.count(t, ctx, `SELECT count(*) FROM auth.users WHERE principal_id = $1 AND email_lower = lower($2)`, user, email),
			e.count(t, ctx, `SELECT count(*) FROM auth.users WHERE principal_id = $1 AND email_verified_at IS NOT NULL`, user),
			e.auditRows(t, ctx, "auth.principal.projection_updated", user.String()),
			e.count(t, ctx, `SELECT count(*) FROM auth.external_identities WHERE principal_id = $1`, user),
			e.auditRows(t, ctx, "auth.identity.projected", user.String()),
			e.count(t, ctx, `SELECT count(*) FROM auth.auth_outbox_events WHERE aggregate_id LIKE '%' || $1::text`, user.String()),
		}
	}
	before := snapshot()
	if before[0] != 1 || before[1] != 1 || before[2] != 1 || before[3] != 1 || before[5] != 1 || before[6] != 0 {
		t.Fatalf("fixture rows = %v, want a live verified projected user", before)
	}
	p, found, err := e.store.IdentityBySubject(ctx, issuer, ProviderGitHub, wanted, socialLegacy(ProviderGitHub))
	if !errors.Is(err, ErrProviderAlreadyLinked) || found || p.ID != user {
		t.Fatalf("IdentityBySubject = %+v, %v, %v; want the refusal naming %v", p, found, err, user)
	}
	if after := snapshot(); after != before {
		t.Fatalf("a refused projection changed the account's rows: before %v, after %v", before, after)
	}
}

// A projection that waits for the account's row lock re-reads the account's
// identities after it holds the lock: an identity at the provider that
// committed meanwhile makes the lookup refuse, and the account rows stay as
// they were.
func TestAProjectionChecksTheProviderIdentitiesUnderTheAccountLock(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	issuer, _ := SocialIssuer(ProviderGitHub)
	held, wanted := "gh-held-"+uuid.NewString(), "gh-wanted-"+uuid.NewString()
	email := uniqueEmail("locked")
	user := e.seedLegacy(t, ctx, email, "github", wanted, false)
	e.project(t, ctx, user)
	if _, err := e.admin.Exec(ctx, `UPDATE public.users SET email = $2, is_active = false, updated_at = now() WHERE id = $1`,
		user, uniqueEmail("changed")); err != nil {
		t.Fatal(err)
	}
	snapshot := func() [7]int {
		return [7]int{
			e.count(t, ctx, `SELECT count(*) FROM auth.principals WHERE id = $1 AND disabled_at IS NULL`, user),
			e.count(t, ctx, `SELECT count(*) FROM auth.users WHERE principal_id = $1 AND email_lower = lower($2)`, user, email),
			e.auditRows(t, ctx, "auth.principal.projection_updated", user.String()),
			e.auditRows(t, ctx, "auth.identity.projected", user.String()),
			e.count(t, ctx, `SELECT count(*) FROM auth.external_identities WHERE principal_id = $1 AND subject = $2`, user, wanted),
			e.count(t, ctx, `SELECT count(*) FROM auth.external_identities WHERE principal_id = $1`, user),
			e.count(t, ctx, `SELECT count(*) FROM auth.auth_outbox_events WHERE aggregate_id LIKE '%' || $1::text`, user.String()),
		}
	}
	before := snapshot()

	holder, err := e.admin.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = holder.Rollback(context.Background()) }()
	if _, err := holder.Exec(ctx, `SELECT 1 FROM auth.principals WHERE id = $1 FOR NO KEY UPDATE`, user); err != nil {
		t.Fatal(err)
	}
	type result struct {
		principal Principal
		found     bool
		err       error
	}
	done := make(chan result, 1)
	go func() {
		p, found, err := e.store.IdentityBySubject(ctx, issuer, ProviderGitHub, wanted, socialLegacy(ProviderGitHub))
		done <- result{p, found, err}
	}()
	deadline := time.Now().Add(20 * time.Second)
	for e.count(t, ctx, `SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND query LIKE '%principals%' AND query NOT LIKE '%pg_stat_activity%'`) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the lookup never waited for the account's row lock")
		}
		time.Sleep(50 * time.Millisecond)
	}
	linked, principal := uuid.NewString(), user.String()
	if err := audit.Commit(ctx, e.runtime, e.store.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			_, err := tx.Exec(ctx, `INSERT INTO auth.external_identities (id, principal_id, issuer, provider, subject) VALUES ($1, $2, $3, 'github', $4)`,
				linked, user, issuer, held)
			return err
		},
		Audit: audit.AuditEvent{EventType: "auth.identity.linked", Outcome: audit.OutcomeAllowed, SubjectPrincipalID: &principal,
			Attributes: map[string]any{"reason_code": "identity_linked", "target_kind": "external_identity", "target_id": linked}},
		Events: []audit.OutboxEvent{{AggregateType: "external_identity", AggregateID: "idn_" + linked, EventType: "external_identity.linked",
			Change: map[string]any{"provider": "github", "principal_id": "prn_" + principal}}},
	}); err != nil {
		t.Fatalf("commit the competing link: %v", err)
	}
	if err := holder.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case r := <-done:
		if !errors.Is(r.err, ErrProviderAlreadyLinked) || r.found || r.principal.ID != user {
			t.Fatalf("IdentityBySubject = %+v, %v, %v; want the refusal naming %v", r.principal, r.found, r.err, user)
		}
	case <-ctx.Done():
		t.Fatal("the lookup did not return")
	}
	after := snapshot()
	want := before
	want[5]++
	if after != want {
		t.Fatalf("account rows before %v, after %v; want only the committed identity added", before, after)
	}
}

func TestIdentityBySubjectLegacyRefusals(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	issuer, _ := SocialIssuer(ProviderGitLab)

	// The legacy row of another provider never matches.
	subject := "gl-" + uuid.NewString()
	e.seedLegacy(t, ctx, uniqueEmail("gl"), "github", subject, false)
	if _, found, err := e.store.IdentityBySubject(ctx, issuer, ProviderGitLab, subject, socialLegacy(ProviderGitLab)); err != nil || found {
		t.Fatalf("another provider's subject matched: %v, %v", found, err)
	}
	// A LegacyMatch that does not fit the key is refused.
	if _, _, err := e.store.IdentityBySubject(ctx, issuer, ProviderGitLab, subject, socialLegacy(ProviderGitHub)); !errors.Is(err, ErrInvalidLegacyMatch) {
		t.Fatalf("mismatched AuthProvider = %v, want ErrInvalidLegacyMatch", err)
	}
	org := uuid.New()
	if _, _, err := e.store.IdentityBySubject(ctx, issuer, ProviderGitLab, subject, &LegacyMatch{AuthProvider: "gitlab", MembershipOrg: &org}); !errors.Is(err, ErrInvalidLegacyMatch) {
		t.Fatalf("social with an org = %v, want ErrInvalidLegacyMatch", err)
	}
	// Two users with one provider subject: no guess.
	dup := "dup-" + uuid.NewString()
	e.seedLegacy(t, ctx, uniqueEmail("dup1"), "gitlab", dup, false)
	e.seedLegacy(t, ctx, uniqueEmail("dup2"), "gitlab", dup, false)
	if _, _, err := e.store.IdentityBySubject(ctx, issuer, ProviderGitLab, dup, socialLegacy(ProviderGitLab)); !errors.Is(err, ErrMultipleUsers) {
		t.Fatalf("two legacy users = %v, want ErrMultipleUsers", err)
	}
	// After the users cutover there is no fallback.
	owned := "own-" + uuid.NewString()
	e.seedLegacy(t, ctx, uniqueEmail("own"), "gitlab", owned, false)
	if _, found, err := e.owned.IdentityBySubject(ctx, issuer, ProviderGitLab, owned, socialLegacy(ProviderGitLab)); err != nil || found {
		t.Fatalf("fallback after the cutover: %v, %v", found, err)
	}
}

// TestIdentityBySubjectNeverMatchesByEmail: a legacy user whose email equals
// the asserted subject, or whose auth_provider_id is empty, is not found.
func TestIdentityBySubjectNeverMatchesByEmail(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	email := uniqueEmail("byemail")
	e.seedLegacy(t, ctx, email, "google", "other-subject-"+uuid.NewString(), true)
	e.seedUser(t, ctx, uniqueEmail("local"), true, true)
	issuer, _ := SocialIssuer(ProviderGoogle)
	if _, found, err := e.store.IdentityBySubject(ctx, issuer, ProviderGoogle, email, socialLegacy(ProviderGoogle)); err != nil || found {
		t.Fatalf("a subject equal to an email matched: %v, %v", found, err)
	}
}

func TestIdentityBySubjectSSOLegacyNeedsTheProviderOrgAndMembership(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	org, otherOrg := e.seedOrg(t, ctx), e.seedOrg(t, ctx)
	provider := e.seedSSOProvider(t, ctx, org, "oidc")
	issuer := "https://idp.example.com/realms/" + uuid.NewString()[:8]

	member := "sso-" + uuid.NewString()
	user := e.seedLegacy(t, ctx, uniqueEmail("sso"), "oidc", member, false)
	e.seedMembership(t, ctx, user, org)

	outsider := "out-" + uuid.NewString()
	stranger := e.seedLegacy(t, ctx, uniqueEmail("out"), "oidc", outsider, false)
	e.seedMembership(t, ctx, stranger, otherOrg)

	for name, legacy := range map[string]*LegacyMatch{
		"no membership in the provider org": {AuthProvider: "oidc", MembershipOrg: &org},
		"org is not the provider row's org": {AuthProvider: "oidc", MembershipOrg: &otherOrg},
	} {
		if _, found, err := e.store.IdentityBySubject(ctx, issuer, provider, outsider, legacy); !errors.Is(err, ErrLegacyNotMember) || found {
			t.Errorf("%s: found %v, %v; want ErrLegacyNotMember", name, found, err)
		}
	}
	if _, found, err := e.store.IdentityBySubject(ctx, issuer, provider, member, &LegacyMatch{AuthProvider: "saml", MembershipOrg: &org}); err != nil || found {
		t.Errorf("another protocol: found %v, %v; want a miss", found, err)
	}
	if _, _, err := e.store.IdentityBySubject(ctx, issuer, provider, member, &LegacyMatch{AuthProvider: "oidc"}); !errors.Is(err, ErrInvalidLegacyMatch) {
		t.Fatalf("SSO without an org = %v, want ErrInvalidLegacyMatch", err)
	}
	p, found, err := e.store.IdentityBySubject(ctx, issuer, provider, member, &LegacyMatch{AuthProvider: "oidc", MembershipOrg: &org})
	if err != nil || !found || p.ID != user {
		t.Fatalf("SSO legacy = %+v, %v, %v; want %v", p, found, err, user)
	}
	// The same subject under another provider row is another identity.
	other := e.seedSSOProvider(t, ctx, org, "oidc")
	if _, found, err := e.store.IdentityBySubject(ctx, issuer, other, member, nil); err != nil || found {
		t.Fatalf("another provider row matched: %v, %v", found, err)
	}
}

// TestIdentityBySubjectSSOLegacyCountsTheSubjectInEveryOrg: one stored
// subject is unique across all organizations; a second account carrying it in
// any organization is ErrMultipleUsers, never the provider org's account.
func TestIdentityBySubjectSSOLegacyCountsTheSubjectInEveryOrg(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	org, otherOrg := e.seedOrg(t, ctx), e.seedOrg(t, ctx)
	provider := e.seedSSOProvider(t, ctx, org, "oidc")
	issuer := "https://idp.example.com/realms/" + uuid.NewString()[:8]

	subject := "two-orgs-" + uuid.NewString()
	inOrg := e.seedLegacy(t, ctx, uniqueEmail("in-org"), "oidc", subject, false)
	e.seedMembership(t, ctx, inOrg, org)
	elsewhere := e.seedLegacy(t, ctx, uniqueEmail("other-org"), "oidc", subject, false)
	e.seedMembership(t, ctx, elsewhere, otherOrg)

	if p, found, err := e.store.IdentityBySubject(ctx, issuer, provider, subject, &LegacyMatch{AuthProvider: "oidc", MembershipOrg: &org}); !errors.Is(err, ErrMultipleUsers) || found {
		t.Fatalf("subject in two orgs = %+v, %v, %v; want ErrMultipleUsers", p, found, err)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.external_identities WHERE subject = $1`, subject); n != 0 {
		t.Fatalf("identity rows = %d, want none", n)
	}
}

func TestIdentityBySubjectReturnsADisabledPrincipal(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	subject := "off-" + uuid.NewString()
	user := e.seedLegacy(t, ctx, uniqueEmail("off"), "google", subject, false)
	if _, err := e.admin.Exec(ctx, `UPDATE public.users SET is_active = false WHERE id = $1`, user); err != nil {
		t.Fatal(err)
	}
	issuer, _ := SocialIssuer(ProviderGoogle)
	p, found, err := e.store.IdentityBySubject(ctx, issuer, ProviderGoogle, subject, socialLegacy(ProviderGoogle))
	if err != nil || !found || p.ID != user || p.IsActive || p.DisabledAt == nil {
		t.Fatalf("disabled legacy = %+v, %v, %v", p, found, err)
	}
}

func TestIdentityBySubjectLegacyKeyHeldByAnotherPrincipalIsAConflict(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	subject := "held-" + uuid.NewString()
	holder := e.seedUser(t, ctx, uniqueEmail("holder"), true, true)
	e.project(t, ctx, holder)
	issuer, _ := SocialIssuer(ProviderGoogle)
	key := ExternalKey{Issuer: issuer, Provider: ProviderGoogle, Subject: subject}
	if _, err := e.store.LinkIdentity(ctx, key, holder, Actor{}); err != nil {
		t.Fatal(err)
	}
	// A hit returns the holder; the legacy row of another user is never read.
	e.seedLegacy(t, ctx, uniqueEmail("legacy"), "google", subject, false)
	if p, found, err := e.store.IdentityBySubject(ctx, issuer, ProviderGoogle, subject, socialLegacy(ProviderGoogle)); err != nil || !found || p.ID != holder {
		t.Fatalf("held key = %+v, %v, %v; want the holder", p, found, err)
	}
}

func TestProvisionFederated(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	key := ExternalKey{Issuer: "https://idp.example.com/" + uuid.NewString()[:8], Provider: Provider(uuid.NewString()), Subject: "new-" + uuid.NewString()}
	email := "New.User-" + uuid.NewString()[:8] + "@Example.com"
	request := uuid.NewString()
	req := ProvisionRequest{Key: key, Email: email, FullName: "New User", Actor: SelfSignUp(&request)}

	if _, err := e.store.ProvisionFederated(ctx, req); !errors.Is(err, ErrSignupNotAvailable) {
		t.Fatalf("provision before the cutover = %v, want ErrSignupNotAvailable", err)
	}
	id, err := e.owned.ProvisionFederated(ctx, req)
	if err != nil {
		t.Fatalf("ProvisionFederated: %v", err)
	}
	want, _ := NormalizeEmail(email)
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.principals p JOIN auth.users u ON u.principal_id = p.id
WHERE p.id = $1 AND p.kind = 'user' AND p.disabled_at IS NULL AND u.email_lower = $2 AND u.email_verified_at IS NOT NULL AND u.password_hash IS NULL`, id, want); n != 1 {
		t.Fatal("provisioned principal/user rows wrong")
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM public.users WHERE id = $1`, id); n != 0 {
		t.Fatal("ProvisionFederated wrote public.users")
	}
	if got, err := e.lookup(ctx, key); err != nil || got != id {
		t.Fatalf("lookup of the provisioned key = %v, %v", got, err)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.principal.provisioned'
AND subject_principal_id::text = $1 AND actor_principal_id::text = $1 AND request_id::text = $2
AND attributes->>'reason_code' = 'principal_provisioned'`, id.String(), request); n != 1 {
		t.Fatalf("self sign-up audit rows (actor = the new principal) = %d, want 1", n)
	}

	if _, err := e.owned.ProvisionFederated(ctx, ProvisionRequest{Key: key, Email: "again-" + uuid.NewString()[:8] + "@example.com"}); !errors.Is(err, ErrIdentityTaken) {
		t.Fatalf("provision of a linked key = %v, want ErrIdentityTaken", err)
	}
	otherKey := key
	otherKey.Subject = "other-" + uuid.NewString()
	if _, err := e.owned.ProvisionFederated(ctx, ProvisionRequest{Key: otherKey, Email: strings.ToUpper(email)}); !errors.Is(err, ErrEmailTaken) {
		t.Fatalf("provision of a held email = %v, want ErrEmailTaken", err)
	}
	if _, err := e.lookup(ctx, otherKey); !errors.Is(err, ErrNotFound) {
		t.Fatal("ErrEmailTaken linked the key to the email's principal")
	}
	if _, err := e.owned.ProvisionFederated(ctx, ProvisionRequest{Key: otherKey, Email: "not an email"}); !errors.Is(err, ErrInvalidEmail) {
		t.Fatalf("provision with a bad email = %v, want ErrInvalidEmail", err)
	}
}

func TestUnlinkOfTheLastSignInMethodIsRefused(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	subject := "last-" + uuid.NewString()
	user := e.seedLegacy(t, ctx, uniqueEmail("last"), "github", subject, false)
	e.project(t, ctx, user)
	issuer, _ := SocialIssuer(ProviderGitHub)
	first := ExternalKey{Issuer: issuer, Provider: ProviderGitHub, Subject: subject}
	second := googleKey("second-" + uuid.NewString())
	if _, err := e.store.LinkIdentity(ctx, first, user, Actor{}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.store.LinkIdentity(ctx, second, user, Actor{}); err != nil {
		t.Fatal(err)
	}
	if has, err := e.store.HasPassword(ctx, user); err != nil || has {
		t.Fatalf("HasPassword = %v, %v; want false", has, err)
	}
	ids, err := e.store.ListIdentities(ctx, user)
	if err != nil || len(ids) != 2 || ids[0].Subject != subject || ids[1].Provider != ProviderGoogle {
		t.Fatalf("ListIdentities = %+v, %v", ids, err)
	}
	if err := e.store.UnlinkIdentity(ctx, first, user, Actor{}); err != nil {
		t.Fatalf("unlink with another identity left: %v", err)
	}
	if err := e.store.UnlinkIdentity(ctx, second, user, Actor{}); !errors.Is(err, ErrLastSignInMethod) {
		t.Fatalf("unlink of the last method = %v, want ErrLastSignInMethod", err)
	}
	if n := e.auditRows(t, ctx, "auth.identity.unlinked", user.String()); n != 1 {
		t.Fatalf("unlink audit rows = %d, want 1 (the refused one writes none)", n)
	}
	// go-api sets a password: HasPassword reads it live, and the unlink is allowed.
	if _, err := e.admin.Exec(ctx, `UPDATE public.users SET password_hash = 'x' WHERE id = $1`, user); err != nil {
		t.Fatal(err)
	}
	if has, err := e.store.HasPassword(ctx, user); err != nil || !has {
		t.Fatalf("HasPassword after a go-api write = %v, %v", has, err)
	}
	if err := e.store.UnlinkIdentity(ctx, second, user, Actor{}); err != nil {
		t.Fatalf("unlink with a password: %v", err)
	}
	if ids, err := e.store.ListIdentities(ctx, user); err != nil || len(ids) != 0 {
		t.Fatalf("ListIdentities after = %+v, %v", ids, err)
	}
}

func TestRaceUnlinksNeverRemoveTheLastSignInMethod(t *testing.T) {
	if raceEnabled {
		t.Logf("%s %s", raceMarker, t.Name())
	}
	e := getEnv(t)
	ctx := context.Background()
	user := e.seedLegacy(t, ctx, uniqueEmail("unrace"), "github", "x-"+uuid.NewString(), false)
	e.project(t, ctx, user)
	keys := make([]ExternalKey, 6)
	for i := range keys {
		// One identity per provider per principal: each key is its own SSO
		// provider row.
		keys[i] = ExternalKey{Issuer: "https://idp.example.com", Provider: Provider(uuid.NewString()),
			Subject: fmt.Sprintf("unrace-%d-%s", i, uuid.NewString())}
		if _, err := e.store.LinkIdentity(ctx, keys[i], user, Actor{}); err != nil {
			t.Fatal(err)
		}
	}
	errs := make([]error, len(keys))
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range keys {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			errs[i] = e.store.UnlinkIdentity(ctx, keys[i], user, Actor{})
		}(i)
	}
	close(start)
	wg.Wait()
	refused := 0
	for i, err := range errs {
		switch {
		case err == nil:
		case errors.Is(err, ErrLastSignInMethod):
			refused++
		default:
			t.Errorf("unlink %d: %v", i, err)
		}
	}
	if ids, err := e.store.ListIdentities(ctx, user); err != nil || len(ids) != 1 || refused != 1 {
		t.Fatalf("after concurrent unlinks: %d identities left, %d refused, %v; want 1 and 1", len(ids), refused, err)
	}
}

func TestRaceLegacyProjectionOfOneSubjectWritesOneAuditRow(t *testing.T) {
	if raceEnabled {
		t.Logf("%s %s", raceMarker, t.Name())
	}
	e := getEnv(t)
	ctx := context.Background()
	subject := "lrace-" + uuid.NewString()
	user := e.seedLegacy(t, ctx, uniqueEmail("lrace"), "gitlab", subject, false)
	issuer, _ := SocialIssuer(ProviderGitLab)
	errs := make([]error, 8)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			p, found, err := e.store.IdentityBySubject(ctx, issuer, ProviderGitLab, subject, socialLegacy(ProviderGitLab))
			if err == nil && (!found || p.ID != user) {
				err = fmt.Errorf("found %v principal %v", found, p.ID)
			}
			errs[i] = err
		}(i)
	}
	close(start)
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Errorf("lookup %d: %v", i, err)
		}
	}
	if n := e.auditRows(t, ctx, "auth.identity.projected", user.String()); n != 1 {
		t.Fatalf("concurrent legacy projections wrote %d audit rows, want 1", n)
	}
}

func TestIdentityBySubjectSAMLLegacySubjectIsMatchedRawAndStoredComposite(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	org := e.seedOrg(t, ctx)
	provider := e.seedSSOProvider(t, ctx, org, "saml")
	issuer := "urn:example:idp:" + uuid.NewString()[:8]
	raw := "nameid-" + uuid.NewString()
	composite := "saml1|urn:oasis:names:tc:SAML:2.0:nameid-format:persistent|sp|" + raw
	user := e.seedLegacy(t, ctx, uniqueEmail("saml"), "saml", raw, false)
	e.seedMembership(t, ctx, user, org)

	if _, found, err := e.store.IdentityBySubject(ctx, issuer, provider, composite, &LegacyMatch{AuthProvider: "saml", MembershipOrg: &org}); err != nil || found {
		t.Fatalf("composite subject matched go-api's raw column: %v, %v", found, err)
	}
	p, found, err := e.store.IdentityBySubject(ctx, issuer, provider, composite, &LegacyMatch{AuthProvider: "saml", MembershipOrg: &org, Subject: raw})
	if err != nil || !found || p.ID != user {
		t.Fatalf("SAML legacy with Subject = %+v, %v, %v; want %v", p, found, err, user)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.external_identities WHERE principal_id = $1 AND provider = $2 AND subject = $3`,
		user, string(provider), composite); n != 1 {
		t.Fatal("the identity was not stored under the key's composite subject")
	}
	if p, found, err := e.store.IdentityBySubject(ctx, issuer, provider, composite, nil); err != nil || !found || p.ID != user {
		t.Fatalf("hit by the composite key = %+v, %v, %v", p, found, err)
	}
}

func TestLinkWithoutAnActorRecordsNone(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	id := e.seedUser(t, ctx, uniqueEmail("noactor"), true, true)
	e.project(t, ctx, id)
	if _, err := e.store.LinkIdentity(ctx, googleKey("na-"+uuid.NewString()), id, Actor{}); err != nil {
		t.Fatal(err)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.identity.linked'
AND subject_principal_id::text = $1 AND actor_principal_id IS NULL`, id.String()); n != 1 {
		t.Fatalf("zero-Actor audit rows with no actor = %d, want 1", n)
	}
}

// testBcrypt is a well-formed bcrypt verifier (cost 4) of a test-only value.
const testBcrypt = "$2b$04$C6UzMDM.H6dfI/f/IKxGhuwM4V2y6W1pYxF1Iy5lC3l3OZ0W6Q2Iu"

func TestProvisionLocalAndByEmailAfterTheCutover(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	email := "Local.User-" + uuid.NewString()[:8] + "@Example.com"
	key, _ := NormalizeEmail(email)
	hash := testBcrypt
	request := uuid.NewString()
	req := LocalProvisionRequest{Email: email, FullName: "Local User", PasswordHash: &hash, Actor: SelfSignUp(&request)}

	if _, err := e.store.ProvisionLocal(ctx, req); !errors.Is(err, ErrSignupNotAvailable) {
		t.Fatalf("ProvisionLocal before the cutover = %v, want ErrSignupNotAvailable", err)
	}
	id, err := e.owned.ProvisionLocal(ctx, req)
	if err != nil {
		t.Fatalf("ProvisionLocal: %v", err)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.principals p JOIN auth.users u ON u.principal_id = p.id
WHERE p.id = $1 AND p.disabled_at IS NULL AND u.email = $2 AND u.email_lower = $2 AND u.password_hash = $3
AND u.password_algorithm = 'bcrypt' AND u.password_updated_at IS NOT NULL AND u.email_verified_at IS NULL`, id, key, hash); n != 1 {
		t.Fatal("provisioned local rows wrong")
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM public.users WHERE id = $1 OR lower(email) = $2`, id, key); n != 0 {
		t.Fatal("ProvisionLocal wrote public.users")
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.principal.provisioned'
AND subject_principal_id::text = $1 AND actor_principal_id::text = $1 AND request_id::text = $2`, id.String(), request); n != 1 {
		t.Fatalf("local provision audit rows = %d, want 1", n)
	}

	// The creation leaves exactly one audit row and the principal and user
	// created events, and nothing else for this principal.
	var auditTypes, outboxTypes []string
	if err := e.admin.QueryRow(ctx, `SELECT ARRAY(SELECT event_type || ':' || (attributes->>'reason_code') FROM auth.security_audit_events
WHERE subject_principal_id::text = $1 OR actor_principal_id::text = $1 OR attributes->>'target_id' = $1 ORDER BY id)`, id.String()).Scan(&auditTypes); err != nil {
		t.Fatal(err)
	}
	if err := e.admin.QueryRow(ctx, `SELECT ARRAY(SELECT aggregate_type || ':' || event_type FROM auth.auth_outbox_events
WHERE aggregate_id = 'prn_' || $1 ORDER BY id)`, id.String()).Scan(&outboxTypes); err != nil {
		t.Fatal(err)
	}
	if len(auditTypes) != 1 || auditTypes[0] != "auth.principal.provisioned:principal_provisioned" {
		t.Fatalf("ProvisionLocal audit rows = %v, want exactly [auth.principal.provisioned:principal_provisioned]", auditTypes)
	}
	if len(outboxTypes) != 2 || outboxTypes[0] != "principal:principal.created" || outboxTypes[1] != "user:user.created" {
		t.Fatalf("ProvisionLocal outbox rows = %v, want exactly [principal:principal.created user:user.created]", outboxTypes)
	}

	got, err := e.owned.ByEmail(ctx, key)
	if err != nil || got == nil || got.ID != id || !got.IsActive || got.IsVerified || got.PasswordHash == nil || *got.PasswordHash != hash ||
		got.AuthProvider == nil || *got.AuthProvider != "local" {
		t.Fatalf("owned ByEmail = %+v, %v", got, err)
	}
	// Before the cutover the same key is read from public.users, where it is absent.
	if got, err := e.store.ByEmail(ctx, key); err != nil || got != nil {
		t.Fatalf("pre-cutover ByEmail read auth.users: %+v, %v", got, err)
	}

	if _, err := e.owned.ProvisionLocal(ctx, LocalProvisionRequest{Email: strings.ToUpper(email), PasswordHash: &hash}); !errors.Is(err, ErrEmailTaken) {
		t.Fatalf("ProvisionLocal of a held email = %v, want ErrEmailTaken", err)
	}
	bad := "not-a-bcrypt-hash"
	if _, err := e.owned.ProvisionLocal(ctx, LocalProvisionRequest{Email: "x-" + uuid.NewString()[:8] + "@example.com", PasswordHash: &bad}); !errors.Is(err, ErrInvalidPasswordHash) {
		t.Fatalf("ProvisionLocal with a non-bcrypt hash = %v, want ErrInvalidPasswordHash", err)
	}
	if _, err := e.owned.ProvisionLocal(ctx, LocalProvisionRequest{Email: "nope"}); !errors.Is(err, ErrInvalidEmail) {
		t.Fatalf("ProvisionLocal with a bad email = %v, want ErrInvalidEmail", err)
	}
}

func TestByEmailAfterTheCutoverIgnoresPublicUsers(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	email := "legacy-only-" + uuid.NewString()[:8] + "@example.com"
	e.seedUser(t, ctx, email, true, true)
	if got, err := e.store.ByEmail(ctx, email); err != nil || got == nil {
		t.Fatalf("pre-cutover ByEmail = %v, %v", got, err)
	}
	if got, err := e.owned.ByEmail(ctx, email); err != nil || got != nil {
		t.Fatalf("post-cutover ByEmail read public.users: %+v, %v", got, err)
	}
}

func TestSetEmailVerifiedAndSetPasswordHash(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	email := "nopw-" + uuid.NewString()[:8] + "@example.com"
	id, err := e.owned.ProvisionLocal(ctx, LocalProvisionRequest{Email: email})
	if err != nil {
		t.Fatal(err)
	}
	if has, err := e.owned.HasPassword(ctx, id); err != nil || has {
		t.Fatalf("owned HasPassword before = %v, %v", has, err)
	}
	if got, err := e.owned.ByEmail(ctx, email); err != nil || got == nil || got.PasswordHash != nil || got.AuthProvider != nil {
		t.Fatalf("passwordless owned ByEmail = %+v, %v", got, err)
	}

	at := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	if err := e.store.SetEmailVerified(ctx, id, at, Actor{}); !errors.Is(err, ErrSignupNotAvailable) {
		t.Fatalf("SetEmailVerified before the cutover = %v", err)
	}
	if err := e.owned.SetEmailVerified(ctx, id, at, SelfSignUp(nil)); err != nil {
		t.Fatalf("SetEmailVerified: %v", err)
	}
	if err := e.owned.SetEmailVerified(ctx, id, at, SelfSignUp(nil)); err != nil {
		t.Fatalf("SetEmailVerified again: %v", err)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.users WHERE principal_id = $1 AND email_verified_at = $2`, id, at); n != 1 {
		t.Fatal("email_verified_at not set")
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.user.email_verified'
AND subject_principal_id::text = $1 AND attributes->>'reason_code' = 'email_verified'`, id.String()); n != 1 {
		t.Fatalf("email_verified audit rows = %d, want 1 (the repeat writes none)", n)
	}

	if err := e.store.SetPasswordHash(ctx, id, testBcrypt, Actor{}); !errors.Is(err, ErrSignupNotAvailable) {
		t.Fatalf("SetPasswordHash before the cutover = %v", err)
	}
	if err := e.owned.SetPasswordHash(ctx, id, "plain-text", Actor{}); !errors.Is(err, ErrInvalidPasswordHash) {
		t.Fatalf("SetPasswordHash with a non-bcrypt value = %v", err)
	}
	if err := e.owned.SetPasswordHash(ctx, id, testBcrypt, Actor{}); err != nil {
		t.Fatalf("SetPasswordHash: %v", err)
	}
	if has, err := e.owned.HasPassword(ctx, id); err != nil || !has {
		t.Fatalf("owned HasPassword after = %v, %v", has, err)
	}
	if has, err := e.store.HasPassword(ctx, id); err != nil || has {
		t.Fatalf("pre-cutover HasPassword read auth.users: %v, %v", has, err)
	}
	got, err := e.owned.ByEmail(ctx, email)
	if err != nil || got == nil || !got.IsVerified || got.PasswordHash == nil || *got.PasswordHash != testBcrypt {
		t.Fatalf("owned ByEmail after = %+v, %v", got, err)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.users WHERE principal_id = $1 AND password_algorithm = 'bcrypt'`, id); n != 1 {
		t.Fatal("password_algorithm not bcrypt")
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.user.password_changed'
AND subject_principal_id::text = $1 AND attributes->>'reason_code' = 'password_changed'`, id.String()); n != 1 {
		t.Fatalf("password audit rows = %d, want 1", n)
	}

	missing := uuid.New()
	if err := e.owned.SetEmailVerified(ctx, missing, at, Actor{}); !errors.Is(err, ErrUserNotFound) {
		t.Fatalf("SetEmailVerified of a missing user = %v", err)
	}
	if err := e.owned.SetPasswordHash(ctx, missing, testBcrypt, Actor{}); !errors.Is(err, ErrUserNotFound) {
		t.Fatalf("SetPasswordHash of a missing user = %v", err)
	}
}

func (e *env) orgAudit(t *testing.T, ctx context.Context, org uuid.UUID) []string {
	t.Helper()
	var rows []string
	if err := e.admin.QueryRow(ctx, `SELECT ARRAY(SELECT event_type || ':' || (attributes->>'reason_code') FROM auth.security_audit_events
WHERE organization_id = $1 ORDER BY id)`, org).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	return rows
}

func TestProjectOrganization(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	org := e.seedOrg(t, ctx)

	if err := e.store.ProjectOrganization(ctx, uuid.New()); !errors.Is(err, ErrOrganizationNotFound) {
		t.Fatalf("missing org = %v, want ErrOrganizationNotFound", err)
	}
	if err := e.store.ProjectOrganization(ctx, org); err != nil {
		t.Fatalf("ProjectOrganization: %v", err)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.organizations a JOIN public.organizations p ON p.id = a.id
WHERE a.id = $1 AND a.slug = p.slug AND a.name = p.name AND a.disabled_at IS NULL`, org); n != 1 {
		t.Fatal("projected organization differs from public.organizations")
	}
	if got := e.orgAudit(t, ctx, org); len(got) != 1 || got[0] != "auth.organization.projected:organization_created" {
		t.Fatalf("first projection audit rows = %v", got)
	}

	// Unchanged: no write, no audit row.
	var before time.Time
	if err := e.admin.QueryRow(ctx, `SELECT updated_at FROM auth.organizations WHERE id = $1`, org).Scan(&before); err != nil {
		t.Fatal(err)
	}
	if err := e.store.ProjectOrganization(ctx, org); err != nil {
		t.Fatal(err)
	}
	var after time.Time
	if err := e.admin.QueryRow(ctx, `SELECT updated_at FROM auth.organizations WHERE id = $1`, org).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if !after.Equal(before) || len(e.orgAudit(t, ctx, org)) != 1 {
		t.Fatal("an unchanged re-projection wrote")
	}

	// go-api renames and deactivates the org.
	if _, err := e.admin.Exec(ctx, `UPDATE public.organizations SET name = 'renamed', is_active = false WHERE id = $1`, org); err != nil {
		t.Fatal(err)
	}
	if err := e.store.ProjectOrganization(ctx, org); err != nil {
		t.Fatalf("re-project after rename: %v", err)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.organizations WHERE id = $1 AND name = 'renamed' AND disabled_at IS NOT NULL`, org); n != 1 {
		t.Fatal("rename/deactivation not projected")
	}
	if got := e.orgAudit(t, ctx, org); len(got) != 2 || got[1] != "auth.organization.projection_updated:organization_updated" {
		t.Fatalf("audit rows after rename = %v", got)
	}
	// Created, then the rename (organization.updated) and the deactivation
	// (organization.suspended) in one commit.
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.auth_outbox_events WHERE aggregate_type = 'organization' AND aggregate_id = 'org_' || $1`, org.String()); n != 3 {
		t.Fatalf("organization outbox rows = %d, want 3", n)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM public.organizations WHERE id = $1 AND name = 'renamed'`, org); n != 1 {
		t.Fatal("public.organizations changed")
	}
}

// TestASessionCanNameAProjectedOrganization: auth.sessions.organization_id
// references auth.organizations, so a session for a member org fails the FK
// until the org is projected, and succeeds after. The insert is the session
// row B1 writes, as the runtime role.
func TestASessionCanNameAProjectedOrganization(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	org := e.seedOrg(t, ctx)
	user := e.seedUser(t, ctx, uniqueEmail("member"), true, true)
	e.seedMembership(t, ctx, user, org)
	e.project(t, ctx, user)
	insert := func() error {
		_, err := e.runtime.Exec(ctx, `INSERT INTO auth.sessions (principal_id, organization_id, token_hash, expires_at)
VALUES ($1, $2, $3, now() + interval '1 hour')`, user, org, []byte(uuid.NewString()))
		return err
	}
	if err := insert(); err == nil {
		t.Fatal("a session named an unprojected organization")
	}
	if err := e.store.ProjectOrganization(ctx, org); err != nil {
		t.Fatal(err)
	}
	if err := insert(); err != nil {
		t.Fatalf("session with a projected organization: %v", err)
	}
}

func TestRaceConcurrentOrganizationProjectionsWriteOneCreation(t *testing.T) {
	if raceEnabled {
		t.Logf("%s %s", raceMarker, t.Name())
	}
	e := getEnv(t)
	ctx := context.Background()
	org := e.seedOrg(t, ctx)
	errs := make([]error, 8)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			errs[i] = e.store.ProjectOrganization(ctx, org)
		}(i)
	}
	close(start)
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Errorf("projection %d: %v", i, err)
		}
	}
	if got := e.orgAudit(t, ctx, org); len(got) != 1 {
		t.Fatalf("concurrent organization projections wrote audit rows %v, want 1", got)
	}
}

func TestProjectOrganizationRefusesASlugHeldByAnotherOrganization(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	first := e.seedOrg(t, ctx)
	if err := e.store.ProjectOrganization(ctx, first); err != nil {
		t.Fatal(err)
	}
	// go-api frees the slug and gives it to another org before the first
	// org's rename is projected.
	second := e.seedOrg(t, ctx)
	if _, err := e.admin.Exec(ctx, `UPDATE public.organizations SET slug = slug || '-old' WHERE id = $1`, first); err != nil {
		t.Fatal(err)
	}
	if _, err := e.admin.Exec(ctx, `UPDATE public.organizations SET slug = $2 WHERE id = $1`, second, "org-"+first.String()); err != nil {
		t.Fatal(err)
	}
	if err := e.store.ProjectOrganization(ctx, second); !errors.Is(err, ErrOrganizationSlugCollision) {
		t.Fatalf("projection onto a held slug = %v, want ErrOrganizationSlugCollision", err)
	}
	if n := e.count(t, ctx, `SELECT count(*) FROM auth.organizations WHERE id = $1`, second); n != 0 {
		t.Fatal("the colliding org was written")
	}
	// Projecting the first org's rename frees the slug; then the second projects.
	if err := e.store.ProjectOrganization(ctx, first); err != nil {
		t.Fatal(err)
	}
	if err := e.store.ProjectOrganization(ctx, second); err != nil {
		t.Fatalf("projection after the slug moved: %v", err)
	}
}

func TestByIDFollowsTheLiveReadRule(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	legacy := e.seedUser(t, ctx, uniqueEmail("byid"), true, false)
	got, err := e.store.ByID(ctx, legacy)
	if err != nil || got == nil || got.ID != legacy || !got.IsActive || got.IsVerified || got.PasswordHash == nil ||
		got.AuthProvider == nil || *got.AuthProvider != "local" {
		t.Fatalf("pre-cutover ByID = %+v, %v", got, err)
	}
	if _, err := e.admin.Exec(ctx, `UPDATE public.users SET password_hash = NULL, is_active = false WHERE id = $1`, legacy); err != nil {
		t.Fatal(err)
	}
	if got, err := e.store.ByID(ctx, legacy); err != nil || got == nil || got.PasswordHash != nil || got.IsActive {
		t.Fatalf("pre-cutover ByID after a go-api write = %+v, %v", got, err)
	}
	if got, err := e.owned.ByID(ctx, legacy); err != nil || got != nil {
		t.Fatalf("post-cutover ByID read public.users: %+v, %v", got, err)
	}

	hash := testBcrypt
	owned, err := e.owned.ProvisionLocal(ctx, LocalProvisionRequest{Email: "byid-" + uuid.NewString()[:8] + "@example.com", PasswordHash: &hash})
	if err != nil {
		t.Fatal(err)
	}
	got, err = e.owned.ByID(ctx, owned)
	if err != nil || got == nil || got.ID != owned || !got.IsActive || got.IsVerified || got.PasswordHash == nil || *got.PasswordHash != hash {
		t.Fatalf("post-cutover ByID = %+v, %v", got, err)
	}
	if got, err := e.store.ByID(ctx, owned); err != nil || got != nil {
		t.Fatalf("pre-cutover ByID read auth.users: %+v, %v", got, err)
	}
	for _, s := range []*Store{e.store, e.owned} {
		if got, err := s.ByID(ctx, uuid.New()); err != nil || got != nil {
			t.Fatalf("ByID(unknown) = %+v, %v; want nil, nil", got, err)
		}
	}
}

// TestEveryOutboxRowIsAProvisioningEvent: auth_outbox_events is the
// provisioning-event.v1 stream (the envelope is the outbox row). Every row this
// store writes must parse as that contract, and its idempotency key must be
// the contract's dedupe key.
func TestEveryOutboxRowIsAProvisioningEvent(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	root, err := contracts.RepoRoot(".")
	if err != nil {
		t.Fatal(err)
	}
	var since int64
	if err := e.admin.QueryRow(ctx, `SELECT COALESCE(max(id), 0) FROM auth.auth_outbox_events`).Scan(&since); err != nil {
		t.Fatal(err)
	}

	org := e.seedOrg(t, ctx)
	if err := e.store.ProjectOrganization(ctx, org); err != nil {
		t.Fatal(err)
	}
	if _, err := e.admin.Exec(ctx, `UPDATE public.organizations SET is_active = false WHERE id = $1`, org); err != nil {
		t.Fatal(err)
	}
	if err := e.store.ProjectOrganization(ctx, org); err != nil {
		t.Fatal(err)
	}
	user := e.seedLegacy(t, ctx, uniqueEmail("wire"), "github", "wire-"+uuid.NewString(), true)
	e.project(t, ctx, user)
	if _, err := e.admin.Exec(ctx, `UPDATE public.users SET is_active = false, is_verified = false WHERE id = $1`, user); err != nil {
		t.Fatal(err)
	}
	e.project(t, ctx, user)
	if _, err := e.admin.Exec(ctx, `UPDATE public.users SET is_active = true WHERE id = $1`, user); err != nil {
		t.Fatal(err)
	}
	e.project(t, ctx, user)
	issuer, _ := SocialIssuer(ProviderGitHub)
	var subject string
	if err := e.admin.QueryRow(ctx, `SELECT auth_provider_id FROM public.users WHERE id = $1`, user).Scan(&subject); err != nil {
		t.Fatal(err)
	}
	if _, _, err := e.store.IdentityBySubject(ctx, issuer, ProviderGitHub, subject, socialLegacy(ProviderGitHub)); err != nil {
		t.Fatal(err)
	}
	key := googleKey("wire-" + uuid.NewString())
	if _, err := e.store.LinkIdentity(ctx, key, user, Actor{PrincipalID: &user}); err != nil {
		t.Fatal(err)
	}
	if err := e.store.UnlinkIdentity(ctx, key, user, Actor{}); err != nil {
		t.Fatal(err)
	}
	request := uuid.NewString()
	hash := testBcrypt
	local, err := e.owned.ProvisionLocal(ctx, LocalProvisionRequest{Email: "wire-" + uuid.NewString()[:8] + "@example.com", PasswordHash: &hash, Actor: SelfSignUp(&request)})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.owned.SetEmailVerified(ctx, local, time.Now(), Actor{}); err != nil {
		t.Fatal(err)
	}
	if err := e.owned.SetPasswordHash(ctx, local, testBcrypt, Actor{}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.owned.ProvisionFederated(ctx, ProvisionRequest{Key: googleKey("wire-fed-" + uuid.NewString()),
		Email: "wire-fed-" + uuid.NewString()[:8] + "@example.com", Actor: SelfSignUp(nil)}); err != nil {
		t.Fatal(err)
	}

	rows, err := e.admin.Query(ctx, `SELECT json_build_object(
	'schema_version', 'provisioning-event.v1', 'aggregate_type', aggregate_type, 'aggregate_id', aggregate_id,
	'event_type', event_type, 'idempotency_key', idempotency_key,
	'created_at', to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'), 'payload', payload)::text
FROM auth.auth_outbox_events WHERE id > $1 ORDER BY id`, since)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			t.Fatal(err)
		}
		n++
		event, err := contracts.ParseProvisioningEvent(root, []byte(raw))
		if err != nil {
			t.Errorf("outbox row is not provisioning-event.v1: %v\n%s", err, raw)
			continue
		}
		if event.IdempotencyKey != event.DedupeKey() {
			t.Errorf("idempotency_key %q is not the dedupe key %q", event.IdempotencyKey, event.DedupeKey())
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if n < 10 {
		t.Fatalf("the scenario wrote %d outbox rows; expected every producer to write", n)
	}
}

// A legacy subject whose user already holds another identity at the provider
// is refused (ErrProviderAlreadyLinked) and the refusal names that user's
// principal, so the sign-in's refusal row can name it.
func TestIdentityBySubjectNamesThePrincipalOfAProviderAlreadyLinkedRefusal(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	first := "gl-" + uuid.NewString()
	user := e.seedLegacy(t, ctx, uniqueEmail("gl-linked"), "gitlab", first, false)
	issuer, _ := SocialIssuer(ProviderGitLab)
	if _, found, err := e.store.IdentityBySubject(ctx, issuer, ProviderGitLab, first, socialLegacy(ProviderGitLab)); err != nil || !found {
		t.Fatalf("first projection: %v, %v", found, err)
	}
	second := "gl-" + uuid.NewString()
	if _, err := e.admin.Exec(ctx, `UPDATE public.users SET auth_provider_id = $2 WHERE id = $1`, user, second); err != nil {
		t.Fatal(err)
	}
	p, found, err := e.store.IdentityBySubject(ctx, issuer, ProviderGitLab, second, socialLegacy(ProviderGitLab))
	if !errors.Is(err, ErrProviderAlreadyLinked) || found || p.ID != user {
		t.Fatalf("second subject = %+v, %v, %v; want ErrProviderAlreadyLinked naming %v", p, found, err, user)
	}
}
