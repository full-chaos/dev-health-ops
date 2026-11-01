//go:build integration

package authapi

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/sessionissue"
	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

// TestValidateCompatJudgesRealRowsAsTheAuthRuntimeRole drives the validate
// route over the migrated ops and auth schemas, every read made as the auth
// runtime role (authschema.Apply's grants, no hand DDL), so a read the role is
// not granted fails the cell. Sessions are made by the session store
// auth-service runs; users and memberships are go-api's rows.
func TestValidateCompatJudgesRealRowsAsTheAuthRuntimeRole(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	})
	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	pgschema.Apply(ctx, t, admin)
	role, err := containers.RoleName("validate_runtime", instance)
	if err != nil {
		t.Fatal(err)
	}
	const password = "validate-test-only-not-a-credential"
	if _, err := admin.Exec(ctx, fmt.Sprintf(`CREATE ROLE %q LOGIN PASSWORD '%s'`, role, password)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { containers.DropRole(admin, role, t.Logf) })
	if _, err := authschema.Apply(ctx, admin, authschema.Options{Schema: "auth", RuntimeRole: role}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	cfg := admin.Config().Copy()
	cfg.ConnConfig.User, cfg.ConnConfig.Password = role, password
	runtime, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(runtime.Close)
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	newStore := func(lifetime time.Duration) *session.Store {
		s, err := session.New(session.Config{Pool: runtime, Schema: schema, SessionLifetime: lifetime,
			SuccessorKey: contracts.NewSecretString(base64.RawURLEncoding.EncodeToString(key))})
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	store := newStore(0)

	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := admin.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	org := uuid.New()
	exec(`INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, 'validate-org', 'Validate Org', 'community', 'stripe', true, now(), now())`, org)
	user := func(name string) uuid.UUID {
		id := uuid.New()
		exec(`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, true, true, false, 0, now(), now())`, id, name+"@example.com")
		exec(`INSERT INTO memberships (id, org_id, user_id, role, joined_at, created_at, updated_at)
VALUES ($1, $2, $3, 'admin', now(), now(), now())`, uuid.New(), org, id)
		exec(`INSERT INTO auth.principals (id, kind, display_name) VALUES ($1, 'user', 'validate-test')`, id.String())
		return id
	}
	create := func(s *session.Store, principal uuid.UUID) uuid.UUID {
		issued, err := s.Create(ctx, principal, &org, []string{"pwd"})
		if err != nil {
			t.Fatalf("create session: %v", err)
		}
		return issued.Session.ID
	}

	signer, _, _ := fileSigner(t)
	issuer, err := NewIssuer(testSecret, testIssuer, testAudience)
	if err != nil {
		t.Fatal(err)
	}
	route := func(users Users) http.Handler {
		routes, err := compatRoutes(CompatDeps{
			Sessions: store, Users: users, Issuer: issuer, Projector: &fakeProjector{}, Logger: slog.New(slog.DiscardHandler),
			AppendAudit: func(context.Context, audit.AuditEvent) error { return nil },
			Limits:      mustLimits(t, httpapi.NewMemoryCounters(nil)),
			Validate: &ValidateConfig{Sessions: policy.PGStore{Pool: runtime, AuthSchema: schema.String()},
				IssuerURL: goldenIssuer, Keys: signer},
		}, validateRowsBoth)
		if err != nil {
			t.Fatal(err)
		}
		return serveRoutes(routes)
	}
	server := route(PGUsers{Pool: runtime, AuthSchema: schema})
	check := func(t *testing.T, server http.Handler, token string) *httptest.ResponseRecorder {
		t.Helper()
		request := httptest.NewRequest(http.MethodPost, PatternCompatValidate, strings.NewReader(tokenBody(token)))
		response := httptest.NewRecorder()
		server.ServeHTTP(response, request)
		return response
	}
	hs256, err := edgetoken.NewSigner(testSecret, testIssuer, testAudience)
	if err != nil {
		t.Fatal(err)
	}
	legacy := func(principal uuid.UUID, sid *uuid.UUID, tv int64) string {
		claims := edgetoken.AccessClaims{UserID: principal.String(), Email: "x@example.com", OrgID: org.String(), Role: "admin", TokenVersion: tv}
		if sid != nil {
			wire := session.WireSessionID(*sid)
			claims.SessionID = &wire
		}
		compact, err := hs256.Access(claims, time.Now(), uuid.NewString())
		if err != nil {
			t.Fatal(err)
		}
		return compact
	}
	userToken := func(principal, sid uuid.UUID) string {
		eddsa, err := NewEdDSAIssuer(testSecret, testIssuer, contracts.PlatformAPIAudience, signer, goldenIssuer)
		if err != nil {
			t.Fatal(err)
		}
		compact, err := eddsa.Access(Grant{PrincipalID: principal, OrganizationID: &org, SessionID: sid})
		if err != nil {
			t.Fatal(err)
		}
		return compact
	}
	valid := func(principal uuid.UUID, email string) string {
		return `{"valid":true,"user_id":"` + principal.String() + `","email":"` + email + `","org_id":"` + org.String() + `","role":"admin","expires_at":null}`
	}

	alice := user("alice")
	live := create(store, alice)
	t.Run("c1 sid-less token of an active user", func(t *testing.T) {
		expectAnswer(t, "c1", check(t, server, legacy(alice, nil, 0)), http.StatusOK, valid(alice, "x@example.com"))
	})
	t.Run("c2 live session", func(t *testing.T) {
		expectAnswer(t, "c2 HS256", check(t, server, legacy(alice, &live, 0)), http.StatusOK, valid(alice, "x@example.com"))
		expectAnswer(t, "c2 user token", check(t, server, userToken(alice, live)), http.StatusOK, valid(alice, "alice@example.com"))
	})
	t.Run("c3 ended sessions and stale versions", func(t *testing.T) {
		revoked := create(store, alice)
		if ok, err := store.Revoke(ctx, revoked, session.Actor{}); err != nil || !ok {
			t.Fatalf("revoke: %v %v", ok, err)
		}
		expectAnswer(t, "revoked", check(t, server, legacy(alice, &revoked, 0)), http.StatusOK, nullAnswer)
		expectAnswer(t, "revoked, user token", check(t, server, userToken(alice, revoked)), http.StatusOK, nullAnswer)
		expectAnswer(t, "stale token_version", check(t, server, legacy(alice, &live, 5)), http.StatusOK, nullAnswer)
		expectAnswer(t, "stale token_version, sid-less", check(t, server, legacy(alice, nil, 5)), http.StatusOK, nullAnswer)

		bob := user("bob")
		bobs := create(store, bob)
		exec(`UPDATE users SET is_active = false WHERE id = $1`, bob)
		expectAnswer(t, "deactivated", check(t, server, legacy(bob, &bobs, 0)), http.StatusOK, nullAnswer)
		expectAnswer(t, "deactivated, sid-less", check(t, server, legacy(bob, nil, 0)), http.StatusOK, nullAnswer)

		carol := user("carol")
		carols := create(store, carol)
		exec(`UPDATE users SET token_version = 1 WHERE id = $1`, carol)
		expectAnswer(t, "stale pin", check(t, server, legacy(carol, &carols, 1)), http.StatusOK, nullAnswer)

		dave := user("dave")
		daves := create(store, dave)
		exec(`INSERT INTO auth.users (principal_id, email, email_lower, password_updated_at) VALUES ($1, $2, $2, clock_timestamp())
			ON CONFLICT (principal_id) DO UPDATE SET password_updated_at = EXCLUDED.password_updated_at`, dave.String(), "dave@example.com")
		expectAnswer(t, "older than a password change", check(t, server, legacy(dave, &daves, 0)), http.StatusOK, nullAnswer)

		erin := user("erin")
		const life = 2 * time.Second
		erins := create(newStore(life), erin)
		expectAnswer(t, "before its lifetime ends", check(t, server, legacy(erin, &erins, 0)), http.StatusOK, valid(erin, "x@example.com"))
		time.Sleep(life + 500*time.Millisecond)
		expectAnswer(t, "expired", check(t, server, legacy(erin, &erins, 0)), http.StatusOK, nullAnswer)
		expectAnswer(t, "expired, user token", check(t, server, userToken(erin, erins)), http.StatusOK, nullAnswer)
	})
	t.Run("c6 store fault", func(t *testing.T) {
		dead, err := pgxpool.New(ctx, "postgres://validate:none@127.0.0.1:1/none?connect_timeout=1")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(dead.Close)
		expectAnswer(t, "c6", check(t, route(PGUsers{Pool: dead, AuthSchema: schema}), legacy(alice, &live, 0)),
			http.StatusServiceUnavailable, `{"detail":{"message":"`+sessionissue.UnavailableText+`"}}`)
	})
}
