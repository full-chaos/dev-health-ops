//go:build integration

package session

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/full-chaos/dev-health-go/authverify"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/accessverify"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

// TestRoutedUserAccessTokenOnRealStores is go-api's profile-routed path on a
// real Postgres: the users row and the memberships role read from public, the
// auth-service session read as a session reader role that holds only the
// column grant auth-migrate gives it (id, principal_id, revoked_at).
func TestRoutedUserAccessTokenOnRealStores(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = instance.Close(closeCtx)
	})
	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	pgschema.Apply(ctx, t, admin)
	roles := map[string]string{}
	for _, base := range []string{"routed_runtime", "routed_api"} {
		role, err := containers.RoleName(base, instance)
		if err != nil {
			t.Fatal(err)
		}
		roleID, err := authschema.NewValidatedIdentifier(role)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := admin.Exec(ctx, fmt.Sprintf(`CREATE ROLE %s LOGIN PASSWORD '%s'`, authschema.Quote(roleID), readerStorePassword)); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { containers.DropRole(admin, role, t.Logf) })
		roles[base] = role
	}
	const schemaName = "acp"
	if _, err := authschema.Apply(ctx, admin, authschema.Options{Schema: schemaName, RuntimeRole: roles["routed_runtime"],
		SessionReaderRoles: []string{roles["routed_api"]}}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	apiRole, err := authschema.NewValidatedIdentifier(roles["routed_api"])
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, `GRANT SELECT ON public.users, public.memberships TO `+authschema.Quote(apiRole)); err != nil {
		t.Fatal(err)
	}
	schema, err := authschema.NewValidatedIdentifier(schemaName)
	if err != nil {
		t.Fatal(err)
	}
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	sessions, err := New(Config{Pool: readerStoreLogin(t, ctx, admin, roles["routed_runtime"]), Schema: schema,
		SuccessorKey: contracts.NewSecretString(base64.RawURLEncoding.EncodeToString(key))})
	if err != nil {
		t.Fatal(err)
	}

	principal, org, otherOrg := uuid.New(), uuid.New(), uuid.New()
	for _, statement := range []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO acp.principals (id, kind, display_name) VALUES ($1, 'user', 'routed-test')`, []any{principal}},
		{`INSERT INTO public.users (id, email, username, full_name, token_version) VALUES ($1, 'routed@example.test', 'routed', 'Routed User', 2)`, []any{principal}},
		{`INSERT INTO public.organizations (id, slug, name) VALUES ($1, 'routed-org', 'Routed'), ($2, 'other-org', 'Other')`, []any{org, otherOrg}},
		{`INSERT INTO public.memberships (id, user_id, org_id, role) VALUES ($1, $2, $3, 'admin')`, []any{uuid.New(), principal, org}},
	} {
		if _, err := admin.Exec(ctx, statement.sql, statement.args...); err != nil {
			t.Fatalf("%s: %v", statement.sql, err)
		}
	}
	live, err := sessions.Create(ctx, principal, nil, []string{"pwd"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	revoked, err := sessions.Create(ctx, principal, nil, []string{"pwd"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := sessions.Revoke(ctx, revoked.Session.ID, Actor{}); err != nil {
		t.Fatalf("Revoke: %v", err)
	}

	api := readerStoreLogin(t, ctx, admin, roles["routed_api"])
	var pin *int64
	if err := api.QueryRow(ctx, `SELECT legacy_token_version FROM acp.sessions WHERE id = $1`, live.Session.ID).Scan(&pin); err == nil {
		t.Fatal("the api role reads sessions.legacy_token_version: the session reader grant is wider than id, principal_id, revoked_at")
	}
	store := policy.PGStore{Pool: api, AuthSchema: schemaName}
	keyText := strings.Repeat("routed-store-signing-", 2)
	edge, err := edgetoken.New(keyText, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	auth, err := policy.NewAuthenticator(edge, store, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	const issuer = "https://auth.routed.test"
	verifier, err := authverify.NewVerifier(authverify.VerifierConfig{Issuer: issuer, Audience: "dev-health-api", Keys: authverify.StaticKeys{"routed-kid": pub}})
	if err != nil {
		t.Fatal(err)
	}
	const profile = "GET /api/v1/auth/me"
	validator, err := accessverify.New(accessverify.Config{
		Rows:  []accessverify.Row{{Service: "dev-health-ops-api", Profile: profile, AcceptsUser: true, LegacyHS256: true}},
		EdDSA: verifier, Meter: noop.NewMeterProvider().Meter("routed"),
		Legacy: func(ctx context.Context, token string) (any, error) {
			user, err := auth.AuthenticateLegacy(ctx, token)
			if errors.Is(err, policy.ErrUnavailable) {
				return nil, fmt.Errorf("%w: %w", accessverify.ErrUnavailable, err)
			}
			return user, err
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := auth.RouteAccess(validator, "dev-health-ops-api"); err != nil {
		t.Fatal(err)
	}
	now := time.Now().Unix()
	mint := func(sid uuid.UUID, orgID *uuid.UUID) string {
		claims := map[string]any{"iss": issuer, "aud": "dev-health-api", "sub": WirePrincipalID(principal), "principal_type": "human",
			"sid": WireSessionID(sid), "jti": strings.Repeat("r", 24), "iat": now - 5, "nbf": now - 5, "exp": now + 300, "scope": "api:access"}
		if orgID != nil {
			claims["org_id"] = orgID.String()
		}
		header, _ := json.Marshal(map[string]any{"alg": "EdDSA", "typ": "at+jwt", "kid": "routed-kid"})
		payload, _ := json.Marshal(claims)
		signing := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(payload)
		return signing + "." + base64.RawURLEncoding.EncodeToString(ed25519.Sign(priv, []byte(signing)))
	}
	routed := policy.WithRouteProfile(ctx, profile)

	user, err := auth.Authenticate(routed, mint(live.Session.ID, &org))
	if err != nil {
		t.Fatalf("a live session's user token: %v", err)
	}
	if user.ID != principal || user.OrgID != org.String() || user.Role != "admin" || user.Email != "routed@example.test" ||
		user.Username == nil || *user.Username != "routed" || user.FullName == nil || *user.FullName != "Routed User" || user.TokenVersion != 2 {
		t.Fatalf("user %+v", user)
	}
	for name, token := range map[string]string{
		"revoked session":      mint(revoked.Session.ID, &org),
		"unknown session":      mint(uuid.New(), &org),
		"org without a member": mint(live.Session.ID, &otherOrg),
	} {
		if _, err := auth.Authenticate(routed, token); !policy.IsRefusal(err) {
			t.Errorf("%s: err = %v, want a refusal", name, err)
		}
	}
	if _, err := auth.Authenticate(ctx, mint(live.Session.ID, &org)); !policy.IsRefusal(err) {
		t.Errorf("no profile: err = %v, want a refusal", err)
	}
	legacy, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub": principal.String(), "type": "access", "iss": "dev-health-ops", "aud": "dev-health-api",
		"exp": now + 300, "iat": now, "tv": 2, "org_id": org.String(), "role": "admin",
	}).SignedString([]byte(keyText))
	if err != nil {
		t.Fatal(err)
	}
	if user, err := auth.Authenticate(routed, legacy); err != nil || user.ID != principal {
		t.Fatalf("a legacy token on a legacy row: user %+v err %v", user, err)
	}
}
