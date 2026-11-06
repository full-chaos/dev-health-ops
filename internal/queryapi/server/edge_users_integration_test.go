//go:build integration

package server

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/auth/sessionid"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/principal"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pinrig"
)

const edgeUsersOrg = "22222222-2222-2222-2222-222222222222"

// CHAOS-6290: a user access token on a REST route is checked against the live users row read
// through the /query route's registry pool (real ClickHouse, the real migrated Postgres schema,
// real auth-service sessions). The real Build starts with the issuer set. The user cells run the
// edge verifier as buildEdgeVerifierFromEnv builds it (newEdgeUserStore on the registry pool,
// routeEdgeVerifier) with the test key source in place of the issuer's JWKS, and EdDSA tokens of
// real sessions. Each refusal asserts its own reason line; the staleness check is the session's
// token_version pin.
func TestBuildChecksEdgeTokensAgainstTheUsersRowThroughTheQueryPool(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	ch, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse: %v", err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = ch.Close(closeCtx)
	})
	pg, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatalf("start PostgreSQL: %v", err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = pg.Close(closeCtx)
	})
	pgschema.ApplyURI(ctx, t, pg.URI)
	seed, err := pgxpool.New(ctx, pg.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(seed.Close) // after the pin rig's role cleanup, which runs on this pool
	addUser := func(active bool, tokenVersion int) uuid.UUID {
		id := uuid.New()
		if _, err := seed.Exec(ctx, `INSERT INTO users
	(id, email, auth_provider, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, 'local', $3, false, false, $4, now(), now())`,
			id, id.String()+"@example.test", active, tokenVersion); err != nil {
			t.Fatalf("seed user: %v", err)
		}
		return id
	}
	// inactiveUser is deactivated after it signs in (a session needs an active user).
	activeUser, inactiveUser, bumpedUser := addUser(true, 0), addUser(true, 0), addUser(true, 2)
	// The token's org, active, with a membership row for each seeded user: the edge
	// refuses a token whose org the user is not a member of (CheckTokenOrg).
	if _, err := seed.Exec(ctx, `INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, 'edge-users', 'Edge users', 'community', 'stripe', true, now(), now())`, edgeUsersOrg); err != nil {
		t.Fatalf("seed org: %v", err)
	}
	for _, id := range []uuid.UUID{activeUser, inactiveUser, bumpedUser} {
		if _, err := seed.Exec(ctx, `INSERT INTO memberships (id, org_id, user_id, role, joined_at, created_at, updated_at)
VALUES ($1, $2, $3, 'viewer', now(), now(), now())`, uuid.New(), edgeUsersOrg, id); err != nil {
			t.Fatalf("seed membership: %v", err)
		}
	}
	nonMember := addUser(true, 0)

	pub, _, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	settings := map[string]string{
		"CLICKHOUSE_URI":               ch.URI,
		"GO_API_REGISTRY_POSTGRES_URI": pg.URI,
		"GO_API_ENVELOPE_JWKS_PATH":    writeTestJWKS(t, pub),
		"GO_API_ENVELOPE_ISSUER":       itTestIssuer,
		"GO_API_ENVELOPE_AUDIENCE":     itTestAudience,
		"DEV_HEALTH_ENV":               "ci",
		accessTokenIssuerEnvVar:        accessTestIssuer,
		edgeJWTSecretEnvVar:            edgeTestSecret,
	}
	for _, name := range switchNames(t) {
		settings[name] = "true"
	}
	getenv := func(name string) string { return settings[name] }
	plane, err := Build(getenv)
	if err != nil {
		t.Fatalf("Build with the issuer and every dependency present: %v", err)
	}
	defer plane.Close()

	// Real auth-service sessions, each pinned at its user's token_version when signed in.
	rig := pinrig.NewPinRig(t, ctx, seed, pg)
	sessionOf := map[uuid.UUID]uuid.UUID{}
	for _, id := range []uuid.UUID{activeUser, inactiveUser, bumpedUser, nonMember} {
		sessionOf[id] = rig.SignIn(t, ctx, id)
	}
	staleUser, goneUser := addUser(true, 0), addUser(true, 0)
	if _, err := seed.Exec(ctx, `INSERT INTO memberships (id, org_id, user_id, role, joined_at, created_at, updated_at)
VALUES ($1, $2, $3, 'viewer', now(), now(), now())`, uuid.New(), edgeUsersOrg, staleUser); err != nil {
		t.Fatalf("seed membership: %v", err)
	}
	sessionOf[staleUser], sessionOf[goneUser] = rig.SignIn(t, ctx, staleUser), rig.SignIn(t, ctx, goneUser)
	for _, change := range []struct {
		sql string
		id  uuid.UUID
	}{
		{`UPDATE users SET is_active = false WHERE id = $1`, inactiveUser},
		{`UPDATE users SET token_version = 2 WHERE id = $1`, staleUser},
		{`DELETE FROM users WHERE id = $1`, goneUser},
	} {
		if _, err := seed.Exec(ctx, change.sql, change.id); err != nil {
			t.Fatalf("%s: %v", change.sql, err)
		}
	}

	registry, err := pgxpool.New(ctx, pg.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Close()
	users, err := newEdgeUserStore(getenv, registry)
	if err != nil {
		t.Fatalf("newEdgeUserStore on the registry pool: %v", err)
	}
	edge, err := principal.NewEdgeVerifier(unusedEdgeSecret(), defaultEdgeJWTIssuer, defaultEdgeJWTAudience, users)
	if err != nil {
		t.Fatal(err)
	}
	m := newAccessMinter(t)
	if err := routeEdgeVerifier(edge, testAccess(m.keys, nil, accessTestNow).opts); err != nil {
		t.Fatal(err)
	}
	envelope, _ := iaVerifier(t)
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/home", func(w http.ResponseWriter, r *http.Request) {
		if _, ok := authenticateRESTRequest(w, r, envelope, edge, "home"); ok {
			_, _ = w.Write([]byte("served"))
		}
	})

	// serve answers the status, the body and the WARN lines of GET /api/v1/home with a user
	// access token of user's session in the seeded org.
	serve := func(user uuid.UUID) (int, string, string) {
		var logged bytes.Buffer
		previous := slog.Default()
		slog.SetDefault(slog.New(slog.NewTextHandler(&logged, &slog.HandlerOptions{Level: slog.LevelWarn})))
		defer slog.SetDefault(previous)
		token := m.mint(user, map[string]any{"sid": sessionid.Wire(sessionOf[user]), "org_id": edgeUsersOrg}, nil)
		req := httptest.NewRequest(http.MethodGet, "/api/v1/home", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		return rec.Code, strings.TrimSpace(rec.Body.String()), logged.String()
	}
	for _, c := range []struct {
		name   string
		user   uuid.UUID
		reason string
	}{
		{"inactive user", inactiveUser, "api user access token valid but user not found or deactivated"},
		{"user with no row", goneUser, "api user access token valid but user not found or deactivated"},
		{"session pinned before a token_version bump", staleUser, "api access denied: session predates the token_version"},
		{"active user with no membership in the token's org", nonMember, "api user access token for an org the user is not a member of"},
	} {
		code, body, logged := serve(c.user)
		if code != http.StatusUnauthorized || body != restRefusal || strings.Count(logged, "level=WARN") != 1 ||
			!strings.Contains(logged, fmt.Sprintf("msg=%q", c.reason)) {
			t.Errorf("%s: %d %q, WARN %q, want 401 %s and the one reason %q", c.name, code, body, logged, restRefusal, c.reason)
		}
	}
	for _, c := range []struct {
		name string
		user uuid.UUID
	}{
		{"active user", activeUser},
		{"active user at a bumped token_version, session pinned at it", bumpedUser},
	} {
		if code, body, logged := serve(c.user); code != http.StatusOK || body != "served" || logged != "" {
			t.Errorf("%s: %d %q, WARN %q, want it served with no refusal", c.name, code, body, logged)
		}
	}
}
