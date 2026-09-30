//go:build integration

package server

import (
	"context"
	"crypto/ed25519"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

// CHAOS-6290 through the real Build: with the /query route configured (real ClickHouse and
// the real migrated Postgres schema) and the edge secret set, an edge access token is
// checked against the live users row THROUGH the /query route's registry pool. An inactive
// user is refused, an active one at the matching token_version is not, and a token_version
// bump refuses the older token. Build handing the edge routes no pool instead of
// handlers.RegistryPool either refuses to start (secret set) or cannot read this row.
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
	defer seed.Close()
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
	activeUser, inactiveUser, bumpedUser := addUser(true, 0), addUser(false, 0), addUser(true, 2)

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
		edgeJWTSecretEnvVar:            edgeTestSecret,
	}
	for _, name := range switchNames(t) {
		settings[name] = "true"
	}
	plane, err := Build(func(name string) string { return settings[name] })
	if err != nil {
		t.Fatalf("Build with the edge secret and every dependency present: %v", err)
	}
	defer plane.Close()

	// serve answers the status and body of GET /api/v1/people with an edge token for user.
	serve := func(user uuid.UUID, tokenVersion int) (int, string) {
		claims := validTestEdgeClaims("org-edge-1")
		claims["sub"] = user.String()
		claims["tv"] = tokenVersion
		req := httptest.NewRequest(http.MethodGet, "/api/v1/people?q=a", nil)
		req.Header.Set("Authorization", "Bearer "+signTestEdgeToken(t, claims))
		rec := httptest.NewRecorder()
		plane.Handler.ServeHTTP(rec, req)
		return rec.Code, rec.Body.String()
	}
	status := func(user uuid.UUID, tokenVersion int) int {
		code, _ := serve(user, tokenVersion)
		return code
	}
	// served: the token passed the users check and reached the route's own work (which may
	// itself answer a data error on this empty ClickHouse), so it is neither a credential
	// refusal (401) nor the users lookup failing (500, or the auth layer's 503).
	served := func(user uuid.UUID, tokenVersion int) (bool, int, string) {
		code, body := serve(user, tokenVersion)
		ok := code != http.StatusUnauthorized && code != http.StatusInternalServerError &&
			!strings.Contains(body, "Database temporarily unavailable")
		return ok, code, body
	}
	if got := status(inactiveUser, 0); got != http.StatusUnauthorized {
		t.Errorf("inactive user's token: status %d, want 401", got)
	}
	if got := status(uuid.New(), 0); got != http.StatusUnauthorized {
		t.Errorf("token of a user with no row: status %d, want 401", got)
	}
	if got := status(bumpedUser, 0); got != http.StatusUnauthorized {
		t.Errorf("stale token_version: status %d, want 401", got)
	}
	if ok, code, body := served(activeUser, 0); !ok {
		t.Errorf("active user's token: status %d body %q, want it to pass the users check and reach the route", code, body)
	}
	if ok, code, body := served(bumpedUser, 2); !ok {
		t.Errorf("active user at the bumped token_version: status %d body %q, want it to pass the users check", code, body)
	}
}
