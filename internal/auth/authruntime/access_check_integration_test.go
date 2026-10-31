//go:build integration

package authruntime

import (
	"context"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/token"
)

// TestEdDSASessionTokenEndsWithItsSessionAndItsTokenVersion drives the session
// routes through configure on a live PostgreSQL in the eddsa access class with
// audience auth-service. An EdDSA token has no tv: the session's token_version
// pin stands for it, so a bump of users.token_version, a single-session
// logout and a revoke-all each end the token.
func TestEdDSASessionTokenEndsWithItsSessionAndItsTokenVersion(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	useKeyStates(t, staticKeyStates(nil))
	dir := t.TempDir()
	writeSigningKey(t, dir, "k1.pem")
	e := newRoutesEnvWith(t, ctx, map[string]string{
		authconfig.EnvSigningKeyFile:      filepath.Join(dir, "k1.pem"),
		authconfig.EnvSigningKeyID:        "k1",
		authconfig.EnvAccessTokenClass:    authconfig.AccessClassEdDSA,
		authconfig.EnvAccessTokenAudience: "dev-health-api",
		authconfig.EnvTokenIssuerURL:      "https://auth.example.test",
	})
	user, org := e.seed(t, ctx)

	signer := token.NewFileSigner(token.FileConfig{KeyPath: filepath.Join(dir, "k1.pem"), KeyID: "k1"})
	minter, err := authapi.NewEdDSAIssuer(testAccessSecret, "dev-health-ops", "dev-health-api", signer, "https://auth.example.test")
	if err != nil {
		t.Fatal(err)
	}
	mint := func() (string, string) {
		issued := e.start(t, ctx, user, &org)
		// The session routes take the session-route token the issuer mints beside the access token.
		tokens, err := minter.Tokens(ctx, authapi.Grant{PrincipalID: user, OrganizationID: &org, SessionID: issued.Session.ID})
		if err != nil {
			t.Fatal(err)
		}
		return tokens.Session, session.WireSessionID(issued.Session.ID)
	}

	bearer, _ := mint()
	if got := e.do(t, http.MethodGet, "/v1/sessions/current", "", bearer); got.status != http.StatusOK {
		t.Fatalf("current with the EdDSA token = %d %s", got.status, got.body)
	}

	if _, err := e.admin.Exec(ctx, `UPDATE users SET token_version = token_version + 1 WHERE id = $1`, user); err != nil {
		t.Fatal(err)
	}
	e.refused(t, e.do(t, http.MethodGet, "/v1/sessions/current", "", bearer), http.StatusUnauthorized, "revision_stale")

	bearer, wire := mint()
	if got := e.do(t, http.MethodGet, "/v1/sessions/current", "", bearer); got.status != http.StatusOK {
		t.Fatalf("current after the bump, on a new session = %d %s", got.status, got.body)
	}
	if got := e.do(t, http.MethodPost, "/v1/sessions/revoke", "", bearer); got.status != http.StatusNoContent {
		t.Fatalf("revoke = %d %s", got.status, got.body)
	}
	if e.revokedAt(t, ctx, wire) == nil {
		t.Fatal("revoke left the session live")
	}
	e.refused(t, e.do(t, http.MethodGet, "/v1/sessions/current", "", bearer), http.StatusUnauthorized, "session_revoked")

	first, _ := mint()
	second, _ := mint()
	if got := e.do(t, http.MethodPost, "/v1/sessions/revoke-all", "", first); got.status != http.StatusNoContent {
		t.Fatalf("revoke-all = %d %s", got.status, got.body)
	}
	e.refused(t, e.do(t, http.MethodGet, "/v1/sessions/current", "", second), http.StatusUnauthorized, "session_revoked")

}
