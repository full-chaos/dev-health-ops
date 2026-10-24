package server

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// TestQueryEdgeAuthenticatorStaysUnroutedWithTheIssuerSet: go-api's
// profile-routed access-token validation never reaches query-api's /query
// authenticator, even with go-api's issuer variable set in the environment.
func TestQueryEdgeAuthenticatorStaysUnroutedWithTheIssuerSet(t *testing.T) {
	t.Setenv("AUTH_ACCESS_TOKEN_ISSUER", "https://auth.example.test")
	pool, err := pgxpool.New(context.Background(), "postgres://nobody:none@127.0.0.1:1/none?connect_timeout=1")
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	settings := edgeSettings(map[string]string{edgeJWTSecretEnvVar: edgeTestSecret, "AUTH_ACCESS_TOKEN_ISSUER": "https://auth.example.test"})
	auth, _, err := buildQueryEdgeAuthenticatorFromEnv(settings, pool)
	if err != nil || auth == nil {
		t.Fatalf("auth %v err %v", auth, err)
	}
	if auth.Routed() {
		t.Fatal("query-api's /query authenticator is profile-routed")
	}
}
