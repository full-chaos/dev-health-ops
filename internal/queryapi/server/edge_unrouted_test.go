package server

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// TestQueryEdgeAuthenticatorRoutesByRowOnlyWithTheIssuerSet: query-api's
// /query and /graphql authenticator is profile-routed exactly when
// AUTH_ACCESS_TOKEN_ISSUER is set, read through the lookup Build is given,
// never from the process environment.
func TestQueryEdgeAuthenticatorRoutesByRowOnlyWithTheIssuerSet(t *testing.T) {
	t.Setenv("AUTH_ACCESS_TOKEN_ISSUER", "https://auth.example.test")
	pool, err := pgxpool.New(context.Background(), "postgres://nobody:none@127.0.0.1:1/none?connect_timeout=1")
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	unset, _, err := buildQueryEdgeAuthenticatorFromEnv(edgeSettings(map[string]string{edgeJWTSecretEnvVar: edgeTestSecret}), pool)
	if err != nil || unset == nil || unset.Routed() {
		t.Fatalf("issuer unset in the lookup: auth %v err %v; want an unrouted authenticator", unset, err)
	}
	set, _, err := buildQueryEdgeAuthenticatorFromEnv(edgeSettings(map[string]string{edgeJWTSecretEnvVar: edgeTestSecret, accessTokenIssuerEnvVar: "https://auth.example.test"}), pool)
	if err != nil || set == nil || !set.Routed() {
		t.Fatalf("issuer set in the lookup: auth %v err %v; want a routed authenticator", set, err)
	}
}
