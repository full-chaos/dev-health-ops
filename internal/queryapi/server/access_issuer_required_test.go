package server

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// CHAOS-13154 D1 (main words 8-9): query-api refuses start without AUTH_ACCESS_TOKEN_ISSUER only when a user-token
// path exists (the /query registry pool). An envelope-only pod starts. GO_API_EDGE_JWT_SECRET alone verifies nothing
// (HS256 retired): no edge verifier, no users store, no refusal.
func TestQueryAPIIssuerRequiredOnlyWithAUserTokenPath(t *testing.T) {
	pool, err := pgxpool.New(context.Background(), "postgres://nobody:none@127.0.0.1:1/none?connect_timeout=1")
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	for name, env := range map[string]map[string]string{
		"neither":     nil,
		"secret only": {edgeJWTSecretEnvVar: edgeTestSecret},
	} {
		if _, _, err := buildQueryEdgeAuthenticatorFromEnv(edgeSettings(env), pool); err == nil || !strings.Contains(err.Error(), accessTokenIssuerEnvVar) {
			t.Errorf("%s, /query pool: err=%v, want a start error naming %s", name, err, accessTokenIssuerEnvVar)
		}
		if auth, store, err := buildQueryEdgeAuthenticatorFromEnv(edgeSettings(env), nil); err != nil || auth != nil || store != nil {
			t.Errorf("%s, no pool: auth %v store %v err %v, want a start with no user-token path", name, auth != nil, store != nil, err)
		}
		users, err := newEdgeUserStore(edgeSettings(env), nil)
		if err != nil || users != nil {
			t.Errorf("%s, no pool: users store %v err %v, want none and a start", name, users != nil, err)
		}
		if edge, err := buildEdgeVerifierFromEnv(edgeSettings(env), nil); err != nil || edge != nil {
			t.Errorf("%s: edge verifier %v err %v, want none (HS256 retired)", name, edge != nil, err)
		}
	}
}
