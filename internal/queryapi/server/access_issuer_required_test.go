package server

import (
	"context"
	"log/slog"
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

// Word 9: a leftover GO_API_EDGE_JWT_SECRET is one WARN naming it, never a refused start; unset, no line.
func TestQueryAPIUnusedEdgeSecretIsOneWarn(t *testing.T) {
	for name, tc := range map[string]struct {
		env   map[string]string
		lines int
	}{"set": {map[string]string{edgeJWTSecretEnvVar: edgeTestSecret}, 1}, "unset": {nil, 0}} {
		var out strings.Builder
		warnUnusedEdgeSecret(edgeSettings(tc.env), slog.New(slog.NewTextHandler(&out, nil)))
		if got := strings.Count(out.String(), "level=WARN"); got != tc.lines || (tc.lines == 1 && !strings.Contains(out.String(), "GO_API_EDGE_JWT_SECRET is set but unused (HS256 retired)")) {
			t.Errorf("%s: %d WARN lines %q, want %d", name, got, out.String(), tc.lines)
		}
		if strings.Contains(out.String(), edgeTestSecret) {
			t.Errorf("%s: the secret value is in the log", name)
		}
	}
}
