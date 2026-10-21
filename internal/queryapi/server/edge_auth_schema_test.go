package server

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
)

// query-api reads auth-service sessions in the schema AUTH_DATABASE_SCHEMA
// names, on both of its edge stores, and refuses to start on a name that is
// not a schema.
func TestEdgeStoresReadSessionsInTheConfiguredAuthSchema(t *testing.T) {
	pool, err := pgxpool.New(context.Background(), "postgres://nobody:none@127.0.0.1:1/none?connect_timeout=1")
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	for value, want := range map[string]string{"": "auth", "acp": "acp"} {
		settings := edgeSettings(map[string]string{edgeJWTSecretEnvVar: edgeTestSecret, policy.EnvAuthSchema: value})
		store, err := newEdgeUserStore(settings, pool)
		if err != nil {
			t.Fatalf("newEdgeUserStore(%q): %v", value, err)
		}
		if pg, ok := store.(policy.PGStore); !ok || pg.AuthSchema != want {
			t.Errorf("newEdgeUserStore(%q) = %#v, want AuthSchema %q", value, store, want)
		}
		_, queryStore, err := buildQueryEdgeAuthenticatorFromEnv(settings, pool)
		if err != nil {
			t.Fatalf("buildQueryEdgeAuthenticatorFromEnv(%q): %v", value, err)
		}
		if pg, ok := queryStore.(policy.PGStore); !ok || pg.AuthSchema != want {
			t.Errorf("buildQueryEdgeAuthenticatorFromEnv(%q) store = %#v, want AuthSchema %q", value, queryStore, want)
		}
	}
	bad := edgeSettings(map[string]string{edgeJWTSecretEnvVar: edgeTestSecret, policy.EnvAuthSchema: "Bad-Name"})
	if _, err := newEdgeUserStore(bad, pool); err == nil {
		t.Error("newEdgeUserStore accepted an unusable AUTH_DATABASE_SCHEMA")
	}
	if _, _, err := buildQueryEdgeAuthenticatorFromEnv(bad, pool); err == nil {
		t.Error("buildQueryEdgeAuthenticatorFromEnv accepted an unusable AUTH_DATABASE_SCHEMA")
	}
}
