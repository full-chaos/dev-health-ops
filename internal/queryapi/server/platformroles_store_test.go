package server

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
)

// Both users stores query-api builds read is_superuser by the platform roles
// switch; a malformed switch builds neither.
func TestQueryAPIUserStoresCarryThePlatformRolesSwitch(t *testing.T) {
	pool, err := pgxpool.New(context.Background(), "postgres://nobody:none@127.0.0.1:1/none?connect_timeout=1")
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	owned := func(store any) bool {
		t.Helper()
		users, ok := store.(queryAPIUserStore)
		if !ok {
			t.Fatalf("store is %T, want queryAPIUserStore", store)
		}
		base, ok := users.queryAPIStoreBase.(policy.PGStore)
		if !ok {
			t.Fatalf("store base is %T, want policy.PGStore", users.queryAPIStoreBase)
		}
		return base.PlatformRolesOwned
	}
	for value, want := range map[string]bool{"": false, "false": false, "true": true, " TRUE ": true} {
		settings := edgeSettings(map[string]string{edgeJWTSecretEnvVar: edgeTestSecret, "AUTH_PLATFORM_ROLES_OWNED": value})
		_, store, err := buildQueryEdgeAuthenticatorFromEnv(settings, pool)
		if err != nil || owned(store) != want {
			t.Errorf("%q: /query store owned %v err %v, want %v", value, store, err, want)
		}
		users, err := newEdgeUserStore(settings, pool)
		if err != nil || owned(users) != want {
			t.Errorf("%q: edge users store err %v, want owned %v", value, err, want)
		}
	}
	settings := edgeSettings(map[string]string{edgeJWTSecretEnvVar: edgeTestSecret, "AUTH_PLATFORM_ROLES_OWNED": "yes"})
	if _, _, err := buildQueryEdgeAuthenticatorFromEnv(settings, pool); err == nil {
		t.Error("a malformed switch built the /query authenticator")
	}
	if _, err := newEdgeUserStore(settings, pool); err == nil {
		t.Error("a malformed switch built the edge users store")
	}
}
