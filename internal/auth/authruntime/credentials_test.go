package authruntime

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

func credentialConfig(t *testing.T, owned string) authconfig.Config {
	t.Helper()
	env := compatEnvironment(map[string]string{
		authconfig.EnvDatabaseURI:    "postgres://auth@127.0.0.1:1/devhealth",
		authconfig.EnvSigningKeyFile: "/nonexistent",
		authconfig.EnvSigningKeyID:   "credentials-test",
	})
	if owned != "" {
		env[authconfig.EnvUsersOwned] = owned
	}
	cfg, err := authconfig.Load(authconfig.Spec{LookupEnv: lookupFrom(env)})
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func TestCredentialRoutesMountOnlyWithTheSessionSecretsAndUsersOwned(t *testing.T) {
	pool, err := pgxpool.New(context.Background(), "postgres://auth@127.0.0.1:1/devhealth")
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	for name, owned := range map[string]string{"unset": "", "false": "false"} {
		routes, components, err := credentialRoutes(credentialConfig(t, owned), logger, pool, schema)
		if err != nil || len(routes) != 0 || len(components) != 0 {
			t.Fatalf("%s: routes %d components %d err %v; want nothing mounted", name, len(routes), len(components), err)
		}
	}
	routes, components, err := credentialRoutes(credentialConfig(t, "true"), logger, pool, schema)
	if err != nil {
		t.Fatal(err)
	}
	if len(routes) != 5 || len(components) != 1 {
		t.Fatalf("routes %d components %d; want 5 and 1", len(routes), len(components))
	}
	if err := components[0].Shutdown(context.Background()); err != nil {
		t.Fatalf("link-mail shutdown: %v", err)
	}
	if err := components[0].Shutdown(context.Background()); err != nil {
		t.Fatalf("second shutdown: %v", err)
	}
}
