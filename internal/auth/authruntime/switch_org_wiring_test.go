package authruntime

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// c8 (runtime): the session runtime builds the switch-org route's collaborators
// and mounts the route once, on its embedded endpoint-profile row.
func TestTheSwitchOrgRouteIsMountedOnItsEmbeddedProfileRow(t *testing.T) {
	env := compatEnvironment(map[string]string{
		authconfig.EnvDatabaseURI:    "postgres://auth@127.0.0.1:1/devhealth",
		authconfig.EnvSigningKeyFile: "/nonexistent",
		authconfig.EnvSigningKeyID:   "switch-org-test",
	})
	cfg, err := authconfig.Load(authconfig.Spec{LookupEnv: lookupFrom(env)})
	if err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(context.Background(), "postgres://auth@127.0.0.1:1/devhealth")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	routes, err := sessionRoutes(cfg, slog.New(slog.NewJSONHandler(&logs, nil)), pool, schema, nil, mustLimits(t))
	if err != nil {
		t.Fatal(err)
	}
	mounted := 0
	for _, route := range routes {
		if route.Method == http.MethodPost && route.Pattern == authapi.PatternCompatSwitchOrg {
			mounted++
		}
	}
	if mounted != 1 {
		t.Fatalf("POST %s is mounted %d times, want 1", authapi.PatternCompatSwitchOrg, mounted)
	}
	if strings.Contains(logs.String(), "auth switch-org route not mounted") {
		t.Fatalf("the start log says the switch-org route is not mounted: %s", logs.String())
	}
}
