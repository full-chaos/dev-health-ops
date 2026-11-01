package authruntime

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// c8 (runtime): the session runtime builds the validate route's collaborators
// and the route stays dark, a 404, until the endpoint-profile row lands
// (PR-2, D6617); the start log names it.
func TestTheValidateRouteIsBuiltAndDarkWithoutItsProfileRow(t *testing.T) {
	env := compatEnvironment(map[string]string{
		authconfig.EnvDatabaseURI:    "postgres://auth@127.0.0.1:1/devhealth",
		authconfig.EnvSigningKeyFile: "/nonexistent",
		authconfig.EnvSigningKeyID:   "validate-test",
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
	for _, route := range routes {
		if route.Pattern == authapi.PatternCompatValidate {
			t.Fatalf("%s %s is mounted without its endpoint-profile row", route.Method, route.Pattern)
		}
	}
	if !strings.Contains(logs.String(), `"msg":"auth validate route not mounted: no endpoint-profile row","route":"`+authapi.PatternCompatValidate+`"`) {
		t.Fatalf("the start log does not name the dark validate route: %s", logs.String())
	}
}
