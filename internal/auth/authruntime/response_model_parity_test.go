package authruntime

import (
	"context"
	"io"
	"log/slog"
	"sort"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/apiservice"
	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// goAPITwinAliases maps an auth-service route key to the go-api key of its
// twin where the pattern's wildcard name differs (net/http refuses two
// wildcard names at one path, so the by-type authorize route reads provider_id).
var goAPITwinAliases = map[string]string{
	"GET /api/v1/auth/oauth/{provider_id}/authorize": "GET /api/v1/auth/oauth/{provider_type}/authorize",
}

// goOnlyRoutes are auth-service routes with no go-api twin: the flag follows
// the handler's own success writer (true: policy.WriteModel, false: any
// other writer or no body written through policy).
var goOnlyRoutes = map[string]struct {
	model  bool
	reason string
}{
	"DELETE /v1/identities/social/{id}": {false, "auth-service native route; no go-api twin; writes no policy body"},
	"GET /v1/sessions/current":          {false, "auth-service native route; no go-api twin; writes no policy body"},
	"POST /internal/v1/sessions":        {false, "internal session issue route; no go-api twin; writes no policy body"},
	"POST /v1/identities/social":        {false, "auth-service native route; no go-api twin; writes no policy body"},
	"POST /v1/sessions/revoke-all":      {false, "auth-service native route; no go-api twin; writes no policy body"},
	"POST /v1/sessions/revoke":          {false, "auth-service native route; no go-api twin; writes no policy body"},
	"POST /v1/token/refresh":            {false, "auth-service native route; no go-api twin; writes no policy body"},
	"POST /api/v1/auth/change-password": {true, "Go-only self-service route; answers through writeCompatModel (policy.WriteModel)"},
}

func parityRoutes(t *testing.T) []httpapi.Route {
	t.Helper()
	env := ssoEnvironment(compatEnvironment(map[string]string{
		authconfig.EnvDatabaseURI:      "postgres://auth@127.0.0.1:1/devhealth",
		authconfig.EnvSigningKeyFile:   "/nonexistent",
		authconfig.EnvSigningKeyID:     "parity-test",
		authconfig.EnvUsersOwned:       "true",
		authconfig.EnvSocialLinkRoutes: "true",
	}))
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
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	routes, err := sessionRoutes(cfg, logger, pool, schema, nil, mustLimits(t))
	if err != nil {
		t.Fatalf("sessionRoutes: %v", err)
	}
	credentials, components, err := credentialRoutes(cfg, logger, pool, schema, mustLimits(t))
	if err != nil {
		t.Fatalf("credentialRoutes: %v", err)
	}
	for _, component := range components {
		t.Cleanup(func() { _ = component.Shutdown(context.Background()) })
	}
	return append(routes, credentials...)
}

func TestAuthServiceRouteResponseModelFlagsMatchGoAPI(t *testing.T) {
	routes := parityRoutes(t)
	seen := map[string]bool{}
	var problems []string
	for _, route := range routes {
		key := route.Method + " " + route.Pattern
		seen[key] = true
		twin := key
		if alias, ok := goAPITwinAliases[key]; ok {
			twin = alias
		}
		if want, named := apiservice.GoAPIResponseModel(twin); named {
			if route.ResponseModel != want {
				problems = append(problems, key+": ResponseModel "+boolText(route.ResponseModel)+", go-api "+twin+" is "+boolText(want))
			}
			continue
		}
		row, goOnly := goOnlyRoutes[key]
		switch {
		case !goOnly:
			problems = append(problems, key+": no go-api twin and no goOnlyRoutes row (its flag is "+boolText(route.ResponseModel)+")")
		case route.ResponseModel != row.model:
			problems = append(problems, key+": ResponseModel "+boolText(route.ResponseModel)+", goOnlyRoutes says "+boolText(row.model)+" ("+row.reason+")")
		}
	}
	for key := range goOnlyRoutes {
		if !seen[key] {
			problems = append(problems, key+": a goOnlyRoutes row for a route the walk did not find")
		}
	}
	for _, anchor := range []string{
		"POST /api/v1/auth/login", "POST /api/v1/auth/social-login", "POST /api/v1/auth/refresh", "POST /api/v1/auth/logout",
		"GET /api/v1/auth/verify", "POST /api/v1/auth/reset-password", "POST /api/v1/auth/change-password",
		"POST /api/v1/auth/oidc/{provider_id}/callback", "POST /api/v1/auth/saml/{provider_id}/acs",
	} {
		if !seen[anchor] {
			problems = append(problems, anchor+": not among the walked routes")
		}
	}
	sort.Strings(problems)
	for _, problem := range problems {
		t.Error(problem)
	}
}

func boolText(value bool) string {
	if value {
		return "true"
	}
	return "false"
}
