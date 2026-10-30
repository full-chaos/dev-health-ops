package authruntime

import (
	"context"
	"io"
	"log/slog"
	"sort"
	"strconv"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/apiservice"
	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/token"
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
	"DELETE /v1/identities/social/{id}":           {false, "auth-service native route; no go-api twin; writes no policy body"},
	"GET /v1/sessions/current":                    {false, "auth-service native route; no go-api twin; writes no policy body"},
	"POST /internal/v1/sessions":                  {false, "internal session issue route; no go-api twin; writes no policy body"},
	"POST /internal/v1/token/exchange":            {false, "internal workload-token exchange; no go-api twin; writes no policy body"},
	"POST /internal/v1/memberships/add":           {false, "internal provisioning route; no go-api twin; writes no policy body"},
	"POST /internal/v1/memberships/role":          {false, "internal provisioning route; no go-api twin; writes no policy body"},
	"POST /internal/v1/memberships/remove":        {false, "internal provisioning route; no go-api twin; writes no policy body"},
	"POST /internal/v1/memberships/transfer":      {false, "internal provisioning route; no go-api twin; writes no policy body"},
	"POST /internal/v1/invites/create":            {false, "internal provisioning route; no go-api twin; writes no policy body"},
	"POST /internal/v1/invites/accept":            {false, "internal provisioning route; no go-api twin; writes no policy body"},
	"POST /internal/v1/invites/revoke":            {false, "internal provisioning route; no go-api twin; writes no policy body"},
	"POST /internal/v1/organizations/deprovision": {false, "internal provisioning route; no go-api twin; writes no policy body"},
	"POST /internal/v1/organizations/reactivate":  {false, "internal provisioning route; no go-api twin; writes no policy body"},
	"POST /v1/identities/social":                  {false, "auth-service native route; no go-api twin; writes no policy body"},
	"POST /v1/sessions/revoke-all":                {false, "auth-service native route; no go-api twin; writes no policy body"},
	"POST /v1/sessions/revoke":                    {false, "auth-service native route; no go-api twin; writes no policy body"},
	"POST /v1/token/refresh":                      {false, "auth-service native route; no go-api twin; writes no policy body"},
	"GET /.well-known/jwks.json":                  {false, "well-known document; no go-api twin; written without a policy body"},
	"HEAD /.well-known/jwks.json":                 {false, "well-known document; no go-api twin; written without a policy body"},
	"GET /.well-known/openid-configuration":       {false, "well-known document; no go-api twin; written without a policy body"},
	"HEAD /.well-known/openid-configuration":      {false, "well-known document; no go-api twin; written without a policy body"},
	"POST /api/v1/auth/change-password":           {true, "Go-only self-service route; answers through writeCompatModel (policy.WriteModel)"},
}

func parityRoutes(t *testing.T) []httpapi.Route {
	t.Helper()
	env := map[string]string{
		authconfig.EnvDatabaseURI:      "postgres://auth@127.0.0.1:1/devhealth",
		authconfig.EnvSigningKeyFile:   "/nonexistent",
		authconfig.EnvSigningKeyID:     "parity-test",
		authconfig.EnvUsersOwned:       "true",
		authconfig.EnvSocialLinkRoutes: "true",
		authconfig.EnvTokenIssuerURL:   "https://issuer.example.test",
	}
	exchangeEnvironment(t, env)
	env = provisioningEnvironment(ssoEnvironment(compatEnvironment(env)))
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
	credentials, components, err := credentialRoutes(cfg, logger, pool, schema, mustLimits(t), nil)
	if err != nil {
		t.Fatalf("credentialRoutes: %v", err)
	}
	for _, component := range components {
		t.Cleanup(func() { _ = component.Shutdown(context.Background()) })
	}
	exchange, err := exchangeRoutes(cfg, logger, pool, schema, token.NewFileSigner(token.FileConfig{KeyPath: "/nonexistent", KeyID: "parity-test"}))
	if err != nil {
		t.Fatalf("exchangeRoutes: %v", err)
	}
	return append(append(routes, credentials...), exchange...)
}

func TestAuthServiceResponseModelTableMatchesGoAPI(t *testing.T) {
	var problems []string
	for key, model := range authapi.ResponseModelRoutes() {
		twin := key
		if alias, ok := goAPITwinAliases[key]; ok {
			twin = alias
		}
		if want, named := apiservice.GoAPIResponseModel(twin); named {
			if model != want {
				problems = append(problems, key+": table "+boolText(model)+", go-api "+twin+" is "+boolText(want))
			}
			continue
		}
		row, goOnly := goOnlyRoutes[key]
		switch {
		case !goOnly:
			problems = append(problems, key+": no go-api twin and no goOnlyRoutes row (table "+boolText(model)+")")
		case model != row.model:
			problems = append(problems, key+": table "+boolText(model)+", goOnlyRoutes says "+boolText(row.model)+" ("+row.reason+")")
		}
	}
	for key := range goOnlyRoutes {
		if _, named := authapi.ResponseModelRoutes()[key]; !named {
			problems = append(problems, key+": a goOnlyRoutes row without a table entry")
		}
	}
	sort.Strings(problems)
	for _, problem := range problems {
		t.Error(problem)
	}
}

func TestEveryMountedAuthServiceRouteHasExactlyOneFlagEntry(t *testing.T) {
	table := authapi.ResponseModelRoutes()
	mounted := map[string]int{}
	for _, route := range append(parityRoutes(t), authapi.WellKnownRoutes(authapi.WellKnownDeps{IssuerURL: "https://issuer.example.test"})...) {
		mounted[route.Method+" "+route.Pattern]++
	}
	var problems []string
	for key, count := range mounted {
		if count != 1 {
			problems = append(problems, key+": mounted "+strconv.Itoa(count)+" times")
		}
		if _, named := table[key]; !named {
			problems = append(problems, key+": mounted without a table entry")
		}
	}
	for key := range table {
		if mounted[key] == 0 {
			problems = append(problems, key+": a table entry for a route that is not mounted")
		}
	}
	sort.Strings(problems)
	for _, problem := range problems {
		t.Error(problem)
	}
}

func TestAuthServerOptionsMarkEveryRouteFromTheTable(t *testing.T) {
	cfg := authconfig.Config{}
	opts := apiServerOptions(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), parityRoutes(t))
	table := authapi.ResponseModelRoutes()
	for _, route := range opts.Routes {
		key := route.Method + " " + route.Pattern
		if route.ResponseModel != table[key] {
			t.Errorf("%s: server route flag %s, table %s", key, boolText(route.ResponseModel), boolText(table[key]))
		}
	}
	if len(opts.Routes) == 0 {
		t.Fatal("no routes")
	}
}

func boolText(value bool) string {
	if value {
		return "true"
	}
	return "false"
}
