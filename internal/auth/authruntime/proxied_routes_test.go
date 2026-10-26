package authruntime

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// proxiedAuthRoute is one route go-api sends to auth-service. fromPhase is the
// first authService.phase whose go-api forwards it (0: the chart never turns
// it on). The old pods of an upgrade, and the new pods of a rollback, run
// phase fromPhase-1, so the route must be mounted from there up.
type proxiedAuthRoute struct {
	key       string
	aggregate string
	fromPhase int
}

// proxiedAuthRoutes is a copy of go-api's route table (internal/apiservice
// authproxy.go sessionProxyRoutes and routeProxyRoutes); the drift test below
// fails when the two differ. The last entry is the session issue route go-api
// calls itself (authapi.PatternIssueSession); it is not in that table.
var proxiedAuthRoutes = []proxiedAuthRoute{
	{"POST /api/v1/auth/refresh", "AuthSessions", 2},
	{"POST /api/v1/auth/logout", "AuthSessions", 2},
	{"POST /api/v1/auth/login", "AuthLocalLogin", 3},
	{"POST /api/v1/auth/register", "AuthCredentials", 4},
	{"GET /api/v1/auth/verify", "AuthCredentials", 4},
	{"POST /api/v1/auth/resend-verification", "AuthCredentials", 4},
	{"POST /api/v1/auth/forgot-password", "AuthCredentials", 4},
	{"POST /api/v1/auth/reset-password", "AuthCredentials", 4},
	{"POST /api/v1/auth/change-password", "AuthCredentials", 4},
	{"POST /api/v1/auth/social-login", "AuthSocial", 3},
	{"POST /api/v1/auth/oidc/{provider_id}/authorize", "AuthOIDC", 0},
	{"POST /api/v1/auth/oidc/{provider_id}/callback", "AuthOIDC", 0},
	{"GET /api/v1/auth/saml/{provider_id}/metadata", "AuthSAML", 0},
	{"POST /api/v1/auth/saml/{provider_id}/initiate", "AuthSAML", 0},
	{"POST /api/v1/auth/saml/{provider_id}/acs", "AuthSAML", 0},
	{"POST " + authapi.PatternIssueSession, "AuthSessions", 2},
}

var goAPITableLine = regexp.MustCompile(`(?m)^\t"((?:GET|POST) /[^"]+)":\s+(Auth[A-Za-z]+),$`)

func TestProxiedRouteCopyMatchesGoAPIRouteTable(t *testing.T) {
	source, err := os.ReadFile("../../apiservice/authproxy.go")
	if err != nil {
		t.Fatal(err)
	}
	table := map[string]string{}
	for _, m := range goAPITableLine.FindAllSubmatch(source, -1) {
		table[string(m[1])] = string(m[2])
	}
	if len(table) == 0 {
		t.Fatal("no route found in go-api's route table: the table layout changed, update this test")
	}
	copied := map[string]string{}
	for _, r := range proxiedAuthRoutes {
		if r.key != "POST "+authapi.PatternIssueSession {
			copied[r.key] = r.aggregate
		}
	}
	for key, aggregate := range table {
		if copied[key] != aggregate {
			t.Errorf("go-api forwards %s (%s) but proxiedAuthRoutes has %q: add it, with its first phase", key, aggregate, copied[key])
		}
	}
	for key := range copied {
		if _, ok := table[key]; !ok {
			t.Errorf("proxiedAuthRoutes lists %s but go-api's table does not", key)
		}
	}
}

type phaseConfig struct {
	phase int
	env   map[string]string
}

func phaseConfigs() []phaseConfig {
	base := func() map[string]string {
		return map[string]string{
			authconfig.EnvDatabaseURI:    "postgres://auth@127.0.0.1:1/devhealth",
			authconfig.EnvSigningKeyFile: "/nonexistent",
			authconfig.EnvSigningKeyID:   "proxied-routes-test",
		}
	}
	owned := compatEnvironment(base())
	owned[authconfig.EnvUsersOwned] = "true"
	return []phaseConfig{
		{1, base()},
		{2, sessionEnvironment(base())},
		{3, compatEnvironment(base())},
		{4, owned},
	}
}

func mountedAuthRoutes(t *testing.T, env map[string]string) map[string]bool {
	t.Helper()
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
		t.Fatal(err)
	}
	credentials, components, err := credentialRoutes(cfg, logger, pool, schema, mustLimits(t))
	if err != nil {
		t.Fatal(err)
	}
	for _, component := range components {
		t.Cleanup(func() { _ = component.Shutdown(context.Background()) })
	}
	mounted := map[string]bool{}
	for _, route := range append(routes, credentials...) {
		mounted[route.Method+" "+route.Pattern] = true
	}
	return mounted
}

// pendingPhase1Gap is the phase 1 sessions gap: pods without the session keys
// mount none of these. It is empty once they answer 503 there.
var pendingPhase1Gap = map[string]bool{
	"POST /api/v1/auth/refresh":           true,
	"POST /api/v1/auth/logout":            true,
	"POST " + authapi.PatternIssueSession: true,
}

func TestEveryGoAPIProxiedAuthRouteIsMountedInEveryAuthServicePhase(t *testing.T) {
	for _, pc := range phaseConfigs() {
		mounted := mountedAuthRoutes(t, pc.env)
		var missing []string
		for _, r := range proxiedAuthRoutes {
			if r.fromPhase == 0 || pc.phase < r.fromPhase-1 {
				continue
			}
			if pc.phase == 1 && pendingPhase1Gap[r.key] {
				continue
			}
			if !mounted[r.key] {
				missing = append(missing, r.key)
			}
		}
		sort.Strings(missing)
		if len(missing) != 0 {
			t.Errorf("auth-service phase %d does not mount %v: a go-api one phase up (or down, on a rollback) would get a 404", pc.phase, missing)
		}
	}
}

func TestNotMountedStubsAnswer503WithRetryAfterInEveryPhase(t *testing.T) {
	stubbed := map[int][]string{
		2: {"GET /api/v1/auth/verify", "POST /api/v1/auth/login", "POST /api/v1/auth/register"},
		3: {"GET /api/v1/auth/verify", "POST /api/v1/auth/register"},
	}
	for _, pc := range phaseConfigs() {
		keys, ok := stubbed[pc.phase]
		if !ok {
			continue
		}
		cfg, err := authconfig.Load(authconfig.Spec{LookupEnv: lookupFrom(pc.env)})
		if err != nil {
			t.Fatal(err)
		}
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
		routes, err := sessionRoutes(cfg, logger, pool, schema, nil, mustLimits(t))
		if err != nil {
			t.Fatal(err)
		}
		credentials, _, err := credentialRoutes(cfg, logger, pool, schema, mustLimits(t))
		if err != nil {
			t.Fatal(err)
		}
		byKey := map[string]httpapi.Route{}
		for _, route := range append(routes, credentials...) {
			byKey[route.Method+" "+route.Pattern] = route
		}
		for _, key := range keys {
			route, ok := byKey[key]
			if !ok {
				t.Fatalf("phase %d: %s is not mounted", pc.phase, key)
			}
			rec := httptest.NewRecorder()
			route.Handler.ServeHTTP(rec, httptest.NewRequest(route.Method, route.Pattern, strings.NewReader("{}")))
			if rec.Code != http.StatusServiceUnavailable || rec.Header().Get("Retry-After") != "1" {
				t.Errorf("phase %d %s: status %d Retry-After %q; want 503 and 1", pc.phase, key, rec.Code, rec.Header().Get("Retry-After"))
			}
		}
	}
}
