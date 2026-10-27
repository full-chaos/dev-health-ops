package authruntime

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

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
// fails when the two differ. The session issue route (authapi.PatternIssueSession)
// is not listed: go-api calls it with a service credential and never forwards it.
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
}

var goAPITableLine = regexp.MustCompile(`(?m)^\t"([A-Z]+ /[^"]+)":\s+(Auth[A-Za-z]+),$`)

// goAPIAggregateLadderKey names each go-api aggregate by its ladder key in the chart; an aggregate the chart
// refuses to turn on (OIDC, SAML) has none.
var goAPIAggregateLadderKey = map[string]string{
	"AuthSessions": "sessions", "AuthLocalLogin": "localLogin", "AuthCredentials": "credentials", "AuthSocial": "social",
}

var chartLadder = regexp.MustCompile(`\$ladder := dict ([^\n]*?) -}}`)

// chartLadderPhases reads the first phase of each ladder flag from the chart helper.
func chartLadderPhases(t *testing.T) map[string]int {
	t.Helper()
	source, err := os.ReadFile("../../../deploy/helm/dev-health/templates/_helpers.tpl")
	if err != nil {
		t.Fatal(err)
	}
	m := chartLadder.FindSubmatch(source)
	if m == nil {
		t.Fatal("the chart's authOn ladder was not found: the helper layout changed, update this test")
	}
	fields := strings.Fields(string(m[1]))
	phases := map[string]int{}
	for i := 0; i+1 < len(fields); i += 2 {
		n, err := strconv.Atoi(fields[i+1])
		if err != nil {
			t.Fatalf("ladder value %q: %v", fields[i+1], err)
		}
		phases[strings.Trim(fields[i], `"`)] = n
	}
	return phases
}

// driftProblems compares go-api's route table source with the copy.
func driftProblems(source []byte, copied []proxiedAuthRoute, ladder map[string]int) []string {
	table := map[string]string{}
	for _, m := range goAPITableLine.FindAllSubmatch(source, -1) {
		table[string(m[1])] = string(m[2])
	}
	var problems []string
	if len(table) == 0 {
		return []string{"no route found in go-api's route table: the table layout changed, update this test"}
	}
	have := map[string]string{}
	for _, r := range copied {
		have[r.key] = r.aggregate
	}
	for key, aggregate := range table {
		if have[key] != aggregate {
			problems = append(problems, fmt.Sprintf("go-api forwards %s (%s) but proxiedAuthRoutes has %q: add it, with its first phase", key, aggregate, have[key]))
		}
	}
	for _, r := range copied {
		if want := ladder[goAPIAggregateLadderKey[r.aggregate]]; r.fromPhase != want {
			problems = append(problems, fmt.Sprintf("%s is listed from phase %d but the chart turns %s on at phase %d", r.key, r.fromPhase, r.aggregate, want))
		}
	}
	for key := range have {
		if _, ok := table[key]; !ok {
			problems = append(problems, fmt.Sprintf("proxiedAuthRoutes lists %s but go-api's table does not", key))
		}
	}
	sort.Strings(problems)
	return problems
}

func TestProxiedRouteCopyMatchesGoAPIRouteTable(t *testing.T) {
	source, err := os.ReadFile("../../apiservice/authproxy.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, problem := range driftProblems(source, proxiedAuthRoutes, chartLadderPhases(t)) {
		t.Error(problem)
	}
}

const driftFixture = "var sessionProxyRoutes = map[string]AuthAggregate{\n" +
	"\t\"POST /api/v1/auth/refresh\": AuthSessions,\n" +
	"\t\"%s\": AuthCredentials,\n" +
	"}\n"

// A route of any method in go-api's table that the copy lacks is drift.
func TestDriftSeesARouteOfAnyMethodInTheTable(t *testing.T) {
	var copied []proxiedAuthRoute
	for _, r := range proxiedAuthRoutes {
		if r.key == "POST /api/v1/auth/refresh" {
			copied = append(copied, r)
		}
	}
	for _, method := range []string{"DELETE", "PUT", "PATCH"} {
		source := []byte(fmt.Sprintf(driftFixture, method+" /api/v1/auth/account"))
		if len(driftProblems(source, copied, chartLadderPhases(t))) == 0 {
			t.Errorf("a %s route in go-api's table is not drift", method)
		}
	}
}

// The copy's first phase of a route must equal the chart ladder's phase of its aggregate.
func TestDriftComparesTheFirstPhaseWithTheChartLadder(t *testing.T) {
	var copied []proxiedAuthRoute
	for _, r := range proxiedAuthRoutes {
		r2 := r
		if r.key == "POST /api/v1/auth/resend-verification" {
			r2.fromPhase = 5
		}
		copied = append(copied, r2)
	}
	source, err := os.ReadFile("../../apiservice/authproxy.go")
	if err != nil {
		t.Fatal(err)
	}
	if len(driftProblems(source, copied, chartLadderPhases(t))) == 0 {
		t.Error("resend-verification listed from phase 5 while the chart ladder turns credentials on at phase 4 is not drift")
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

// pendingPhase1Gap lists routes a phase 1 pod does not mount yet; it is empty.
var pendingPhase1Gap = map[string]bool{}

func TestEveryGoAPIProxiedAuthRouteIsMountedInEveryAuthServicePhase(t *testing.T) {
	for _, pc := range phaseConfigs() {
		mounted := mountedAuthRoutes(t, pc.env)
		var missing []string
		for _, r := range proxiedAuthRoutes {
			if r.fromPhase == 0 || pc.phase < r.fromPhase-1 {
				continue
			}
			if pc.phase == 1 && pendingPhase1Gap[r.key] {
				if mounted[r.key] {
					t.Errorf("%s is mounted at phase 1 but is still listed in pendingPhase1Gap: empty the list", r.key)
				}
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
		1: {"POST /api/v1/auth/refresh", "POST /api/v1/auth/logout"},
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
