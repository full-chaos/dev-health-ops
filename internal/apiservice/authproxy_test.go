package apiservice

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/apimetrics"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
)

const proxyTestKey = "fixture-key-fixture-key-fixture-key-"

// proxyOrigin is the fixture's one CORS origin; register checks it.
const proxyOrigin = "https://app.example"

// wantSessionProxyRoutes and wantRouteProxyRoutes are the D1 route map
// (CHAOS-10231). A change here is a deliberate routing change.
var (
	wantSessionProxyRoutes = map[string]AuthAggregate{
		"POST /api/v1/auth/refresh":             AuthSessions,
		"POST /api/v1/auth/logout":              AuthSessions,
		"POST /api/v1/auth/login":               AuthLocalLogin,
		"POST /api/v1/auth/register":            AuthCredentials,
		"GET /api/v1/auth/verify":               AuthCredentials,
		"POST /api/v1/auth/resend-verification": AuthCredentials,
		"POST /api/v1/auth/forgot-password":     AuthCredentials,
		"POST /api/v1/auth/reset-password":      AuthCredentials,
		"POST /api/v1/auth/change-password":     AuthCredentials,
		"POST /api/v1/auth/social-login":        AuthSocial,
	}
	wantRouteProxyRoutes = map[string]AuthAggregate{
		"POST /api/v1/auth/oidc/{provider_id}/authorize": AuthOIDC,
		"POST /api/v1/auth/oidc/{provider_id}/callback":  AuthOIDC,
		"GET /api/v1/auth/saml/{provider_id}/metadata":   AuthSAML,
		"POST /api/v1/auth/saml/{provider_id}/initiate":  AuthSAML,
		"POST /api/v1/auth/saml/{provider_id}/acs":       AuthSAML,
	}
)

func lookupFrom(env map[string]string) func(string) (string, bool) {
	return func(name string) (string, bool) {
		value, ok := env[name]
		return value, ok
	}
}

func allOn(upstream string) map[string]string {
	env := map[string]string{EnvAuthProxyURL: upstream}
	for _, aggregate := range AuthAggregates {
		env[AuthProxyFlagEnv(aggregate)] = "true"
	}
	return env
}

func mustProxyConfig(t *testing.T, env map[string]string) AuthProxyConfig {
	t.Helper()
	cfg, err := ParseAuthProxyConfig(lookupFrom(env))
	if err != nil {
		t.Fatalf("ParseAuthProxyConfig(%v): %v", env, err)
	}
	return cfg
}

func TestAuthProxyFlagNamesAreTheDocumentedOnes(t *testing.T) {
	want := map[AuthAggregate]string{
		AuthSessions: "AUTH_PROXY_SESSIONS", AuthLocalLogin: "AUTH_PROXY_LOCAL_LOGIN", AuthCredentials: "AUTH_PROXY_CREDENTIALS",
		AuthSocial: "AUTH_PROXY_SOCIAL", AuthOIDC: "AUTH_PROXY_OIDC", AuthSAML: "AUTH_PROXY_SAML",
	}
	if len(AuthAggregates) != len(want) {
		t.Fatalf("%d aggregates, want %d", len(AuthAggregates), len(want))
	}
	for _, aggregate := range AuthAggregates {
		if got := AuthProxyFlagEnv(aggregate); got != want[aggregate] {
			t.Errorf("flag of %s = %q, want %q", aggregate, got, want[aggregate])
		}
	}
	if EnvAuthProxyURL != "AUTH_PROXY_URL" {
		t.Errorf("EnvAuthProxyURL = %q", EnvAuthProxyURL)
	}
}

// Every flag defaults off: with nothing set, no aggregate leaves go-api.
func TestAuthProxyEveryFlagDefaultsOff(t *testing.T) {
	for name, env := range map[string]map[string]string{
		"unset":           {},
		"empty":           {AuthProxyFlagEnv(AuthSessions): "", AuthProxyFlagEnv(AuthSAML): " "},
		"url only":        {EnvAuthProxyURL: "http://auth-service:8095"},
		"explicit false":  {EnvAuthProxyURL: "http://auth-service:8095", AuthProxyFlagEnv(AuthSessions): "false"},
		"process default": nil,
	} {
		cfg := mustProxyConfig(t, env)
		for _, aggregate := range AuthAggregates {
			if cfg.Enabled(aggregate) {
				t.Errorf("%s: %s is on", name, aggregate)
			}
		}
	}
	if cfg := (AuthProxyConfig{}); cfg.Enabled(AuthSessions) {
		t.Error("the zero AuthProxyConfig routes sessions away")
	}
}

func TestAuthProxyConfigRefusesUnsafeShapes(t *testing.T) {
	on := func(flags ...AuthAggregate) map[string]string {
		env := map[string]string{EnvAuthProxyURL: "http://auth-service:8095"}
		for _, flag := range flags {
			env[AuthProxyFlagEnv(flag)] = "true"
		}
		return env
	}
	refused := map[string]map[string]string{
		"flag without url":   {AuthProxyFlagEnv(AuthSessions): "true"},
		"not a boolean":      {EnvAuthProxyURL: "http://auth-service:8095", AuthProxyFlagEnv(AuthSessions): "yes"},
		"scheme":             {EnvAuthProxyURL: "ftp://auth-service", AuthProxyFlagEnv(AuthSessions): "true"},
		"no host":            {EnvAuthProxyURL: "http://", AuthProxyFlagEnv(AuthSessions): "true"},
		"relative":           {EnvAuthProxyURL: "auth-service:8095", AuthProxyFlagEnv(AuthSessions): "true"},
		"path":               {EnvAuthProxyURL: "http://auth-service:8095/prefix", AuthProxyFlagEnv(AuthSessions): "true"},
		"query":              {EnvAuthProxyURL: "http://auth-service:8095?x=1", AuthProxyFlagEnv(AuthSessions): "true"},
		"fragment":           {EnvAuthProxyURL: "http://auth-service:8095#x", AuthProxyFlagEnv(AuthSessions): "true"},
		"userinfo":           {EnvAuthProxyURL: "http://u:p@auth-service:8095", AuthProxyFlagEnv(AuthSessions): "true"},
		"bad url with flags": {EnvAuthProxyURL: "http://[::1", AuthProxyFlagEnv(AuthSessions): "true"},
	}
	for _, aggregate := range AuthAggregates[1:] {
		refused[string(aggregate)+" without sessions"] = on(aggregate)
	}
	for name, env := range refused {
		if _, err := ParseAuthProxyConfig(lookupFrom(env)); err == nil {
			t.Errorf("%s: accepted %v", name, env)
		}
	}
	for name, env := range map[string]map[string]string{
		"sessions only":      on(AuthSessions),
		"all":                on(AuthAggregates...),
		"root path":          {EnvAuthProxyURL: "https://auth-service:8095/", AuthProxyFlagEnv(AuthSessions): "true"},
		"url without a flag": {EnvAuthProxyURL: "http://auth-service:8095"},
	} {
		if _, err := ParseAuthProxyConfig(lookupFrom(env)); err != nil {
			t.Errorf("%s: refused: %v", name, err)
		}
	}
}

func TestAuthProxyRouteMapIsTheD1Map(t *testing.T) {
	if len(sessionProxyRoutes) != len(wantSessionProxyRoutes) || len(routeProxyRoutes) != len(wantRouteProxyRoutes) {
		t.Fatalf("map sizes %d/%d, want %d/%d", len(sessionProxyRoutes), len(routeProxyRoutes), len(wantSessionProxyRoutes), len(wantRouteProxyRoutes))
	}
	for key, aggregate := range wantSessionProxyRoutes {
		if sessionProxyRoutes[key] != aggregate {
			t.Errorf("session route %s -> %q, want %q", key, sessionProxyRoutes[key], aggregate)
		}
	}
	for key, aggregate := range wantRouteProxyRoutes {
		if routeProxyRoutes[key] != aggregate {
			t.Errorf("route %s -> %q, want %q", key, routeProxyRoutes[key], aggregate)
		}
	}
	mounted := map[string]bool{}
	for _, route := range proxyFixtureRoutes(t, AuthProxyConfig{}) {
		mounted[route.Method+" "+route.Pattern] = true
	}
	for key := range wantSessionProxyRoutes {
		if !mounted[key] {
			t.Errorf("%s is mapped but not mounted", key)
		}
	}
	for key := range wantRouteProxyRoutes {
		if !mounted[key] {
			t.Errorf("%s is mapped but not mounted", key)
		}
	}
}

// proxyScope authenticates every signed token as an active user.
func proxyFixtureRoutes(t *testing.T, cfg AuthProxyConfig) []httpapi.Route {
	t.Helper()
	return proxyFixtureRoutesLogging(t, cfg, quietLogger())
}

func proxyFixtureRoutesLogging(t *testing.T, cfg AuthProxyConfig, logger *slog.Logger) []httpapi.Route {
	t.Helper()
	verifier, err := edgetoken.New(proxyTestKey, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	signer, err := edgetoken.NewSigner(proxyTestKey, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	auth, err := policy.NewAuthenticator(verifier, scopeStore{admin: uuid.New(), target: uuid.New(), targetOrg: uuid.New()}, quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(context.Background(), "postgres://u:p@127.0.0.1:1/db?sslmode=disable&connect_timeout=1")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return Routes(Deps{Pool: pool, Auth: auth, Guard: policy.NewGuard(auth, quietLogger()), Verifier: verifier, Signer: signer,
		AuthProxy: cfg, Limits: httpapi.NewMemoryCounters(time.Now)}, logger)
}

func proxyFixture(t *testing.T, cfg AuthProxyConfig) http.Handler {
	t.Helper()
	server, err := NewServer(config.Config{APIAddress: "127.0.0.1:0", CORSAllowedOrigins: []string{proxyOrigin}}, quietLogger(), proxyFixtureRoutes(t, cfg))
	if err != nil {
		t.Fatal(err)
	}
	return server.Handler()
}

func proxyBearer(t *testing.T) string {
	t.Helper()
	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub": uuid.NewString(), "type": "access", "org_id": uuid.NewString(), "iss": "dev-health-ops", "aud": "dev-health-api",
		"exp": time.Now().Add(time.Hour).Unix(),
	}).SignedString([]byte(proxyTestKey))
	if err != nil {
		t.Fatal(err)
	}
	return token
}

type seenRequest struct {
	method, path, rawPath, query, body string
	header                             http.Header
}

type stubUpstream struct {
	*httptest.Server
	mu   sync.Mutex
	seen []seenRequest
}

func newStubUpstream(t *testing.T, respond func(w http.ResponseWriter, r *http.Request)) *stubUpstream {
	t.Helper()
	stub := &stubUpstream{}
	stub.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		stub.mu.Lock()
		stub.seen = append(stub.seen, seenRequest{method: r.Method, path: r.URL.Path, rawPath: r.URL.RawPath, query: r.URL.RawQuery,
			body: string(body), header: r.Header.Clone()})
		stub.mu.Unlock()
		respond(w, r)
	}))
	t.Cleanup(stub.Close)
	return stub
}

func (s *stubUpstream) requests() []seenRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]seenRequest(nil), s.seen...)
}

func (s *stubUpstream) last(t *testing.T) seenRequest {
	t.Helper()
	seen := s.requests()
	if len(seen) == 0 {
		t.Fatal("the upstream saw no request")
	}
	return seen[len(seen)-1]
}

// proxyMetrics scrapes the api instruments the operator /metrics serves.
func proxyMetrics(t *testing.T) string {
	t.Helper()
	registry := health.NewRegistry(time.Second)
	if err := apimetrics.Register(registry); err != nil {
		t.Fatal(err)
	}
	operator, err := health.NewServer(health.ServerOptions{Address: "127.0.0.1:0", Registry: registry, Service: "test", Version: "test"})
	if err != nil {
		t.Fatal(err)
	}
	scrape := httptest.NewRecorder()
	operator.Handler().ServeHTTP(scrape, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	return scrape.Body.String()
}

func seriesValue(text, series string) float64 {
	scanner := bufio.NewScanner(strings.NewReader(text))
	scanner.Buffer(make([]byte, 1<<20), 1<<20)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, series+" ") {
			value, err := strconv.ParseFloat(strings.TrimSpace(strings.TrimPrefix(line, series)), 64)
			if err == nil {
				return value
			}
		}
	}
	return 0
}

func proxySeries(aggregate AuthAggregate, outcome string) string {
	return `devhealth_api_auth_proxy_requests_total{aggregate="` + string(aggregate) + `",outcome="` + outcome + `"}`
}

// clientClaimHeaders are client-address and forwarding claims a client can
// write; none may reach auth-service.
var clientClaimHeaders = []string{
	"X-Forwarded-Scheme", "X-Forwarded-Ssl", "X-Forwarded-Prefix", "X-Forwarded-Server", "X-Forwarded-Uri",
	"X-Original-Forwarded-For", "X-Client-IP", "True-Client-IP", "CF-Connecting-IP", "X-Cluster-Client-IP", "Fastly-Client-IP",
}

type proxyCall struct {
	method, target, body string
	bearer               bool
	aggregate            AuthAggregate
}

// proxyCalls is one request per mapped route that passes go-api's own
// front checks (body, validator, limiter, guard).
var proxyCalls = []proxyCall{
	{http.MethodPost, "/api/v1/auth/refresh", `{"refresh_token":  "opaque-refresh-1"}`, false, AuthSessions},
	{http.MethodPost, "/api/v1/auth/logout", `{ "refresh_token": "opaque-refresh-1" }`, false, AuthSessions},
	{http.MethodPost, "/api/v1/auth/login", `{"password": "pw", "email": "a@example.com"}`, false, AuthLocalLogin},
	{http.MethodPost, "/api/v1/auth/register", `{"email": "new@example.com", "password": "long-enough-1"}`, false, AuthCredentials},
	{http.MethodGet, "/api/v1/auth/verify?token=link%2Btoken&token=second", "", false, AuthCredentials},
	{http.MethodPost, "/api/v1/auth/resend-verification", `{"email": "a@example.com"}`, false, AuthCredentials},
	{http.MethodPost, "/api/v1/auth/forgot-password", `{"email": "a@example.com"}`, false, AuthCredentials},
	{http.MethodPost, "/api/v1/auth/reset-password", `{"token": "t", "new_password": "long-enough-1"}`, false, AuthCredentials},
	{http.MethodPost, "/api/v1/auth/change-password", `{"current_password": "old", "new_password": "long-enough-1"}`, true, AuthCredentials},
	{http.MethodPost, "/api/v1/auth/social-login", `{"provider": "github", "access_token": "gho_x"}`, false, AuthSocial},
	{http.MethodPost, "/api/v1/auth/oidc/p-1/authorize", `{}`, false, AuthOIDC},
	{http.MethodPost, "/api/v1/auth/oidc/p-1/callback", `{"code": "c", "state": "s"}`, false, AuthOIDC},
	{http.MethodGet, "/api/v1/auth/saml/p-1/metadata", "", false, AuthSAML},
	{http.MethodPost, "/api/v1/auth/saml/p-1/initiate", `{"relay_state": "/x"}`, false, AuthSAML},
	{http.MethodPost, "/api/v1/auth/saml/p-1/acs", `{"SAMLResponse": "PHNhbWw+", "RelayState": "r"}`, false, AuthSAML},
}

func (c proxyCall) request(t *testing.T) *http.Request {
	t.Helper()
	var body io.Reader
	if c.body != "" {
		body = strings.NewReader(c.body)
	}
	request := httptest.NewRequest(c.method, c.target, body)
	request.RemoteAddr = "192.0.2.10:4711"
	request.Header.Set("Origin", proxyOrigin)
	if c.body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	if c.bearer {
		request.Header.Set("Authorization", "Bearer "+proxyBearer(t))
	}
	return request
}

// With every flag on, each mapped route reaches auth-service byte for byte:
// method, path, query, body, credentials and cookies; the client address is
// the one go-api trusts, never a header the client wrote; go-api's request
// id goes up and comes back; upstream CORS and correlation headers do not
// reach the client.
func TestAuthProxyForwardsEveryMappedRouteByteForByte(t *testing.T) {
	stub := newStubUpstream(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Add("Set-Cookie", "dho_sso_nonce=n1; Path=/api/v1/auth; HttpOnly")
		w.Header().Set("X-Auth-Request-ID", "req_AAAAAAAAAAAAAAAAAAAAAAAAAA")
		w.Header().Set("X-Request-ID", "upstream-own-id")
		w.Header().Set("Access-Control-Allow-Origin", "https://evil.example")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, `{"from":"auth-service"}`)
	})
	handler := proxyFixture(t, mustProxyConfig(t, allOn(stub.URL)))
	before := proxyMetrics(t)
	counts := map[AuthAggregate]float64{}
	for _, call := range proxyCalls {
		request := call.request(t)
		request.Header.Set("X-Request-ID", "client-correlation-1")
		request.Header.Set("X-Forwarded-For", "203.0.113.99")
		request.Header.Set("X-Real-IP", "203.0.113.98")
		for _, name := range clientClaimHeaders {
			request.Header.Set(name, "client-supplied")
		}
		request.Header.Set("Forwarded", "for=203.0.113.97")
		request.Header.Set("Cookie", "dho_sso_nonce=n0")
		request.Header.Set("Connection", "keep-alive, X-Hop")
		request.Header.Set("X-Hop", "hop-only")
		request.Header.Set("X-Org-Id", "org-header")
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		if recorder.Code != http.StatusCreated || recorder.Body.String() != `{"from":"auth-service"}` {
			t.Errorf("%s %s = %d %s, want the upstream 201", call.method, call.target, recorder.Code, recorder.Body.String())
			continue
		}
		counts[call.aggregate]++
		seen := stub.last(t)
		wantURL, _ := url.Parse(call.target)
		if seen.method != call.method || seen.path != wantURL.Path || seen.query != wantURL.RawQuery || seen.body != call.body {
			t.Errorf("%s %s reached upstream as %s %s?%s body %q", call.method, call.target, seen.method, seen.path, seen.query, seen.body)
		}
		if got := seen.header.Values("X-Forwarded-For"); len(got) != 1 || got[0] != "192.0.2.10" {
			t.Errorf("%s: X-Forwarded-For = %v, want only the trusted client 192.0.2.10", call.target, got)
		}
		for _, name := range append([]string{"X-Real-IP", "Forwarded", "X-Hop", "X-Forwarded-Host", "X-Forwarded-Proto"}, clientClaimHeaders...) {
			if got := seen.header.Values(name); len(got) != 0 {
				t.Errorf("%s: %s reached upstream: %v", call.target, name, got)
			}
		}
		if got := seen.header.Get("X-Request-ID"); got != "client-correlation-1" {
			t.Errorf("%s: X-Request-ID upstream = %q, want go-api's id", call.target, got)
		}
		if got := seen.header.Get("Cookie"); got != "dho_sso_nonce=n0" {
			t.Errorf("%s: Cookie upstream = %q", call.target, got)
		}
		if got := seen.header.Get("X-Org-Id"); got != "org-header" {
			t.Errorf("%s: X-Org-Id upstream = %q", call.target, got)
		}
		if call.bearer && !strings.HasPrefix(seen.header.Get("Authorization"), "Bearer ") {
			t.Errorf("%s: Authorization did not reach upstream", call.target)
		}
		if call.body != "" && seen.header.Get("Content-Type") != "application/json" {
			t.Errorf("%s: Content-Type upstream = %q", call.target, seen.header.Get("Content-Type"))
		}
		if got := recorder.Header().Values("Set-Cookie"); len(got) != 1 || got[0] != "dho_sso_nonce=n1; Path=/api/v1/auth; HttpOnly" {
			t.Errorf("%s: Set-Cookie to the client = %v", call.target, got)
		}
		if got := recorder.Header().Values("X-Request-ID"); len(got) != 1 || got[0] != "client-correlation-1" {
			t.Errorf("%s: X-Request-ID to the client = %v, want go-api's own", call.target, got)
		}
		if got := recorder.Header().Values("X-Auth-Request-ID"); len(got) != 0 {
			t.Errorf("%s: upstream X-Auth-Request-ID reached the client: %v", call.target, got)
		}
		if got := recorder.Header().Values("Access-Control-Allow-Origin"); len(got) != 1 || got[0] != proxyOrigin {
			t.Errorf("%s: Access-Control-Allow-Origin to the client = %v, want only go-api's own %s", call.target, got, proxyOrigin)
		}
	}
	if got := len(stub.requests()); got != len(proxyCalls) {
		t.Errorf("upstream saw %d requests, want %d", got, len(proxyCalls))
	}
	after := proxyMetrics(t)
	for _, aggregate := range AuthAggregates {
		series := proxySeries(aggregate, "forwarded")
		if delta := seriesValue(after, series) - seriesValue(before, series); delta != counts[aggregate] {
			t.Errorf("%s moved by %v, want %v", series, delta, counts[aggregate])
		}
	}
	if !strings.Contains(after, "devhealth_api_auth_proxy_upstream_duration_seconds_count{aggregate=\"sessions\"}") {
		t.Error("the upstream duration histogram is not on the operator scrape")
	}
}

// With every flag on, the routes outside the D1 map never leave go-api.
func TestAuthProxyLeavesUnmappedAuthRoutesInGoAPI(t *testing.T) {
	stub := newStubUpstream(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusTeapot) })
	handler := proxyFixture(t, mustProxyConfig(t, allOn(stub.URL)))
	for _, call := range []proxyCall{
		{method: http.MethodGet, target: "/api/v1/auth/me", bearer: true},
		{method: http.MethodGet, target: "/api/v1/auth/me/organizations", bearer: true},
		{method: http.MethodPost, target: "/api/v1/auth/switch-org", body: `{"org_id": "x"}`, bearer: true},
		{method: http.MethodPost, target: "/api/v1/auth/validate", body: `{"token": "x"}`},
		{method: http.MethodPost, target: "/api/v1/auth/accept-invite", body: `{"token": "x"}`, bearer: true},
		{method: http.MethodPost, target: "/api/v1/auth/onboard", body: `{}`, bearer: true},
		{method: http.MethodGet, target: "/api/v1/auth/onboarding/state", bearer: true},
		{method: http.MethodGet, target: "/api/v1/auth/sso/providers", bearer: true},
		{method: http.MethodPost, target: "/api/v1/auth/oauth/providers", body: `{}`, bearer: true},
		{method: http.MethodPost, target: "/api/v1/auth/oauth/p-1/authorize", body: `{}`},
		{method: http.MethodGet, target: "/api/v1/auth/oauth/github/authorize"},
	} {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, call.request(t))
		if recorder.Code == http.StatusTeapot {
			t.Errorf("%s %s was forwarded", call.method, call.target)
		}
	}
	if seen := stub.requests(); len(seen) != 0 {
		t.Errorf("upstream saw %d unmapped requests (first %s %s)", len(seen), seen[0].method, seen[0].path)
	}
}

// Flags off: go-api answers every mapped route itself, auth-service sees
// nothing, and the requests count as local.
func TestAuthProxyFlagsOffServeEveryRouteLocally(t *testing.T) {
	stub := newStubUpstream(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusTeapot) })
	handler := proxyFixture(t, mustProxyConfig(t, map[string]string{EnvAuthProxyURL: stub.URL}))
	before := proxyMetrics(t)
	counts := map[AuthAggregate]float64{}
	for _, call := range proxyCalls {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, call.request(t))
		if recorder.Code == http.StatusTeapot {
			t.Errorf("%s %s was forwarded with its flag off", call.method, call.target)
		}
		counts[call.aggregate]++
	}
	if seen := stub.requests(); len(seen) != 0 {
		t.Fatalf("upstream saw %d requests with every flag off", len(seen))
	}
	after := proxyMetrics(t)
	for _, aggregate := range AuthAggregates {
		series := proxySeries(aggregate, "local")
		if delta := seriesValue(after, series) - seriesValue(before, series); delta != counts[aggregate] {
			t.Errorf("%s moved by %v, want %v", series, delta, counts[aggregate])
		}
	}
}

// One flag forwards its own aggregate only.
func TestAuthProxyOneFlagMovesOnlyItsAggregate(t *testing.T) {
	stub := newStubUpstream(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusTeapot) })
	handler := proxyFixture(t, mustProxyConfig(t, map[string]string{EnvAuthProxyURL: stub.URL, AuthProxyFlagEnv(AuthSessions): "true"}))
	for _, call := range proxyCalls {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, call.request(t))
		forwarded := recorder.Code == http.StatusTeapot
		if forwarded != (call.aggregate == AuthSessions) {
			t.Errorf("%s %s forwarded=%v with only the sessions flag on", call.method, call.target, forwarded)
		}
	}
}

// go-api's rate limits stay in front of a forwarded route: a flag flip never
// removes a limit.
func TestAuthProxyKeepsGoAPIRateLimitsInFront(t *testing.T) {
	for _, testCase := range []struct {
		call  proxyCall
		limit int
	}{
		{proxyCalls[0], 10}, // refresh: auth_refresh 10 / 15 min
		{proxyCalls[2], 20}, // login: auth_login_ip 20 / 15 min
		{proxyCalls[3], 3},  // register: auth_register 3 / hour
		{proxyCalls[4], 10}, // verify: auth_verify 10 / hour
		{proxyCalls[5], 3},  // resend-verification: 3 / hour
		{proxyCalls[6], 3},  // forgot-password: 3 / hour
	} {
		t.Run(testCase.call.target, func(t *testing.T) {
			stub := newStubUpstream(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
			handler := proxyFixture(t, mustProxyConfig(t, allOn(stub.URL)))
			for i := 0; i <= testCase.limit; i++ {
				recorder := httptest.NewRecorder()
				handler.ServeHTTP(recorder, testCase.call.request(t))
				want := http.StatusOK
				if i == testCase.limit {
					want = http.StatusTooManyRequests
				}
				if recorder.Code != want {
					t.Fatalf("request %d = %d %s, want %d", i+1, recorder.Code, recorder.Body.String(), want)
				}
			}
			if got := len(stub.requests()); got != testCase.limit {
				t.Fatalf("upstream saw %d requests, want %d (the limited one stays in go-api)", got, testCase.limit)
			}
		})
	}
}

// go-api's own front checks still answer before any forward: a body that is
// not JSON, and a guarded route without a bearer.
func TestAuthProxyKeepsGoAPIFrontChecks(t *testing.T) {
	stub := newStubUpstream(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusTeapot) })
	handler := proxyFixture(t, mustProxyConfig(t, allOn(stub.URL)))
	for _, call := range []proxyCall{
		{method: http.MethodPost, target: "/api/v1/auth/login", body: `{"email": `},
		{method: http.MethodPost, target: "/api/v1/auth/login", body: `{"email": "not-an-email", "password": "x"}`},
		{method: http.MethodPost, target: "/api/v1/auth/change-password", body: `{"current_password": "a", "new_password": "long-enough-1"}`},
		{method: http.MethodGet, target: "/api/v1/auth/verify"},
		{method: http.MethodPost, target: "/api/v1/auth/oidc/p-1/authorize", body: `{"redirect_uri": `},
		{method: http.MethodPost, target: "/api/v1/auth/oidc/p-1/callback", body: "not-json"},
		{method: http.MethodPost, target: "/api/v1/auth/saml/p-1/initiate", body: `{"relay_state": `},
		{method: http.MethodPost, target: "/api/v1/auth/saml/p-1/acs", body: "{\"SAMLResponse\": \"\xff\"}"},
	} {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, call.request(t))
		if recorder.Code == http.StatusTeapot || recorder.Code < 400 {
			t.Errorf("%s %s %q = %d, want go-api's own refusal", call.method, call.target, call.body, recorder.Code)
		}
	}
	if seen := stub.requests(); len(seen) != 0 {
		t.Fatalf("upstream saw %d requests go-api refuses", len(seen))
	}
}

// An unreachable or slow auth-service is a refusal in go-api's shape, never
// a go-api answer to the same request.
func TestAuthProxyFailsClosed(t *testing.T) {
	closed := "http://" + closedAddress(t)
	slow := newStubUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(5 * time.Second):
		case <-r.Context().Done():
		}
		w.WriteHeader(http.StatusOK)
	})
	for _, testCase := range []struct {
		name, upstream string
		timeout        time.Duration
		status         int
		body           string
	}{
		{"unreachable", closed, 0, http.StatusServiceUnavailable, `{"detail":"Authentication service unavailable"}`},
		{"timeout", slow.URL, 200 * time.Millisecond, http.StatusGatewayTimeout, `{"detail":"Authentication service timed out"}`},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			cfg := mustProxyConfig(t, allOn(testCase.upstream))
			cfg.Timeout = testCase.timeout
			handler := proxyFixture(t, cfg)
			before := proxyMetrics(t)
			for _, call := range proxyCalls {
				recorder := httptest.NewRecorder()
				handler.ServeHTTP(recorder, call.request(t))
				if recorder.Code != testCase.status || recorder.Body.String() != testCase.body {
					t.Errorf("%s %s = %d %s, want %d %s", call.method, call.target, recorder.Code, recorder.Body.String(), testCase.status, testCase.body)
				}
				if recorder.Header().Get("X-Request-ID") == "" {
					t.Errorf("%s: the refusal has no correlation id", call.target)
				}
			}
			after := proxyMetrics(t)
			series := proxySeries(AuthSessions, "upstream_error")
			if delta := seriesValue(after, series) - seriesValue(before, series); delta != 2 {
				t.Errorf("%s moved by %v, want 2", series, delta)
			}
		})
	}
}

// An answer auth-service does not finish (cut short, or over the bound) is a
// refusal, never a partial answer under the upstream's status.
func TestAuthProxyFailsClosedOnAnIncompleteUpstreamAnswer(t *testing.T) {
	for name, respond := range map[string]func(w http.ResponseWriter, r *http.Request){
		"cut short": func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Length", "100")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, `{"partial"`)
		},
		"over the bound": func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, `{"pad":"`+strings.Repeat("x", maxAuthProxyResponseBytes)+`"}`)
		},
	} {
		t.Run(name, func(t *testing.T) {
			stub := newStubUpstream(t, respond)
			handler := proxyFixture(t, mustProxyConfig(t, allOn(stub.URL)))
			before := proxyMetrics(t)
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, proxyCalls[2].request(t))
			if recorder.Code != http.StatusBadGateway || recorder.Body.String() != `{"detail":"Authentication service unavailable"}` {
				t.Fatalf("incomplete upstream answer = %d %.120q, want 502 and go-api's refusal", recorder.Code, recorder.Body.String())
			}
			after := proxyMetrics(t)
			series := proxySeries(AuthLocalLogin, "upstream_error")
			if delta := seriesValue(after, series) - seriesValue(before, series); delta != 1 {
				t.Errorf("%s moved by %v, want 1", series, delta)
			}
		})
	}
}

// An answer whose body stalls past the deadline is a timeout (504), the same
// refusal as a stall before the headers.
func TestAuthProxyBodyStallIsATimeout(t *testing.T) {
	stub := newStubUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"access_token":`)
		w.(http.Flusher).Flush()
		select {
		case <-time.After(5 * time.Second):
		case <-r.Context().Done():
		}
	})
	cfg := mustProxyConfig(t, allOn(stub.URL))
	cfg.Timeout = 200 * time.Millisecond
	handler := proxyFixture(t, cfg)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, proxyCalls[2].request(t))
	if recorder.Code != http.StatusGatewayTimeout || recorder.Body.String() != `{"detail":"Authentication service timed out"}` {
		t.Fatalf("body stall = %d %q, want 504 and the timeout refusal", recorder.Code, recorder.Body.String())
	}
}

// An upstream error status is auth-service's decision and passes through.
func TestAuthProxyPassesUpstreamRefusalsThrough(t *testing.T) {
	stub := newStubUpstream(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Retry-After", "7")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = io.WriteString(w, `{"detail": {"message": "locked", "retry_after_seconds": 7}}`)
	})
	handler := proxyFixture(t, mustProxyConfig(t, allOn(stub.URL)))
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, proxyCalls[2].request(t))
	if recorder.Code != http.StatusTooManyRequests || recorder.Body.String() != `{"detail": {"message": "locked", "retry_after_seconds": 7}}` ||
		recorder.Header().Get("Retry-After") != "7" {
		t.Fatalf("%d %v %s", recorder.Code, recorder.Header(), recorder.Body.String())
	}
}

// The proxy settings are read where go-api starts: a bad shape stops
// start-up with its own reason, and a good one forwards on a real socket.
func TestConfigureReadsTheAuthProxySettings(t *testing.T) {
	t.Setenv("ENVIRONMENT", "development")
	t.Setenv(AuthProxyFlagEnv(AuthLocalLogin), "true")
	t.Setenv(EnvAuthProxyURL, "http://auth-service:8095")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	components, err := configure(ctx, reachableDatabase(config.Config{APIAddress: "127.0.0.1:0"}), health.NewRegistry(time.Second), quietLogger())
	closeComponents(components)
	var coded interface{ DependencyReason() string }
	if !errors.As(err, &coded) || coded.DependencyReason() != "api_auth_proxy_config_invalid" {
		t.Fatalf("local login without sessions: err = %v, want reason api_auth_proxy_config_invalid", err)
	}

	stub := newStubUpstream(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "null")
	})
	t.Setenv(AuthProxyFlagEnv(AuthLocalLogin), "")
	t.Setenv(AuthProxyFlagEnv(AuthSessions), "true")
	t.Setenv(EnvAuthProxyURL, stub.URL)
	components, err = configure(ctx, reachableDatabase(config.Config{APIAddress: "127.0.0.1:0"}), health.NewRegistry(time.Second), quietLogger())
	if err != nil {
		t.Fatalf("configure: %v", err)
	}
	defer closeComponents(components)
	var api *httpapi.Server
	for _, component := range components {
		if server, ok := component.(*httpapi.Server); ok && server.Name() == "api-http" {
			api = server
		}
	}
	if api == nil {
		t.Fatal("no api listener")
	}
	if err := api.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = api.Shutdown(context.Background()) }()
	response, err := http.Post("http://"+api.Address()+"/api/v1/auth/logout", "application/json", strings.NewReader(`{"refresh_token": "r"}`))
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK || string(body) != "null" {
		t.Fatalf("logout through configure = %d %s, want the upstream answer", response.StatusCode, body)
	}
	seen := stub.last(t)
	if seen.path != "/api/v1/auth/logout" || seen.body != `{"refresh_token": "r"}` || seen.header.Get("X-Forwarded-For") != "127.0.0.1" {
		t.Fatalf("upstream saw %s %q XFF %q", seen.path, seen.body, seen.header.Get("X-Forwarded-For"))
	}
}

// go-api states its auth routing at start-up, at Info: each aggregate on or
// off and the upstream host, however the flags were set (helm or envFrom),
// and never a credential or query string.
func TestConfigureLogsTheAuthProxyRouting(t *testing.T) {
	t.Setenv("ENVIRONMENT", "development")
	for name, testCase := range map[string]struct {
		env  map[string]string
		host string
		on   map[string]bool
	}{
		"all off": {env: map[string]string{}, host: "", on: map[string]bool{}},
		"sessions and saml": {
			env:  map[string]string{EnvAuthProxyURL: "http://auth-service.internal:8095/", AuthProxyFlagEnv(AuthSessions): "true", AuthProxyFlagEnv(AuthSAML): "true"},
			host: "auth-service.internal:8095",
			on:   map[string]bool{string(AuthSessions): true, string(AuthSAML): true},
		},
	} {
		t.Run(name, func(t *testing.T) {
			for _, aggregate := range AuthAggregates {
				t.Setenv(AuthProxyFlagEnv(aggregate), testCase.env[AuthProxyFlagEnv(aggregate)])
			}
			t.Setenv(EnvAuthProxyURL, testCase.env[EnvAuthProxyURL])
			var logs bytes.Buffer
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			components, err := configure(ctx, reachableDatabase(config.Config{APIAddress: "127.0.0.1:0"}), health.NewRegistry(time.Second),
				slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelInfo})))
			closeComponents(components)
			if err != nil {
				t.Fatalf("configure: %v", err)
			}
			var record map[string]any
			for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
				var candidate map[string]any
				if json.Unmarshal([]byte(line), &candidate) == nil && candidate["msg"] == "auth proxy routing" {
					if record != nil {
						t.Fatal("the routing line was logged twice")
					}
					record = candidate
				}
			}
			if record == nil {
				t.Fatalf("no \"auth proxy routing\" line at Info:\n%.2000s", logs.String())
			}
			if record["level"] != "INFO" || record["upstream_host"] != testCase.host {
				t.Errorf("routing line level=%v upstream_host=%v, want INFO %q", record["level"], record["upstream_host"], testCase.host)
			}
			for _, aggregate := range AuthAggregates {
				if got := record[string(aggregate)]; got != testCase.on[string(aggregate)] {
					t.Errorf("routing line %s=%v, want %v", aggregate, got, testCase.on[string(aggregate)])
				}
			}
		})
	}
}

// The proxy's log lines carry no client-written text: the request id only in
// a loggable form (a client id can hold a credential), the route pattern, not
// the raw path, and auth-service's id only in its req_ wire form.
func TestAuthProxyLogsCarryNoClientWrittenText(t *testing.T) {
	const credential = "client-written-request-id"
	const upstreamJunk = "upstream-written-junk"
	const clientProvider = "client-written-provider"
	for name, upstreamID := range map[string]string{"wire-form id": "req_AAAAAAAAAAAAAAAAAAAAAAAAAA", "junk id": upstreamJunk} {
		t.Run(name, func(t *testing.T) {
			stub := newStubUpstream(t, func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("X-Auth-Request-ID", upstreamID)
				w.WriteHeader(http.StatusOK)
			})
			for upstream, wantMsg := range map[string]string{stub.URL: "auth proxy forwarded", "http://" + closedAddress(t): "auth proxy: upstream failed; refused"} {
				var logs bytes.Buffer
				server, err := NewServer(config.Config{APIAddress: "127.0.0.1:0", CORSAllowedOrigins: []string{proxyOrigin}}, quietLogger(),
					proxyFixtureRoutesLogging(t, mustProxyConfig(t, allOn(upstream)), slog.New(slog.NewJSONHandler(&logs, nil))))
				if err != nil {
					t.Fatal(err)
				}
				request := proxyCall{method: http.MethodPost, target: "/api/v1/auth/oidc/" + clientProvider + "/authorize", body: `{}`}.request(t)
				request.Header.Set("X-Request-ID", credential)
				server.Handler().ServeHTTP(httptest.NewRecorder(), request)
				text := logs.String()
				if !strings.Contains(text, wantMsg) {
					t.Fatalf("no %q line:\n%s", wantMsg, text)
				}
				for _, leaked := range []string{credential, clientProvider, upstreamJunk} {
					if strings.Contains(text, leaked) {
						t.Errorf("%s: the log carries client-written or unchecked text %q:\n%s", wantMsg, leaked, text)
					}
				}
				if !strings.Contains(text, "/api/v1/auth/oidc/{provider_id}/authorize") {
					t.Errorf("%s: the log does not name the route pattern:\n%s", wantMsg, text)
				}
				if upstream == stub.URL && upstreamID != upstreamJunk && !strings.Contains(text, upstreamID) {
					t.Errorf("the wire-form upstream id is not logged:\n%s", text)
				}
			}
		})
	}
}

// An error.v1 answer auth-service writes reaches the client as written: its
// request_id is the one the server minted, never empty, and never the
// X-Request-ID the client sent.
func TestAuthProxyKeepsTheServerRequestIDOfAForwardedError(t *testing.T) {
	const serverID = "req_SERVERMINTEDAAAAAAAAAAAAAA"
	const clientID = "client-written-request-id"
	upstreamBody := `{"error":{"code":"not_found","message":"Not Found","request_id":"` + serverID + `"}}`
	stub := newStubUpstream(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Auth-Request-ID", serverID)
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, upstreamBody)
	})
	handler := proxyFixture(t, mustProxyConfig(t, allOn(stub.URL)))
	for _, call := range proxyCalls {
		request := call.request(t)
		request.Header.Set("X-Request-ID", clientID)
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		if recorder.Code != http.StatusNotFound || recorder.Body.String() != upstreamBody {
			t.Errorf("%s %s = %d %s, want auth-service's 404 as written", call.method, call.target, recorder.Code, recorder.Body.String())
			continue
		}
		var envelope struct {
			Error struct {
				RequestID string `json:"request_id"`
			} `json:"error"`
		}
		if err := json.Unmarshal(recorder.Body.Bytes(), &envelope); err != nil {
			t.Fatalf("%s: the forwarded error is not JSON: %v", call.target, err)
		}
		if envelope.Error.RequestID == "" || envelope.Error.RequestID != serverID {
			t.Errorf("%s: error.request_id = %q, want the server's %q", call.target, envelope.Error.RequestID, serverID)
		}
		if strings.Contains(recorder.Body.String(), clientID) {
			t.Errorf("%s: the client's X-Request-ID is in the forwarded error: %s", call.target, recorder.Body.String())
		}
	}
}

// The acp corpus leg reads which routes make up each aggregate from
// ci/auth_corpus_acp_routes.tsv; it must be this file's proxy map.
func TestAuthCorpusAcpRoutesAreTheProxyMap(t *testing.T) {
	raw, err := os.ReadFile("../../ci/auth_corpus_acp_routes.tsv")
	if err != nil {
		t.Fatal(err)
	}
	listed := map[string]AuthAggregate{}
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		if strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Split(line, "\t")
		if len(fields) != 3 {
			t.Fatalf("not aggregate<TAB>method<TAB>pattern: %q", line)
		}
		key := fields[1] + " " + fields[2]
		if _, dup := listed[key]; dup {
			t.Errorf("%s is listed twice", key)
		}
		listed[key] = AuthAggregate(fields[0])
	}
	want := map[string]AuthAggregate{}
	for _, routes := range []map[string]AuthAggregate{sessionProxyRoutes, routeProxyRoutes} {
		for key, aggregate := range routes {
			want[key] = aggregate
		}
	}
	if len(listed) != len(want) {
		t.Errorf("the corpus lists %d routes, the proxy maps %d", len(listed), len(want))
	}
	for key, aggregate := range want {
		if listed[key] != aggregate {
			t.Errorf("%s: the corpus lists aggregate %q, the proxy maps %q", key, listed[key], aggregate)
		}
	}
}
