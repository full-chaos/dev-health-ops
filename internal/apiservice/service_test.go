package apiservice

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestWriteErrorRendersEveryCodeInThePythonShape(t *testing.T) {
	cases := map[httpapi.Code]struct {
		status int
		body   string
	}{
		httpapi.CodeNotFound:         {404, `{"detail":"Not Found"}`},
		httpapi.CodeMethodNotAllowed: {405, `{"detail":"Method Not Allowed"}`},
		httpapi.CodePayloadTooLarge:  {413, `{"detail":"Request Entity Too Large"}`},
		httpapi.CodeInvalidRequest:   {400, `{"detail":"Bad Request"}`},
		httpapi.CodeRateLimited:      {429, `{"detail":{"message":"Rate limit exceeded. Please try again later."}}`},
		httpapi.CodeInternal:         {500, `{"detail":"Internal Server Error"}`},
		httpapi.Code("not_a_code"):   {500, `{"detail":"Internal Server Error"}`},
	}
	for code, want := range cases {
		recorder := httptest.NewRecorder()
		WriteError(recorder, httptest.NewRequest(http.MethodGet, "/", nil), code)
		if recorder.Code != want.status || recorder.Body.String() != want.body {
			t.Errorf("%s: %d %s, want %d %s", code, recorder.Code, recorder.Body.String(), want.status, want.body)
		}
		if got := recorder.Header().Get("Content-Type"); got != "application/json" {
			t.Errorf("%s: content type %q", code, got)
		}
		if got := recorder.Header().Get("Content-Length"); got != itoaTest(len(want.body)) {
			t.Errorf("%s: content length %q", code, got)
		}
	}
	// Every code the transport can emit has a Python mapping.
	for _, code := range []httpapi.Code{httpapi.CodeNotFound, httpapi.CodeMethodNotAllowed, httpapi.CodePayloadTooLarge,
		httpapi.CodeRateLimited, httpapi.CodeInvalidRequest, httpapi.CodeInternal} {
		if _, mapped := pythonErrors[code]; !mapped {
			t.Errorf("%s has no Python mapping", code)
		}
	}
}

func itoaTest(n int) string { b, _ := json.Marshal(n); return string(b) }

func TestSecurityHeadersAddOnlyWhatIsMissing(t *testing.T) {
	cases := map[string]struct {
		handler http.HandlerFunc
		check   func(t *testing.T, header http.Header)
	}{
		"handler writes nothing": {
			handler: func(http.ResponseWriter, *http.Request) {},
			check: func(t *testing.T, header http.Header) {
				for _, pair := range securityHeaders {
					if header.Get(pair[0]) != pair[1] {
						t.Errorf("%s = %q", pair[0], header.Get(pair[0]))
					}
				}
			},
		},
		"handler writes a body without a status": {
			handler: func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("x")) },
			check: func(t *testing.T, header http.Header) {
				if header.Get("X-Frame-Options") != "DENY" {
					t.Error("header missing after an implicit 200")
				}
			},
		},
		"handler sets its own value": {
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("X-Frame-Options", "SAMEORIGIN")
				w.WriteHeader(http.StatusTeapot)
			},
			check: func(t *testing.T, header http.Header) {
				if got := header.Values("X-Frame-Options"); len(got) != 1 || got[0] != "SAMEORIGIN" {
					t.Errorf("handler value overridden: %q", got)
				}
			},
		},
		"handler flushes first": {
			handler: func(w http.ResponseWriter, _ *http.Request) { w.(http.Flusher).Flush() },
			check: func(t *testing.T, header http.Header) {
				if header.Get("Content-Security-Policy") == "" {
					t.Error("header missing after a flush")
				}
			},
		},
	}
	for name, test := range cases {
		recorder := httptest.NewRecorder()
		SecurityHeaders(test.handler).ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/", nil))
		// Result().Header is the header set as it was when the status line was
		// written, so a header added after that point does not count.
		t.Run(name, func(t *testing.T) { test.check(t, recorder.Result().Header) })
	}
}

func TestHeaderWriterUnwrapsAndRefusesHijackWhenUnsupported(t *testing.T) {
	recorder := httptest.NewRecorder()
	writer := &headerWriter{ResponseWriter: recorder, commit: func(http.Header) {}}
	if writer.Unwrap() != recorder {
		t.Fatal("Unwrap must return the wrapped writer")
	}
	if _, _, err := writer.Hijack(); err == nil {
		t.Fatal("Hijack on a non-hijacker must fail")
	}
}

func TestNewServerRequiresAnAddress(t *testing.T) {
	if _, err := NewServer(config.Config{}, quietLogger(), nil); err == nil {
		t.Fatal("an empty api address must be refused")
	}
}

// TestConfigureRegistersTheListenerReadinessCheck exercises `dho api` with
// no APIDatabaseURI/ValkeyURI configured (the pre-bootstrap shape the
// w1-route-lanes brief documents as legal: "PRs can merge with goApi off",
// and the exact shape the go-container-smoke CI job runs `dho api` under --
// it passes no database or Valkey configuration at all and asserts /readyz
// still reaches 200). buildDeps registers api_database/api_valkey ONLY when
// each URI is configured (matching CHAOS-6244's original acr wiring, where
// RegisterRequired lived inside the same `if …Configured()` block); with
// neither configured, the listener check is the only one, and readiness
// follows the listener alone.
func TestConfigureRegistersTheListenerReadinessCheck(t *testing.T) {
	registry := health.NewRegistry(time.Second)
	components, err := configure(context.Background(), config.Config{APIAddress: "127.0.0.1:0"}, registry, quietLogger())
	if err != nil {
		t.Fatalf("configure: %v", err)
	}
	if len(components) != 1 {
		t.Fatalf("%d components", len(components))
	}
	if registry.RequiredCount() != 1 {
		t.Fatalf("%d required checks, want only the listener (no database/valkey configured)", registry.RequiredCount())
	}
	if ready := registry.CheckRequired(context.Background()); ready.Ready {
		t.Fatal("ready before the listener is bound")
	}
	server := components[0].(*httpapi.Server)
	if err := server.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = server.Shutdown(context.Background()) }()
	// Neither dependency is configured, so nothing else can hold readiness
	// back -- once the listener binds, the process is ready.
	if ready := registry.CheckRequired(context.Background()); !ready.Ready {
		t.Fatalf("not ready once the listener is bound, with no database/valkey configured: %+v", ready)
	}
	if server.Name() != "api-http" {
		t.Fatalf("component name %q", server.Name())
	}

	// The bound listener serves the Python-shaped 404 with the transport
	// headers, end to end over TCP.
	response, err := http.Get("http://" + server.Address() + "/api/v1/anything")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)
	if response.StatusCode != http.StatusNotFound || string(body) != `{"detail":"Not Found"}` {
		t.Fatalf("%d %s", response.StatusCode, body)
	}
	if response.Header.Get("X-Request-ID") == "" || response.Header.Get("X-Frame-Options") != "DENY" {
		t.Fatalf("transport headers missing: %v", response.Header)
	}
}

// TestConfigureRegistersTheDatabaseCheckOnlyWhenConfigured proves the other
// half of the buildDeps contract: when APIDatabaseURI IS configured, the
// api_database readiness check IS registered (RequiredCount grows to 2,
// listener + api_database), even though the address is unreachable here --
// postgres.New pools lazily (no dial at construction, per its own doc
// comment), so configure() itself must not fail or block on an unreachable
// database; only the registered check reports it.
func TestConfigureRegistersTheDatabaseCheckOnlyWhenConfigured(t *testing.T) {
	registry := health.NewRegistry(time.Second)
	cfg := config.Config{
		APIAddress:     "127.0.0.1:0",
		APIDatabaseURI: secrets.NewValue("postgres://user:pass@127.0.0.1:1/nonexistent"),
	}
	components, err := configure(context.Background(), cfg, registry, quietLogger())
	if err != nil {
		t.Fatalf("configure must not fail on an unreachable-but-configured database: %v", err)
	}
	if registry.RequiredCount() != 2 {
		t.Fatalf("%d required checks, want listener + api_database", registry.RequiredCount())
	}
	if len(components) != 2 {
		t.Fatalf("%d components, want the pgx pool component + the server", len(components))
	}
	server := components[len(components)-1].(*httpapi.Server)
	if err := server.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = server.Shutdown(context.Background()) }()
	if ready := registry.CheckRequired(context.Background()); ready.Ready {
		t.Fatalf("ready against an unreachable configured database: %+v", ready)
	}
}

// TestRoutesMountsEveryArea proves both business-route areas (acr,
// CHAOS-6244; external-ingest, CHAOS-6246) are always on the mux,
// regardless of whether Deps is live: with a nil Pool/Valkey the acr health
// path still answers and its entitlement path answers 503 rather than
// being absent (see acr.Deps's doc comment for why omitting the route
// entirely would be the wrong failure mode -- a silent 404 reads as "no
// such route", not "not ready"), and external-ingest's routes register
// unconditionally too (a handler that needs a live dependency answers
// CodeInternal at request time instead).
func TestRoutesMountsEveryArea(t *testing.T) {
	routes := Routes(Deps{}, nil)
	want := map[string]bool{
		"GET /api/v1/internal/acr/health":                      false,
		"GET /api/v1/internal/acr/entitlements/{org_id}":       false,
		"GET /api/v1/external-ingest/schemas":                  false,
		"GET /api/v1/external-ingest/schemas/{schema_version}": false,
		"GET /api/v1/external-ingest/availability":             false,
		"POST /api/v1/external-ingest/validate":                false,
		"POST /api/v1/external-ingest/batches":                 false,
		"GET /api/v1/external-ingest/batches":                  false,
		"GET /api/v1/external-ingest/batches/{ingestion_id}":   false,
	}
	if len(routes) != len(want) {
		t.Fatalf("route count = %d, want %d: %+v", len(routes), len(want), routes)
	}
	for _, route := range routes {
		key := route.Method + " " + route.Pattern
		if _, ok := want[key]; !ok {
			t.Fatalf("unexpected route %q", key)
		}
		want[key] = true
		if route.Handler == nil {
			t.Fatalf("route %q has no handler", key)
		}
	}
	for key, seen := range want {
		if !seen {
			t.Fatalf("route %q missing", key)
		}
	}
}

// TestCommandRunsTheServiceThroughTheShell exercises the real `dho api`
// command for every outcome that stops before serving.
func TestCommandRunsTheServiceThroughTheShell(t *testing.T) {
	command := Command()
	if command.Kind != cli.Service || command.Name != "api" {
		t.Fatalf("%+v", command)
	}
	cases := []struct {
		name string
		args []string
		env  map[string]string
		code int
		out  string
	}{
		{"help", []string{"--help"}, nil, 0, "--api-addr"},
		{"version", []string{"--version"}, nil, 0, `"service":"dev-health-api"`},
		{"unknown flag", []string{"--nope"}, nil, 2, "run dho api --help"},
		{"positional", []string{"extra"}, nil, 2, "positional arguments are not accepted"},
		{"bad api address", []string{"--api-addr=nohostport"}, nil, 1, "DEV_HEALTH_API_ADDR"},
		{"same address as the operator listener", []string{"--api-addr=:9000", "--http-addr=:9000"}, nil, 1, "must differ"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := command.Run(context.Background(), cli.Env{
				Args:   test.args,
				Lookup: func(key string) (string, bool) { value, ok := test.env[key]; return value, ok },
				Stdout: &stdout,
				Stderr: &stderr,
			})
			if code != test.code {
				t.Fatalf("exit %d, want %d (stdout %q stderr %q)", code, test.code, stdout.String(), stderr.String())
			}
			if !strings.Contains(stdout.String()+stderr.String(), test.out) {
				t.Fatalf("output lacks %q: %q %q", test.out, stdout.String(), stderr.String())
			}
		})
	}
}

// TestCORSJoinsAnExistingVary pins Starlette's MutableHeaders.add_vary_header:
// a Vary the handler already set is kept and "Origin" is appended to it.
func TestCORSJoinsAnExistingVary(t *testing.T) {
	for _, origins := range [][]string{{"https://a.example"}, {"*"}} {
		handler := NewCORS(origins).Wrap(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Vary", "Accept-Encoding")
			w.WriteHeader(http.StatusOK)
		}))
		request := httptest.NewRequest(http.MethodGet, "/", nil)
		request.Header.Set("Origin", "https://a.example")
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		if got := recorder.Result().Header.Values("Vary"); len(got) != 1 || got[0] != "Accept-Encoding, Origin" {
			t.Fatalf("origins %v: Vary %q, want [Accept-Encoding, Origin]", origins, got)
		}
	}
}

// apiTestConfig is a configuration with the api database and signing key
// set. The DSN points at a closed local port and the key is a fixture built
// at runtime.
func apiTestConfig() config.Config {
	return config.Config{
		APIAddress:          "127.0.0.1:0",
		APIDatabaseURI:      secrets.NewValue("postgresql://devhealth_api@127.0.0.1:1/devhealth?connect_timeout=1"),
		APIDatabaseRole:     "devhealth_api",
		RiverDatabaseSchema: "river",
		APIJWTSecret:        secrets.NewValue(strings.Repeat("fixture-key-", 3)),
		APIJWTIssuer:        "dev-health-ops",
		APIJWTAudience:      "dev-health-api",
	}
}

// TestConfigureRefusesADatabaseWithoutAUsableSigningKey: with a database the
// api authenticates callers, so JWT_SECRET_KEY must be present and at least
// 32 characters; the error names the key and never carries configuration.
func TestConfigureRefusesADatabaseWithoutAUsableSigningKey(t *testing.T) {
	cases := map[string]struct {
		mutate func(*config.Config)
		names  string
	}{
		"no signing key": {func(cfg *config.Config) { cfg.APIJWTSecret = secrets.Value{} }, "JWT_SECRET_KEY"},
		"short key":      {func(cfg *config.Config) { cfg.APIJWTSecret = secrets.NewValue("short") }, "32 characters"},
	}
	for name, test := range cases {
		t.Run(name, func(t *testing.T) {
			cfg := apiTestConfig()
			test.mutate(&cfg)
			components, err := configure(context.Background(), cfg, health.NewRegistry(time.Second), quietLogger())
			if err == nil {
				t.Fatalf("configure accepted it: %d components", len(components))
			}
			if !strings.Contains(err.Error(), test.names) {
				t.Fatalf("error %q does not name %q", err, test.names)
			}
			if strings.Contains(err.Error(), "fixture-key") || strings.Contains(err.Error(), "127.0.0.1:1") {
				t.Fatalf("error leaks configuration: %v", err)
			}
		})
	}
	// Without a database no caller is authenticated, so no key is needed.
	cfg := apiTestConfig()
	cfg.APIDatabaseURI, cfg.APIJWTSecret = secrets.Value{}, secrets.Value{}
	if _, err := configure(context.Background(), cfg, health.NewRegistry(time.Second), quietLogger()); err != nil {
		t.Fatalf("transport-only configure: %v", err)
	}
}

// TestConfigureInstallsTheScopeMiddlewaresWithADatabase proves the wiring:
// a request carrying a valid token for a caller the (unreachable) database
// cannot confirm fails in the org scope (500), where a server without the
// scope would answer 404.
func TestConfigureInstallsTheScopeMiddlewaresWithADatabase(t *testing.T) {
	cfg := apiTestConfig()
	components, err := configure(context.Background(), cfg, health.NewRegistry(time.Second), quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		for index := len(components) - 1; index >= 0; index-- {
			_ = components[index].Shutdown(context.Background())
		}
	}()
	var server *httpapi.Server
	for _, component := range components {
		if candidate, ok := component.(*httpapi.Server); ok {
			server = candidate
		}
	}
	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub": uuid.NewString(), "type": "access", "exp": time.Now().Add(time.Hour).Unix(),
	}).SignedString([]byte(cfg.APIJWTSecret.Reveal()))
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet, "/api/v1/anything", nil)
	request.Header.Set("Authorization", "Bearer "+token)
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("%d %s, want the org scope's 500 for an unreachable database", recorder.Code, recorder.Body.String())
	}
}

// scopeStore is a policy.Store with one superuser who is impersonating, and
// no memberships.
type scopeStore struct{ admin, target, targetOrg uuid.UUID }

func (s scopeStore) UserState(_ context.Context, id uuid.UUID) (policy.UserState, bool, error) {
	return policy.UserState{IsActive: true, IsSuperuser: id == s.admin}, true, nil
}
func (scopeStore) IsMember(context.Context, uuid.UUID, uuid.UUID) (bool, error) { return false, nil }
func (s scopeStore) ActiveImpersonation(_ context.Context, admin uuid.UUID) (*policy.Impersonation, error) {
	if admin != s.admin {
		return nil, nil
	}
	return &policy.Impersonation{AdminUserID: s.admin, TargetUserID: s.target, TargetOrgID: s.targetOrg}, nil
}

// TestServerRunsTheScopeMiddlewaresOutsideSecurityHeadersAndCORS pins the
// Python request order on the real server: OrgIdMiddleware's 403 carries no
// security or CORS header (they are inner), while an impersonated request's
// response carries the impersonation headers and the security headers.
func TestServerRunsTheScopeMiddlewaresOutsideSecurityHeadersAndCORS(t *testing.T) {
	key := strings.Repeat("fixture-key-", 3)
	verifier, err := edgetoken.New(key, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	store := scopeStore{admin: uuid.New(), target: uuid.New(), targetOrg: uuid.New()}
	auth, err := policy.NewAuthenticator(verifier, store, quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	scope := policy.NewScope(auth, quietLogger())
	cfg := apiTestConfig()
	cfg.CORSAllowedOrigins = []string{"https://app.example"}
	server, err := NewServer(cfg, quietLogger(), nil, scope.OrgScope, scope.Impersonation)
	if err != nil {
		t.Fatal(err)
	}
	sign := func(sub uuid.UUID, superuser bool) string {
		token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
			"sub": sub.String(), "type": "access", "org_id": uuid.NewString(), "is_superuser": superuser,
			"exp": time.Now().Add(time.Hour).Unix(),
		}).SignedString([]byte(key))
		if err != nil {
			t.Fatal(err)
		}
		return token
	}

	request := httptest.NewRequest(http.MethodGet, "/api/v1/anything", nil)
	request.Header.Set("Authorization", "Bearer "+sign(uuid.New(), false))
	request.Header.Set("X-Org-Id", uuid.NewString())
	request.Header.Set("Origin", "https://app.example")
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusForbidden || recorder.Body.String() != `{"detail": "X-Org-Id not permitted for this user"}` {
		t.Fatalf("%d %s", recorder.Code, recorder.Body.String())
	}
	if recorder.Header().Get("X-Frame-Options") != "" || recorder.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Fatalf("the org scope 403 carries inner headers: %v", recorder.Header())
	}
	if recorder.Header().Get("X-Request-ID") == "" {
		t.Fatal("the org scope 403 lacks the correlation id (outer)")
	}

	request = httptest.NewRequest(http.MethodGet, "/api/v1/anything", nil)
	request.Header.Set("Authorization", "Bearer "+sign(store.admin, true))
	recorder = httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusNotFound || recorder.Header().Get("X-Impersonating") != "true" ||
		recorder.Header().Get("X-Impersonated-User-Id") != store.target.String() ||
		recorder.Header().Get("X-Frame-Options") != "DENY" {
		t.Fatalf("%d %v", recorder.Code, recorder.Header())
	}
}
