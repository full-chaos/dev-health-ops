package authruntime

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/clientaddr"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/auth/workloadexchange"
)

const (
	internalAddressEnv   = "AUTH_SERVICE_INTERNAL_HTTP_ADDR"
	internalListenerName = "auth-internal-http"
	apiListenerName      = "auth-api-http"
)

// pepRoutes are the only routes the internal listener serves.
var pepRoutes = []string{
	http.MethodPost + " " + workloadexchange.Pattern,
	http.MethodPost + " " + authapi.PatternAuthorize,
	http.MethodPost + " " + authapi.PatternAuthorizeBatch,
}

// listenerHandlers returns the handler of each httpapi listener configure built, by component name.
func listenerHandlers(t *testing.T, cfg authconfig.Config, logger *slog.Logger, extra []httpapi.Route) map[string]http.Handler {
	t.Helper()
	components, err := configureWithRoutes(context.Background(), cfg, logger, extra)
	if err != nil {
		t.Fatalf("configure: %v", err)
	}
	t.Cleanup(func() {
		for _, component := range components {
			_ = component.Shutdown(context.Background())
		}
	})
	handlers := map[string]http.Handler{}
	for _, component := range components {
		if announced, ok := component.(announcedListener); ok {
			if server, ok := announced.listener.(*httpapi.Server); ok {
				if _, dup := handlers[server.Name()]; dup {
					t.Fatalf("two listeners named %s", server.Name())
				}
				handlers[server.Name()] = server.Handler()
			}
		}
	}
	return handlers
}

func loadInternalConfig(t *testing.T, env map[string]string) authconfig.Config {
	t.Helper()
	cfg, err := authconfig.Load(authconfig.Spec{LookupEnv: lookupFrom(env)})
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	return cfg
}

// fullInternalEnv is the route probe's configuration (every route group mounted, the exchange on) with the internal
// listener on and one listed authorize caller.
func fullInternalEnv(t *testing.T, caller string) map[string]string {
	t.Helper()
	env := map[string]string{
		authconfig.EnvDatabaseURI:        "postgres://auth@127.0.0.1:1/devhealth",
		authconfig.EnvSigningKeyFile:     "/nonexistent",
		authconfig.EnvSigningKeyID:       "internal-test",
		authconfig.EnvRateLimitPerSecond: "100000",
		authconfig.EnvRateLimitBurst:     "100000",
		authconfig.EnvAPIAddress:         "127.0.0.1:0",
		authconfig.EnvOperatorAddress:    "127.0.0.1:1",
		authconfig.EnvSocialLinkRoutes:   "true",
		authconfig.EnvUsersOwned:         "true",
		authconfig.EnvTokenIssuerURL:     wellKnownIssuer,
		internalAddressEnv:               "127.0.0.1:0",
	}
	if caller != "" {
		env[authconfig.EnvAuthorizeCallerSubjects] = caller
	}
	exchangeEnvironment(t, env)
	// The go-api workload subject mounts the session issue routes on the API listener, as servedAuthRoutes does.
	workloadIssueEnvironment(env)
	return provisioningEnvironment(ssoEnvironment(compatEnvironment(env)))
}

func mustListener(t *testing.T, handlers map[string]http.Handler, name string) http.Handler {
	t.Helper()
	handler := handlers[name]
	if handler == nil {
		names := make([]string, 0, len(handlers))
		for n := range handlers {
			names = append(names, n)
		}
		sort.Strings(names)
		t.Fatalf("configure built no %s listener (listeners %v)", name, names)
	}
	return handler
}

func probe(handler http.Handler, method, path string) int {
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(method, path, strings.NewReader("{}")))
	return recorder.Code
}

// The internal listener serves the exchange and the two authorize routes and nothing else: every other route the API
// listener serves, the operator paths and an unknown path are 404 there, and a PEP path on another method is 405.
func TestTheInternalListenerServesOnlyThePEPRoutes(t *testing.T) {
	cfg := loadInternalConfig(t, fullInternalEnv(t, session.WirePrincipalID(uuid.New())))
	internal := mustListener(t, listenerHandlers(t, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), nil), internalListenerName)

	pep := map[string]bool{}
	for _, route := range pepRoutes {
		pep[route] = true
		method, path, _ := strings.Cut(route, " ")
		if code := probe(internal, method, path); code == http.StatusNotFound || code == http.StatusMethodNotAllowed || code == http.StatusTooManyRequests {
			t.Errorf("%s answered %d on the internal listener, want the route's own answer", route, code)
		}
	}
	paths := map[string]bool{"/": true, "/healthz": true, "/readyz": true, "/metrics": true, "/v1/nothing-here": true}
	for route := range servedAuthRoutes(t) {
		methodPath, _, _ := strings.Cut(route, "@")
		_, path, _ := strings.Cut(methodPath, " ")
		paths[path] = true
	}
	served := 0
	for path := range paths {
		for _, method := range probeMethods {
			code := probe(internal, method, path)
			switch {
			case pep[method+" "+path]:
				served++
			case code == http.StatusMethodNotAllowed && (pep[http.MethodPost+" "+path]):
			case code != http.StatusNotFound:
				t.Errorf("%s %s answered %d on the internal listener, want 404", method, path, code)
			}
		}
	}
	if served != len(pepRoutes) {
		t.Fatalf("the probe reached %d PEP routes on the internal listener, want %d", served, len(pepRoutes))
	}
}

// The main API listener serves exactly the routes it serves with the internal listener off.
func TestTheMainListenerIsUnchangedByTheInternalListener(t *testing.T) {
	cfg := loadInternalConfig(t, fullInternalEnv(t, ""))
	handlers := listenerHandlers(t, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	mustListener(t, handlers, internalListenerName)
	api := mustListener(t, handlers, apiListenerName)
	if len(handlers) != 2 {
		t.Fatalf("configure built %d HTTP API listeners, want the API and the internal one", len(handlers))
	}
	served := servedAuthRoutes(t)
	for route := range served {
		listener := route[strings.LastIndex(route, "@")+1:]
		if listener != "api" {
			continue
		}
		method, path, _ := strings.Cut(strings.TrimSuffix(route, "@api"), " ")
		if code := probe(api, method, path); code == http.StatusNotFound || code == http.StatusMethodNotAllowed {
			t.Errorf("%s %s answered %d on the API listener with the internal listener on", method, path, code)
		}
	}
}

// clientAddressProbe answers the client address the listener bound to the request.
func clientAddressProbe() httpapi.Route {
	return httpapi.Route{Method: http.MethodPost, Pattern: authapi.PatternAuthorize, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if text := clientaddr.Text(r.Context()); text != nil {
			_, _ = io.WriteString(w, *text)
		}
	})}
}

// A peer the API listener trusts as a proxy gets no such trust on the internal listener: X-Forwarded-For is not read
// there, the client address is the TCP peer, and its X-Request-ID is not logged.
func TestTheInternalListenerKeepsTheTCPPeerWhenXForwardedForIsSent(t *testing.T) {
	withAuthorizeRows(t, []byte(`{"rows": []}`))
	cfg := loadInternalConfig(t, map[string]string{
		authconfig.EnvDatabaseURI:        "postgres://auth@127.0.0.1:1/devhealth",
		authconfig.EnvSigningKeyFile:     "/nonexistent",
		authconfig.EnvSigningKeyID:       "internal-test",
		authconfig.EnvRateLimitPerSecond: "100000",
		authconfig.EnvRateLimitBurst:     "100000",
		authconfig.EnvAPIAddress:         "127.0.0.1:0",
		authconfig.EnvOperatorAddress:    "127.0.0.1:1",
		authconfig.EnvTrustedProxies:     "192.0.2.10",
		internalAddressEnv:               "127.0.0.1:0",
	})
	logs := &safeBuffer{}
	handlers := listenerHandlers(t, cfg, slog.New(slog.NewJSONHandler(logs, nil)), []httpapi.Route{clientAddressProbe()})
	send := func(handler http.Handler) string {
		request := httptest.NewRequest(http.MethodPost, authapi.PatternAuthorize, strings.NewReader("{}"))
		request.RemoteAddr = "192.0.2.10:4321"
		request.Header.Set("X-Forwarded-For", "203.0.113.9")
		request.Header.Set(httpapi.RequestIDHeader, "0123456789abcdef0123456789abcdef")
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		return recorder.Body.String()
	}
	if got := send(mustListener(t, handlers, apiListenerName)); got != "203.0.113.9" {
		t.Fatalf("the API listener bound %q, want the forwarded address from its trusted proxy", got)
	}
	if got := send(mustListener(t, handlers, internalListenerName)); got != "192.0.2.10" {
		t.Fatalf("the internal listener bound %q, want the TCP peer 192.0.2.10", got)
	}
	var lines int
	for _, raw := range bytes.Split([]byte(logs.String()), []byte("\n")) {
		var line map[string]any
		if json.Unmarshal(raw, &line) != nil || line["msg"] != "http request" {
			continue
		}
		lines++
		switch line["listener"] {
		case "internal":
			if line["caller_request_id"] != "untrusted" {
				t.Errorf("internal access line caller_request_id = %v, want untrusted", line["caller_request_id"])
			}
		case apiListenerName:
			if line["caller_request_id"] != "0123456789abcdef0123456789abcdef" {
				t.Errorf("API access line caller_request_id = %v, want the trusted proxy's id", line["caller_request_id"])
			}
		default:
			t.Errorf("access line from listener %v", line["listener"])
		}
	}
	if lines != 2 {
		t.Fatalf("%d access lines, want one per listener", lines)
	}
}

func unsignedBearer(header, payload string) string {
	enc := base64.RawURLEncoding
	return enc.EncodeToString([]byte(header)) + "." + enc.EncodeToString([]byte(payload)) + "." + enc.EncodeToString(make([]byte, 64))
}

// On the internal listener the authorize routes identify the caller by its workload token alone: no bearer, a token
// of another class, a wrong audience and an unlisted subject are each refused with 401 invalid_credentials.
func TestTheInternalListenerRefusesANonWorkloadOrUnlistedCaller(t *testing.T) {
	listed := session.WirePrincipalID(uuid.New())
	cfg := loadInternalConfig(t, fullInternalEnv(t, listed))
	internal := mustListener(t, listenerHandlers(t, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), nil), internalListenerName)
	claims := func(aud, sub string) string {
		raw, _ := json.Marshal(map[string]any{
			"iss": wellKnownIssuer, "aud": aud, "sub": sub, "principal_type": "workload", "jti": uuid.NewString(),
			"iat": 1, "nbf": 1, "exp": 4102444800,
		})
		return string(raw)
	}
	bearers := map[string]string{
		"no bearer":         "",
		"access token":      unsignedBearer(`{"alg":"EdDSA","typ":"at+jwt","kid":"k"}`, claims("auth-service", listed)),
		"wrong audience":    unsignedBearer(`{"alg":"EdDSA","typ":"workload+jwt","kid":"k"}`, claims("dev-health-api", listed)),
		"unlisted subject":  unsignedBearer(`{"alg":"EdDSA","typ":"workload+jwt","kid":"k"}`, claims("auth-service", session.WirePrincipalID(uuid.New()))),
		"not a jwt at all":  "opaque-not-a-token",
		"forwarded address": unsignedBearer(`{"alg":"EdDSA","typ":"workload+jwt","kid":"k"}`, claims("auth-service", session.WirePrincipalID(uuid.New()))),
	}
	for name, bearer := range bearers {
		for _, path := range []string{authapi.PatternAuthorize, authapi.PatternAuthorizeBatch} {
			request := httptest.NewRequest(http.MethodPost, path, strings.NewReader("{}"))
			request.RemoteAddr = "127.0.0.1:4321"
			if bearer != "" {
				request.Header.Set("Authorization", "Bearer "+bearer)
			}
			if name == "forwarded address" {
				request.Header.Set("X-Forwarded-For", "10.0.0.1")
			}
			recorder := httptest.NewRecorder()
			internal.ServeHTTP(recorder, request)
			if recorder.Code != http.StatusUnauthorized || !strings.Contains(recorder.Body.String(), "invalid_credentials") {
				t.Errorf("%s on %s: %d %q, want 401 invalid_credentials", name, path, recorder.Code, recorder.Body.String())
			}
		}
	}
}

const injectedInternalOnly = http.MethodPost + " /internal/v1/test-only/get"

func injectInternalOnly(t *testing.T, keys ...string) {
	t.Helper()
	saved := internalOnlyRoutes
	internalOnlyRoutes = map[string]bool{}
	for _, key := range keys {
		internalOnlyRoutes[key] = true
	}
	t.Cleanup(func() { internalOnlyRoutes = saved })
}

func internalOnlyProbeRoute() httpapi.Route {
	return httpapi.Route{Method: http.MethodPost, Pattern: "/internal/v1/test-only/get", Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	})}
}

// A route keyed in internalOnlyRoutes is served by the internal listener and is 404 on the public API listener; the
// three PEP routes stay on both.
func TestAnInternalOnlyRouteIsServedInternallyAndNotPublicly(t *testing.T) {
	injectInternalOnly(t, injectedInternalOnly)
	cfg := loadInternalConfig(t, fullInternalEnv(t, session.WirePrincipalID(uuid.New())))
	handlers := listenerHandlers(t, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), []httpapi.Route{internalOnlyProbeRoute()})
	internal := mustListener(t, handlers, internalListenerName)
	api := mustListener(t, handlers, apiListenerName)
	if code := probe(internal, http.MethodPost, "/internal/v1/test-only/get"); code != http.StatusTeapot {
		t.Errorf("internal listener answered %d for the internal-only route, want %d", code, http.StatusTeapot)
	}
	if code := probe(api, http.MethodPost, "/internal/v1/test-only/get"); code != http.StatusNotFound {
		t.Errorf("public listener answered %d for the internal-only route, want 404", code)
	}
	for _, route := range pepRoutes {
		method, path, _ := strings.Cut(route, " ")
		for name, handler := range map[string]http.Handler{internalListenerName: internal, apiListenerName: api} {
			if code := probe(handler, method, path); code == http.StatusNotFound || code == http.StatusMethodNotAllowed {
				t.Errorf("%s answered %d on the %s listener, want the route's own answer", route, code, name)
			}
		}
	}
}

// A key in internalOnlyRoutes that is no registered route stops startup: no silent dead key.
func TestAnInternalOnlyKeyWithNoRouteFailsStartup(t *testing.T) {
	injectInternalOnly(t, injectedInternalOnly)
	cfg := loadInternalConfig(t, fullInternalEnv(t, ""))
	components, err := configureWithRoutes(context.Background(), cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	if err == nil {
		for _, component := range components {
			_ = component.Shutdown(context.Background())
		}
		t.Fatal("configure accepted an internalOnlyRoutes key that is not a registered route")
	}
	if !strings.Contains(err.Error(), injectedInternalOnly) {
		t.Errorf("startup error %q does not name the dead key", err)
	}
}
