package authruntime

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

// authorizeInternalExclusions names each authorize route that is deliberately not served on the internal listener,
// with the reason. Empty: every authorize route is a PEP call and is served there.
var authorizeInternalExclusions = map[string]string{}

// derivedAuthorizeRoutes is every route AuthorizeRoutes can mount, derived from the code, never typed: with no
// endpoint-profile row it logs each route it leaves unmounted; the routes are then mounted from a document holding a
// workload_token row for each of them.
func derivedAuthorizeRoutes(t *testing.T) []string {
	t.Helper()
	pool, schema, signer := authorizeTestDeps(t)
	cfg := authconfig.Config{TokenIssuerURL: "https://auth.example.com", AuthorizeCallerSubjects: []string{session.WirePrincipalID(uuid.New())}}
	var logs bytes.Buffer
	withAuthorizeRows(t, []byte(`{"rows": []}`))
	if _, err := authorizeRoutes(cfg, slog.New(slog.NewJSONHandler(&logs, nil)), pool, schema, signer, nil); err != nil {
		t.Fatal(err)
	}
	var patterns []string
	for line := range strings.SplitSeq(strings.TrimSpace(logs.String()), "\n") {
		var entry map[string]any
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatalf("log line %q: %v", line, err)
		}
		if route, ok := entry["route"].(string); ok && strings.Contains(entry["msg"].(string), "route not mounted") {
			patterns = append(patterns, route)
		}
	}
	if len(patterns) == 0 {
		t.Fatal("AuthorizeRoutes logged no unmounted route: the derivation found nothing")
	}
	rows := make([]map[string]any, 0, len(patterns))
	for _, p := range patterns {
		rows = append(rows, map[string]any{"service": authapi.AccessService, "method": http.MethodPost, "route": p, "accepted_credential_classes": []string{"workload_token"}})
	}
	raw, err := json.Marshal(map[string]any{"rows": rows})
	if err != nil {
		t.Fatal(err)
	}
	withAuthorizeRows(t, raw)
	routes, err := authorizeRoutes(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), pool, schema, signer, nil)
	if err != nil || len(routes) != len(patterns) {
		t.Fatalf("mounted %d routes (%v), want the %d derived %v", len(routes), err, len(patterns), patterns)
	}
	keys := make([]string, len(routes))
	for i, r := range routes {
		keys[i] = r.Method + " " + r.Pattern
	}
	return keys
}

// Every route AuthorizeRoutes mounts is served on the internal listener (internalRoutes or internalOnlyRoutes) unless
// authorizeInternalExclusions names it with a reason, and every one carries the PDP rate-limit ceiling.
func TestEveryAuthorizeRouteIsServedOnTheInternalListener(t *testing.T) {
	keys := derivedAuthorizeRoutes(t)
	cfg := authconfig.Config{RateLimit: authconfig.RateLimit{PerSecond: 20, Burst: 40}, PDPRoute: authconfig.RateLimit{PerSecond: 2000, Burst: 4000}}
	pdp := map[string]bool{}
	var probe []httpapi.Route
	for _, key := range keys {
		method, pattern, _ := strings.Cut(key, " ")
		probe = append(probe, httpapi.Route{Method: method, Pattern: pattern})
	}
	for _, r := range apiServerOptions(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), probe).Routes {
		pdp[r.Method+" "+r.Pattern] = r.RateLimitPerSecond == 2000 && r.RateLimitBurst == 4000
	}
	for _, key := range keys {
		reason, excluded := authorizeInternalExclusions[key]
		internal := internalRoutes[key] || isInternalOnly(key)
		t.Logf("SWEEP route=%q internal=%v internal_only=%v public=%v pdp_limit=%v excluded=%q", key, internalRoutes[key], isInternalOnly(key), !isInternalOnly(key), pdp[key], reason)
		if !internal && !excluded {
			t.Errorf("%s is mounted by AuthorizeRoutes but served on no internal listener and not excluded with a reason", key)
		}
		if !pdp[key] {
			t.Errorf("%s does not carry the PDP rate-limit ceiling", key)
		}
	}
}

// With its endpoint-profile row planted, the listing route answers on the internal listener (and on the API
// listener, as its siblings) with the handler's own answer for both read actions: a caller with no workload token
// is refused 401 invalid_credentials, never 404. A 200 needs the Postgres decision state (integration tests).
func TestTheListingRouteIsServedOnBothListenersWithItsRow(t *testing.T) {
	var rows []map[string]any
	for _, p := range []string{authapi.PatternAuthorize, authapi.PatternAuthorizeBatch, authapi.PatternAuthorizeRepositories} {
		rows = append(rows, map[string]any{"service": authapi.AccessService, "method": http.MethodPost, "route": p, "accepted_credential_classes": []string{"workload_token"}})
	}
	raw, err := json.Marshal(map[string]any{"rows": rows})
	if err != nil {
		t.Fatal(err)
	}
	withAuthorizeRows(t, raw)
	cfg := loadInternalConfig(t, fullInternalEnv(t, session.WirePrincipalID(uuid.New())))
	handlers := listenerHandlers(t, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	for _, name := range []string{internalListenerName, apiListenerName} {
		listener := mustListener(t, handlers, name)
		for _, action := range []string{"acr.context.read", "acr.evidence.read"} {
			body, err := json.Marshal(map[string]any{
				"schema_version": "authorization_request.v1", "principal_reference": "opaque", "action": action,
				"resource": map[string]any{"type": "repository", "key": "*", "organization_id": uuid.NewString()},
				"context":  map[string]any{"request_id": "req_EXAMPLE0000000000000000001", "client_service": "dev-health-web"},
			})
			if err != nil {
				t.Fatal(err)
			}
			request := httptest.NewRequest(http.MethodPost, authapi.PatternAuthorizeRepositories, bytes.NewReader(body))
			request.RemoteAddr = "127.0.0.1:4321"
			recorder := httptest.NewRecorder()
			listener.ServeHTTP(recorder, request)
			if recorder.Code != http.StatusUnauthorized || !strings.Contains(recorder.Body.String(), "invalid_credentials") {
				t.Errorf("%s on %s: %d %q, want the handler's 401 invalid_credentials", action, name, recorder.Code, recorder.Body.String())
			}
		}
	}
}
