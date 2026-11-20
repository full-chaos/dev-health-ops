package apiservice

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
)

// f3aProdFlags enables every auth aggregate except SAML.
func f3aProdFlags(upstream string) map[string]string {
	env := map[string]string{EnvAuthProxyURL: upstream}
	for _, aggregate := range AuthAggregates {
		env[AuthProxyFlagEnv(aggregate)] = "true"
	}
	env[AuthProxyFlagEnv(AuthSAML)] = "false"
	return env
}

// With SAML forwarding off the routes refuse locally; OIDC still forwards.
func TestAuthProxySAMLForwardOffAnswersConfigMissingAndNeverReachesAuthService(t *testing.T) {
	stub := newStubUpstream(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusTeapot) })
	auth := proxyTokens.AuthenticatorOver(t, scopeStore{admin: uuid.New(), target: uuid.New(), targetOrg: uuid.New()}, quietLogger())
	routes := Routes(Deps{Pool: &pgxpool.Pool{}, Auth: auth, Guard: policy.NewGuard(auth, quietLogger()),
		SSOStateSecret: "saml-forward-off-state-secret", SessionIssuer: stubSessionIssuer{},
		AuthProxy: mustProxyConfig(t, f3aProdFlags(stub.URL)), Limits: httpapi.NewMemoryCounters(time.Now)}, quietLogger())
	server, err := NewServer(config.Config{APIAddress: "127.0.0.1:0", CORSAllowedOrigins: []string{proxyOrigin}}, quietLogger(), routes)
	if err != nil {
		t.Fatal(err)
	}
	handler := server.Handler()
	want := httptest.NewRecorder()
	policy.WriteConfigMissing(want, "AUTH_PROXY_SAML")
	seen := map[AuthAggregate]int{}
	for _, call := range proxyCalls {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, call.request(t))
		forwarded := recorder.Code == http.StatusTeapot
		if forwarded != (call.aggregate != AuthSAML) {
			t.Errorf("%s %s forwarded=%v with the F3a prod flags", call.method, call.target, forwarded)
		}
		if call.aggregate == AuthSAML {
			if recorder.Code != want.Code || !bytes.Equal(recorder.Body.Bytes(), want.Body.Bytes()) {
				t.Errorf("%s %s = %d %s, want %d %s", call.method, call.target, recorder.Code, recorder.Body, want.Code, want.Body)
			}
		}
		seen[call.aggregate]++
	}
	if seen[AuthSAML] != 3 || seen[AuthOIDC] != 5 {
		t.Fatalf("proxyCalls cover SAML %d and OIDC %d routes, want 3 and 5", seen[AuthSAML], seen[AuthOIDC])
	}
	for _, request := range stub.requests() {
		if strings.Contains(request.path, "/saml/") {
			t.Errorf("auth-service saw %s %s with the SAML proxy off", request.method, request.path)
		}
	}
}
