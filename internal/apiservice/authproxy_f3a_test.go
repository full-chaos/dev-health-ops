package apiservice

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// f3aProdFlags is the go-api auth proxy env of the F3a prod values (deploy
// values.prod.yaml with ops.authService.sso oidc true, saml false): every
// aggregate forwards to auth-service except SAML.
func f3aProdFlags(upstream string) map[string]string {
	env := map[string]string{EnvAuthProxyURL: upstream}
	for _, aggregate := range AuthAggregates {
		env[AuthProxyFlagEnv(aggregate)] = "true"
	}
	env[AuthProxyFlagEnv(AuthSAML)] = "false"
	return env
}

// With the F3a prod flags the SAML sign-in routes stay in go-api (they never
// reach auth-service) while the OIDC sign-in routes are forwarded.
func TestAuthProxyF3aProdFlagsKeepSAMLInGoAPI(t *testing.T) {
	stub := newStubUpstream(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusTeapot) })
	handler := proxyFixture(t, mustProxyConfig(t, f3aProdFlags(stub.URL)))
	none := proxyFixture(t, AuthProxyConfig{})
	seen := map[AuthAggregate]int{}
	for _, call := range proxyCalls {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, call.request(t))
		forwarded := recorder.Code == http.StatusTeapot
		if forwarded != (call.aggregate != AuthSAML) {
			t.Errorf("%s %s forwarded=%v with the F3a prod flags", call.method, call.target, forwarded)
		}
		if call.aggregate == AuthSAML {
			// go-api's own answer: byte for byte the answer of a go-api with no proxy.
			local := httptest.NewRecorder()
			none.ServeHTTP(local, call.request(t))
			if got, want := fmt.Sprintf("%d %s", recorder.Code, recorder.Body), fmt.Sprintf("%d %s", local.Code, local.Body); got != want {
				t.Errorf("%s %s = %q with the F3a prod flags, want go-api's own %q", call.method, call.target, got, want)
			}
		}
		seen[call.aggregate]++
	}
	if seen[AuthSAML] != 3 || seen[AuthOIDC] != 2 {
		t.Fatalf("proxyCalls cover SAML %d and OIDC %d routes, want 3 and 2", seen[AuthSAML], seen[AuthOIDC])
	}
	for _, request := range stub.requests() {
		if strings.Contains(request.path, "/saml/") {
			t.Errorf("auth-service saw %s %s with the SAML proxy off", request.method, request.path)
		}
	}
}
