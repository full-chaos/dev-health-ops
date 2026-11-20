package sso

import (
	"log/slog"
	"maps"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
)

// signInRoute is one of the three sign-in routes that read a provider id from the path, with a body that passes its
// checks and a body that fails them (empty for the GET).
type signInRoute struct {
	name, method, pattern, verb, goodBody, badBody string
}

var signInRoutes = []signInRoute{
	{"oauth callback", http.MethodPost, "/api/v1/auth/oauth/{first}/{second}", "callback", `{"code":"c","state":"s"}`, `{}`},
	{"oidc authorize", http.MethodPost, "/api/v1/auth/oidc/{provider_id}/authorize", "authorize", `{}`, `{"use_pkce":"sometimes"}`},
	{"oidc callback", http.MethodPost, "/api/v1/auth/oidc/{provider_id}/callback", "callback", `{"code":"c","state":"s"}`,
		`{"code_verifier":1}`},
}

// serve sends id to the mounted route with sign-in configured and no forward. The pool is zero: a request that reaches the
// database panics.
func (route signInRoute) serve(t *testing.T, id, body string) *httptest.ResponseRecorder {
	t.Helper()
	routes := Routes(Deps{
		Pool: &pgxpool.Pool{}, Guard: policy.NewGuard(&policy.Authenticator{}, slog.New(slog.DiscardHandler)),
		Logger: slog.New(slog.DiscardHandler), Issuer: okIssuer{}, StateSecret: "state-secret-for-tests",
	})
	var handler http.Handler
	for _, mounted := range routes {
		if mounted.Method == route.method && mounted.Pattern == route.pattern {
			handler = mounted.Handler
		}
	}
	if handler == nil {
		t.Fatal("route not mounted")
	}
	family := strings.Split(route.pattern, "/")[4]
	request := httptest.NewRequest(route.method, "/api/v1/auth/"+family+"/"+id+"/"+route.verb, strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	if family == "oauth" {
		request.SetPathValue("first", id)
		request.SetPathValue("second", route.verb)
	} else {
		request.SetPathValue("provider_id", id)
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return recorder
}

const knownProviderID = "00000000-0000-4000-8000-000000000001"

// With the OIDC forward off, every provider id (malformed, or a form uuid.UUID() takes) answers 503 config_missing naming
// AUTH_PROXY_OIDC, and the database is never reached (a zero pool would panic): go-api has no own OIDC or OAuth sign-in
// (CHAOS-14131).
func TestSignInRoutesWithTheForwardOffAnswerConfigMissingForAnyProviderID(t *testing.T) {
	want := httptest.NewRecorder()
	policy.WriteConfigMissing(want, "AUTH_PROXY_OIDC")
	for _, route := range signInRoutes {
		for _, id := range []string{"not-a-uuid", "providers", "00000000-0000-4000-8000-00000000000g", knownProviderID,
			"{" + knownProviderID + "}", "urn:uuid:" + knownProviderID, strings.ReplaceAll(knownProviderID, "-", "")} {
			t.Run(route.name+"/"+id, func(t *testing.T) {
				recorder := route.serve(t, id, route.goodBody)
				if recorder.Code != want.Code || recorder.Body.String() != want.Body.String() {
					t.Fatalf("got %d %s, want %d %s", recorder.Code, recorder.Body.String(), want.Code, want.Body.String())
				}
			})
		}
	}
}

// The body checks come first: a malformed id with a bad body answers exactly what a valid id with that body answers.
func TestSignInRoutesCheckTheBodyBeforeAMalformedProviderID(t *testing.T) {
	for _, route := range signInRoutes {
		if route.method == http.MethodGet {
			continue
		}
		t.Run(route.name, func(t *testing.T) {
			malformed := route.serve(t, "not-a-uuid", route.badBody)
			valid := route.serve(t, knownProviderID, route.badBody)
			if malformed.Code != http.StatusUnprocessableEntity || malformed.Code != valid.Code ||
				malformed.Body.String() != valid.Body.String() || !maps.EqualFunc(malformed.Header(), valid.Header(),
				func(a, b []string) bool { return strings.Join(a, "\n") == strings.Join(b, "\n") }) {
				t.Fatalf("malformed id %d %v %s, valid id %d %v %s", malformed.Code, malformed.Header(),
					malformed.Body.String(), valid.Code, valid.Header(), valid.Body.String())
			}
		})
	}
}
