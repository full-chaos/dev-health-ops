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

// signInRoute is one of the six sign-in routes that read a provider id from the path, with a body that passes its
// checks, a body that fails them (empty for the GET), and the route's own unknown-id answer.
type signInRoute struct {
	name, method, pattern, verb, goodBody, badBody, notFound string
}

var signInRoutes = []signInRoute{
	{"oauth callback", http.MethodPost, "/api/v1/auth/oauth/{first}/{second}", "callback", `{"code":"c","state":"s"}`, `{}`,
		`{"detail":"OAuth provider not found"}`},
	{"oidc authorize", http.MethodPost, "/api/v1/auth/oidc/{provider_id}/authorize", "authorize", `{}`, `{"use_pkce":"sometimes"}`,
		`{"detail":"SSO provider not found"}`},
	{"oidc callback", http.MethodPost, "/api/v1/auth/oidc/{provider_id}/callback", "callback", `{"code":"c","state":"s"}`,
		`{"code_verifier":1}`, `{"detail":"SSO provider not found"}`},
	{"saml metadata", http.MethodGet, "/api/v1/auth/saml/{provider_id}/metadata", "metadata", "", "",
		`{"detail":"SSO provider not found"}`},
	{"saml initiate", http.MethodPost, "/api/v1/auth/saml/{provider_id}/initiate", "initiate", `{}`, `{"relay_state":1}`,
		`{"detail":"SSO provider not found"}`},
	{"saml acs", http.MethodPost, "/api/v1/auth/saml/{provider_id}/acs", "acs", `{"SAMLResponse":"x"}`, `{}`,
		`{"detail":"SSO provider not found"}`},
}

// serve sends id to the mounted route with the real sign-in handlers on. The pool is zero: a request that reaches the
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

// reachesDatabase reports whether the request went on to the (zero) pool.
func (route signInRoute) reachesDatabase(t *testing.T, id string) bool {
	t.Helper()
	reached := false
	func() {
		defer func() { reached = recover() != nil }()
		route.serve(t, id, route.goodBody)
	}()
	return reached
}

const knownProviderID = "00000000-0000-4000-8000-000000000001"

// A provider id uuid.UUID() refuses answers the route's own unknown-id answer, byte for byte, and the database is never
// reached.
func TestSignInRoutesAnswerAMalformedProviderIDAsUnknown(t *testing.T) {
	for _, route := range signInRoutes {
		for _, id := range []string{"not-a-uuid", "providers", "00000000-0000-4000-8000-00000000000g"} {
			t.Run(route.name+"/"+id, func(t *testing.T) {
				recorder := route.serve(t, id, route.goodBody)
				if recorder.Code != http.StatusNotFound || recorder.Body.String() != route.notFound ||
					recorder.Header().Get("Content-Type") != "application/json" {
					t.Fatalf("got %d %q %s, want 404 application/json %s", recorder.Code,
						recorder.Header().Get("Content-Type"), recorder.Body.String(), route.notFound)
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

// A valid provider id, and each form uuid.UUID() also takes, goes on to the database (control).
func TestSignInRoutesGoOnWithAValidProviderID(t *testing.T) {
	for _, route := range signInRoutes {
		for _, id := range []string{knownProviderID, "{" + knownProviderID + "}", "urn:uuid:" + knownProviderID,
			strings.ReplaceAll(knownProviderID, "-", "")} {
			t.Run(route.name+"/"+id, func(t *testing.T) {
				if !route.reachesDatabase(t, id) {
					t.Fatal("refused a provider id uuid.UUID() takes before the database")
				}
			})
		}
	}
}
