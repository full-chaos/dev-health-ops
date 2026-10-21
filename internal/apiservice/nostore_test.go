package apiservice

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/adminroutes"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/apiservice/admin"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

const modelsPath = "/api/v1/admin/llm-settings/models"

func noStoreServer(t *testing.T, routes []httpapi.Route, scope ...func(http.Handler) http.Handler) http.Handler {
	t.Helper()
	server, err := NewServer(apiTestConfig(), quietLogger(), routes, scope...)
	if err != nil {
		t.Fatal(err)
	}
	return server.Handler()
}

func stubRoute(method, pattern string, status int) httpapi.Route {
	return httpapi.Route{Method: method, Pattern: pattern, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(status)
	})}
}

func serve(handler http.Handler, request *http.Request) *httptest.ResponseRecorder {
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return recorder
}

// Responses written before the route handler runs (body bound 413, org scope
// 403) and by the handler (200) all carry no-store through the real chain; a
// non-admin route does not.
func TestAdminRoutesAnswerNoStoreThroughTheRealServerChain(t *testing.T) {
	key := strings.Repeat("fixture-key-", 3)
	verifier, err := edgetoken.New(key, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	auth, err := policy.NewAuthenticator(verifier, scopeStore{admin: uuid.New(), target: uuid.New(), targetOrg: uuid.New()}, quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	scope := policy.NewScope(auth, quietLogger())
	handler := noStoreServer(t, []httpapi.Route{
		stubRoute(http.MethodGet, modelsPath, http.StatusOK),
		stubRoute(http.MethodPut, "/api/v1/admin/llm-settings", http.StatusNoContent),
		stubRoute(http.MethodGet, "/api/v1/other", http.StatusOK),
	}, scope.OrgScope)

	over := httptest.NewRequest(http.MethodPut, "/api/v1/admin/llm-settings", io.LimitReader(zeros{}, maxBodyBytes+1))
	over.ContentLength = maxBodyBytes + 1
	forbidden := httptest.NewRequest(http.MethodGet, modelsPath, nil)
	forbidden.Header.Set("Authorization", "Bearer "+signedToken(t, key, uuid.New(), false))
	forbidden.Header.Set("X-Org-Id", uuid.NewString())

	for _, tc := range []struct {
		name    string
		request *http.Request
		status  int
		want    string
	}{
		{"body bound 413", over, http.StatusRequestEntityTooLarge, "no-store"},
		{"org scope 403", forbidden, http.StatusForbidden, "no-store"},
		{"handler 200", httptest.NewRequest(http.MethodGet, modelsPath, nil), http.StatusOK, "no-store"},
		{"non-admin route (negative control)", httptest.NewRequest(http.MethodGet, "/api/v1/other", nil), http.StatusOK, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			response := serve(handler, tc.request)
			if response.Code != tc.status {
				t.Fatalf("status %d, want %d", response.Code, tc.status)
			}
			if got := response.Header().Get("Cache-Control"); got != tc.want {
				t.Fatalf("Cache-Control = %q, want %q", got, tc.want)
			}
		})
	}
}

// Every route of the real admin.Routes answers no-store on a refused caller.
func TestEveryAdminRouteAnswersNoStoreThroughTheServer(t *testing.T) {
	routes := admin.Routes(admin.Deps{Guard: policy.NewGuard(nil, nil)})
	if len(routes) != len(adminroutes.Table) {
		t.Fatalf("%d admin routes, %d in the table", len(routes), len(adminroutes.Table))
	}
	handler := noStoreServer(t, routes)
	sawModels := false
	for _, route := range routes {
		path := strings.NewReplacer("{", "", "}", "").Replace(route.Pattern)
		var response *httptest.ResponseRecorder
		func() {
			defer func() { _ = recover() }()
			response = serve(handler, httptest.NewRequest(route.Method, path, nil))
		}()
		if response == nil || response.Header().Get("Cache-Control") != "no-store" {
			t.Errorf("%s %s: no Cache-Control: no-store", route.Method, route.Pattern)
		}
		sawModels = sawModels || (route.Method == http.MethodGet && route.Pattern == modelsPath)
	}
	if !sawModels {
		t.Fatal("GET llm-settings/models not in the admin route table")
	}
}

func signedToken(t *testing.T, key string, sub uuid.UUID, superuser bool) string {
	t.Helper()
	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub": sub.String(), "type": "access", "org_id": uuid.NewString(), "is_superuser": superuser,
		"exp": time.Now().Add(time.Hour).Unix(),
	}).SignedString([]byte(key))
	if err != nil {
		t.Fatal(err)
	}
	return token
}

// Accepted limit: an UNHANDLED 500 keeps the Python shape (content headers
// only, generic body), so an admin route's unhandled 500 carries no
// Cache-Control. The body holds no org data.
func TestAdminUnhandledErrorKeepsThePythonShapeWithoutNoStore(t *testing.T) {
	panicking := httpapi.Route{Method: http.MethodGet, Pattern: modelsPath, Handler: http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("boom")
	})}
	response := serve(noStoreServer(t, []httpapi.Route{panicking}), httptest.NewRequest(http.MethodGet, modelsPath, nil))
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("status %d, want 500", response.Code)
	}
	if got := response.Header().Get("Cache-Control"); got != "" {
		t.Fatalf("unhandled 500 carries Cache-Control %q; the Python shape strips it", got)
	}
	if body := response.Body.String(); body != `{"detail":"Internal Server Error"}` && body != "Internal Server Error" {
		t.Fatalf("body %q is not the generic 500 body", body)
	}
}
