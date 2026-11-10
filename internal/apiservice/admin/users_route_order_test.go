package admin

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/edtokens"
)

type superuserPolicyStore struct{}

func (superuserPolicyStore) UserState(context.Context, uuid.UUID) (policy.UserState, bool, error) {
	return policy.UserState{IsActive: true, IsSuperuser: true}, true, nil
}

func (superuserPolicyStore) Membership(context.Context, uuid.UUID, uuid.UUID) (string, bool, error) {
	return "", false, nil
}

func (s superuserPolicyStore) OrgMembership(ctx context.Context, user, org uuid.UUID) (string, bool, bool, error) {
	role, member, err := s.Membership(ctx, user, org)
	return role, member, true, err
}

func (superuserPolicyStore) ActiveImpersonation(context.Context, uuid.UUID) (*policy.Impersonation, error) {
	return nil, nil
}

// mountedUserRoutes mounts the real user routes (bodyFirst, guard, handler) over a database that records any call.
func mountedUserRoutes(t *testing.T) (server *httptest.Server, bearer string, touched *bool) {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	auth, bearer := routedAuth(t, superuserPolicyStore{})
	touched = new(bool)
	h := &handlers{store: pgStore{Pool: touchedDB{touched}}, guard: policy.NewGuard(auth, logger), logger: logger}
	mux := http.NewServeMux()
	for _, route := range h.userRoutes() {
		if (route.Method == http.MethodPatch || route.Method == http.MethodPost) && !strings.HasSuffix(route.Pattern, "/password") {
			mux.Handle(route.Method+" "+route.Pattern, route.Handler)
		}
	}
	server = httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return server, bearer, touched
}

func sendUserRoute(t *testing.T, server *httptest.Server, bearer, method, path, body string) (int, string) {
	t.Helper()
	request, err := http.NewRequest(method, server.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+bearer)
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var out bytes.Buffer
	_, _ = io.Copy(&out, response.Body)
	return response.StatusCode, out.String()
}

// A body that fails validation is answered 422 before the handler makes any store call, on the mounted create and
// update routes. The control is a valid body: it reaches the store.
func TestMountedUserRoutesValidateTheBodyBeforeAnyStoreCall(t *testing.T) {
	target := "/api/v1/admin/users/" + uuid.NewString()
	for name, tc := range map[string]struct{ method, path, body string }{
		"update full_name nul":      {http.MethodPatch, target, `{"full_name":"A\u0000B"}`},
		"update avatar_url control": {http.MethodPatch, target, `{"avatar_url":"https://example.test/\u0001"}`},
		"update email replacement":  {http.MethodPatch, target, `{"email":"a�b@example.test"}`},
		"update wrong bool type":    {http.MethodPatch, target, `{"is_active":"maybe"}`},
		"create full_name nul":      {http.MethodPost, "/api/v1/admin/users", `{"email":"a@example.test","full_name":"A\u0000B"}`},
		"create email replacement":  {http.MethodPost, "/api/v1/admin/users", `{"email":"a�b@example.test"}`},
	} {
		t.Run(name, func(t *testing.T) {
			server, bearer, touched := mountedUserRoutes(t)
			code, response := sendUserRoute(t, server, bearer, tc.method, tc.path, tc.body)
			if code != http.StatusUnprocessableEntity || *touched {
				t.Fatalf("status %d, store touched %v, body %q; want 422 and no store call", code, *touched, response)
			}
		})
	}
	t.Run("control: a valid update body reaches the store", func(t *testing.T) {
		server, bearer, touched := mountedUserRoutes(t)
		if code, _ := sendUserRoute(t, server, bearer, http.MethodPatch, target, `{"full_name":"A B"}`); !*touched || code == http.StatusUnprocessableEntity {
			t.Fatalf("status %d, store touched %v; want the store to be reached", code, *touched)
		}
	})
}

func (superuserPolicyStore) ReadsPlatformRoles() bool { return false }

// routedAuth is the api's routed Authenticator over store and an EdDSA user access token it accepts. The
// superuser and any impersonation come from store's users and impersonation rows, never from a claim.
func routedAuth(t *testing.T, store policy.Store) (*policy.Authenticator, string) {
	t.Helper()
	tokens := edtokens.New(t)
	return tokens.AuthenticatorOver(t, store, slog.New(slog.DiscardHandler)), tokens.MustMint(t, edtokens.Claims{UserID: uuid.New()})
}

// profiled sets, in front of a hand-built chain, the route profiles NewServer sets before its scope layers.
func profiled(t *testing.T, routes []httpapi.Route, next http.Handler) http.Handler {
	t.Helper()
	templates, err := httpapi.NewRouteTemplates(routes)
	if err != nil {
		t.Fatal(err)
	}
	return policy.RouteProfiles(templates.Match)(next)
}
