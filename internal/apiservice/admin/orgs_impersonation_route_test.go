package admin

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
)

type impersonatingStore struct {
	superuserPolicyStore
	active bool
}

func (s impersonatingStore) ActiveImpersonation(_ context.Context, admin uuid.UUID) (*policy.Impersonation, error) {
	if !s.active {
		return nil, nil
	}
	return &policy.Impersonation{ID: uuid.New(), AdminUserID: admin, TargetUserID: uuid.New(), TargetOrgID: uuid.New(), TargetRole: "admin"}, nil
}

const impersonationRefusal = `{"detail":"Not allowed in an impersonation session"}`

func serveOrgWrite(t *testing.T, active bool, method, path, body string) (status int, text string, touched bool) {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	auth, bearer := routedAuth(t, impersonatingStore{active: active})
	scope := policy.NewScope(auth, logger)
	touchedFlag := new(bool)
	h := &handlers{store: pgStore{Pool: touchedDB{touchedFlag}}, guard: policy.NewGuard(auth, logger), logger: logger, invites: InviteConfig{LinkSecret: "route-order-test-signing-key-32-bytes!!!!"}}
	mux := http.NewServeMux()
	routes := append(h.orgRoutes(), h.impersonationRoutes()...)
	for _, route := range routes {
		mux.Handle(route.Method+" "+route.Pattern, route.Handler)
	}
	server := httptest.NewServer(profiled(t, routes, scope.Principal(scope.OrgScope(scope.Impersonation(mux)))))
	defer server.Close()
	req, _ := http.NewRequest(method, server.URL+path, strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+bearer)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, strings.TrimSpace(string(b)), *touchedFlag
}

type orgWriteCase struct{ name, method, path, body string }

func orgWriteCases() []orgWriteCase {
	org, member := uuid.NewString(), uuid.NewString()
	base := "/api/v1/admin/orgs/" + org
	return []orgWriteCase{
		{"add", http.MethodPost, base + "/members", `{"user_id":"` + member + `","role":"admin"}`},
		{"role", http.MethodPatch, base + "/members/" + member, `{"role":"admin"}`},
		{"remove", http.MethodDelete, base + "/members/" + member, ``},
		{"transfer", http.MethodPost, base + "/transfer-ownership", `{"new_owner_user_id":"` + member + `"}`},
		{"invite", http.MethodPost, base + "/invites", `{"email":"new@example.com","role":"member"}`},
	}
}

func TestImpersonatedOrgWriteRoutesAreRefusedThroughGuard(t *testing.T) {
	for _, tc := range orgWriteCases() {
		t.Run(tc.name, func(t *testing.T) {
			status, body, touched := serveOrgWrite(t, true, tc.method, tc.path, tc.body)
			if status != http.StatusForbidden || body != impersonationRefusal || touched {
				t.Fatalf("impersonated %s: status %d body %s, store touched %v; want 403 %s and no store call", tc.name, status, body, touched, impersonationRefusal)
			}
		})
	}
}

func TestNonImpersonatingAdminOrgWritesStillReachTheHandler(t *testing.T) {
	for _, tc := range orgWriteCases() {
		t.Run(tc.name, func(t *testing.T) {
			status, body, touched := serveOrgWrite(t, false, tc.method, tc.path, tc.body)
			if body == impersonationRefusal || !touched {
				t.Fatalf("%s without a session: status %d body %s, store touched %v; want the handler reached", tc.name, status, body, touched)
			}
		})
	}
}

func TestStopImpersonationStaysAllowedInASession(t *testing.T) {
	status, body, _ := serveOrgWrite(t, true, http.MethodPost, "/api/v1/admin/impersonate/stop", ``)
	if body == impersonationRefusal || status == http.StatusForbidden {
		t.Fatalf("stop in a session: status %d body %s; want it allowed", status, body)
	}
}
