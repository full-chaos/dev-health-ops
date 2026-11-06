package admin

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
)

// callOrgWrite sends body to a real orgRoutes() mount behind the real guard and scope; anonymous drops the bearer,
// session gives the superuser an active impersonation session.
func callOrgWrite(t *testing.T, anonymous, session bool, tc orgWriteCase, body string) (int, string, bool) {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	auth, bearer := routedAuth(t, impersonatingStore{active: session})
	scope := policy.NewScope(auth, logger)
	touched := new(bool)
	h := &handlers{store: pgStore{Pool: touchedDB{touched}}, guard: policy.NewGuard(auth, logger), logger: logger}
	mux := http.NewServeMux()
	for _, route := range h.orgRoutes() {
		mux.Handle(route.Method+" "+route.Pattern, route.Handler)
	}
	server := httptest.NewServer(profiled(t, h.orgRoutes(), scope.Principal(scope.OrgScope(scope.Impersonation(mux)))))
	defer server.Close()
	req, _ := http.NewRequest(tc.method, server.URL+tc.path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if !anonymous {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b), *touched
}

// The body-first order is the decided behaviour (FastAPI parity, see bodyFirst): a body that does not decode is
// answered 422 before the credential is read, and writes nothing; a body that decodes meets the guard.
func TestOrgWriteRoutesKeepTheBodyFirstOrder(t *testing.T) {
	for _, tc := range orgWriteCases() {
		if tc.name == "remove" { // DELETE has no body: not a BodyFirst route
			continue
		}
		t.Run(tc.name+"/unauthenticated-malformed-body", func(t *testing.T) {
			status, body, touched := callOrgWrite(t, true, false, tc, "{not json")
			if status != http.StatusUnprocessableEntity || !strings.Contains(body, "json_invalid") || touched {
				t.Fatalf("status %d body %s touched %v; want 422 json_invalid and no store call", status, body, touched)
			}
		})
		t.Run(tc.name+"/unauthenticated-valid-body", func(t *testing.T) {
			status, _, touched := callOrgWrite(t, true, false, tc, tc.body)
			if status != http.StatusUnauthorized || touched {
				t.Fatalf("status %d touched %v; want 401 and no store call", status, touched)
			}
		})
		t.Run(tc.name+"/impersonating-valid-body", func(t *testing.T) {
			status, body, touched := callOrgWrite(t, false, true, tc, tc.body)
			if status != http.StatusForbidden || strings.TrimSpace(body) != impersonationRefusal || touched {
				t.Fatalf("status %d body %s touched %v; want 403 %s and no store call", status, body, touched, impersonationRefusal)
			}
		})
	}
}
