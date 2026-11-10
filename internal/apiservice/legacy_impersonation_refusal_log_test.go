package apiservice

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/edtokens"
)

// TestSwitchOrgRefusalWarnNamesTheRoutePattern: the WARN of the switch-org
// impersonation refusal carries the mux pattern as its path, never the raw
// request path.
func TestSwitchOrgRefusalWarnNamesTheRoutePattern(t *testing.T) {
	tokens := edtokens.New(t)
	store := &endableSessionStore{legacySessionStore: legacySessionStore{admin: uuid.New(), target: uuid.New(), tOrg: uuid.New()}}
	auth := tokens.AuthenticatorOver(t, store, quietLogger())
	rows, err := goAPIScopeRows()
	if err != nil {
		t.Fatal(err)
	}
	stub := newStubUpstream(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	})
	upstream, _ := url.Parse(stub.URL)
	proxy := AuthProxyConfig{Upstream: upstream, Impersonation: true, On: map[AuthAggregate]bool{AuthSessions: true}}
	protected, err := newProtection(apiTestConfig(), nil, false, proxy, quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	scope := policy.NewScope(auth, quietLogger())
	scope.DeclaredRows(rows)
	scope.RefuseLegacyImpersonation(protected.scope.RefusesLegacyImpersonation())
	pool, err := pgxpool.New(context.Background(), "postgres://u:p@127.0.0.1:1/db?sslmode=disable&connect_timeout=1")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	var logs bytes.Buffer
	guardLogger := slog.New(slog.NewJSONHandler(&logs, nil))
	routes := Routes(Deps{Pool: pool, Auth: auth, Guard: policy.NewGuard(auth, guardLogger),
		AuthProxy: proxy, Limits: httpapi.NewMemoryCounters(time.Now)}, quietLogger())
	server, err := newServer(apiTestConfig(), quietLogger(), routes, nil, proxy.ScopeExemptRoutes(), auth.ReadOnce, scope.Principal, scope.OrgScope, scope.Impersonation)
	if err != nil {
		t.Fatal(err)
	}
	access := tokens.MustMint(t, edtokens.Claims{UserID: store.admin, OrgID: uuid.NewString(), TokenVersion: 1})
	request := httptest.NewRequest(http.MethodPost, "/api/v1/auth/switch-org", strings.NewReader(switchOrgBody))
	request.RemoteAddr = "192.0.2.10:4711"
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+access)
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusForbidden || recorder.Body.String() != switchOrgRefusal {
		t.Fatalf("legacy caller with a live row: %d %s, want 403 %s", recorder.Code, recorder.Body.String(), switchOrgRefusal)
	}

	var paths []string
	for _, line := range bytes.Split(bytes.TrimSpace(logs.Bytes()), []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var record struct {
			Level string `json:"level"`
			Msg   string `json:"msg"`
			Path  string `json:"path"`
		}
		if err := json.Unmarshal(line, &record); err != nil {
			t.Fatalf("guard log line is not JSON: %q", line)
		}
		if record.Level == "WARN" && record.Msg == "api guard: route refused in an impersonation session" {
			paths = append(paths, record.Path)
		}
	}
	const want = "POST /api/v1/auth/switch-org"
	if len(paths) != 1 || paths[0] != want {
		t.Errorf("refusal WARN paths %q, want exactly one %q (the mux pattern)", paths, want)
	}
}
