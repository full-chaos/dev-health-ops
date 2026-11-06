package server

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-go/pep/pepfake"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/drilldown"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/principal"
)

// Enforce: the PDP allows with a repository allowlist ([] = no repository, [x] = one), and the repository-keyed
// GET /api/v1/drilldown/prs must not run an organization-wide read: 503 and no statement reaches ClickHouse.
// (The repro of chaos PLAN-10864-v2 F-A, executed red on 86cd04c7: status=200 statements=1.)
func TestRESTPDPEnforceAllowlistRunsNoOrgWideRead(t *testing.T) {
	for name, allowlist := range map[string][]string{"empty": {}, "one repository": {"acme/widgets"}} {
		t.Run(name, func(t *testing.T) { enforceAllowlistRunsNoOrgWideRead(t, allowlist) })
	}
}

func enforceAllowlistRunsNoOrgWideRead(t *testing.T, allowlist []string) {
	t.Helper()
	quietStdLog(t)
	m := newAccessMinter(t)
	setup := testAccess(m.keys, nil, accessTestNow)
	verifier, _ := iaVerifier(t)
	edge, err := principal.NewEdgeVerifier(ecSecret, ecIssuer, ecAudience, newQueryAPIUserStoreFrom(newAccessStore()))
	if err != nil {
		t.Fatal(err)
	}
	if err := routeEdgeVerifier(edge, setup.opts); err != nil {
		t.Fatal(err)
	}
	client := &teamScopeReachClient{}
	reader, err := drilldown.NewReader(client)
	if err != nil {
		t.Fatal(err)
	}
	get := newDrilldownPRsGetHandler(reader)
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/drilldown/prs", func(w http.ResponseWriter, r *http.Request) {
		claims, ok := authenticateRESTRequest(w, r, verifier, edge, "drilldown_prs")
		if !ok {
			return
		}
		get(w, r.WithContext(authctx.WithClaims(r.Context(), claims)))
	})
	token := m.mint(ecUser, nil, nil)
	org := strings.ToLower(ecOrg.String())
	decider := pepfake.New(pepfake.Rule{PrincipalReference: token, Action: "git.read", ResourceType: "organization",
		ResourceKey: org, OrganizationID: org, TenantScope: org, RepositoryAllowlist: allowlist})
	gate := pdp.New(pdp.Config{Mode: pdp.ModeEnforce, Decider: decider, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/drilldown/prs", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	planeHandler(mux, gate).ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable || strings.TrimSpace(rec.Body.String()) != `{"detail":"Authorization unavailable"}` {
		t.Fatalf("status=%d body=%q, want 503 Authorization unavailable", rec.Code, rec.Body.String())
	}
	if len(client.statements) != 0 {
		t.Fatalf("%d statements reached ClickHouse, want 0:\n%s", len(client.statements), strings.Join(client.statements, "\n"))
	}
}

// The WARN line of a PDP refusal is one line: a request id with CR or LF never puts a line break in it (CodeQL
// go/log-injection, alert 3601). The id is also filtered by acceptableCorrelationID; the header map is written
// directly so a value the HTTP parser would reject reaches the handler.
func TestPDPRefusalLogLineHasNoLineBreakFromTheRequestID(t *testing.T) {
	for name, id := range map[string]string{
		"lf":         "abc\ndef",
		"cr":         "abc\rdef",
		"crlf forge": "abc\r\nlevel=ERROR msg=forged",
		"clean":      "req-123",
	} {
		t.Run(name, func(t *testing.T) {
			var logs strings.Builder
			previous := slog.Default()
			slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
			t.Cleanup(func() { slog.SetDefault(previous) })
			r := httptest.NewRequest(http.MethodGet, "/api/v1/home", nil)
			r.Header["X-Request-Id"] = []string{id}
			writePDPDenied(httptest.NewRecorder(), r, "rest", &pdp.DenyError{Reason: "role_lacks_action"})
			out := strings.TrimSuffix(logs.String(), "\n")
			if out == "" || strings.ContainsAny(out, "\r\n") {
				t.Fatalf("log output %q is not exactly one line", out)
			}
		})
	}
}
