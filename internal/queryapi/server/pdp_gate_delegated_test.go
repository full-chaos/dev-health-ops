package server

import (
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/pep"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp/pdptest"
)

// A delegated session's bearer on an enforced REST row: the PDP judges it (as its target); an allow with the
// delegated obligations (tenant scope and minimum_assurance aal1) serves, aal2 is 503, a deny is 403.
func TestRESTPDPEnforceDelegatedBearerIsJudgedAndServedOnAAL1(t *testing.T) {
	actor := wirePrincipal(uuid.MustParse("22222222-2222-4222-8222-222222222222"))
	for _, tc := range []struct {
		name   string
		set    func(*pdptest.Server)
		answer answer
	}{
		{"allow aal1", func(s *pdptest.Server) { s.Obligations(delegatedAllow("aal1")) }, gatedOK},
		{"allow aal2", func(s *pdptest.Server) { s.Obligations(delegatedAllow("aal2")) }, gatedUnavailable},
		{"deny", func(s *pdptest.Server) { s.Deny("impersonation_denied") }, gatedForbidden},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := pdptest.New(t)
			tc.set(srv)
			g := newGatedREST(t, pdp.ModeEnforce, srv.URL, true)
			token := g.minter.mint(ecUser, map[string]any{"act": map[string]any{"sub": actor}}, nil)
			if got := g.do(http.MethodGet, token); got != tc.answer {
				t.Fatalf("got %+v, want %+v", got, tc.answer)
			}
			if got := srv.Requests(); len(got) != 1 || got[0].Action != "analytics.read" || got[0].OrganizationID != ecOrg.String() {
				t.Fatalf("PDP calls %+v, want one analytics.read question on the token's organization", got)
			}
		})
	}
}

func delegatedAllow(assurance string) map[string]any {
	return map[string]any{"tenant_scope": strings.ToLower(ecOrg.String()), "repository_scope": "organization", "minimum_assurance": assurance}
}

// A delegated bearer reaches the gate as delegated: on a row whose action is not a read it is refused (403) before
// any PDP call, while a plain bearer on the same row is asked.
func TestRESTPDPEnforceDelegatedBearerOnANonReadRowIsRefusedBeforeThePDP(t *testing.T) {
	srv := pdptest.New(t)
	srv.Obligations(delegatedAllow("aal1"))
	g := newGatedREST(t, pdp.ModeEnforce, srv.URL, true)
	rows := `{"rows":[{"id":"GET /api/v1/home [dev-health-query-api @query]","service":"dev-health-query-api","method":"GET",` +
		`"route":"/api/v1/home","action":"teams.write","classification":"protected"}]}`
	client, err := pep.New(pep.Config{BaseURL: srv.URL, AllowPlainHTTP: true, HTTPClient: &http.Client{}, ClientService: pdp.ClientService, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	g.gate = pdp.New(pdp.Config{Mode: pdp.ModeEnforce, Decider: client, Rows: []byte(rows), Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	g.handler = planeHandler(g.f.handler.(*http.ServeMux), g.gate)
	actor := wirePrincipal(uuid.MustParse("22222222-2222-4222-8222-222222222222"))
	delegated := g.minter.mint(ecUser, map[string]any{"act": map[string]any{"sub": actor}}, nil)
	if got := g.do(http.MethodGet, delegated); got != gatedForbidden || len(srv.Requests()) != 0 {
		t.Fatalf("delegated bearer on a write row: %+v with %d PDP calls, want %+v and none", got, len(srv.Requests()), gatedForbidden)
	}
	g.do(http.MethodGet, g.userToken())
	if got := srv.Requests(); len(got) != 1 || got[0].Action != "teams.write" {
		t.Fatalf("plain bearer: PDP calls %+v, want one teams.write question", got)
	}
}
