package server

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp/pdptest"
)

// The organization Gate.Authorize is asked about is the organization the REST caller hands its handler, whatever
// X-Org-Id or an org_id query parameter names: the PDP judges the org that is served.
func TestRESTPDPJudgesTheServedOrganization(t *testing.T) {
	other := "44444444-4444-4444-8444-444444444444"
	cases := map[string]func(*http.Request){
		"plain":            func(*http.Request) {},
		"X-Org-Id other":   func(r *http.Request) { r.Header.Set("X-Org-Id", other) },
		"org_id query":     func(r *http.Request) { r.URL.RawQuery = "org_id=" + other },
		"header and query": func(r *http.Request) { r.Header.Set("X-Org-Id", other); r.URL.RawQuery = "org_id=" + other },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			srv := pdptest.New(t)
			g := newGatedREST(t, pdp.ModeEnforce, srv.URL, true)
			req := httptest.NewRequest(http.MethodGet, "/api/v1/home", nil)
			req.Header.Set("Authorization", "Bearer "+g.userToken())
			mutate(req)
			rec := httptest.NewRecorder()
			g.handler.ServeHTTP(rec, req)
			g.gate.Wait()
			raw, _ := io.ReadAll(rec.Result().Body)
			var served struct {
				Org string `json:"org"`
			}
			if rec.Code != http.StatusOK || json.Unmarshal(raw, &served) != nil {
				t.Fatalf("%d %s, want 200 and the served org", rec.Code, raw)
			}
			asked := srv.Requests()
			if len(asked) != 1 || asked[0].OrganizationID != served.Org {
				t.Fatalf("PDP asked %+v, handler served %q: want one question on the served org", asked, served.Org)
			}
		})
	}
}
