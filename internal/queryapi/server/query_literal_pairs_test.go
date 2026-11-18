package server

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pepgate"
)

// Starlette reads the query with parse_qsl: a pair holding an unescaped ';'
// or a malformed escape is kept with its literal value, never dropped.
func TestPeopleSearchKeepsPairsGoWouldDrop(t *testing.T) {
	for _, tc := range []struct {
		name, query string
		status      int
	}{
		{"semicolon in a forbidden param", "compare_to=x;y", http.StatusBadRequest},
		{"bad escape in a forbidden param", "rank=%zz", http.StatusBadRequest},
		{"semicolon in limit", "limit=5;x", http.StatusUnprocessableEntity},
		{"bad escape in limit", "limit=5%zz", http.StatusUnprocessableEntity},
		{"semicolon in the key", "top;x=1&limit=5;", http.StatusUnprocessableEntity},
	} {
		t.Run(tc.name, func(t *testing.T) {
			handler := newPeopleSearchHandler(newFailingPeopleReader(t))
			req := httptest.NewRequest(http.MethodGet, "/api/v1/people?"+tc.query, nil)
			req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1", Scope: pepgate.Legacy("org-1")}))
			rec := httptest.NewRecorder()
			serveRoute(t, handler, rec, req)
			if rec.Code != tc.status {
				t.Fatalf("status = %d, want %d, body=%s", rec.Code, tc.status, rec.Body.String())
			}
		})
	}
}
