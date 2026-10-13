package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
)

// A validation 422 names the field and the rule, never the submitted value.
func TestQueryAPIValidationErrorNeverEchoesTheSubmittedValue(t *testing.T) {
	const probe = "zz-probe-value-81f3"
	handler := newHomeGetHandler(emptyRowsHomeClient{}, nil)
	for _, target := range []string{
		"/api/v1/home?range_days=" + probe,
		"/api/v1/home?start_date=2026-13-01",
	} {
		req := httptest.NewRequest(http.MethodGet, target, nil)
		req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
		rec := httptest.NewRecorder()
		serveRoute(t, handler, rec, req)
		body := rec.Body.String()
		if rec.Code != http.StatusUnprocessableEntity {
			t.Fatalf("%s: status %d, want 422: %s", target, rec.Code, body)
		}
		for _, forbidden := range []string{probe, `"input"`, `"ctx"`, "2026-13-01"} {
			if strings.Contains(body, forbidden) {
				t.Fatalf("%s: 422 body carries %q: %s", target, forbidden, body)
			}
		}
		if !strings.Contains(body, `"loc"`) || !strings.Contains(body, `"msg"`) {
			t.Fatalf("%s: 422 body lost loc or msg: %s", target, body)
		}
	}
}
