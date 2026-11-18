package oidc

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// org_id is read the way FastAPI reads it: a ";" or a bad escape stays in the
// value (422 uuid_parsing), never drops the pair so an earlier org_id serves.
func TestTypeRouteReadsOrgIDLiterally(t *testing.T) {
	c := routeCases[len(routeCases)-1]
	row := c.activeRow()
	org := row.OrgID.String()
	for _, tc := range []struct {
		query, want string
		status      int
	}{
		{"org_id=" + org, "", http.StatusOK},
		{"org_id=" + org + "&x=;", "", http.StatusOK},
		{"org_id=" + org + "&x=%zz", "", http.StatusOK},
		{"org_id=" + org + ";x", "uuid_parsing", http.StatusUnprocessableEntity},
		{"org_id=" + org + "%zz", "uuid_parsing", http.StatusUnprocessableEntity},
		{"org_id=" + org + "&org_id=" + org + ";x", "uuid_parsing", http.StatusUnprocessableEntity},
		{"org_id=" + org + "&org_id=%FF", "uuid_parsing", http.StatusUnprocessableEntity},
	} {
		env := newUnitEnv(t, &fakeProviders{before: row})
		rec := env.serve(c, httptest.NewRequest(http.MethodGet, "/api/v1/auth/oauth/gitlab/authorize?"+tc.query, nil))
		if rec.Code != tc.status || !strings.Contains(rec.Body.String(), tc.want) {
			t.Errorf("%s: %d %s, want %d with %q", tc.query, rec.Code, rec.Body.String(), tc.status, tc.want)
		}
	}
}
