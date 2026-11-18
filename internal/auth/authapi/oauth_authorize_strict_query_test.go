package authapi

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// GET /oauth/authorize stays strict while the other auth routes read their
// query with httpapi.Query: a valid authorize query plus a pair the shared
// reader would keep (a ";", a bad escape) is refused 400 with no redirect
// and no request row.
func TestAuthorizeStaysStrictOnPairsTheSharedReaderKeeps(t *testing.T) {
	valid := browserQuery().Encode()
	if r := newPlainOAuthFixture(t).get(browserQuery()); r.Code != http.StatusFound {
		t.Fatalf("the valid query answers %d, want 302", r.Code)
	}
	for _, raw := range []string{valid + "&x=a;b", valid + ";", valid + "&x=%zz", valid + "%zz"} {
		o := newPlainOAuthFixture(t)
		request := httptest.NewRequest(http.MethodGet, PatternOAuthAuthorize+"?"+raw, nil)
		r := httptest.NewRecorder()
		o.mux.ServeHTTP(r, request)
		if r.Code != http.StatusBadRequest || r.Header().Get("Location") != "" || len(o.store.requests) != 0 {
			t.Errorf("%s: status %d, location %q, %d rows; want 400, none, 0", raw, r.Code, r.Header().Get("Location"), len(o.store.requests))
		}
	}
}
