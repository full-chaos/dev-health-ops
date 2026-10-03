package synccli

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// The client a production binary builds (cited in the CHAOS-7910 sweep) follows a redirect to another host, but the
// credential header does not go with it.
func TestTheDefaultProductionDoerDropsTheCredentialOnAHostChange(t *testing.T) {
	var hit bool
	var got string
	target := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { hit, got = true, r.Header.Get("Authorization") }))
	defer target.Close()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer origin.Close()
	request, _ := http.NewRequest(http.MethodGet, origin.URL, nil)
	request.Header.Set("Authorization", "Bearer SECRET")
	response, err := productionDoer().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if !hit || got != "" {
		t.Fatalf("followed=%v, Authorization on the other host %q: want followed with none", hit, got)
	}
}
