package atlassianteams

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// r1 P1: the gateway client must not follow a redirect (the stored credential rides every request).
func TestGatewayHTTPClientRefusesRedirects(t *testing.T) {
	var landed atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { landed.Add(1) }))
	defer target.Close()
	redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusFound)
	}))
	defer redirector.Close()
	response, err := GatewayHTTPClient(5 * time.Second).Get(redirector.URL)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusFound || landed.Load() != 0 {
		t.Fatalf("status %d, redirected host hit %d times: the client followed a redirect", response.StatusCode, landed.Load())
	}
}

// The gateway client's own policy (CHAOS-7910): CheckRedirect refuses every redirect.
func TestGatewayHTTPClientCheckRedirectRefuses(t *testing.T) {
	client := GatewayHTTPClient(time.Second)
	if client.CheckRedirect == nil || client.CheckRedirect(nil, nil) != http.ErrUseLastResponse {
		t.Fatal("the gateway client follows redirects")
	}
}
