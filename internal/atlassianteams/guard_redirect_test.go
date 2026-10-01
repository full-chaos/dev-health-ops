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
