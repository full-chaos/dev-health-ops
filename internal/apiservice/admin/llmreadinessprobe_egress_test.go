package admin

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

// CHAOS-2558: the readiness probe sends the org's saved key to the org's
// base_url; the production client dials through the SSRF-guarded transport,
// so an internal address is refused at dial time and the key never leaves.
func TestReadinessProbeRefusesAnInternalBaseURLAtDial(t *testing.T) {
	var hits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
	}))
	defer server.Close()
	prober := newOpenAICompatibleReadinessProber(nil)
	outcome, code := prober.probe(t.Context(), "openai", "m", server.URL, "org-secret-key")
	if hits.Load() != 0 {
		t.Fatalf("the internal server received %d requests (the key was sent)", hits.Load())
	}
	if outcome == "ready" {
		t.Fatal("an internal base_url reported ready")
	}
	if code != nil && strings.Contains(*code, "org-secret-key") {
		t.Fatalf("safe error code %q carries the key", *code)
	}
}
