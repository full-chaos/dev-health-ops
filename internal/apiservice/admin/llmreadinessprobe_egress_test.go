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

// An empty base_url probes OpenAI's public endpoint, so the unconditional
// dial guard never blocks the default path; and the default prober's client
// really is the guarded one (its dial refuses a loopback address, not only the
// endpoint string being public).
func TestReadinessProbeEmptyBaseURLTargetsThePublicDefault(t *testing.T) {
	endpoint := chatCompletionsEndpoint("")
	if !strings.HasPrefix(endpoint, "https://api.openai.com/") {
		t.Fatalf("empty base_url endpoint = %q, want OpenAI's public endpoint", endpoint)
	}
	client, ok := newOpenAICompatibleReadinessProber(nil).client.(*http.Client)
	if !ok {
		t.Fatalf("the default prober client is %T, want *http.Client", newOpenAICompatibleReadinessProber(nil).client)
	}
	transport, ok := client.Transport.(*http.Transport)
	if !ok || transport.DialContext == nil || transport.Proxy != nil {
		t.Fatalf("the default transport = %#v, want a proxy-free transport with a guarded dialer", client.Transport)
	}
	conn, err := transport.DialContext(t.Context(), "tcp", "127.0.0.1:9")
	if err == nil {
		conn.Close()
		t.Fatal("the default prober dialled a loopback address")
	}
	if !strings.Contains(err.Error(), "Connection to private/internal networks is not allowed") {
		t.Fatalf("dial error = %v, want the internal-network refusal", err)
	}
}
