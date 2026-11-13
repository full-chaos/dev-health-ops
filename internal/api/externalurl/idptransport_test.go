package externalurl

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const testIdPAllowEnv = "SSO_EGRESS_ALLOW_CIDRS"

// countingServer is a server on 127.0.0.1 that counts the requests it receives; its body is bounded.
func countingServer(t *testing.T) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var hits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		_, _ = w.Write([]byte("ok"))
	}))
	t.Cleanup(server.Close)
	return server, &hits
}

// The SSO list never reaches GuardedTransport: credentials, teams identity, the LLM readiness probes and categorize
// share it, and it still refuses 127.0.0.1 with the list naming it.
func TestSSOEgressAllowNeverWidensGuardedTransport(t *testing.T) {
	t.Setenv(testIdPAllowEnv, "127.0.0.1/32")
	server, hits := countingServer(t)
	client := &http.Client{Transport: GuardedTransport(), Timeout: 5 * time.Second}
	response, err := client.Get(server.URL)
	if err == nil {
		response.Body.Close()
		t.Fatal("GuardedTransport reached 127.0.0.1 with the SSO list set")
	}
	if !strings.Contains(err.Error(), "Connection to private/internal networks is not allowed") {
		t.Fatalf("error = %v, want the internal-network refusal", err)
	}
	if hits.Load() != 0 {
		t.Fatalf("the server saw %d request(s)", hits.Load())
	}
}
