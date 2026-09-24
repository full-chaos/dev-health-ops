//go:build integration

package server

// Red-first proof for metrics_route.go: the unit tests elsewhere in this
// package (registry_drift_telemetry_test.go, readyz tests in main_test.go)
// already prove each recorder writes the value it claims to a meter -- none
// of them prove that value is reachable over the wire this binary actually
// serves. Before newPrometheusMeterProvider existed, GET /metrics 404'd
// (main() never mounted the route at all), so this is the genuine
// end-to-end claim: start the real handlers this package ships, drive them
// exactly as production traffic would, and read the result back off an
// actual HTTP response.

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMetricsRoute_ServesDriftAndReadyzGaugesOverHTTP(t *testing.T) {
	// Same mux shape main() assembles: healthz, readyz, metrics all on one
	// handler, hit over real HTTP rather than called as bare functions.
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", healthzHandler())
	mux.HandleFunc("/readyz", readyzHandler(nil))
	mux.Handle("/metrics", metricsHandler(metricsRegistryForTests))

	server := httptest.NewServer(mux)
	defer server.Close()

	// Drives readyzOutcomeCounter through the real handler, not a direct
	// call to recordReadyzOutcome -- proves the wiring from an actual
	// request down to the gauge, not just that the recorder function
	// itself works.
	readyzResp, err := http.Get(server.URL + "/readyz")
	if err != nil {
		t.Fatalf("GET /readyz: %v", err)
	}
	readyzResp.Body.Close()

	// The drift recorder needs no live Postgres pool -- logRoutingStateDrift
	// is the piece that queries one, and this test's concern stops at "does
	// a value this recorder wrote reach /metrics", which
	// registry_drift_telemetry_test.go already isolates the same way.
	defaultRecordRoutingRowsForDigest(t.Context(), map[string]int64{"sha256:live": 3}, "sha256:live")

	metricsResp, err := http.Get(server.URL + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer metricsResp.Body.Close()

	if metricsResp.StatusCode != http.StatusOK {
		t.Fatalf("GET /metrics status = %d, want 200", metricsResp.StatusCode)
	}
	if ct := metricsResp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Errorf("GET /metrics Content-Type = %q, want a text/plain prefix", ct)
	}

	body, err := io.ReadAll(metricsResp.Body)
	if err != nil {
		t.Fatalf("read /metrics body: %v", err)
	}
	text := string(body)

	for _, name := range []string{
		"devhealth_query_api_routing_rows_for_digest",
		"devhealth_query_api_routing_rows_total",
		"devhealth_query_api_readyz_total",
	} {
		if !strings.Contains(text, name) {
			t.Errorf("GET /metrics body missing %q\nfull body:\n%s", name, text)
		}
	}
}
