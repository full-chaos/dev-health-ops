package server

import (
	"net/http"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/internalidentity"
)

// The MCP route does not depend on the envelope key: a request that reached the handler through the MCP listener is
// refused for its media type, not for a missing mount. Only a request that skipped the listener middleware gets the
// off-listener 404, which a mount probe must not mistake for an unmounted route.
func TestMCPRouteAnswersWithoutEnvelopeKey(t *testing.T) {
	mux, mcpMux := http.NewServeMux(), http.NewServeMux()
	mountQueryRouteSets(absentKeyEnv(nil), mux, http.NewServeMux(), mcpMux, queryRouteHandlers{
		Query:     http.NotFoundHandler().ServeHTTP,
		Registry:  http.NotFoundHandler().ServeHTTP,
		BuildInfo: http.NotFoundHandler().ServeHTTP,
		MCP:       newMCPHandler(&countingMCPClient{}, nil, allMCPRootsEnabled(), absentKeyEnv(nil)),
	}, graphQLEdgeDeps{})
	if got := probe(mcpMux, http.MethodPost, "/query"); got != http.StatusNotFound {
		t.Fatalf("off the MCP listener: status %d, want the off-listener 404", got)
	}
	if got := probe(internalidentity.MCP(mcpMux), http.MethodPost, "/query"); got != http.StatusUnsupportedMediaType {
		t.Fatalf("on the MCP listener: status %d, want 415", got)
	}
}
