package server

import (
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

// CHAOS-9802: the MCP class proof route (POST /query/proof-mcp) is retired with the proof ceremony; the CI parity step
// (TestMCPClassParityVenueOracle) replaces it. No route set serves it, with or without the proof flag, and the MCP
// listener keeps its one route.
func TestTheRetiredMCPProofRouteIsServedByNoRouteSet(t *testing.T) {
	marker := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusTeapot) })
	stub := func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }
	for _, flag := range []string{"true", ""} {
		public, internal, mcp := http.NewServeMux(), http.NewServeMux(), http.NewServeMux()
		getenv := getenvFunc(func(key string) string {
			if key == proofWriteRouteEnabledEnv {
				return flag
			}
			return ""
		})
		mountQueryRouteSets(getenv, public, internal, mcp, queryRouteHandlers{
			Query: stub, Registry: stub, BuildInfo: stub, MCP: marker,
		}, graphQLEdgeDeps{})
		for name, mux := range map[string]*http.ServeMux{"public": public, "internal": internal, "mcp": mcp} {
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/query/proof-mcp", nil))
			if rec.Code == http.StatusTeapot || rec.Code == http.StatusNoContent {
				t.Errorf("flag %q: the %s set serves /query/proof-mcp (status %d)", flag, name, rec.Code)
			}
		}
	}
}

// The retired route's name must not come back in the server package's non-test sources.
func TestNoServerSourceNamesTheRetiredMCPProofRoute(t *testing.T) {
	for _, name := range []string{"newMCPProofHandler", "mountProofMCPRoute", "NewClassDecisionProofSwitch", "MCPProof:"} {
		for _, file := range []string{"query_route.go", "server.go", "class_row_gate.go", "mcp_route.go"} {
			raw, err := os.ReadFile(file)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(raw), name) {
				t.Errorf("%s names the retired %s", file, name)
			}
		}
	}
}
