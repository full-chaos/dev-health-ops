package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/principal"
)

// The proof variant of the MCP class is mounted on the INTERNAL route set only,
// only with its env flag, and never on the public or the MCP listener's set: the
// MCP listener has one route and a caller on it can never reach a root whose row
// is shadow.
func TestMCPProofRouteIsMountedOnTheInternalSetOnlyAndOnlyWithItsFlag(t *testing.T) {
	marker := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusTeapot) })
	stub := func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }
	build := func(flag string) (public, internal, mcp *http.ServeMux) {
		public, internal, mcp = http.NewServeMux(), http.NewServeMux(), http.NewServeMux()
		getenv := getenvFunc(func(key string) string {
			if key == proofWriteRouteEnabledEnv {
				return flag
			}
			return ""
		})
		mountQueryRouteSets(getenv, public, internal, mcp, queryRouteHandlers{
			Query: stub, Registry: stub, BuildInfo: stub, MCP: marker, MCPProof: marker,
		}, graphQLEdgeDeps{})
		return public, internal, mcp
	}
	status := func(mux *http.ServeMux) int {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/query/proof-mcp", nil))
		return rec.Code
	}
	public, internal, mcp := build("true")
	if got := status(internal); got != http.StatusTeapot {
		t.Fatalf("internal set with the flag: status %d, want the proof handler", got)
	}
	if got := status(public); got == http.StatusTeapot {
		t.Fatal("the public set serves /query/proof-mcp")
	}
	if got := status(mcp); got == http.StatusTeapot {
		t.Fatal("the MCP listener's set serves /query/proof-mcp: a caller there could read a shadow root")
	}
	_, internal, _ = build("")
	if got := status(internal); got == http.StatusTeapot {
		t.Fatal("the proof route is mounted without GO_API_PROOF_WRITE_ROUTE_ENABLED")
	}
}

// The MCP route's own handler and its proof variant are one pipeline: the same
// concrete type, differing only in the switch, the admission and the identity.
func TestMCPProofHandlerIsTheSamePipelineAsTheMCPHandler(t *testing.T) {
	base := newMCPHandler(&countingMCPClient{}, nil, allMCPRootsEnabled(), getenvFunc(func(string) string { return "" }))
	if _, ok := base.(*mcpHandler); !ok {
		t.Fatalf("the MCP route is %T, want *mcpHandler", base)
	}
	verifier, err := principal.NewVerifier(t.TempDir()+"/missing-jwks.json", "test-issuer", "test-audience")
	if err != nil {
		t.Fatal(err)
	}
	allowed := func(context.Context, string) bool { return true }
	if got := newMCPProofHandler(base, allMCPRootsEnabled(), verifier, allowed); got == nil {
		t.Fatal("a complete proof handler was not built")
	}
	// Each of the four parts is required on its own: a proof handler missing any of
	// them would admit anyone, or serve through no switch.
	for name, build := range map[string]func() http.Handler{
		"no verifier":  func() http.Handler { return newMCPProofHandler(base, allMCPRootsEnabled(), nil, allowed) },
		"no allowlist": func() http.Handler { return newMCPProofHandler(base, allMCPRootsEnabled(), verifier, nil) },
		"no switch":    func() http.Handler { return newMCPProofHandler(base, nil, verifier, allowed) },
		"not an MCP handler": func() http.Handler {
			return newMCPProofHandler(http.NotFoundHandler(), allMCPRootsEnabled(), verifier, allowed)
		},
	} {
		if got := build(); got != nil {
			t.Errorf("%s: a proof handler was built", name)
		}
	}
}

// A nil handler with the flag on mounts nothing (and does not panic at mount time).
func TestMountProofMCPRouteWithANilHandlerMountsNothing(t *testing.T) {
	mux := http.NewServeMux()
	mountProofMCPRoute(getenvFunc(func(string) string { return "true" }), mux, nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/query/proof-mcp", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status %d, want 404: a nil handler was mounted", rec.Code)
	}
}
