package server

import (
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// CHAOS-9802: the MCP class proof route (POST /query/proof-mcp) is retired with the proof ceremony; the CI parity step
// (TestMCPClassParityVenueOracle) replaces it. No route set serves it, with or without the proof flag: every set
// answers 404, so a handler that refuses it with 401, 403 or 500 would fail here too.
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
			for _, method := range []string{http.MethodPost, http.MethodGet} {
				rec := httptest.NewRecorder()
				mux.ServeHTTP(rec, httptest.NewRequest(method, "/query/proof-mcp", nil))
				if rec.Code != http.StatusNotFound {
					t.Errorf("flag %q: %s /query/proof-mcp on the %s set answers %d, want 404", flag, method, name, rec.Code)
				}
			}
		}
	}
}

// No non-test Go source of the module names the retired route or the symbols that served and measured it. The scan walks
// every package under internal and cmd, so a client (the prove command, an enable error text) or a doc comment cannot
// keep pointing at a route that is gone.
func TestNoSourceNamesTheRetiredMCPProofRoute(t *testing.T) {
	retired := []string{"proof-mcp", "newMCPProofHandler", "mountProofMCPRoute", "NewClassDecisionProofSwitch", "MCPProof:", "MCPClassReceipts", "requireMCPProofURL"}
	root := filepath.Join("..", "..", "..")
	checked := 0
	for _, dir := range []string{"internal", "cmd"} {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			checked++
			for _, name := range retired {
				if strings.Contains(string(raw), name) {
					t.Errorf("%s names the retired %s", path, name)
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if checked < 1000 {
		t.Fatalf("the scan read %d Go files: the walk did not reach the module (wrong root?)", checked)
	}
}
