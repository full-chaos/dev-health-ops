package server

// CHAOS-7831: a root's class row (go_api_routing_state, "mcp:<root>") is the single serving decision for that root. The MCP listener (:8092) reads
// it in mcp_route.go; this gate reads the SAME rows, through the SAME switch type and key function, for the named-operation route acr's
// run_operation calls (the internal listener, /query), so a root that is dark on :8092 is dark on :8091 too. The document rows (routeMux) stay as
// they are: this check runs IN FRONT of them.
//
// Scope: only a request that arrived on the INTERNAL listener AND carries the acr identity headers (run_operation's carrier). The web app and
// ops-api reach /query with an envelope or edge bearer (and the public listener drops the headers), and a dark class root must not go dark for them.
//
// Live per request: Enabled() is a SELECT per call; nothing here caches class state. What is cached is only operation -> root fields, which is a
// property of the digest-verified document text and cannot change while the process runs.

import (
	"net/http"
	"sync"

	"github.com/vektah/gqlparser/v2"

	"github.com/full-chaos/dev-health-ops/internal/mcpclass"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/internalidentity"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/routeswitch"
)

// documentGate refuses a resolved operation before its document-row dispatch. It returns true when it wrote the refusal.
type documentGate func(w http.ResponseWriter, r *http.Request, operation, query string) (refused bool)

const classRowRefusalMessage = "a root field is not enabled for the MCP caller class"

// newClassRowGate builds the gate over sw, the class-row switch (the one type :8092 uses: routeswitch.NewPostgresSwitch over mcpRoutingDigests()).
func newClassRowGate(sw routeswitch.Switch) documentGate {
	schema := graph.NewExecutableSchema(graph.Config{}).Schema()
	var rootsByOperation sync.Map // operation -> []string; the root fields of its registered document
	rootsOf := func(operation, query string) ([]string, bool) {
		if cached, ok := rootsByOperation.Load(operation); ok {
			return cached.([]string), true
		}
		doc, errs := gqlparser.LoadQuery(schema, query)
		if len(errs) > 0 || len(doc.Operations) != 1 {
			return nil, false
		}
		roots, _ := mcpRootFields(doc.Operations[0].SelectionSet, doc.Fragments)
		rootsByOperation.Store(operation, roots)
		return roots, true
	}
	allowed := mcpclass.AllowedRoots()
	return func(w http.ResponseWriter, r *http.Request, operation, query string) bool {
		if !internalidentity.OnInternalListener(r.Context()) || !internalidentity.Present(r.Header) {
			return false
		}
		roots, ok := rootsOf(operation, query)
		if !ok {
			// A registered document that cannot be read cannot be shown to be outside the class: fail closed.
			writeMCPRefusal(w, http.StatusNotFound, mcpReasonRootFieldNotEnabled, classRowRefusalMessage)
			return true
		}
		for _, root := range roots {
			if allowed[root] && !sw.Enabled(mcpclass.Operation(root)) {
				writeMCPRefusal(w, http.StatusNotFound, mcpReasonRootFieldNotEnabled, classRowRefusalMessage)
				return true
			}
		}
		return false
	}
}
