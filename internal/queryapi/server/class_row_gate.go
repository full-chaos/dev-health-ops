package server

// CHAOS-7831: a root's class row (go_api_routing_state, "mcp:<root>") is the single serving decision for that root. The MCP listener (:8092) reads
// it in mcp_route.go; this gate reads the SAME rows, through the SAME switch type and key function, for the named-operation route acr's
// run_operation calls (the internal listener, /query), so a root that is dark on :8092 is dark on :8091 too. The document rows (routeMux) stay as
// they are: this check runs IN FRONT of them.
//
// The identity reads go through mcpProofAdmit / mcpIdentityHeadersPresent (mcp_route.go): TestOnlyInternalPathsReadTheInternalIdentity pins which
// files may import the identity package, and this one may not.
//
// Scope: only a request that came through the dedicated route (/query/run-operation, markClassGated), on the INTERNAL listener, carrying the acr identity
// headers. /query itself is NOT gated: the Python /graphql edge forwards the web's requests to it over the same internal identity carrier (the Venue
// oracle TestGraphQLEdgeVenueOracle pins that), and a dark class root must not go dark for the web.
//
// Live per request: Enabled() is a SELECT per call; nothing here caches class state. What is cached is only operation -> root fields, which is a
// property of the digest-verified document text and cannot change while the process runs.

import (
	"context"
	"net/http"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/vektah/gqlparser/v2"

	"github.com/full-chaos/dev-health-ops/internal/mcpclass"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/routeswitch"
)

// newClassRowSwitch is the ONE constructor of the class-row switch: the MCP listener (:8092) and the named-operation route (:8091) both serve through it,
// so a root's decision is one row (go_api_class_decision, keyed by operation: no schema digest, CHAOS-8735). It serves canary and primary decisions
// only: a SHADOW decision is dark. (routeswitch.NewClassDecisionProofSwitch admits shadow and is for the measurement-only proof route; using it here
// would SERVE a shadow root on both ports.)
func newClassRowSwitch(pool *pgxpool.Pool) routeswitch.Switch {
	return routeswitch.NewClassDecisionSwitch(pool, mcpRoutingDigests())
}

// runOperationPath is the internal-listener route acr's run_operation posts to. Only requests that came through it are class-gated.
const runOperationPath = "/query/run-operation"

type classGateKey struct{}

// markClassGated marks every request served through next as arriving by the class-gated route.
func markClassGated(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		next(w, r.WithContext(context.WithValue(r.Context(), classGateKey{}, true)))
	}
}

func classGated(ctx context.Context) bool {
	on, _ := ctx.Value(classGateKey{}).(bool)
	return on
}

// mountRunOperationRoute registers the class-gated route on the internal mux, with the same provenance wrapper /query carries.
func mountRunOperationRoute(internalMux *http.ServeMux, handler http.HandlerFunc) {
	if handler == nil {
		return
	}
	internalMux.HandleFunc(runOperationPath, withProofProvenance(handler, runningBuild()))
}

// documentGate refuses a resolved operation before its document-row dispatch. It returns true when it wrote the refusal.
type documentGate func(w http.ResponseWriter, r *http.Request, operation, query string) (refused bool)

const classRowRefusalMessage = "a root field is not enabled for the MCP caller class"

// newClassRowGate builds the gate over sw, the class-row switch (the one type :8092 uses: routeswitch.NewPostgresSwitch over mcpRoutingDigests()).
func newClassRowGate(sw routeswitch.Switch) documentGate {
	schema := graph.NewExecutableSchema(graph.Config{}).Schema()
	// digest of the document text -> []string, the root fields of that document. Keyed by the DOCUMENT digest, not the operation name: an operation
	// may be registered under more than one document (CHAOS-8000), and each document's own roots decide, so the check holds for every one of them.
	var rootsByDocument sync.Map
	rootsOf := func(query string) ([]string, bool) {
		key := digestHex(query)
		if cached, ok := rootsByDocument.Load(key); ok {
			return cached.([]string), true
		}
		doc, errs := gqlparser.LoadQuery(schema, query)
		if len(errs) > 0 || len(doc.Operations) != 1 {
			return nil, false
		}
		roots, _ := mcpRootFields(doc.Operations[0].SelectionSet, doc.Fragments)
		rootsByDocument.Store(key, roots)
		return roots, true
	}
	allowed := mcpclass.AllowedRoots()
	return func(w http.ResponseWriter, r *http.Request, operation, query string) bool {
		if !classGated(r.Context()) || !mcpProofAdmit(r.Context()) || !mcpIdentityHeadersPresent(r.Header) {
			return false
		}
		roots, ok := rootsOf(query)
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
