package server

// CHAOS-7831: a root's class row (go_api_routing_state, "mcp:<root>") is the single serving decision for that root. The MCP listener (:8092) reads
// it in mcp_route.go; this gate reads the SAME rows, through the SAME switch type and key function, for the named-operation route acr's
// run_operation calls (the internal listener, /query), so a root that is dark on :8092 is dark on :8091 too. The document rows (routeMux) stay as
// they are: this check runs IN FRONT of them.
//
// The identity reads go through mcpProofAdmit / mcpIdentityHeadersPresent (mcp_route.go): TestOnlyInternalPathsReadTheInternalIdentity pins which
// files may import the identity package, and this one may not.
//
// Scope: only a request that arrived on the INTERNAL listener AND carries the acr identity headers (run_operation's carrier). The web app and
// ops-api reach /query with an envelope or edge bearer (and the public listener drops the headers), and a dark class root must not go dark for them.
//
// Live per request: Enabled() is a SELECT per call; nothing here caches class state. What is cached is only operation -> root fields, which is a
// property of the digest-verified document text and cannot change while the process runs.

import (
	"net/http"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/vektah/gqlparser/v2"

	"github.com/full-chaos/dev-health-ops/internal/mcpclass"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/routeswitch"
)

// newClassRowSwitch is the ONE constructor of the class-row switch: the MCP listener (:8092) and the named-operation route (:8091) both serve through it,
// so a root's class row is one decision. It reads canary and primary rows only: a SHADOW class row is dark. (routeswitch.NewProofSwitch admits shadow
// rows and is for the measurement-only proof routes; using it here would SERVE a shadow root on both ports.)
func newClassRowSwitch(pool *pgxpool.Pool, schemaDigest string) routeswitch.Switch {
	return routeswitch.NewPostgresSwitch(pool, schemaDigest, mcpRoutingDigests())
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
		if !mcpProofAdmit(r.Context()) || !mcpIdentityHeadersPresent(r.Header) {
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
