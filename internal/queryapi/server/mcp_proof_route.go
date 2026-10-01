package server

// CHAOS-7214: the proof variant of the MCP caller class's route.
//
// WHY. A class row for a root field is enabled only after a proof receipt for
// that root exists, and the listener answers 404 root_field_not_enabled for a
// root without a canary|primary row -- so the root cannot be measured through
// the listener before it is enabled. The doc operations broke the same
// circularity with /query/proof (a measurement-only Switch that also admits
// shadow) and /query/proof-write (an org allowlist). This is the same device
// for the class: POST /query/proof-mcp, the SAME handler pipeline over
// routeswitch.ProofSwitch (shadow rows are reachable), on the INTERNAL
// listener only, behind the same gates as /query/proof-write:
//
//   - GO_API_PROOF_WRITE_ROUTE_ENABLED=true (off by default; the route is
//     simply not mounted otherwise);
//   - an envelope bearer verified by the same principal.Verifier /buildinfo
//     and /query use (no internal identity headers, no edge token): the
//     prover is an operator, not acr-api;
//   - the envelope's org must be on the go_api_proof_orgs allowlist (empty by
//     default; a store error refuses).
//
// What it deliberately is NOT: a way for a caller on the MCP listener to read
// a root whose row is shadow. mcpRoute and this route never share a switch,
// and the MCP listener's mux has no such path (a test pins both).
//
// What it does not prove: the 8092 listener's own header parsing, its
// identity middleware and the NetworkPolicy boundary. Those are covered by
// the CHAOS-7215 enablement probe and by a handler-level test that both
// routes are one *mcpHandler type with one ServeHTTP.
//
// The org is the envelope's, with every elevated claim dropped: the pipeline
// sees exactly the claims the MCP listener would hand it (org only, no role,
// no superuser, no impersonation).

import (
	"context"
	"log"
	"net/http"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/platform/version"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/principal"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/routeswitch"
)

// Refusal reasons of the proof variant's own gates (the pipeline's own
// reasons are mcpReason*). A closed vocabulary, never a request value.
const (
	mcpReasonProofOffInternal = "proof_off_internal_listener"
	mcpReasonProofCarrier     = "proof_carrier"
	mcpReasonProofOrg         = "proof_org_not_allowed"
)

// newMCPProofHandler derives the proof variant from the MCP route's own
// handler: a copy that differs in the switch, in where it may be reached and
// in how the caller is identified, and in nothing else.
func newMCPProofHandler(base http.Handler, sw routeswitch.Switch, verifier *principal.Verifier, orgAllowed func(context.Context, string) bool) http.Handler {
	h, ok := base.(*mcpHandler)
	if !ok || h == nil || verifier == nil || orgAllowed == nil || sw == nil {
		return nil
	}
	proof := *h
	proof.sw = sw
	proof.admit = mcpProofAdmit
	proof.authenticate = func(r *http.Request) (authctx.Claims, int, string) {
		if mcpIdentityHeadersPresent(r.Header) {
			return authctx.Claims{}, http.StatusUnauthorized, mcpReasonProofCarrier
		}
		values := r.Header.Values("Authorization")
		if len(values) != 1 {
			return authctx.Claims{}, http.StatusUnauthorized, mcpReasonProofCarrier
		}
		token, ok := bearerToken(values[0])
		if !ok {
			return authctx.Claims{}, http.StatusUnauthorized, mcpReasonProofCarrier
		}
		verified, err := verifier.Verify(principal.WithRequestMeta(r.Context(), r.RemoteAddr, envelopeRequestID(r)), token)
		if err != nil {
			return authctx.Claims{}, http.StatusUnauthorized, mcpReasonProofCarrier
		}
		org := verified.OrgID
		if org == "" || org != strings.TrimSpace(org) || !orgAllowed(r.Context(), org) {
			log.Printf("query-api: /query/proof-mcp refused: org_id=%q is not on the proof-org allowlist", org)
			return authctx.Claims{}, http.StatusForbidden, mcpReasonProofOrg
		}
		return authctx.Claims{OrgID: org}, 0, ""
	}
	return &proof
}

// mountProofMCPRoute mounts POST /query/proof-mcp on internalMux ONLY. It
// shares GO_API_PROOF_WRITE_ROUTE_ENABLED with /query/proof-write: both are
// the internal listener's operator proof routes, and a deployment that wants
// one has opted into the proof-org allowlist for both.
func mountProofMCPRoute(getenv getenvFunc, internalMux *http.ServeMux, handler http.Handler) {
	enabled := strings.EqualFold(strings.TrimSpace(getenv(proofWriteRouteEnabledEnv)), "true")
	switch {
	case !enabled:
		log.Printf("query-api: /query/proof-mcp NOT registered: %s is not \"true\" (routes_registered=0); MCP class rows cannot be proven in this deployment", proofWriteRouteEnabledEnv)
		return
	case handler == nil:
		log.Printf("query-api: /query/proof-mcp NOT registered: no handler was built (routes_registered=0)")
		return
	}
	internalMux.Handle("/query/proof-mcp", withProofProvenance(handler.ServeHTTP, version.Current("query-api").Commit))
	log.Printf("query-api: /query/proof-mcp REGISTERED (routes_registered=1, %s=true): internal listener only, measurement-only (admits shadow), envelope-authenticated, proof-org allowlist gated", proofWriteRouteEnabledEnv)
}
