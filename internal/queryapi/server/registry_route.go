package server

// GET /registry, and the startup routing-state drift log.
//
// Both exist because of one failure. On 2026-09-01 an SDL change (#2065,
// 33b3f3f21d) moved the canonical schema digest from sha256:67b87d38... to
// sha256:29d509cd.... Twelve go_api_routing_state rows had been seeded
// hours earlier at the OLD digest, every one of them mode=canary,
// rollout_percentage=100. PostgresSwitch keys its lookup on
// (schema_digest, document_digest, selected_operation), so from that
// commit onward every Enabled() call returned false, every request fell
// back to Python, and both planes did so EXACTLY AS DESIGNED -- a missing
// row is the documented safe default (see PostgresSwitch's doc comment).
//
// Nothing was wrong with the fail-closed behaviour. What was wrong is
// that "no operation is enabled" and "every enablement silently died"
// produced identical, invisible output. It took six days and a
// hand-written psql query to notice.
//
// The surface here closes that (the startup drift line and its gauges were removed with the routing gate, CHAOS-8704):
//
//   - GET /registry lets `dev-hops go-api routing` ask this process --
//     not a checkout, not a checked-in mirror -- what it registers and
//     what digest it computed, so an enablement can REFUSE before writing
//     rows the running binary could never read.
//
// It is built from the same digestByOperation map and schemaDigest
// newQueryHandler hands to the switches, so it cannot drift from the
// real registration set. That is deliberately the same by-construction
// discipline mountedRouteLogMessage already uses, and for the same reason:
// the hand-typed version of that log line went stale for three waves.

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
)

// registryResponse is GET /registry's body.
//
// Deliberately minimal: the schema digest this process computed, and the
// operation -> document digest map it registered. Nothing else. Every
// value here is already public in the repository (both are pure functions
// of checked-in files), which is why this route is unauthenticated
// alongside /healthz and /readyz -- but that is the reason to keep the
// body to exactly this, not an invitation to add build paths, env, or
// pool state later. It answers one question: what does the RUNNING binary
// serve?
type registryResponse struct {
	SchemaDigest string              `json:"schema_digest"`
	Operations   []registryOperation `json:"operations"`
}

type registryOperation struct {
	Operation      string `json:"operation"`
	DocumentDigest string `json:"document_digest"`
}

// newRegistryHandler serves GET /registry from the SAME schemaDigest and
// digestByOperation map newQueryHandler registers against.
//
// The response is built once, at construction, not per request: both
// inputs are fixed for the process lifetime (the digest is a pure
// function of the embedded SDL; the map is a composite literal), so
// rebuilding per request would be pure waste and would open the door to
// the two diverging. Operations are emitted in sorted order -- Go
// randomises map iteration, so an unsorted body would differ between
// requests and make a diff of two /registry responses useless.
func newRegistryHandler(schemaDigest string, digestByOperation map[string]string) http.HandlerFunc {
	operations := make([]registryOperation, 0, len(digestByOperation))
	for _, operation := range sortedOperationNames(digestByOperation) {
		operations = append(operations, registryOperation{
			Operation:      operation,
			DocumentDigest: digestByOperation[operation],
		})
	}
	body, err := json.Marshal(registryResponse{
		SchemaDigest: schemaDigest,
		Operations:   operations,
	})
	if err != nil {
		// Unreachable in practice (two string fields and a slice of
		// strings), but a silently-empty registry body would make the
		// enablement preflight refuse for the wrong reason -- and a
		// preflight that refuses for the wrong reason teaches an operator
		// to bypass it. Fail loudly at construction instead: a library
		// panics on an impossible input; it does not exit the process.
		panic(fmt.Sprintf("query-api: /registry response could not be encoded: %v", err))
	}

	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		// nosniff makes a browser honour the declared Content-Type instead
		// of guessing one from the body.
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.WriteHeader(http.StatusOK)
		// Encoded rather than written raw: body is already the marshalled,
		// constant response computed above, so json.RawMessage passes it
		// through byte-for-byte (plus the encoder's trailing newline) --
		// there is no second marshal to diverge from it. An encode failure
		// is logged rather than dropped, matching writeRESTError's contract.
		if encodeErr := json.NewEncoder(w).Encode(json.RawMessage(body)); encodeErr != nil {
			log.Printf("query-api: /registry: encode response failed: err=%v", encodeErr)
		}
	}
}
