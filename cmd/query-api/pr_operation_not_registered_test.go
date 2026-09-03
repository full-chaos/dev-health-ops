package main

// CHAOS-4980 condition 2 (team-lead ruling): the Pr resolver ships in this
// PR as a PARTIAL port (linkedIssues + a nil-for-unknown existence check
// only -- see schema.resolvers.go's Pr doc comment and
// workgraph/pr.go's). It must not be reachable as a served GraphQL
// operation until the PR core row, reviews, and commits are also ported
// (a follow-up ticket) -- routeswitch.Mux only dispatches an operation
// that appears in query_route.go's digestByOperation map (see
// routeswitch.go's package doc comment: "An operation with a registered
// handler but a disabled switch is NOT reachable"; an operation with no
// entry at all is stronger than merely disabled -- PostgresSwitch/
// StaticSwitch can never be flipped on for a name that was never
// registered in the first place).
//
// This test derives digestByOperation from source on every run (reusing
// registered_document_field_gate_test.go's own parseQueryRouteDocuments
// helper, same package/directory) rather than hand-copying the operation
// list -- exactly this file's neighbor's own stated discipline, so a
// future accidental registration of a "pr"-named operation fails this
// gate immediately instead of silently making the partial port
// reachable.
import "testing"

func TestPrOperationIsNotRegistered(t *testing.T) {
	repoRoot := documentFieldGateRepoRoot(t)
	_, operationToIdentifier := parseQueryRouteDocuments(t, repoRoot)

	for _, candidate := range []string{"pr", "Pr", "prDetail", "PrDetail"} {
		if identifier, registered := operationToIdentifier[candidate]; registered {
			t.Fatalf("operation %q is registered against %q in query_route.go's digestByOperation -- "+
				"this makes the Pr resolver's PARTIAL port (see schema.resolvers.go's Pr doc comment) "+
				"reachable via routeswitch before the PR core row/reviews/commits ports land; "+
				"do not register it until that follow-up ticket lands", candidate, identifier)
		}
	}
}
