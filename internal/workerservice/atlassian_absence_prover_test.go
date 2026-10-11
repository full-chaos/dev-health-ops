package workerservice

import (
	"testing"

	"atlassian/atlassian/graph"

	"github.com/full-chaos/dev-health-ops/internal/atlassianteams"
)

// The run takes the absence prover from the client it read with (CHAOS-9124): the
// real gateway client can ask, so a run closes on the gateway's answers; a client
// that cannot ask gives no prover, and nothing closes.
func TestTheAtlassianRunTakesItsAbsenceProverFromTheClientItReadWith(t *testing.T) {
	prover := atlassianAbsenceProver(&graph.Client{}, "site-uuid")
	gateway, ok := prover.(*atlassianteams.GatewayAbsence)
	if !ok || gateway.SiteID != "site-uuid" || gateway.Reads == nil {
		t.Fatalf("the prover of the real gateway client = %#v, want a GatewayAbsence of the site", prover)
	}
	if prover := atlassianAbsenceProver(readOnlyClient{}, "site-uuid"); prover != nil {
		t.Fatalf("the prover of a client that cannot ask = %#v, want none", prover)
	}
}

// readOnlyClient reads the lists and cannot ask TeamById.
type readOnlyClient struct{ atlassianteams.Client }
