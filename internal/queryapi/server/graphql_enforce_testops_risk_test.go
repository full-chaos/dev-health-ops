package server

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp/pdptest"
)

// CHAOS-13814: a caller under a repository ceiling (an allowlist decision) is refused on Query.testopsRisk with
// ReasonNotCarriedOut, before any read, because the root is not in the cut-over registry (rootCutOver, empty). The
// answer is the same whatever the root class of the census says, so the class is a pin for the future cut-over only.
func TestGraphQLEnforceAllowlistIsRefusedOnTestopsRisk(t *testing.T) {
	doc := `query Q { testopsRisk(orgId: "` + ecOrg.String() + `", input: {startDate: "2026-01-01", endDate: "2026-01-31"}) { releaseConfidence } }`
	for name, keys := range map[string][]string{"one": {"acme/widgets"}, "empty": {}} {
		t.Run(name, func(t *testing.T) {
			srv := pdptest.New(t)
			srv.Obligations(map[string]any{"tenant_scope": ecOrg.String(), "repository_allowlist": keys})
			gate, _ := enforceGate(t, pdp.ModeEnforce, "metrics", pepOf(t, srv.URL))
			runGraphQLEnforce(t, gate, callerUser, doc).code503(t, string(pdp.ReasonNotCarriedOut))
		})
	}
}
