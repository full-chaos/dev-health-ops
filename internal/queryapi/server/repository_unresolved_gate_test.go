package server

import (
	"net/http"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp/pdptest"
)

// A well-formed PDP deny with a reason the contract names is a 403 deny counted deny, whichever reason it carries;
// a client that does not know the reason reads it as pdp_malformed and answers 503 (counted unavailable).
func TestRESTPDPEnforceDenyOfRepositoryUnresolvedIs403LikeAnyWireReason(t *testing.T) {
	for _, reason := range []string{"role_lacks_action", "repository_unresolved"} {
		t.Run(reason, func(t *testing.T) {
			srv := pdptest.New(t)
			srv.Deny(reason)
			g := newGatedREST(t, pdp.ModeEnforce, "analytics", srv.URL, true)
			if got := g.do(http.MethodGet, g.userToken()); got != gatedForbidden {
				t.Fatalf("deny %s: got %+v, want %+v", reason, got, gatedForbidden)
			}
			results := g.counter(t, "dev_health_query_api_pdp_results_total")
			if len(results) != 1 || results["action=analytics.read,mode=enforce,result=deny"] != 1 {
				t.Fatalf("results %v, want exactly one deny", results)
			}
		})
	}
}
