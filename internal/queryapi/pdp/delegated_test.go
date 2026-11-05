package pdp

import (
	"context"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-go/pep"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp/pdptest"
)

// delegatedObligations are the obligations auth-service sets on a delegated allow of an organization read.
func delegatedObligations(assurance string) map[string]any {
	return map[string]any{"tenant_scope": strings.ToLower(testOrg), "repository_scope": "organization", "minimum_assurance": assurance}
}

// A delegated allow carries minimum_assurance. The gate carries out aal1 (every accepted session bearer is aal1);
// any higher or unknown level stays no verdict: a local deny, counted unavailable.
func TestPDPEnforceCarriesOutMinimumAssuranceAAL1Only(t *testing.T) {
	for _, tc := range []struct {
		assurance string
		pass      bool
	}{
		{"aal1", true},
		{"aal2", false},
		{"aal3", false},
		{"aal9", false},
		{"", false},
	} {
		t.Run("minimum_assurance="+tc.assurance, func(t *testing.T) {
			srv := pdptest.New(t)
			srv.Obligations(delegatedObligations(tc.assurance))
			m, cfg := newMeter(t)
			cfg.Mode, cfg.Decider = ModeEnforce, pepClient(t, srv.URL)
			err := New(cfg).Authorize(context.Background(), testProfile, testBearer, testOrg)
			got := m.points(t, resultMetric)
			if tc.pass {
				if err != nil || got["action="+testAction+",mode=enforce,result=allow"] != 1 {
					t.Fatalf("answered %v with results %v, want a pass counted allow", err, got)
				}
				return
			}
			denied, ok := err.(*DenyError)
			if !ok || !denied.Local || (denied.Reason != pep.ReasonUnknownObligation && denied.Reason != pep.ReasonMalformed) {
				t.Fatalf("answered %#v, want a local unknown_obligation or malformed deny", err)
			}
		})
	}
}
