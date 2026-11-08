package apiservice

import (
	"testing"

	"github.com/full-chaos/dev-health-go/pep"

	"github.com/full-chaos/dev-health-ops/internal/authzenforce"
)

// go-api's decision point client is the plain PEP client: no allow cache sits in front of it, so a read on an enforced
// row asks the decision point every time and a PDP outage refuses it at once, never at a cache expiry.
func TestTheDecisionPointClientIsNotCached(t *testing.T) {
	pdp := newShadowPDPServer(t)
	proxy := mustProxyConfig(t, map[string]string{EnvAuthProxyURL: pdp.URL})
	dp := newDecisionPoint(proxy, lookupFrom(map[string]string{EnvAuthWorkloadBootstrapFile: bootstrapFile(t), EnvAuthWorkloadIssuer: testWorkloadIssuer}), nil)
	decider, err := dp.decider(workloadAsk{Flag: authzenforce.GoAPI.Env() + " lists a namespace"})
	if err != nil {
		t.Fatal(err)
	}
	if _, plain := decider.(*pep.Client); !plain {
		t.Fatalf("decision point client %T, want *pep.Client with no cache", decider)
	}
}
