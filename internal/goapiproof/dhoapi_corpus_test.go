package goapiproof

import (
	"slices"
	"testing"
)

// TestDHOAPICorpusPinsTheReadRoutes names the routes the corpus must cover.
// The mounted-route test only proves every entry is a real route; this pins
// the other direction, so deleting an entry (or its run-order line) fails
// here instead of silently shrinking what a run measures.
func TestDHOAPICorpusPinsTheReadRoutes(t *testing.T) {
	want := []string{
		"REST:GET:/ready",
		"REST:GET:/health",
		"REST:GET:/health/workers",
		"REST:GET:/api/v1/webhooks/health",
		"REST:GET:/api/v1/orgs/me",
		"REST:GET:/api/v1/licensing/entitlements/{org_id}",
		"REST:GET:/api/v1/telemetry/status",
	}
	if got := RESTRunOrderFor(RESTServiceDHOAPI); !slices.Equal(got, want) {
		t.Fatalf("dho-api run order = %v, want %v", got, want)
	}
	for _, operation := range want {
		spec, err := SpecForREST(operation)
		if err != nil {
			t.Fatal(err)
		}
		if spec.EffectiveService() != RESTServiceDHOAPI {
			t.Errorf("%s targets %s", operation, spec.EffectiveService())
		}
	}
	if !restOperatorSuppliedProducers[dhoAPIOrgIDProducer] {
		t.Errorf("%s must be an operator-supplied producer: the entitlements path binds it", dhoAPIOrgIDProducer)
	}
}
