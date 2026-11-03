package acraudience

import "testing"

func TestOnlyTheTwoACRAudiencesAreRefused(t *testing.T) {
	for _, audience := range []string{API, MCP} {
		if !Is(audience) || Refuse("owner", audience) == nil {
			t.Errorf("%q is not refused", audience)
		}
	}
	for _, audience := range []string{"dev-health-api", "auth-service", "query-api", "", "acr", "ACR-API", "acr-api "} {
		if Is(audience) || Refuse("owner", audience) != nil {
			t.Errorf("%q is refused", audience)
		}
	}
}
