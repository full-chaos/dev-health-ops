package workloadexchange

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

// The exchange compares subject_token_type with these constants; the endpoint-profile gate selects the class by the
// registry marker. A constant that drifts from its marker would make the code and the contract disagree.
func TestTokenTypeConstantsEqualTheRegistryMarkers(t *testing.T) {
	registry, err := contracts.LoadRegistryClasses(repoRoot)
	if err != nil {
		t.Fatal(err)
	}
	for class, want := range map[string]string{
		"kubernetes_sa_token_exchange":  TokenTypeJWT,
		"workload_bootstrap_credential": TokenTypeWorkloadBootstrap,
	} {
		var markers []string
		for _, marker := range registry[class].Markers {
			if marker.Kind == "subject_token_type" {
				markers = append(markers, marker.Name)
			}
		}
		if len(markers) != 1 || markers[0] != want {
			t.Errorf("class %s: subject_token_type markers %q, the code's constant is %q", class, markers, want)
		}
	}
}
