package providerfamilycontract

import (
	"strings"
	"testing"
)

// The dispatcher stores ValidateClaim's error text in sync_run_units.error. It is stored as
// returned, so this pins that no input (here a credential-shaped flag name and dataset)
// can reach the text: every refusal is exactly ErrInvalidClaim.
func TestValidateClaimErrorNeverEchoesInput(t *testing.T) {
	marker := "ghp_aaaaaaaaaaaaaaaaaaaaaaaa"
	var refusals int
	for _, policy := range policies {
		for provider := range policy.providers {
			for _, dataset := range []string{policy.CanonicalDataset, marker} {
				flags := map[string]bool{familyDatasetFlagPrefix + marker: true, marker: false}
				err := ValidateClaim(provider, dataset, flags, true)
				if err == nil {
					continue
				}
				refusals++
				if err != ErrInvalidClaim || strings.Contains(err.Error(), marker) {
					t.Fatalf("ValidateClaim(%q, %q) = %q", provider, dataset, err)
				}
			}
		}
	}
	if refusals == 0 {
		t.Fatal("no refusal was exercised")
	}
}
