package syncdispatchruntime

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// CHAOS-7132: a run failed by reference discovery used to read only error_category
// "reference_discovery_failed". The unit result now also carries a fixed-vocabulary, value-free
// reason and the run's result copies the first failed unit's.
func TestUnitDiscoveryFailureResultCarriesAValueFreeReason(t *testing.T) {
	cause := fmt.Errorf("resolve client: %w", providerfoundation.ValidateCredentialShape(providerfoundation.Credential{Provider: "jira"}))
	raw, err := unitDiscoveryFailureResultJSON(cause)
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded["error_category"] != referenceDiscoveryErrorCategory {
		t.Errorf("error_category = %v", decoded["error_category"])
	}
	if got, _ := decoded["reason"].(string); got != "missing_fields:api_token,email,base_url" {
		t.Errorf("reason = %q, want the missing field names", got)
	}
	plain, _ := unitDiscoveryFailureResultJSON(errors.New("Invalid Organization Ari: some-uuid"))
	if strings.Contains(string(plain), `"reason"`) {
		t.Errorf("an unclassified error must carry no reason field: %s", plain)
	}
}

func TestAggregateFinalizeUnitsCarriesTheFirstFailedUnitsReason(t *testing.T) {
	aggregate := aggregateFinalizeUnits([]finalizeSyncRunUnit{
		{status: syncRunUnitStatusSuccess},
		{status: syncRunUnitStatusFailed, errorCategory: "reference_discovery_failed", reason: "decrypt_failed"},
		{status: syncRunUnitStatusFailed, errorCategory: "reference_discovery_failed", reason: "ciphertext_missing"},
	})
	if aggregate.reason != "decrypt_failed" || aggregate.errorCategory != "reference_discovery_failed" {
		t.Errorf("aggregate = %+v, want the first failed unit's category and reason", aggregate)
	}
}
