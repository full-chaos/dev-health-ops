package sync

import (
	_ "embed"
	"encoding/json"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/programoracle"
)

// goFeatureDecisionReasons is the closed set this package declares in
// materializer.go -- CHAOS-4175's carry-through of the real
// FeatureDecisionReason Python attaches to a canonical-incident denial.
// Keyed by the same UPPER_SNAKE member name Python's StrEnum uses, so a
// mismatch report names the exact member that drifted.
var goFeatureDecisionReasons = map[string]FeatureDecisionReason{
	"ENABLED_BY_ORG_OVERRIDE":     FeatureDecisionReasonEnabledByOrgOverride,
	"ENABLED_BY_LICENSE_OVERRIDE": FeatureDecisionReasonEnabledByLicenseOverride,
	"ENABLED_BY_TIER":             FeatureDecisionReasonEnabledByTier,
	"FEATURE_NOT_REGISTERED":      FeatureDecisionReasonFeatureNotRegistered,
	"GLOBAL_DISABLED":             FeatureDecisionReasonGlobalDisabled,
	"INVALID_FEATURE_STATE":       FeatureDecisionReasonInvalidFeatureState,
	"STORAGE_ERROR":               FeatureDecisionReasonStorageError,
	"ORG_OVERRIDE_EXPIRED":        FeatureDecisionReasonOrgOverrideExpired,
	"ORG_OVERRIDE_DISABLED":       FeatureDecisionReasonOrgOverrideDisabled,
	"ORG_OVERRIDE_REQUIRED":       FeatureDecisionReasonOrgOverrideRequired,
	"LICENSE_OVERRIDE_DISABLED":   FeatureDecisionReasonLicenseOverrideDisabled,
	"EXPLICIT_PURCHASE_REQUIRED":  FeatureDecisionReasonExplicitPurchaseRequired,
	"TIER_REQUIRED":               FeatureDecisionReasonTierRequired,
}

// featureDecisionReasonScript prints {member name: value} for every member of
// the FeatureDecisionReason StrEnum of the build it is executed on.
//
//go:embed testdata/python_feature_decision_reason_oracle.py
var featureDecisionReasonScript string

// TestFeatureDecisionReasonMatchesFrozenPythonEnum pins the closed vocabulary:
// every member Python's real FeatureDecisionReason StrEnum declared must
// have a Go constant with the IDENTICAL string value, and Go must declare
// no member Python did not have. A Go addition, rename, or removal fails this
// test instead of silently producing a Go reason string that can never appear
// in a real CanonicalIncidentFeatureDisabledError message, or missing one
// that can.
func TestFeatureDecisionReasonMatchesFrozenPythonEnum(t *testing.T) {
	output := frozenPython(t, "feature-decision-reason.golden.json", programoracle.Script("feature decision reasons",
		"internal/scheduler/sync/testdata/python_feature_decision_reason_oracle.py", featureDecisionReasonScript, nil))[0]
	var pythonReasons map[string]string
	if err := json.Unmarshal([]byte(output), &pythonReasons); err != nil {
		t.Fatalf("decode the frozen Python feature-decision-reason answer: %v\n%s", err, output)
	}
	if len(pythonReasons) == 0 {
		t.Fatal("the frozen Python answer holds no FeatureDecisionReason members")
	}

	for name, pythonValue := range pythonReasons {
		goValue, declared := goFeatureDecisionReasons[name]
		if !declared {
			t.Errorf("Python declares FeatureDecisionReason.%s=%q, Go has no matching constant", name, pythonValue)
			continue
		}
		if string(goValue) != pythonValue {
			t.Errorf("FeatureDecisionReason.%s: Go=%q Python=%q", name, goValue, pythonValue)
		}
	}
	for name := range goFeatureDecisionReasons {
		if _, present := pythonReasons[name]; !present {
			t.Errorf("Go declares FeatureDecisionReason%s but Python's enum has no %s member", name, name)
		}
	}
}
