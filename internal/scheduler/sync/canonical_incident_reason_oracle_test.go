package sync

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
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

// TestFeatureDecisionReasonMatchesLivePythonEnum pins the closed vocabulary:
// every member Python's real FeatureDecisionReason StrEnum declares must
// have a Go constant with the IDENTICAL string value, and Go must declare
// no member Python doesn't have. A future Python addition, rename, or
// removal fails this test instead of silently producing a Go reason string
// that can never appear in a real CanonicalIncidentFeatureDisabledError
// message, or missing one that can.
func TestFeatureDecisionReasonMatchesLivePythonEnum(t *testing.T) {
	python := livePythonExecutable(t)
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate Python feature-decision-reason oracle")
	}
	command := exec.Command(python, filepath.Join(filepath.Dir(currentFile), "testdata", "python_feature_decision_reason_oracle.py"))
	var stdout, stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	if err := command.Run(); err != nil {
		t.Fatalf("execute live Python feature-decision-reason oracle: %v\nstderr:\n%s", err, stderr.String())
	}

	var pythonReasons map[string]string
	if err := json.Unmarshal(stdout.Bytes(), &pythonReasons); err != nil {
		t.Fatalf("decode live Python feature-decision-reason oracle: %v\n%s", err, stdout.String())
	}
	if len(pythonReasons) == 0 {
		t.Fatal("live Python oracle returned no FeatureDecisionReason members")
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

	// Shares the same proof-file marker planner_oracle_test.go writes: both
	// tests run under the identical `go test ./internal/scheduler/sync/...`
	// invocation ci/check_go.sh's live-python-oracles gate drives, and that
	// gate only checks the marker's existence, not which test produced it.
	proof := filepath.Join(os.Getenv(livePythonOracleProofDir), livePythonOracleProofFile)
	if err := os.WriteFile(proof, []byte("executed\n"), 0o600); err != nil {
		t.Fatalf("write live Python oracle proof: %v", err)
	}
}
