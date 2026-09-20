package main

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
)

const testAmbiguousRequestID = "00000000-0000-4000-8000-000000000401"
const testAmbiguousExecutionID = "00000000-0000-4000-8000-000000000402"

// --- workgraph repair -------------------------------------------------

func TestDispatchWorkgraphRepairRequiresReviewEvidence(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := dispatchWorkgraphRepair(context.Background(), &operatorRuntime{}, []string{
		"--request", testAmbiguousRequestID,
		"--resolution", "retry_safe",
		"--expected-attempt-count", "3",
	}, &stdout, &stderr)
	if code == 0 {
		t.Fatal("dispatchWorkgraphRepair with no --review-evidence = 0, want non-zero")
	}
	if !strings.Contains(stderr.String(), "invalid_request") {
		t.Fatalf("stderr = %q, want invalid_request", stderr.String())
	}
}

func TestDispatchWorkgraphRepairRejectsInvalidRequestID(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := dispatchWorkgraphRepair(context.Background(), &operatorRuntime{}, []string{
		"--request", "not-a-uuid",
		"--resolution", "retry_safe",
		"--expected-attempt-count", "3",
		"--review-evidence", "checked",
	}, &stdout, &stderr)
	if code == 0 {
		t.Fatal("dispatchWorkgraphRepair with an invalid --request = 0, want non-zero")
	}
}

func TestDispatchWorkgraphRepairRejectsInvalidResolution(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := dispatchWorkgraphRepair(context.Background(), &operatorRuntime{}, []string{
		"--request", testAmbiguousRequestID,
		"--resolution", "retry", // not the bridge's actual literal -- must be rejected, not translated
		"--expected-attempt-count", "3",
		"--review-evidence", "checked",
	}, &stdout, &stderr)
	if code == 0 {
		t.Fatal("dispatchWorkgraphRepair with --resolution=retry = 0, want non-zero (only retry_safe/confirm_succeeded are valid)")
	}
}

func TestDispatchWorkgraphRepairRequiresPositiveExpectedAttemptCount(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := dispatchWorkgraphRepair(context.Background(), &operatorRuntime{}, []string{
		"--request", testAmbiguousRequestID,
		"--resolution", "retry_safe",
		"--review-evidence", "checked",
	}, &stdout, &stderr)
	if code == 0 {
		t.Fatal("dispatchWorkgraphRepair with no --expected-attempt-count = 0, want non-zero")
	}
}

func TestDispatchWorkgraphRepairConfirmSucceededRequiresOutputEvidence(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := dispatchWorkgraphRepair(context.Background(), &operatorRuntime{}, []string{
		"--request", testAmbiguousRequestID,
		"--resolution", "confirm_succeeded",
		"--expected-attempt-count", "1",
		"--review-evidence", "checked",
	}, &stdout, &stderr)
	if code == 0 {
		t.Fatal("dispatchWorkgraphRepair confirm_succeeded with no --output-evidence = 0, want non-zero")
	}
}

func TestDispatchWorkgraphRepairRetrySafeRejectsOutputEvidence(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := dispatchWorkgraphRepair(context.Background(), &operatorRuntime{}, []string{
		"--request", testAmbiguousRequestID,
		"--resolution", "retry_safe",
		"--expected-attempt-count", "1",
		"--review-evidence", "checked",
		"--output-evidence", `{"rows": 1}`,
	}, &stdout, &stderr)
	if code == 0 {
		t.Fatal("dispatchWorkgraphRepair retry_safe with --output-evidence = 0, want non-zero")
	}
}

func TestDispatchWorkgraphRepairDryRunStillRequiresReviewEvidence(t *testing.T) {
	// --dry-run previews a VALID request -- it must not bypass the same
	// friction-by-design validation a real call requires.
	var stdout, stderr bytes.Buffer
	code := dispatchWorkgraphRepair(context.Background(), &operatorRuntime{}, []string{
		"--request", testAmbiguousRequestID,
		"--resolution", "retry_safe",
		"--expected-attempt-count", "1",
		"--dry-run",
	}, &stdout, &stderr)
	if code == 0 {
		t.Fatal("dispatchWorkgraphRepair --dry-run with no --review-evidence = 0, want non-zero")
	}
}

func TestDispatchWorkgraphListAmbiguousRejectsInvalidOrg(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := dispatchWorkgraphListAmbiguous(context.Background(), &operatorRuntime{}, []string{
		"--org", "not-a-uuid",
	}, &stdout, &stderr)
	if code == 0 {
		t.Fatal("dispatchWorkgraphListAmbiguous with an invalid --org = 0, want non-zero")
	}
}

func TestDispatchWorkgraphListUndeliveredRejectsInvalidCeiling(t *testing.T) {
	for _, value := range []string{"0", "-1", "2161", "soon"} {
		var stdout, stderr bytes.Buffer
		code := dispatchWorkgraphListUndelivered(context.Background(), &operatorRuntime{}, []string{
			"--ceiling-hours", value,
		}, &stdout, &stderr)
		if code == 0 || !strings.Contains(stderr.String(), "invalid_request") {
			t.Fatalf("--ceiling-hours %s = %d (%s), want invalid_request", value, code, stderr.String())
		}
	}
}

func TestDispatchWorkgraphListUndeliveredWithoutPoolsIsUnavailable(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := dispatchWorkgraph(context.Background(), &operatorRuntime{}, []string{"list-undelivered"}, &stdout, &stderr)
	if code == 0 || !strings.Contains(stderr.String(), "operator_backend_unavailable") {
		t.Fatalf("list-undelivered without pools = %d (%s), want operator_backend_unavailable", code, stderr.String())
	}
}

// --- metrics execution-repair ------------------------------------------

func TestDispatchMetricsExecutionRepairRequiresReviewEvidence(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := dispatchMetricsExecutionRepair(context.Background(), &operatorRuntime{}, []string{
		"--execution", testAmbiguousExecutionID,
		"--expected-state", "ambiguous",
		"--expected-attempt-count", "1",
		"--resolution", "retry_safe",
	}, &stdout, &stderr)
	if code == 0 {
		t.Fatal("dispatchMetricsExecutionRepair with no --review-evidence = 0, want non-zero")
	}
}

func TestDispatchMetricsExecutionRepairRejectsInvalidExpectedState(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := dispatchMetricsExecutionRepair(context.Background(), &operatorRuntime{}, []string{
		"--execution", testAmbiguousExecutionID,
		"--expected-state", "succeeded", // not a state the repair endpoint accepts as "expected"
		"--expected-attempt-count", "1",
		"--resolution", "retry_safe",
		"--review-evidence", "checked",
	}, &stdout, &stderr)
	if code == 0 {
		t.Fatal("dispatchMetricsExecutionRepair with --expected-state=succeeded = 0, want non-zero")
	}
}

func TestDispatchMetricsExecutionRepairConfirmSucceededRequiresOutputEvidence(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := dispatchMetricsExecutionRepair(context.Background(), &operatorRuntime{}, []string{
		"--execution", testAmbiguousExecutionID,
		"--expected-state", "ambiguous",
		"--expected-attempt-count", "1",
		"--resolution", "confirm_succeeded",
		"--review-evidence", "checked",
	}, &stdout, &stderr)
	if code == 0 {
		t.Fatal("dispatchMetricsExecutionRepair confirm_succeeded with no --output-evidence = 0, want non-zero")
	}
}
func TestDispatchMetricsListAmbiguousExecutionsRejectsInvalidOrg(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := dispatchMetricsListAmbiguousExecutions(context.Background(), &operatorRuntime{}, []string{
		"--org", "not-a-uuid",
	}, &stdout, &stderr)
	if code == 0 {
		t.Fatal("dispatchMetricsListAmbiguousExecutions with an invalid --org = 0, want non-zero")
	}
}

// --- postWorkerBridge (shared helper) -----------------------------------
func TestParseOutputEvidencePreservesLargeIntegerPrecision(t *testing.T) {
	evidence, err := parseOutputEvidence(`{"source_sequence": 9007199254740993}`)
	if err != nil {
		t.Fatalf("parseOutputEvidence: %v", err)
	}
	reencoded, err := json.Marshal(evidence)
	if err != nil {
		t.Fatalf("re-marshal: %v", err)
	}
	if !strings.Contains(string(reencoded), "9007199254740993") {
		t.Fatalf("re-encoded payload = %s, want it to contain the exact input digits 9007199254740993 (lost precision)", reencoded)
	}
}

func TestParseOutputEvidenceRejectsNonObjectJSON(t *testing.T) {
	for _, input := range []string{"null", "42", "[]", `"a string"`, "true"} {
		if _, err := parseOutputEvidence(input); err == nil {
			t.Errorf("parseOutputEvidence(%q) = nil error, want a rejection (not a JSON object)", input)
		}
	}
}

func TestValidateReviewEvidenceRejectsOverlongText(t *testing.T) {
	if validateReviewEvidence(strings.Repeat("x", reviewEvidenceMaxBytes+1)) {
		t.Fatal("validateReviewEvidence accepted text over the bridge's 2048-byte bound")
	}
	if !validateReviewEvidence(strings.Repeat("x", reviewEvidenceMaxBytes)) {
		t.Fatal("validateReviewEvidence rejected text exactly at the 2048-byte bound")
	}
	if validateReviewEvidence("   ") {
		t.Fatal("validateReviewEvidence accepted whitespace-only text")
	}
}

// TestDispatchWorkgraphRepairRejectsNullOutputEvidenceWithoutCallingTheBridge
// is codex round 1's P2 finding on the CLI's own dispatch path (not just the
// parser unit): --output-evidence null previously unmarshalled successfully
// into a nil map and was sent as literal JSON null, which the bridge only
