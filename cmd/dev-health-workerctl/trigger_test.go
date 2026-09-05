package main

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
)

const validTriggerOrg = "00000000-0000-4000-8000-000000000001"

// --- workgraph trigger -----------------------------------------------------

func TestDispatchWorkgraphTriggerRequiresOrg(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := dispatchWorkgraph(context.Background(), &operatorRuntime{}, []string{
		"trigger", "--review-evidence", "testing",
	}, &stdout, &stderr)
	if code != 1 || stderr.String() != invalidRequestJSON {
		t.Fatalf("missing --org: code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
}

func TestDispatchWorkgraphTriggerRequiresReviewEvidence(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := dispatchWorkgraph(context.Background(), &operatorRuntime{}, []string{
		"trigger", "--org", validTriggerOrg,
	}, &stdout, &stderr)
	if code != 1 || stderr.String() != invalidRequestJSON {
		t.Fatalf("missing --review-evidence: code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
}

func TestDispatchWorkgraphTriggerRejectsInvalidOrg(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := dispatchWorkgraph(context.Background(), &operatorRuntime{}, []string{
		"trigger", "--org", "not-a-uuid", "--review-evidence", "testing",
	}, &stdout, &stderr)
	if code != 1 || stderr.String() != invalidRequestJSON {
		t.Fatalf("invalid org: code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
}

func TestDispatchWorkgraphTriggerRejectsMalformedDate(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := dispatchWorkgraph(context.Background(), &operatorRuntime{}, []string{
		"trigger", "--org", validTriggerOrg, "--review-evidence", "testing", "--from", "not-a-date",
	}, &stdout, &stderr)
	if code != 1 || stderr.String() != invalidRequestJSON {
		t.Fatalf("malformed --from: code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
}

func TestDispatchWorkgraphTriggerReachesBackendUnavailableWithValidFlags(t *testing.T) {
	// Mirrors TestDispatchMetricsRemainingTriggerBackstopAcceptsDayScopedFamiliesUniformly's
	// shape: a zero-value &operatorRuntime{} has nil pools/registry, so this
	// proves the command cleared every validation gate before ever touching
	// a backend, without needing a real Postgres.
	var stdout, stderr bytes.Buffer
	code := dispatchWorkgraph(context.Background(), &operatorRuntime{}, []string{
		"trigger", "--org", validTriggerOrg, "--review-evidence", "testing",
		"--from", "2026-01-01", "--to", "2026-01-31",
	}, &stdout, &stderr)
	if code != 1 || stderr.String() != "{\"error\":{\"code\":\"operator_backend_unavailable\"}}\n" {
		t.Fatalf("code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
}

func TestDispatchWorkgraphTriggerDryRunNeedsNoBackend(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := dispatchWorkgraph(context.Background(), &operatorRuntime{}, []string{
		"trigger", "--org", validTriggerOrg, "--review-evidence", "testing",
		"--from", "2026-01-01", "--to", "2026-01-31", "--dry-run",
	}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("dry-run should not require a backend: code=%d stderr=%q", code, stderr.String())
	}
	var result map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatalf("parse dry-run output: %v\n%s", err, stdout.String())
	}
	if result["dry_run"] != true {
		t.Errorf("dry_run: got %v, want true", result["dry_run"])
	}
	if result["kind"] != "workgraph.build" {
		t.Errorf("kind: got %v, want workgraph.build", result["kind"])
	}
	requestID, _ := result["request_id"].(string)
	if requestID == "" {
		t.Fatalf("request_id missing from dry-run output: %v", result)
	}
}

func TestDispatchWorkgraphTriggerRequestIDIsDeterministic(t *testing.T) {
	// The SAME flags must produce the SAME request id every time -- this is
	// exactly what lets a repeated CLI invocation land on WriteTx's own
	// ON CONFLICT idempotency path instead of creating a second request.
	run := func() string {
		var stdout, stderr bytes.Buffer
		code := dispatchWorkgraph(context.Background(), &operatorRuntime{}, []string{
			"trigger", "--org", validTriggerOrg, "--review-evidence", "testing",
			"--from", "2026-01-01", "--to", "2026-01-31", "--dry-run",
		}, &stdout, &stderr)
		if code != 0 {
			t.Fatalf("unexpected failure: %s", stderr.String())
		}
		var result map[string]any
		if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
			t.Fatalf("parse: %v", err)
		}
		id, _ := result["request_id"].(string)
		return id
	}
	first, second := run(), run()
	if first == "" || first != second {
		t.Fatalf("request id not deterministic: %q vs %q", first, second)
	}
}

func TestDispatchWorkgraphTriggerRequestIDDiffersByWindow(t *testing.T) {
	idFor := func(from string) string {
		var stdout, stderr bytes.Buffer
		code := dispatchWorkgraph(context.Background(), &operatorRuntime{}, []string{
			"trigger", "--org", validTriggerOrg, "--review-evidence", "testing",
			"--from", from, "--to", "2026-01-31", "--dry-run",
		}, &stdout, &stderr)
		if code != 0 {
			t.Fatalf("unexpected failure: %s", stderr.String())
		}
		var result map[string]any
		if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
			t.Fatalf("parse: %v", err)
		}
		id, _ := result["request_id"].(string)
		return id
	}
	a, b := idFor("2026-01-01"), idFor("2026-02-01")
	if a == b {
		t.Fatalf("different windows produced the SAME request id: %q", a)
	}
}

func TestDispatchWorkgraphTriggerRequestIDDiffersFromInvestmentTrigger(t *testing.T) {
	// Two different producers (workgraph.build vs investment.materialize)
	// for the SAME org/window must never collide -- they use distinct
	// UUIDv5 namespaces specifically to guarantee this.
	dryRunID := func(dispatch func(context.Context, *operatorRuntime, []string, *bytes.Buffer, *bytes.Buffer) int) string {
		var stdout, stderr bytes.Buffer
		code := dispatch(context.Background(), &operatorRuntime{}, []string{
			"trigger", "--org", validTriggerOrg, "--review-evidence", "testing",
			"--from", "2026-01-01", "--to", "2026-01-31", "--dry-run",
		}, &stdout, &stderr)
		if code != 0 {
			t.Fatalf("unexpected failure: %s", stderr.String())
		}
		var result map[string]any
		if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
			t.Fatalf("parse: %v", err)
		}
		id, _ := result["request_id"].(string)
		return id
	}
	workgraphID := dryRunID(func(ctx context.Context, rt *operatorRuntime, args []string, stdout, stderr *bytes.Buffer) int {
		return dispatchWorkgraphTrigger(ctx, rt, args[1:], stdout, stderr)
	})
	investmentID := dryRunID(func(ctx context.Context, rt *operatorRuntime, args []string, stdout, stderr *bytes.Buffer) int {
		return dispatchInvestmentTrigger(ctx, rt, args[1:], stdout, stderr)
	})
	if workgraphID == investmentID {
		t.Fatalf("workgraph and investment triggers produced the SAME request id for the same org/window: %q", workgraphID)
	}
}

// --- investment trigger ------------------------------------------------------

func TestDispatchInvestmentTriggerRequiresOrg(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := dispatchInvestment(context.Background(), &operatorRuntime{}, []string{
		"trigger", "--review-evidence", "testing",
	}, &stdout, &stderr)
	if code != 1 || stderr.String() != invalidRequestJSON {
		t.Fatalf("missing --org: code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
}

func TestDispatchInvestmentTriggerRequiresReviewEvidence(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := dispatchInvestment(context.Background(), &operatorRuntime{}, []string{
		"trigger", "--org", validTriggerOrg,
	}, &stdout, &stderr)
	if code != 1 || stderr.String() != invalidRequestJSON {
		t.Fatalf("missing --review-evidence: code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
}

func TestDispatchInvestmentTriggerRejectsInvalidOrg(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := dispatchInvestment(context.Background(), &operatorRuntime{}, []string{
		"trigger", "--org", "not-a-uuid", "--review-evidence", "testing",
	}, &stdout, &stderr)
	if code != 1 || stderr.String() != invalidRequestJSON {
		t.Fatalf("invalid org: code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
}

func TestDispatchInvestmentDispatchesUnknownVerb(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := dispatchInvestment(context.Background(), &operatorRuntime{}, []string{"bogus"}, &stdout, &stderr)
	if code != 1 || stderr.String() != invalidRequestJSON {
		t.Fatalf("unknown investment verb should be invalid_request: code=%d stderr=%q", code, stderr.String())
	}
}

func TestDispatchInvestmentTriggerReachesBackendUnavailableWithValidFlags(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := dispatchInvestment(context.Background(), &operatorRuntime{}, []string{
		"trigger", "--org", validTriggerOrg, "--review-evidence", "testing",
	}, &stdout, &stderr)
	if code != 1 || stderr.String() != "{\"error\":{\"code\":\"operator_backend_unavailable\"}}\n" {
		t.Fatalf("code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
}

func TestDispatchInvestmentTriggerDryRunUsesDateOnlyScope(t *testing.T) {
	// investment.materialize's scope is date-only (postSyncWorkGraphScope's
	// KindMaterialize branch), NOT RFC3339 like workgraph.build's -- this
	// pins that distinction so a future refactor collapsing the two scope
	// builders back into one shape is caught here, not in production.
	var stdout, stderr bytes.Buffer
	code := dispatchInvestment(context.Background(), &operatorRuntime{}, []string{
		"trigger", "--org", validTriggerOrg, "--review-evidence", "testing",
		"--from", "2026-01-01", "--to", "2026-01-31", "--dry-run",
	}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("dry-run should not require a backend: code=%d stderr=%q", code, stderr.String())
	}
	var result map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatalf("parse: %v\n%s", err, stdout.String())
	}
	scopeRaw, ok := result["scope"].(map[string]any)
	if !ok {
		t.Fatalf("scope missing or wrong shape: %v", result)
	}
	if scopeRaw["from_date"] != "2026-01-01" || scopeRaw["to_date"] != "2026-01-31" {
		t.Errorf("scope not date-only: %v", scopeRaw)
	}
}

func TestManualTriggerDateRangeScopeOmitsUnsetDates(t *testing.T) {
	scope, err := manualTriggerDateRangeScope(nil, nil, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(scope) != "{}" {
		t.Errorf("expected empty scope object, got %s", scope)
	}
}

func TestParseOptionalUTCDateRangeRejectsMalformedDates(t *testing.T) {
	if _, _, err := parseOptionalUTCDateRange("2026-13-99", ""); err == nil {
		t.Fatal("expected an error for an invalid --from date")
	}
	if _, _, err := parseOptionalUTCDateRange("", "not-a-date"); err == nil {
		t.Fatal("expected an error for an invalid --to date")
	}
	from, to, err := parseOptionalUTCDateRange("", "")
	if err != nil || from != nil || to != nil {
		t.Fatalf("both unset should return (nil, nil, nil), got (%v, %v, %v)", from, to, err)
	}
}
