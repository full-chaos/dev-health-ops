package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/joboperator"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph"
	"github.com/google/uuid"
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

func TestDispatchWorkgraphTriggerDryRunNeedsNoDataBackend(t *testing.T) {
	// Renamed from "...NeedsNoBackend": dry-run now DOES need an
	// authorized *joboperator.Service (codex review, 2026-09-05, CHAOS-5170
	// r1 P1 -- authorization covers the preview too, matching
	// AuthorizeProvidersyncCleanup's own convention), it just never touches
	// runtime.pools/registry. commandRuntime wires a real Service with an
	// always-allow fake Authorizer, so this still proves the command clears
	// every check WITHOUT a real Postgres.
	var stdout, stderr bytes.Buffer
	code := dispatchWorkgraph(context.Background(), commandRuntime(t, commandAuthorizer{}), []string{
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
		code := dispatchWorkgraph(context.Background(), commandRuntime(t, commandAuthorizer{}), []string{
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
		code := dispatchWorkgraph(context.Background(), commandRuntime(t, commandAuthorizer{}), []string{
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
		code := dispatch(context.Background(), commandRuntime(t, commandAuthorizer{}), []string{
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
	code := dispatchInvestment(context.Background(), commandRuntime(t, commandAuthorizer{}), []string{
		"trigger", "--org", validTriggerOrg, "--review-evidence", "testing",
		"--from", "2026-01-01", "--to", "2026-01-31", "--dry-run",
	}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("dry-run should not require a data backend: code=%d stderr=%q", code, stderr.String())
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

// --- codex r1 findings: authorization, contract-invalid org, year-zero -----

// TestDispatchWorkgraphTriggerRejectsReadOnlyCredentialEvenOnDryRun proves
// r1 P1's fix: a `workers:read`-scoped credential must never clear the
// authorization gate, dry-run included. Before this fix, dispatchWorkgraphTrigger
// called no Authorize method at all, so ANY authenticated credential reached
// WriteTx -- this uses commandAuthorizer{err: joboperator.ErrAuthorization}
// (the same fake Authorizer shape main_test.go's other authorization tests
// use) to simulate that denial and asserts it is surfaced, not bypassed.
func TestDispatchWorkgraphTriggerRejectsReadOnlyCredentialEvenOnDryRun(t *testing.T) {
	runtime := commandRuntime(t, commandAuthorizer{err: joboperator.ErrAuthorization})
	for _, dryRun := range []bool{false, true} {
		args := []string{
			"trigger", "--org", validTriggerOrg, "--review-evidence", "testing",
			"--from", "2026-01-01", "--to", "2026-01-31",
		}
		if dryRun {
			args = append(args, "--dry-run")
		}
		var stdout, stderr bytes.Buffer
		code := dispatchWorkgraph(context.Background(), runtime, args, &stdout, &stderr)
		if code != 1 || !bytes.Contains(stderr.Bytes(), []byte("unauthorized")) {
			t.Fatalf("dry_run=%t: expected unauthorized, got code=%d stdout=%q stderr=%q",
				dryRun, code, stdout.String(), stderr.String())
		}
		if stdout.Len() != 0 {
			t.Fatalf("dry_run=%t: an unauthorized credential must get no preview output either: %q",
				dryRun, stdout.String())
		}
	}
}

func TestDispatchInvestmentTriggerRejectsReadOnlyCredentialEvenOnDryRun(t *testing.T) {
	runtime := commandRuntime(t, commandAuthorizer{err: joboperator.ErrAuthorization})
	for _, dryRun := range []bool{false, true} {
		args := []string{
			"trigger", "--org", validTriggerOrg, "--review-evidence", "testing",
			"--from", "2026-01-01", "--to", "2026-01-31",
		}
		if dryRun {
			args = append(args, "--dry-run")
		}
		var stdout, stderr bytes.Buffer
		code := dispatchInvestment(context.Background(), runtime, args, &stdout, &stderr)
		if code != 1 || !bytes.Contains(stderr.Bytes(), []byte("unauthorized")) {
			t.Fatalf("dry_run=%t: expected unauthorized, got code=%d stdout=%q stderr=%q",
				dryRun, code, stdout.String(), stderr.String())
		}
	}
}

// TestDispatchWorkgraphTriggerRejectsContractInvalidOrg proves r1 P2's fix:
// an org that satisfies uuid.Parse's permissive grammar but not
// workgraph.ValidUUID's RFC-4122 version/variant check (codex's own
// example: an all-zero UUID with a '0' version nibble) is now rejected as
// invalid_request BEFORE a transaction is ever opened -- previously it
// passed canonicalUUID, reached WriteTx, and only failed there as
// ErrInvalidState.
func TestDispatchWorkgraphTriggerRejectsContractInvalidOrg(t *testing.T) {
	const contractInvalidOrg = "00000000-0000-0000-0000-000000000001" // version nibble '0', not [1-5]
	if _, err := uuid.Parse(contractInvalidOrg); err != nil {
		t.Fatalf("test fixture must be uuid.Parse-valid: %v", err)
	}
	if workgraph.ValidUUID(contractInvalidOrg) {
		t.Fatalf("test fixture must be RFC-4122-invalid: %q", contractInvalidOrg)
	}
	var stdout, stderr bytes.Buffer
	code := dispatchWorkgraph(context.Background(), commandRuntime(t, commandAuthorizer{}), []string{
		"trigger", "--org", contractInvalidOrg, "--review-evidence", "testing", "--dry-run",
	}, &stdout, &stderr)
	if code != 1 || stderr.String() != invalidRequestJSON {
		t.Fatalf("contract-invalid org: code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
}

func TestDispatchInvestmentTriggerRejectsContractInvalidOrg(t *testing.T) {
	const contractInvalidOrg = "00000000-0000-0000-0000-000000000001"
	var stdout, stderr bytes.Buffer
	code := dispatchInvestment(context.Background(), commandRuntime(t, commandAuthorizer{}), []string{
		"trigger", "--org", contractInvalidOrg, "--review-evidence", "testing", "--dry-run",
	}, &stdout, &stderr)
	if code != 1 || stderr.String() != invalidRequestJSON {
		t.Fatalf("contract-invalid org: code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
}

// TestDispatchWorkgraphTriggerRejectsYearZeroScope proves r1 P2's fix: Go's
// time.Parse accepts "0000-01-01" (year zero is a representable
// time.Time), which is not a real calendar date -- this is now rejected as
// invalid_request before a transaction opens, matching every other
// malformed-flag path, instead of being written and only caught later,
// ambiguously, by the build pre-step.
func TestDispatchWorkgraphTriggerRejectsYearZeroScope(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := dispatchWorkgraph(context.Background(), commandRuntime(t, commandAuthorizer{}), []string{
		"trigger", "--org", validTriggerOrg, "--review-evidence", "testing",
		"--from", "0000-01-01", "--dry-run",
	}, &stdout, &stderr)
	if code != 1 || stderr.String() != invalidRequestJSON {
		t.Fatalf("year-zero --from: code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
}

func TestDispatchInvestmentTriggerRejectsYearZeroScope(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := dispatchInvestment(context.Background(), commandRuntime(t, commandAuthorizer{}), []string{
		"trigger", "--org", validTriggerOrg, "--review-evidence", "testing",
		"--to", "0000-12-31", "--dry-run",
	}, &stdout, &stderr)
	if code != 1 || stderr.String() != invalidRequestJSON {
		t.Fatalf("year-zero --to: code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
}

// TestDispatchWorkgraphTriggerWhitespaceDateMatchesOmittedDate proves r2
// P2's fix: a whitespace-only --from (or --to) produces the same empty
// scope as an omitted one (parseOptionalUTCDateRange treats both as nil),
// so both must hash to the SAME generation/request id. Before the fix,
// the generation string was built from the RAW flag text (`*from`), so
// "" and " " produced different generations for an identical scope --
// allowing a duplicate durable request past WriteTx's own idempotency
// check, which keys on the generation-derived id (codex review,
// 2026-09-05, CHAOS-5170 r2 P2, exact repro).
func TestDispatchWorkgraphTriggerWhitespaceDateMatchesOmittedDate(t *testing.T) {
	dryRunID := func(from string) string {
		var stdout, stderr bytes.Buffer
		args := []string{"trigger", "--org", validTriggerOrg, "--review-evidence", "testing", "--dry-run"}
		if from != "" {
			args = append(args, "--from", from)
		}
		code := dispatchWorkgraph(context.Background(), commandRuntime(t, commandAuthorizer{}), args, &stdout, &stderr)
		if code != 0 {
			t.Fatalf("from=%q: unexpected failure: %s", from, stderr.String())
		}
		var result map[string]any
		if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
			t.Fatalf("from=%q: parse: %v", from, err)
		}
		id, _ := result["request_id"].(string)
		return id
	}
	omitted := dryRunID("")
	whitespace := dryRunID(" ")
	if omitted == "" || omitted != whitespace {
		t.Fatalf("omitted --from (%q) and whitespace-only --from (%q) must produce the SAME request id -- both build the same empty scope", omitted, whitespace)
	}
}

func TestDispatchInvestmentTriggerWhitespaceDateMatchesOmittedDate(t *testing.T) {
	dryRunID := func(to string) string {
		var stdout, stderr bytes.Buffer
		args := []string{"trigger", "--org", validTriggerOrg, "--review-evidence", "testing", "--dry-run"}
		if to != "" {
			args = append(args, "--to", to)
		}
		code := dispatchInvestment(context.Background(), commandRuntime(t, commandAuthorizer{}), args, &stdout, &stderr)
		if code != 0 {
			t.Fatalf("to=%q: unexpected failure: %s", to, stderr.String())
		}
		var result map[string]any
		if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
			t.Fatalf("to=%q: parse: %v", to, err)
		}
		id, _ := result["request_id"].(string)
		return id
	}
	omitted := dryRunID("")
	whitespace := dryRunID("   ")
	if omitted == "" || omitted != whitespace {
		t.Fatalf("omitted --to (%q) and whitespace-only --to (%q) must produce the SAME request id", omitted, whitespace)
	}
}

// captureDefaultSlogForUnitTest swaps slog's default logger for a
// text-handler writing into a buffer for the duration of the test,
// restoring the original on cleanup. A separate, non-integration-tagged
// copy of trigger_integration_test.go's captureDefaultSlog: that file is
// //go:build integration and unavailable to this plain unit test file.
func captureDefaultSlogForUnitTest(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return &buf
}

// TestDispatchWorkgraphTriggerLogsAuthorizationDenials is r3 P2's direct
// repro: an authorization denial occurs before request_id/generation exist,
// so it has no idempotency key to log -- but the ACTION and ORG are known,
// and were previously logged nowhere at all. This makes an attempted
// manual mutation a workers:read credential was not entitled to make
// invisible in telemetry.
func TestDispatchWorkgraphTriggerLogsAuthorizationDenials(t *testing.T) {
	logs := captureDefaultSlogForUnitTest(t)
	var stdout, stderr bytes.Buffer
	code := dispatchWorkgraph(context.Background(),
		commandRuntime(t, commandAuthorizer{err: joboperator.ErrAuthorization}),
		[]string{"trigger", "--org", validTriggerOrg, "--review-evidence", "testing", "--dry-run"},
		&stdout, &stderr)
	if code != 1 || stdout.Len() != 0 || stderr.String() != "{\"error\":{\"code\":\"unauthorized\"}}\n" {
		t.Fatalf("unexpected result: code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	logged := logs.String()
	for _, want := range []string{"workgraph.manual_trigger", validTriggerOrg, "worker operator authorization failed"} {
		if !bytes.Contains([]byte(logged), []byte(want)) {
			t.Fatalf("missing authorization-denial telemetry %q; observed logs=%q", want, logged)
		}
	}
}

func TestDispatchInvestmentTriggerLogsAuthorizationDenials(t *testing.T) {
	logs := captureDefaultSlogForUnitTest(t)
	var stdout, stderr bytes.Buffer
	code := dispatchInvestment(context.Background(),
		commandRuntime(t, commandAuthorizer{err: joboperator.ErrAuthorization}),
		[]string{"trigger", "--org", validTriggerOrg, "--review-evidence", "testing", "--dry-run"},
		&stdout, &stderr)
	if code != 1 || stdout.Len() != 0 || stderr.String() != "{\"error\":{\"code\":\"unauthorized\"}}\n" {
		t.Fatalf("unexpected result: code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	logged := logs.String()
	for _, want := range []string{"investment.manual_trigger", validTriggerOrg, "worker operator authorization failed"} {
		if !bytes.Contains([]byte(logged), []byte(want)) {
			t.Fatalf("missing authorization-denial telemetry %q; observed logs=%q", want, logged)
		}
	}
}

// TestManualTriggerNamespacesDifferFromAutomaticProducers is the reachability
// control the context file's own "weakest point" section asked for: proves
// (by actual UUIDv5 computation, not just "the constants are textually
// different") that neither manual namespace can produce the same request id
// as either automatic producer's OWN namespace for a plausible shared
// generation string.
func TestManualTriggerNamespacesDifferFromAutomaticProducers(t *testing.T) {
	// postSyncFanoutNamespace and occurrenceDomainNamespace are unexported
	// vars in OTHER packages (cmd/dev-health-worker/sync_dispatch.go:271,
	// internal/scheduler/fixed/producers.go:45 respectively) and cannot be
	// imported from here -- their literal values are duplicated below
	// specifically so this test can compute what those packages' own
	// derived ids would be, without modifying either package's exported
	// surface just for this check. If either source constant ever changes,
	// this test's copy must be updated to match, or it silently stops
	// testing the real collision risk.
	postSyncFanoutNamespace := uuid.MustParse("0713fbcf-ec5c-49dc-b7dc-18ae3de17536")
	occurrenceDomainNamespace := uuid.MustParse("6f2f2ba4-2c2f-5f8a-9a1e-9a2c7b6d4e11")

	const generation = "shared-generation-string:" + validTriggerOrg + ":2026-01-01:2026-01-31"
	namespaces := map[string]uuid.UUID{
		"manualWorkGraphTrigger":          manualWorkGraphTriggerNamespace,
		"manualInvestmentTrigger":         manualInvestmentTriggerNamespace,
		"postSyncFanout":                  postSyncFanoutNamespace,
		"scheduledFanoutOccurrenceDomain": occurrenceDomainNamespace,
	}
	ids := make(map[string]string, len(namespaces))
	for name, ns := range namespaces {
		ids[name] = manualTriggerRequestID(ns, generation)
	}
	seen := map[string]string{}
	for name, id := range ids {
		if other, dup := seen[id]; dup {
			t.Fatalf("namespace collision for generation %q: %s and %s both produced %s",
				generation, name, other, id)
		}
		seen[id] = name
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
