package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const testAmbiguousRequestID = "00000000-0000-4000-8000-000000000401"
const testAmbiguousExecutionID = "00000000-0000-4000-8000-000000000402"

// fakeBridge returns an httptest.Server that answers a fixed status code and
// JSON body, and records the last request it saw -- the shared fixture every
// 401/409/422/500/2xx case below uses, plus the "never prints token" and
// "sends the right token/path" cases.
type fakeBridge struct {
	server     *httptest.Server
	status     int
	body       string
	lastAuth   string
	lastPath   string
	lastMethod string
	lastBody   map[string]any
}

func newFakeBridge(t *testing.T, status int, body string) *fakeBridge {
	t.Helper()
	fake := &fakeBridge{status: status, body: body}
	fake.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fake.lastAuth = r.Header.Get("Authorization")
		fake.lastPath = r.URL.Path
		fake.lastMethod = r.Method
		_ = json.NewDecoder(r.Body).Decode(&fake.lastBody)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(fake.status)
		_, _ = w.Write([]byte(fake.body))
	}))
	t.Cleanup(fake.server.Close)
	return fake
}

// --- workgraph repair -------------------------------------------------

func TestDispatchWorkgraphRepairRequiresReviewEvidence(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := dispatchWorkgraphRepair(context.Background(), []string{
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
	code := dispatchWorkgraphRepair(context.Background(), []string{
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
	code := dispatchWorkgraphRepair(context.Background(), []string{
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
	code := dispatchWorkgraphRepair(context.Background(), []string{
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
	code := dispatchWorkgraphRepair(context.Background(), []string{
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
	code := dispatchWorkgraphRepair(context.Background(), []string{
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

func TestDispatchWorkgraphRepairSendsExpectedPathTokenAndPayload(t *testing.T) {
	fake := newFakeBridge(t, http.StatusOK, `{"status":"repaired","request_id":"`+testAmbiguousRequestID+`"}`)
	t.Setenv("WORKER_OPERATIONAL_BRIDGE_URL", fake.server.URL)
	t.Setenv("WORKER_WORKGRAPH_REPAIR_TOKEN", "workgraph-secret-token")
	t.Setenv("WORKER_METRIC_REPAIR_TOKEN", "unrelated-metric-token")

	var stdout, stderr bytes.Buffer
	code := dispatchWorkgraphRepair(context.Background(), []string{
		"--request", testAmbiguousRequestID,
		"--resolution", "confirm_succeeded",
		"--expected-attempt-count", "2",
		"--review-evidence", "confirmed zero output rows",
		"--output-evidence", `{"rows_written": 0}`,
	}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("dispatchWorkgraphRepair on a 200 bridge response = %d, want 0; stderr=%s", code, stderr.String())
	}
	if fake.lastPath != "/internal/worker/workgraph/v1/executions/"+testAmbiguousRequestID+"/repair" {
		t.Fatalf("request path = %q, want the workgraph repair endpoint", fake.lastPath)
	}
	if fake.lastMethod != http.MethodPost {
		t.Fatalf("request method = %q, want POST", fake.lastMethod)
	}
	if fake.lastAuth != "Bearer workgraph-secret-token" {
		t.Fatalf("Authorization = %q, want the WORKGRAPH token, not the metric-repair token", fake.lastAuth)
	}
	if resolution, _ := fake.lastBody["resolution"].(string); resolution != "confirm_succeeded" {
		t.Fatalf("request resolution = %v, want confirm_succeeded", fake.lastBody["resolution"])
	}
	if evidence, ok := fake.lastBody["output_evidence"].(map[string]any); !ok || evidence["rows_written"] != float64(0) {
		t.Fatalf("request output_evidence = %v, want {rows_written: 0}", fake.lastBody["output_evidence"])
	}
	if !strings.Contains(stdout.String(), "repaired") {
		t.Fatalf("stdout = %q, want the bridge's verbatim response", stdout.String())
	}
}

func TestDispatchWorkgraphRepairPrintsBridgeResponseVerbatimOnEveryStatus(t *testing.T) {
	for _, statusCase := range []struct {
		status int
		body   string
	}{
		{http.StatusUnauthorized, `{"detail":"Unauthorized"}`},
		{http.StatusConflict, `{"detail":"Only unleased ambiguous executions can be repaired"}`},
		{http.StatusUnprocessableEntity, `{"detail":"Resolution evidence is invalid"}`},
		{http.StatusInternalServerError, `{"detail":"internal error"}`},
		{http.StatusOK, `{"status":"repaired","request_id":"` + testAmbiguousRequestID + `"}`},
	} {
		t.Run(http.StatusText(statusCase.status), func(t *testing.T) {
			fake := newFakeBridge(t, statusCase.status, statusCase.body)
			t.Setenv("WORKER_OPERATIONAL_BRIDGE_URL", fake.server.URL)
			t.Setenv("WORKER_WORKGRAPH_REPAIR_TOKEN", "workgraph-secret-token")

			var stdout, stderr bytes.Buffer
			code := dispatchWorkgraphRepair(context.Background(), []string{
				"--request", testAmbiguousRequestID,
				"--resolution", "retry_safe",
				"--expected-attempt-count", "1",
				"--review-evidence", "checked",
			}, &stdout, &stderr)

			wantExitZero := statusCase.status >= 200 && statusCase.status < 300
			if (code == 0) != wantExitZero {
				t.Fatalf("status %d: exit code = %d, want zero=%t", statusCase.status, code, wantExitZero)
			}
			if !strings.Contains(stdout.String(), "detail") && statusCase.status != http.StatusOK {
				t.Fatalf("status %d: stdout = %q, want the bridge's detail message printed verbatim", statusCase.status, stdout.String())
			}
		})
	}
}

func TestDispatchWorkgraphRepairNeverPrintsTheToken(t *testing.T) {
	const secretToken = "workgraph-token-must-never-appear-in-output"
	fake := newFakeBridge(t, http.StatusUnauthorized, `{"detail":"Unauthorized"}`)
	t.Setenv("WORKER_OPERATIONAL_BRIDGE_URL", fake.server.URL)
	t.Setenv("WORKER_WORKGRAPH_REPAIR_TOKEN", secretToken)

	var stdout, stderr bytes.Buffer
	dispatchWorkgraphRepair(context.Background(), []string{
		"--request", testAmbiguousRequestID,
		"--resolution", "retry_safe",
		"--expected-attempt-count", "1",
		"--review-evidence", "checked",
	}, &stdout, &stderr)

	if strings.Contains(stdout.String(), secretToken) || strings.Contains(stderr.String(), secretToken) {
		t.Fatalf("token leaked into CLI output: stdout=%q stderr=%q", stdout.String(), stderr.String())
	}
}

// dispatchWorkgraph itself must fail closed on an unconfigured token too --
// not just the inner helper.
func TestDispatchWorkgraphRepairFailsClosedWithoutConfiguredToken(t *testing.T) {
	t.Setenv("WORKER_OPERATIONAL_BRIDGE_URL", "http://unused.invalid")
	t.Setenv("WORKER_WORKGRAPH_REPAIR_TOKEN", "")

	var stdout, stderr bytes.Buffer
	code := dispatchWorkgraphRepair(context.Background(), []string{
		"--request", testAmbiguousRequestID,
		"--resolution", "retry_safe",
		"--expected-attempt-count", "1",
		"--review-evidence", "checked",
	}, &stdout, &stderr)
	if code == 0 {
		t.Fatal("dispatchWorkgraphRepair with no WORKER_WORKGRAPH_REPAIR_TOKEN = 0, want fail-closed")
	}
}

func TestDispatchWorkgraphRepairDryRunNeverCallsTheBridgeAndRedactsTheToken(t *testing.T) {
	const secretToken = "workgraph-token-must-never-appear-in-dry-run-output"
	called := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)
	t.Setenv("WORKER_OPERATIONAL_BRIDGE_URL", server.URL)
	t.Setenv("WORKER_WORKGRAPH_REPAIR_TOKEN", secretToken)

	var stdout, stderr bytes.Buffer
	code := dispatchWorkgraphRepair(context.Background(), []string{
		"--request", testAmbiguousRequestID,
		"--resolution", "confirm_succeeded",
		"--expected-attempt-count", "2",
		"--review-evidence", "confirmed zero output rows",
		"--output-evidence", `{"rows_written": 0}`,
		"--dry-run",
	}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("dispatchWorkgraphRepair --dry-run = %d, want 0; stderr=%s", code, stderr.String())
	}
	if called {
		t.Fatal("dispatchWorkgraphRepair --dry-run called the bridge; want no network call at all")
	}
	if strings.Contains(stdout.String(), secretToken) {
		t.Fatalf("token leaked into --dry-run output: %q", stdout.String())
	}
	var decoded map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &decoded); err != nil {
		t.Fatalf("--dry-run stdout is not JSON: %v (%q)", err, stdout.String())
	}
	if decoded["dry_run"] != true {
		t.Fatalf("decoded[dry_run] = %v, want true", decoded["dry_run"])
	}
	if payload, ok := decoded["payload"].(map[string]any); !ok || payload["resolution"] != "confirm_succeeded" {
		t.Fatalf("decoded[payload] = %v, want the real resolved payload", decoded["payload"])
	}
	if auth, _ := decoded["authorization"].(string); !strings.Contains(auth, "REDACTED") {
		t.Fatalf("decoded[authorization] = %q, want it redacted", auth)
	}
}

func TestDispatchWorkgraphRepairDryRunStillRequiresReviewEvidence(t *testing.T) {
	// --dry-run previews a VALID request -- it must not bypass the same
	// friction-by-design validation a real call requires.
	var stdout, stderr bytes.Buffer
	code := dispatchWorkgraphRepair(context.Background(), []string{
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

// --- metrics execution-repair ------------------------------------------

func TestDispatchMetricsExecutionRepairRequiresReviewEvidence(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := dispatchMetricsExecutionRepair(context.Background(), []string{
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
	code := dispatchMetricsExecutionRepair(context.Background(), []string{
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
	code := dispatchMetricsExecutionRepair(context.Background(), []string{
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

func TestDispatchMetricsExecutionRepairSendsExpectedPathTokenAndPayload(t *testing.T) {
	fake := newFakeBridge(t, http.StatusOK, `{"status":"repaired","execution_id":"`+testAmbiguousExecutionID+`","state":"succeeded"}`)
	t.Setenv("WORKER_OPERATIONAL_BRIDGE_URL", fake.server.URL)
	t.Setenv("WORKER_METRIC_REPAIR_TOKEN", "metric-secret-token")
	t.Setenv("WORKER_WORKGRAPH_REPAIR_TOKEN", "unrelated-workgraph-token")

	var stdout, stderr bytes.Buffer
	code := dispatchMetricsExecutionRepair(context.Background(), []string{
		"--execution", testAmbiguousExecutionID,
		"--expected-state", "ambiguous",
		"--expected-attempt-count", "4",
		"--resolution", "confirm_succeeded",
		"--review-evidence", "file_hotspot_daily already has rows for this run/family",
		"--output-evidence", `{"rows_written": 128}`,
	}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("dispatchMetricsExecutionRepair on a 200 bridge response = %d, want 0; stderr=%s", code, stderr.String())
	}
	if fake.lastPath != "/internal/worker/metric-executions/v1/"+testAmbiguousExecutionID+"/repair" {
		t.Fatalf("request path = %q, want the metric-execution repair endpoint", fake.lastPath)
	}
	if fake.lastAuth != "Bearer metric-secret-token" {
		t.Fatalf("Authorization = %q, want the METRIC repair token, not the workgraph token", fake.lastAuth)
	}
	if state, _ := fake.lastBody["expected_state"].(string); state != "ambiguous" {
		t.Fatalf("request expected_state = %v, want ambiguous", fake.lastBody["expected_state"])
	}
	if !strings.Contains(stdout.String(), "repaired") {
		t.Fatalf("stdout = %q, want the bridge's verbatim response", stdout.String())
	}
}

func TestDispatchMetricsExecutionRepairPrintsBridgeResponseVerbatimOnEveryStatus(t *testing.T) {
	for _, statusCase := range []struct {
		status int
		body   string
	}{
		{http.StatusUnauthorized, `{"detail":"Unauthorized"}`},
		{http.StatusConflict, `{"detail":"Execution state or attempt changed"}`},
		{http.StatusUnprocessableEntity, `{"detail":"output_evidence exceeds the durable bound"}`},
		{http.StatusInternalServerError, `{"detail":"internal error"}`},
		{http.StatusOK, `{"status":"repaired","execution_id":"` + testAmbiguousExecutionID + `","state":"succeeded"}`},
	} {
		t.Run(http.StatusText(statusCase.status), func(t *testing.T) {
			fake := newFakeBridge(t, statusCase.status, statusCase.body)
			t.Setenv("WORKER_OPERATIONAL_BRIDGE_URL", fake.server.URL)
			t.Setenv("WORKER_METRIC_REPAIR_TOKEN", "metric-secret-token")

			var stdout, stderr bytes.Buffer
			code := dispatchMetricsExecutionRepair(context.Background(), []string{
				"--execution", testAmbiguousExecutionID,
				"--expected-state", "ambiguous",
				"--expected-attempt-count", "1",
				"--resolution", "retry_safe",
				"--review-evidence", "checked",
			}, &stdout, &stderr)

			wantExitZero := statusCase.status >= 200 && statusCase.status < 300
			if (code == 0) != wantExitZero {
				t.Fatalf("status %d: exit code = %d, want zero=%t", statusCase.status, code, wantExitZero)
			}
			if !strings.Contains(stdout.String(), "detail") && statusCase.status != http.StatusOK {
				t.Fatalf("status %d: stdout = %q, want the bridge's detail message printed verbatim", statusCase.status, stdout.String())
			}
		})
	}
}

func TestDispatchMetricsExecutionRepairNeverPrintsTheToken(t *testing.T) {
	const secretToken = "metric-token-must-never-appear-in-output"
	fake := newFakeBridge(t, http.StatusUnauthorized, `{"detail":"Unauthorized"}`)
	t.Setenv("WORKER_OPERATIONAL_BRIDGE_URL", fake.server.URL)
	t.Setenv("WORKER_METRIC_REPAIR_TOKEN", secretToken)

	var stdout, stderr bytes.Buffer
	dispatchMetricsExecutionRepair(context.Background(), []string{
		"--execution", testAmbiguousExecutionID,
		"--expected-state", "ambiguous",
		"--expected-attempt-count", "1",
		"--resolution", "retry_safe",
		"--review-evidence", "checked",
	}, &stdout, &stderr)

	if strings.Contains(stdout.String(), secretToken) || strings.Contains(stderr.String(), secretToken) {
		t.Fatalf("token leaked into CLI output: stdout=%q stderr=%q", stdout.String(), stderr.String())
	}
}

func TestDispatchMetricsExecutionRepairFailsClosedWithoutConfiguredToken(t *testing.T) {
	t.Setenv("WORKER_OPERATIONAL_BRIDGE_URL", "http://unused.invalid")
	t.Setenv("WORKER_METRIC_REPAIR_TOKEN", "")

	var stdout, stderr bytes.Buffer
	code := dispatchMetricsExecutionRepair(context.Background(), []string{
		"--execution", testAmbiguousExecutionID,
		"--expected-state", "ambiguous",
		"--expected-attempt-count", "1",
		"--resolution", "retry_safe",
		"--review-evidence", "checked",
	}, &stdout, &stderr)
	if code == 0 {
		t.Fatal("dispatchMetricsExecutionRepair with no WORKER_METRIC_REPAIR_TOKEN = 0, want fail-closed")
	}
}

func TestDispatchMetricsExecutionRepairDryRunNeverCallsTheBridgeAndRedactsTheToken(t *testing.T) {
	const secretToken = "metric-token-must-never-appear-in-dry-run-output"
	called := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)
	t.Setenv("WORKER_OPERATIONAL_BRIDGE_URL", server.URL)
	t.Setenv("WORKER_METRIC_REPAIR_TOKEN", secretToken)

	var stdout, stderr bytes.Buffer
	code := dispatchMetricsExecutionRepair(context.Background(), []string{
		"--execution", testAmbiguousExecutionID,
		"--expected-state", "ambiguous",
		"--expected-attempt-count", "4",
		"--resolution", "confirm_succeeded",
		"--review-evidence", "file_hotspot_daily already has rows for this run/family",
		"--output-evidence", `{"rows_written": 128}`,
		"--dry-run",
	}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("dispatchMetricsExecutionRepair --dry-run = %d, want 0; stderr=%s", code, stderr.String())
	}
	if called {
		t.Fatal("dispatchMetricsExecutionRepair --dry-run called the bridge; want no network call at all")
	}
	if strings.Contains(stdout.String(), secretToken) {
		t.Fatalf("token leaked into --dry-run output: %q", stdout.String())
	}
	var decoded map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &decoded); err != nil {
		t.Fatalf("--dry-run stdout is not JSON: %v (%q)", err, stdout.String())
	}
	if decoded["dry_run"] != true {
		t.Fatalf("decoded[dry_run] = %v, want true", decoded["dry_run"])
	}
	if auth, _ := decoded["authorization"].(string); !strings.Contains(auth, "REDACTED") {
		t.Fatalf("decoded[authorization] = %q, want it redacted", auth)
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

func TestPostWorkerBridgeReturnsStatusAndBodyOnNon2xxWithoutError(t *testing.T) {
	// The repair verbs depend on postWorkerBridge NOT treating a 409/422 as a
	// Go error -- they need the decoded body to print verbatim. Only a
	// transport-level failure (bad config, network error) is a Go error.
	fake := newFakeBridge(t, http.StatusConflict, `{"detail":"conflict"}`)
	t.Setenv("WORKER_OPERATIONAL_BRIDGE_URL", fake.server.URL)
	t.Setenv("WORKER_WORKGRAPH_REPAIR_TOKEN", "t")
	status, body, err := postWorkerBridge(context.Background(), "WORKER_WORKGRAPH_REPAIR_TOKEN", "/x", map[string]any{})
	if err != nil {
		t.Fatalf("postWorkerBridge on a 409: err = %v, want nil", err)
	}
	if status != http.StatusConflict {
		t.Fatalf("status = %d, want 409", status)
	}
	if body["detail"] != "conflict" {
		t.Fatalf("body = %v, want the decoded conflict detail", body)
	}
}
