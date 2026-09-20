package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/joboperator"
)

// repairVerbCases are the operator repair verbs that reach a repair
// transaction, each with a request that passes every local flag check.
func repairVerbCases(extra ...string) map[string]func(ctx context.Context, runtime *operatorRuntime, stdout, stderr io.Writer) int {
	return map[string]func(ctx context.Context, runtime *operatorRuntime, stdout, stderr io.Writer) int{
		"workgraph repair": func(ctx context.Context, runtime *operatorRuntime, stdout, stderr io.Writer) int {
			return dispatchWorkgraphRepair(ctx, runtime, append([]string{
				"--request", "0b1f3b0a-6d0c-4f5e-9a55-1c2d3e4f5a6b",
				"--resolution", "retry_safe",
				"--expected-attempt-count", "1",
				"--review-evidence", "checked the target has no rows",
			}, extra...), stdout, stderr)
		},
		"metrics execution-repair": func(ctx context.Context, runtime *operatorRuntime, stdout, stderr io.Writer) int {
			return dispatchMetricsExecutionRepair(ctx, runtime, append([]string{
				"--execution", "0b1f3b0a-6d0c-4f5e-9a55-1c2d3e4f5a6b",
				"--expected-state", "ambiguous",
				"--expected-attempt-count", "1",
				"--resolution", "retry_safe",
				"--review-evidence", "checked the target has no rows",
			}, extra...), stdout, stderr)
		},
	}
}

// A credential the authorizer refuses reaches no repair transaction: the
// answer is the bounded unauthorized code and nothing on stdout, dry-run
// included.
func TestRepairVerbsRefuseAnUnauthorizedCredentialEvenOnDryRun(t *testing.T) {
	for _, dryRun := range [][]string{nil, {"--dry-run"}} {
		for name, run := range repairVerbCases(dryRun...) {
			runtime := commandRuntime(t, commandAuthorizer{err: joboperator.ErrAuthorization})
			var stdout, stderr bytes.Buffer
			code := run(context.Background(), runtime, &stdout, &stderr)
			if code != 1 || !bytes.Contains(stderr.Bytes(), []byte("unauthorized")) || stdout.Len() != 0 {
				t.Fatalf("%s %v: code=%d stdout=%q stderr=%q, want code 1, unauthorized, empty stdout", name, dryRun, code, stdout.String(), stderr.String())
			}
		}
	}
}

// An authorized credential is not refused by the gate: without a database the
// verb reports the backend unavailable instead.
func TestRepairVerbsPassTheGateForAnAuthorizedCredential(t *testing.T) {
	for name, run := range repairVerbCases() {
		runtime := commandRuntime(t, commandAuthorizer{})
		var stdout, stderr bytes.Buffer
		code := run(context.Background(), runtime, &stdout, &stderr)
		if code != 1 || !bytes.Contains(stderr.Bytes(), []byte("operator_backend_unavailable")) {
			t.Fatalf("%s: code=%d stderr=%q, want operator_backend_unavailable", name, code, stderr.String())
		}
	}
}

// The bulk redrive (metrics daily-redrive and the finalize ledger gate) takes
// its redriver from the same gate.
func TestLedgerRedriverForRefusesAnUnauthorizedCredential(t *testing.T) {
	runtime := commandRuntime(t, commandAuthorizer{err: joboperator.ErrAuthorization})
	redrive, err := ledgerRedriverFor(context.Background(), runtime)
	if redrive != nil || !errors.Is(err, joboperator.ErrAuthorization) {
		t.Fatalf("redrive=%v err=%v, want no redriver and ErrAuthorization", redrive != nil, err)
	}
}

func TestParseOutputEvidenceRejectsASecondJSONValue(t *testing.T) {
	for _, raw := range []string{
		`{"verified":true}{"dropped":true}`,
		`{"verified":true} {}`,
		`{"verified":true} garbage`,
		`{"verified":true}[1]`,
	} {
		if _, err := parseOutputEvidence(raw); err == nil {
			t.Errorf("parseOutputEvidence(%q) accepted trailing content", raw)
		}
	}
	for _, raw := range []string{`{"verified":true}`, "  {\"verified\":true}\n"} {
		if _, err := parseOutputEvidence(raw); err != nil {
			t.Errorf("parseOutputEvidence(%q) = %v, want accepted", raw, err)
		}
	}
}
