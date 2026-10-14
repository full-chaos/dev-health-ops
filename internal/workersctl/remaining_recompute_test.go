package workersctl

import (
	"bytes"
	"context"
	"testing"
	"time"
)

func TestDispatchMetricsRemainingRecomputeValidatesFlagsBeforeTouchingTheBackend(t *testing.T) {
	const org = "00000000-0000-4000-8000-000000000001"
	closedDay := time.Now().UTC().AddDate(0, 0, -3).Format("2006-01-02")
	closedDayTo := time.Now().UTC().AddDate(0, 0, -2).Format("2006-01-02")
	today := time.Now().UTC().Format("2006-01-02")
	audit := []string{"--reason", "operator_test", "--correlation-id", "corr-1"}
	base := func(extra ...string) []string {
		args := []string{"remaining", "start", "--family", "dora", "--org", org, "--review-evidence", "testing"}
		return append(append(args, extra...), audit...)
	}
	cases := map[string][]string{
		"recompute without basis":      base("--recompute", "--day", closedDay, "--to", closedDayTo),
		"recompute with a bad basis":   base("--recompute", "--basis", "Not A Label", "--day", closedDay, "--to", closedDayTo),
		"recompute with a long basis":  base("--recompute", "--basis", "a123456789a123456789a123456789a1234567890", "--day", closedDay, "--to", closedDayTo),
		"recompute without --to":       base("--recompute", "--basis", "cfr-125", "--day", closedDay),
		"recompute reaching today":     base("--recompute", "--basis", "cfr-125", "--day", closedDay, "--to", today),
		"recompute over the day limit": base("--recompute", "--basis", "cfr-125", "--day", "2026-01-01", "--to", "2026-12-31"),
		"recompute without evidence": {"remaining", "start", "--recompute", "--basis", "cfr-125", "--family", "dora", "--org", org,
			"--day", closedDay, "--to", closedDayTo, "--reason", "operator_test", "--correlation-id", "corr-1"},
		"basis without recompute":   base("--basis", "cfr-125", "--day", closedDay),
		"dry-run without recompute": base("--dry-run", "--day", closedDay),
		"recompute of complexity": {"remaining", "start", "--recompute", "--basis", "cfr-125", "--family", "complexity", "--org", org,
			"--day", closedDay, "--to", closedDayTo, "--review-evidence", "testing", "--reason", "operator_test", "--correlation-id", "corr-1"},
		"recompute of release_impact": {"remaining", "start", "--recompute", "--basis", "cfr-125", "--family", "release_impact", "--org", org,
			"--day", closedDay, "--to", closedDayTo, "--review-evidence", "testing", "--reason", "operator_test", "--correlation-id", "corr-1"},
		"recompute of capacity": {"remaining", "start", "--recompute", "--basis", "cfr-125", "--family", "capacity", "--org", org,
			"--day", closedDay, "--to", closedDayTo, "--review-evidence", "testing", "--reason", "operator_test", "--correlation-id", "corr-1"},
	}
	for name, args := range cases {
		var stdout, stderr bytes.Buffer
		code := dispatchMetrics(context.Background(), &operatorRuntime{}, args, &stdout, &stderr)
		if code != 2 || stderr.String() != invalidRequestJSON || stdout.Len() != 0 {
			t.Errorf("%s: code=%d stdout=%q stderr=%q, want invalid_request", name, code, stdout.String(), stderr.String())
		}
	}
}

func TestDispatchMetricsRemainingRecomputeDryRunNeedsNoAuditFlagsOrEvidence(t *testing.T) {
	closedDay := time.Now().UTC().AddDate(0, 0, -3).Format("2006-01-02")
	var stdout, stderr bytes.Buffer
	code := dispatchMetrics(context.Background(), &operatorRuntime{}, []string{
		"remaining", "start", "--recompute", "--dry-run", "--basis", "cfr-125", "--family", "dora",
		"--org", "00000000-0000-4000-8000-000000000001", "--day", closedDay, "--to", closedDay,
	}, &stdout, &stderr)
	const operatorBackendUnavailableJSON = "{\"error\":{\"code\":\"operator_backend_unavailable\"}}\n"
	if code != 1 || stderr.String() != operatorBackendUnavailableJSON {
		t.Fatalf("dry run refused at validation: code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
}
