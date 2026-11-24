package workersctl

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/joboperator"
)

// DO NOT MERGE. The card's verb argv (preview and apply), byte for byte after `dho workers`, through the stdin preflight and
// the verb's flag parser. The card's --from/--to/--through come from its coverage read; the values here are the exec leg
// fixture's (2026-11-20, 2026-11-22, 2026-11-25). The plan read is the package's one-day stub, so the preview prints a plan
// and the apply reaches its write, which stops at the missing database pools after its audit row opened.
func TestCardArgvFcs14544ParsesTheCardFlags(t *testing.T) {
	preview := []string{"metrics", "testops-slot-recompute", "--org-stdin", "--from", "2026-11-20", "--to", "2026-11-22",
		"--through", "2026-11-25", "--rerun-tag", "testops-slots-1", "--max-repo-days", "200"}
	apply := append(append([]string{}, preview...), "--apply", "--reason", "testops-slot-recompute", "--correlation-id", "card-14544-1")
	run := func(argv []string) (int, string, string, *recordingAuditor) {
		t.Helper()
		auditor := &recordingAuditor{}
		runtime := commandRuntimeWithAuditor(t, commandAuthorizer{}, auditor)
		var stdout, stderr bytes.Buffer
		stdin, code := preflightOrgStdin(argv, strings.NewReader(auditTestOrg+"\n"), &stderr)
		if code != 0 {
			return code, stdout.String(), stderr.String(), auditor
		}
		runtime.stdin = stdin
		code = dispatch(context.Background(), runtime, argv, &stdout, &stderr)
		t.Logf("argv %v: exit %d\nstdout: %s\nstderr: %s", argv, code, stdout.String(), stderr.String())
		return code, stdout.String(), stderr.String(), auditor
	}

	code, stdout, stderr, auditor := run(preview)
	if code != 0 || stderr != "" || len(auditor.events) != 0 {
		t.Fatalf("preview: code=%d stderr=%q audit rows %d, want 0, no stderr, no audit row", code, stderr, len(auditor.events))
	}
	for _, want := range []string{`"preview":true`, `"rerun_tag":"testops-slots-1"`, `"from":"2026-11-20"`, `"to":"2026-11-22"`,
		`"through":"2026-11-25"`, `"repo_days":1`, `"repos":1`, `"repo_id_prefixes":["0b1f3b0a"]`} {
		if !strings.Contains(stdout, want) {
			t.Errorf("preview stdout %q holds no %s", stdout, want)
		}
	}
	if strings.Contains(stdout, auditTestOrg) {
		t.Errorf("preview stdout names the organization")
	}

	code, stdout, stderr, auditor = run(apply)
	if code != 1 || stdout != "" || !strings.Contains(stderr, "operator_backend_unavailable") {
		t.Fatalf("apply: code=%d stdout=%q stderr=%q, want the flags accepted and the write stopped at the missing pools", code, stdout, stderr)
	}
	if len(auditor.events) != 1 {
		t.Fatalf("apply: %d audit rows, want 1", len(auditor.events))
	}
	event := auditor.events[0]
	if event.Action != joboperator.ActionMetricsDailyStart || event.ReasonCode != "testops-slot-recompute" ||
		event.CorrelationID != "card-14544-1" || event.ResourceType != "organization" || event.ResourceID != auditTestOrg {
		t.Errorf("apply audit row %+v, want metrics.daily_start, reason testops-slot-recompute, correlation card-14544-1 on the org", event)
	}

	// Controls: a positional, and a cap over the verb's bound, are usage errors with no audit row.
	for name, argv := range map[string][]string{
		"positional": append(append([]string{}, preview...), "extra"),
		"cap 10001":  append(append([]string{}, preview[:len(preview)-1]...), "10001"),
	} {
		code, _, stderr, auditor := run(argv)
		if code != 2 || stderr != invalidRequestJSON || len(auditor.events) != 0 {
			t.Errorf("%s: code=%d stderr=%q audit rows %d, want invalid_request and no audit row", name, code, stderr, len(auditor.events))
		}
	}
}
