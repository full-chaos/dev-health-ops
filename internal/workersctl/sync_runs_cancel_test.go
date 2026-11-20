package workersctl

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/joboperator"
)

const syncRunsCancelTestRun = "0b1f3b0a-6d0c-4f5e-9a55-1c2d3e4f5a6b"

// `sync-runs cancel` is an audited direct write on one run of one organization: the intent row
// carries the jobs.cancel action, the sync_run resource (canonical org:run), the reason and the
// correlation id, and is written before anything else.
func TestSyncRunsCancelAuditsTheRunBeforeItWrites(t *testing.T) {
	auditor := &refusingAuditor{}
	runtime := commandRuntimeWithAuditor(t, commandAuthorizer{}, auditor)
	var stdout, stderr bytes.Buffer
	code := dispatch(context.Background(), runtime, []string{
		"sync-runs", "cancel", "--org", strings.ToUpper(auditTestOrg), "--run", strings.ToUpper(syncRunsCancelTestRun),
		"--reason", "operator_test", "--correlation-id", "corr-1",
	}, &stdout, &stderr)
	if code != 1 || stderr.String() != "{\"error\":{\"code\":\"audit_unavailable\"}}\n" || stdout.Len() != 0 {
		t.Fatalf("code=%d stdout=%q stderr=%q, want audit_unavailable before any write", code, stdout.String(), stderr.String())
	}
	if len(auditor.events) != 1 {
		t.Fatalf("audit intents = %d, want 1", len(auditor.events))
	}
	event := auditor.events[0]
	if event.Action != "jobs.cancel" || event.ResourceType != "sync_run" ||
		event.ResourceID != auditTestOrg+":"+syncRunsCancelTestRun ||
		event.ReasonCode != "operator_test" || event.CorrelationID != "corr-1" || event.Principal != joboperator.OperatorPrincipal {
		t.Fatalf("audit intent %+v, want jobs.cancel on sync_run %s:%s with the reason and correlation id",
			event, auditTestOrg, syncRunsCancelTestRun)
	}
}

// A malformed request is a usage error that asks for no audit row.
func TestSyncRunsCancelRefusesAMalformedRequestBeforeTheAudit(t *testing.T) {
	audit := []string{"--reason", "operator_test", "--correlation-id", "corr-1"}
	for name, argv := range map[string][]string{
		"no run":          append([]string{"sync-runs", "cancel", "--org", auditTestOrg}, audit...),
		"bad run":         append([]string{"sync-runs", "cancel", "--org", auditTestOrg, "--run", "run-1"}, audit...),
		"no org":          append([]string{"sync-runs", "cancel", "--run", syncRunsCancelTestRun}, audit...),
		"bad org":         append([]string{"sync-runs", "cancel", "--org", "acme", "--run", syncRunsCancelTestRun}, audit...),
		"positional":      append([]string{"sync-runs", "cancel", "--org", auditTestOrg, "--run", syncRunsCancelTestRun, syncRunsCancelTestRun}, audit...),
		"no reason":       {"sync-runs", "cancel", "--org", auditTestOrg, "--run", syncRunsCancelTestRun, "--correlation-id", "corr-1"},
		"unknown subverb": append([]string{"sync-runs", "abort", "--org", auditTestOrg, "--run", syncRunsCancelTestRun}, audit...),
		"no subverb":      {"sync-runs"},
	} {
		auditor := &refusingAuditor{}
		runtime := commandRuntimeWithAuditor(t, commandAuthorizer{}, auditor)
		var stdout, stderr bytes.Buffer
		code := dispatch(context.Background(), runtime, argv, &stdout, &stderr)
		if code != 2 || stderr.String() != invalidRequestJSON || stdout.Len() != 0 || len(auditor.events) != 0 {
			t.Errorf("%s: code=%d stdout=%q stderr=%q intents=%d, want invalid_request and no intent",
				name, code, stdout.String(), stderr.String(), len(auditor.events))
		}
	}
}

// A write that cannot reach the domain pool completes its audit row failed and prints nothing.
func TestSyncRunsCancelCompletesTheAuditRowFailedWhenTheWriteCannotRun(t *testing.T) {
	auditor := &recordingAuditor{}
	runtime := commandRuntimeWithAuditor(t, commandAuthorizer{}, auditor)
	var stdout, stderr bytes.Buffer
	code := dispatch(context.Background(), runtime, []string{
		"sync-runs", "cancel", "--org", auditTestOrg, "--run", syncRunsCancelTestRun,
		"--reason", "operator_test", "--correlation-id", "corr-1",
	}, &stdout, &stderr)
	if code != 1 || stderr.String() != "{\"error\":{\"code\":\"operator_backend_unavailable\"}}\n" || stdout.Len() != 0 {
		t.Fatalf("code=%d stdout=%q stderr=%q, want operator_backend_unavailable", code, stdout.String(), stderr.String())
	}
	if len(auditor.events) != 1 || len(auditor.statuses) != 1 || auditor.statuses[0] != joboperator.AuditFailed {
		t.Fatalf("intents=%d statuses=%v, want one intent completed failed", len(auditor.events), auditor.statuses)
	}
}
