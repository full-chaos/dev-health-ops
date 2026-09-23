package workersctl

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"sort"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/joboperator"
)

// auditFlags is a valid --reason/--correlation-id pair, the flags every
// audited write verb requires unless --dry-run.
func auditFlags(extra ...string) []string {
	return append([]string{"--reason", "operator_test", "--correlation-id", "corr-1"}, extra...)
}

// refusingAuditor records every audit intent it is asked to write and
// refuses it, the way PostgresAuditor answers when its insert fails.
type refusingAuditor struct{ events []joboperator.AuditEvent }

func (auditor *refusingAuditor) Begin(_ context.Context, event joboperator.AuditEvent) (joboperator.AuditHandle, error) {
	auditor.events = append(auditor.events, event)
	return nil, joboperator.ErrAuditUnavailable
}

// directWriteVerb is one direct-write `dho workers` invocation whose flags
// pass every local check.
type directWriteVerb struct {
	argv   []string
	dryRun bool // the verb has --dry-run
}

const auditTestOrg = "00000000-0000-4000-8000-000000000001"

func directWriteVerbs() map[string]directWriteVerb {
	org := auditTestOrg
	id := "0b1f3b0a-6d0c-4f5e-9a55-1c2d3e4f5a6b"
	return map[string]directWriteVerb{
		"workgraph trigger":                                  {argv: []string{"workgraph", "trigger", "--org", org, "--review-evidence", "testing"}, dryRun: true},
		"investment trigger":                                 {argv: []string{"investment", "trigger", "--org", org, "--review-evidence", "testing"}, dryRun: true},
		"workgraph repair":                                   {argv: []string{"workgraph", "repair", "--request", id, "--resolution", "retry_safe", "--expected-attempt-count", "1", "--review-evidence", "checked the target has no rows"}, dryRun: true},
		"metrics exec-repair":                                {argv: []string{"metrics", "execution-repair", "--execution", id, "--expected-state", "ambiguous", "--expected-attempt-count", "1", "--resolution", "retry_safe", "--review-evidence", "checked the target has no rows"}, dryRun: true},
		"metrics daily-start":                                {argv: []string{"metrics", "daily-start", "--org", org, "--day", "2026-08-01"}},
		"metrics daily-redrive":                              {argv: []string{"metrics", "daily-redrive", "--org", org, "--from", "2026-08-01", "--to", "2026-08-01", "--review-evidence", "testing"}},
		"metrics daily-finalize":                             {argv: []string{"metrics", "daily-finalize", "--run", id, "--review-evidence", "testing"}},
		"metrics finalize-redrive":                           {argv: []string{"metrics", "finalize-redrive", "--org", org, "--from", "2026-08-01", "--to", "2026-08-01", "--review-evidence", "testing"}, dryRun: true},
		"metrics partition-recompute":                        {argv: []string{"metrics", "partition-recompute", "--org", org, "--from", "2026-08-01", "--to", "2026-08-01", "--family", "repo_user_commit", "--review-evidence", "testing"}, dryRun: true},
		"metrics remaining start":                            {argv: []string{"metrics", "remaining", "start", "--family", "dora", "--org", org, "--day", "2026-01-01", "--review-evidence", "testing"}},
		"metrics remaining trigger-backstop":                 {argv: []string{"metrics", "remaining", "trigger-backstop", "--family", "dora", "--org", org, "--review-evidence", "testing"}},
		"metrics remaining redrive":                          {argv: []string{"metrics", "remaining", "redrive", "--org", org, "--review-evidence", "testing"}, dryRun: true},
		"external-recompute replay":                          {argv: []string{"external-recompute", "replay", "--review-evidence", "testing"}, dryRun: true},
		"providersync retire-linear-pseudo-projects":         {argv: []string{"providersync", "retire-linear-pseudo-projects", "--org", org}, dryRun: true},
		"providersync retire-stale-linear-project-ownership": {argv: []string{"providersync", "retire-stale-linear-project-ownership", "--org", org}, dryRun: true},
		"sync-dispatch-outbox close-backlog":                 {argv: []string{"sync-dispatch-outbox", "close-backlog"}, dryRun: true},
	}
}

// TestEveryDirectWriteVerbIsAuditedBeforeItWrites runs every direct-write
// verb against an auditor that refuses the intent row. Each verb must ask
// for exactly one audit row, as the operator principal with the given
// reason and correlation id, and stop with audit_unavailable and nothing on
// stdout: the runtime has no database, so a write that ran anyway would
// answer with a backend or configuration error instead. The Actions the
// verbs ask for must be exactly joboperator.DirectWriteActions, so a new
// direct-write Action fails here until a verb uses it.
func TestEveryDirectWriteVerbIsAuditedBeforeItWrites(t *testing.T) {
	seen := map[string]bool{}
	for name, verb := range directWriteVerbs() {
		auditor := &refusingAuditor{}
		runtime := commandRuntimeWithAuditor(t, commandAuthorizer{}, auditor)
		var stdout, stderr bytes.Buffer
		code := dispatch(context.Background(), runtime, append(slices.Clone(verb.argv), auditFlags()...), &stdout, &stderr)
		if code != 1 || stderr.String() != "{\"error\":{\"code\":\"audit_unavailable\"}}\n" || stdout.Len() != 0 {
			t.Errorf("%s: code=%d stdout=%q stderr=%q, want audit_unavailable before any write", name, code, stdout.String(), stderr.String())
			continue
		}
		if len(auditor.events) != 1 {
			t.Errorf("%s: %d audit intents, want 1", name, len(auditor.events))
			continue
		}
		event := auditor.events[0]
		if event.Principal != joboperator.OperatorPrincipal || event.ReasonCode != "operator_test" || event.CorrelationID != "corr-1" ||
			event.ResourceType == "" || event.ResourceID == "" {
			t.Errorf("%s: audit intent %+v, want the operator principal, the given reason and correlation, and a resource", name, event)
		}
		seen[string(event.Action)] = true
	}
	want := map[string]bool{}
	for _, action := range joboperator.DirectWriteActions {
		want[string(action)] = true
	}
	if !slices.Equal(sortedSet(seen), sortedSet(want)) {
		t.Fatalf("verbs audit %v, DirectWriteActions = %v", sortedSet(seen), sortedSet(want))
	}
}

// A --dry-run preview writes nothing, so it asks for no audit row and needs
// no --reason/--correlation-id.
func TestDirectWriteDryRunWritesNoAuditRow(t *testing.T) {
	for name, verb := range directWriteVerbs() {
		if !verb.dryRun {
			continue
		}
		auditor := &refusingAuditor{}
		runtime := commandRuntimeWithAuditor(t, commandAuthorizer{}, auditor)
		var stdout, stderr bytes.Buffer
		code := dispatch(context.Background(), runtime, append(slices.Clone(verb.argv), "--dry-run"), &stdout, &stderr)
		if len(auditor.events) != 0 {
			t.Errorf("%s --dry-run: %d audit intents, want 0", name, len(auditor.events))
		}
		if code == 2 {
			t.Errorf("%s --dry-run: refused as a usage error without --reason: stderr=%q", name, stderr.String())
		}
	}
}

// A write without --reason and --correlation-id, or with a malformed one, is
// a usage error that asks for no audit row -- whatever the caller's authority:
// the authorizer here refuses everything, so a verb that checked authority
// before its flags would answer unauthorized instead.
func TestDirectWriteRequiresReasonAndCorrelation(t *testing.T) {
	for name, verb := range directWriteVerbs() {
		for label, flags := range map[string][]string{
			"none":             nil,
			"reason only":      {"--reason", "operator_test"},
			"correlation only": {"--correlation-id", "corr-1"},
			"bad reason":       {"--reason", "Not A Code", "--correlation-id", "corr-1"},
			"bad correlation":  {"--reason", "operator_test", "--correlation-id", "has space"},
		} {
			auditor := &refusingAuditor{}
			runtime := commandRuntimeWithAuditor(t, commandAuthorizer{err: joboperator.ErrAuthorization}, auditor)
			var stdout, stderr bytes.Buffer
			code := dispatch(context.Background(), runtime, append(slices.Clone(verb.argv), flags...), &stdout, &stderr)
			if code != 2 || stderr.String() != invalidRequestJSON || len(auditor.events) != 0 {
				t.Errorf("%s (%s): code=%d stderr=%q intents=%d, want invalid_request and no intent", name, label, code, stderr.String(), len(auditor.events))
			}
		}
	}
}

// An unauthorized caller is refused before any audit row or write.
func TestDirectWriteRefusesAnUnauthorizedCaller(t *testing.T) {
	for name, verb := range directWriteVerbs() {
		auditor := &refusingAuditor{}
		runtime := commandRuntimeWithAuditor(t, commandAuthorizer{err: joboperator.ErrAuthorization}, auditor)
		var stdout, stderr bytes.Buffer
		code := dispatch(context.Background(), runtime, append(slices.Clone(verb.argv), auditFlags()...), &stdout, &stderr)
		if code != 1 || stderr.String() != "{\"error\":{\"code\":\"unauthorized\"}}\n" || stdout.Len() != 0 || len(auditor.events) != 0 {
			t.Errorf("%s: code=%d stdout=%q stderr=%q intents=%d, want unauthorized and no intent", name, code, stdout.String(), stderr.String(), len(auditor.events))
		}
	}
}

// recordingAuditor accepts every intent and records how each completes.
type recordingAuditor struct {
	events   []joboperator.AuditEvent
	statuses []joboperator.AuditStatus
	complete error
}

type recordingHandle struct{ auditor *recordingAuditor }

func (handle recordingHandle) Complete(_ context.Context, status joboperator.AuditStatus) error {
	handle.auditor.statuses = append(handle.auditor.statuses, status)
	return handle.auditor.complete
}

func (auditor *recordingAuditor) Begin(_ context.Context, event joboperator.AuditEvent) (joboperator.AuditHandle, error) {
	auditor.events = append(auditor.events, event)
	return recordingHandle{auditor: auditor}, nil
}

// A write that fails completes its audit row failed and keeps the verb's own
// answer; a write whose audit row cannot be completed keeps its answer too,
// and the failure is not swallowed.
func TestAuditedWriteCompletesTheRowWithTheWriteOutcome(t *testing.T) {
	flags := mutationFlags{reason: ptr("operator_test"), correlation: ptr("corr-1")}
	for _, test := range []struct {
		name       string
		writeCode  int
		complete   error
		wantCode   int
		wantStatus joboperator.AuditStatus
		wantStderr string
	}{
		{"success", 0, nil, 0, joboperator.AuditSucceeded, ""},
		{"write failed", 1, nil, 1, joboperator.AuditFailed, "{\"error\":{\"code\":\"write_failed\"}}\n"},
		{"success, completion failed", 0, errors.New("complete failed"), 1, joboperator.AuditSucceeded, "{\"error\":{\"code\":\"audit_pending\"}}\n"},
		{"write failed, completion failed", 1, errors.New("complete failed"), 1, joboperator.AuditFailed, "{\"error\":{\"code\":\"write_failed\"}}\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			auditor := &recordingAuditor{complete: test.complete}
			runtime := commandRuntimeWithAuditor(t, commandAuthorizer{}, auditor)
			var stderr bytes.Buffer
			ran := 0
			code := auditedWrite(context.Background(), runtime, &stderr, flags, joboperator.ActionMetricsDailyStart, "organization", auditTestOrg,
				func(context.Context) int {
					ran++
					if test.writeCode != 0 {
						return writeError(&stderr, "write_failed")
					}
					return 0
				})
			if ran != 1 || code != test.wantCode || stderr.String() != test.wantStderr {
				t.Fatalf("ran=%d code=%d stderr=%q, want ran=1 code=%d stderr=%q", ran, code, stderr.String(), test.wantCode, test.wantStderr)
			}
			if len(auditor.events) != 1 || len(auditor.statuses) != 1 || auditor.statuses[0] != test.wantStatus {
				t.Fatalf("intents=%d statuses=%v, want one intent completed %s", len(auditor.events), auditor.statuses, test.wantStatus)
			}
		})
	}
}

func ptr(value string) *string { return &value }

func sortedSet(set map[string]bool) []string {
	keys := make([]string, 0, len(set))
	for key := range set {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
