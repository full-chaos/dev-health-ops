package workersctl

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestTestopsSlotRecomputeRefusesAMalformedRequestBeforeAnyRead(t *testing.T) {
	org := auditTestOrg
	base := func(extra ...string) []string {
		return append([]string{"metrics", "testops-slot-recompute", "--org", org, "--from", "2026-07-01", "--to", "2026-07-02", "--rerun-tag", "fix-1"}, extra...)
	}
	cases := map[string][]string{
		"no rerun tag":         {"metrics", "testops-slot-recompute", "--org", org, "--from", "2026-07-01", "--to", "2026-07-02"},
		"malformed rerun tag":  {"metrics", "testops-slot-recompute", "--org", org, "--from", "2026-07-01", "--to", "2026-07-02", "--rerun-tag", "a|b"},
		"no from":              {"metrics", "testops-slot-recompute", "--org", org, "--to", "2026-07-02", "--rerun-tag", "fix-1"},
		"to before from":       {"metrics", "testops-slot-recompute", "--org", org, "--from", "2026-07-02", "--to", "2026-07-01", "--rerun-tag", "fix-1"},
		"window over 31 days":  {"metrics", "testops-slot-recompute", "--org", org, "--from", "2026-07-01", "--to", "2026-08-01", "--rerun-tag", "fix-1"},
		"malformed through":    base("--through", "soon"),
		"cap zero":             base("--max-repo-days", "0"),
		"malformed repo id":    base("--repo-id", "not-a-uuid"),
		"positional":           base("extra"),
		"apply without reason": base("--apply"),
		"malformed org":        {"metrics", "testops-slot-recompute", "--org", "not-a-uuid", "--from", "2026-07-01", "--to", "2026-07-02", "--rerun-tag", "fix-1"},
	}
	for name, argv := range cases {
		t.Run(name, func(t *testing.T) {
			auditor := &recordingAuditor{}
			runtime := commandRuntimeWithAuditor(t, commandAuthorizer{}, auditor)
			var stdout, stderr bytes.Buffer
			code := dispatch(context.Background(), runtime, argv, &stdout, &stderr)
			if code != 2 || stderr.String() != invalidRequestJSON || stdout.Len() != 0 {
				t.Fatalf("code=%d stdout=%q stderr=%q, want invalid_request", code, stdout.String(), stderr.String())
			}
			if len(auditor.events) != 0 {
				t.Fatalf("%d audit rows for a malformed request", len(auditor.events))
			}
		})
	}
	// Control: a well-formed preview passes the flags and stops only at the missing ClickHouse DSN, with no audit row.
	auditor := &recordingAuditor{}
	runtime := commandRuntimeWithAuditor(t, commandAuthorizer{}, auditor)
	runtime.lookup = func(string) (string, bool) { return "", false }
	runtime.retiredSlotPlan = nil
	var stdout, stderr bytes.Buffer
	code := dispatch(context.Background(), runtime, base("--through", "2026-07-31", "--max-repo-days", "5", "--repo-id", org), &stdout, &stderr)
	if code != 1 || !strings.Contains(stderr.String(), "configuration_error") || stdout.Len() != 0 || len(auditor.events) != 0 {
		t.Fatalf("well-formed preview: code=%d stdout=%q stderr=%q audit rows %d; want configuration_error and no audit row",
			code, stdout.String(), stderr.String(), len(auditor.events))
	}
}

// --max-repo-days is bounded (10,000); a larger cap, up to the largest UInt32, is a usage error, never a wrapped LIMIT.
// --apply needs an explicit --through, so a replay on a later clock day cannot plan a new day.
func TestTestopsSlotRecomputeBoundsTheCapAndAnApplyNamesItsThrough(t *testing.T) {
	org := auditTestOrg
	base := []string{"metrics", "testops-slot-recompute", "--org", org, "--from", "2026-07-01", "--to", "2026-07-02", "--rerun-tag", "fix-1"}
	run := func(extra ...string) (int, string, int) {
		t.Helper()
		auditor := &recordingAuditor{}
		runtime := commandRuntimeWithAuditor(t, commandAuthorizer{}, auditor)
		runtime.lookup = func(string) (string, bool) { return "", false }
		runtime.retiredSlotPlan = nil
		var stdout, stderr bytes.Buffer
		code := dispatch(context.Background(), runtime, append(append([]string{}, base...), extra...), &stdout, &stderr)
		return code, stderr.String(), len(auditor.events)
	}
	for _, cap := range []string{"10001", "4294967295", "4294967296"} {
		if code, stderr, audits := run("--max-repo-days", cap); code != 2 || stderr != invalidRequestJSON || audits != 0 {
			t.Errorf("--max-repo-days %s: code=%d stderr=%q audit rows %d; want invalid_request", cap, code, stderr, audits)
		}
	}
	if code, stderr, audits := run("--max-repo-days", "10000"); code != 1 || !strings.Contains(stderr, "configuration_error") || audits != 0 {
		t.Errorf("--max-repo-days 10000: code=%d stderr=%q audit rows %d; want the flags accepted (configuration_error, no ClickHouse DSN)", code, stderr, audits)
	}
	if code, stderr, audits := run(auditFlags("--apply")...); code != 2 || stderr != invalidRequestJSON || audits != 0 {
		t.Errorf("--apply without --through: code=%d stderr=%q audit rows %d; want invalid_request and no audit row", code, stderr, audits)
	}
}
