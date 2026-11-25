package workersctl

import (
	"bytes"
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/joboperator"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily"
)

// DO NOT MERGE. A card's narrow recompute argv (preview, apply, replay form), byte for byte after `dho workers`, through the
// stdin preflight and the verb's flag parser: one --repo-id, the window flags, the rerun tag, the cap, and the audited write
// flags. The plan read is a recording stub, so the cell sees the request the flags build; the apply stops at the missing
// database pools after its audit row opened. Bad forms refuse with invalid_request and no audit row.
func TestCardArgvSlotRecomputeRepoIDFlags(t *testing.T) {
	const repoID = "a1a1a1a1-0000-4000-8000-0000000000b1"
	preview := []string{"metrics", "testops-slot-recompute", "--org-stdin", "--repo-id", repoID, "--from", "2026-03-09",
		"--to", "2026-03-10", "--through", "2026-03-14", "--rerun-tag", "testops-slots-a1", "--max-repo-days", "200"}
	apply := append(append([]string{}, preview...), "--apply", "--reason", "testops-slot-recompute", "--correlation-id", "card-00000-a1")
	var requests []daily.RetiredSlotPlanRequest
	run := func(argv []string) (int, string, string, *recordingAuditor) {
		t.Helper()
		auditor := &recordingAuditor{}
		runtime := commandRuntimeWithAuditor(t, commandAuthorizer{}, auditor)
		runtime.retiredSlotPlan = func(ctx context.Context, org string, request daily.RetiredSlotPlanRequest) (daily.RetiredSlotPlan, error) {
			requests = append(requests, request)
			return commandRetiredSlotPlan(ctx, org, request)
		}
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
	for _, want := range []string{`"preview":true`, `"rerun_tag":"testops-slots-a1"`, `"from":"2026-03-09"`, `"to":"2026-03-10"`, `"through":"2026-03-14"`} {
		if !strings.Contains(stdout, want) {
			t.Errorf("preview stdout %q holds no %s", stdout, want)
		}
	}
	if strings.Contains(stdout, auditTestOrg) || strings.Contains(stdout, repoID) {
		t.Errorf("preview stdout names the organization or the full repository id")
	}
	if len(requests) != 1 {
		t.Fatalf("plan reads: %d, want 1", len(requests))
	}
	request := requests[0]
	day := func(text string) time.Time {
		parsed, err := time.Parse(time.DateOnly, text)
		if err != nil {
			t.Fatal(err)
		}
		return parsed
	}
	if !slices.Equal(request.RepoIDs, []string{repoID}) || !request.From.Equal(day("2026-03-09")) || !request.To.Equal(day("2026-03-10")) ||
		!request.Through.Equal(day("2026-03-14")) || request.MaxRepoDays != 200 {
		t.Errorf("plan request %+v, want repo %s, 2026-03-09 .. 2026-03-10 through 2026-03-14, cap 200", request, repoID)
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
		event.CorrelationID != "card-00000-a1" || event.ResourceType != "organization" || event.ResourceID != auditTestOrg {
		t.Errorf("apply audit row %+v, want metrics.daily_start, reason testops-slot-recompute, the correlation id, on the org", event)
	}
	if len(requests) != 2 || !slices.Equal(requests[1].RepoIDs, []string{repoID}) {
		t.Errorf("apply plan request %+v, want the same repository", requests)
	}

	// Bad forms: each is a usage error with no audit row and no plan read.
	without := func(flag string) []string {
		out := []string{}
		for index := 0; index < len(apply); index++ {
			if apply[index] == flag {
				index++
				continue
			}
			out = append(out, apply[index])
		}
		return out
	}
	bad := map[string][]string{
		"repo id not a uuid":    append(append([]string{}, preview[:4]...), append([]string{"a1a1a1a1"}, preview[5:]...)...),
		"positional":            append(append([]string{}, preview...), "extra"),
		"cap 10001":             append(append([]string{}, preview[:len(preview)-1]...), "10001"),
		"apply without through": without("--through"),
		"apply without reason":  without("--reason"),
		"no rerun tag":          append(append([]string{}, preview[:11]...), preview[13:]...),
	}
	for name, argv := range bad {
		before := len(requests)
		code, _, stderr, auditor := run(argv)
		if code != 2 || stderr != invalidRequestJSON || len(auditor.events) != 0 || len(requests) != before {
			t.Errorf("%s: code=%d stderr=%q audit rows %d plan reads %d, want invalid_request, no audit row, no plan read", name, code, stderr,
				len(auditor.events), len(requests)-before)
		}
	}
}

// DO NOT MERGE. The card's stop-mid-way argv for one sync run (a dry run first, then the audited cancel with its exact reason
// code), through the stdin preflight and the verb's flag parser. The run read stops at the missing database pools, after the
// dry run (no audit row) or after the audit row opened. Bad forms refuse with invalid_request and no audit row.
func TestCardArgvSyncRunCancelFlags(t *testing.T) {
	const runID = "0c0c0c0c-0000-4000-8000-0000000000d1"
	dry := []string{"sync-runs", "cancel", "--org-stdin", "--run", runID, "--dry-run"}
	apply := []string{"sync-runs", "cancel", "--org-stdin", "--run", runID, "--reason", "rewalk-stop", "--correlation-id", "card-00000-a1"}
	run := func(argv []string) (int, string, *recordingAuditor) {
		t.Helper()
		auditor := &recordingAuditor{}
		runtime := commandRuntimeWithAuditor(t, commandAuthorizer{}, auditor)
		var stdout, stderr bytes.Buffer
		stdin, code := preflightOrgStdin(argv, strings.NewReader(auditTestOrg+"\n"), &stderr)
		if code != 0 {
			return code, stderr.String(), auditor
		}
		runtime.stdin = stdin
		code = dispatch(context.Background(), runtime, argv, &stdout, &stderr)
		t.Logf("argv %v: exit %d\nstdout: %s\nstderr: %s", argv, code, stdout.String(), stderr.String())
		return code, stderr.String(), auditor
	}
	for name, argv := range map[string][]string{"dry run": dry, "cancel": apply} {
		code, stderr, auditor := run(argv)
		if code == 2 || strings.Contains(stderr, "invalid_request") {
			t.Errorf("%s: code=%d stderr=%q, want the flags accepted", name, code, stderr)
		}
		if !strings.Contains(stderr, "operator_backend_unavailable") {
			t.Errorf("%s: stderr %q, want the run read stopped at the missing pools", name, stderr)
		}
		wantRows := 0
		if name == "cancel" {
			wantRows = 1
		}
		if len(auditor.events) != wantRows {
			t.Errorf("%s: %d audit rows, want %d", name, len(auditor.events), wantRows)
		}
		if name == "cancel" && len(auditor.events) == 1 {
			event := auditor.events[0]
			if event.Action != joboperator.ActionSyncRunCancel || event.ReasonCode != "rewalk-stop" || event.CorrelationID != "card-00000-a1" ||
				event.ResourceType != "sync_run" {
				t.Errorf("cancel audit row %+v, want sync run cancel, reason rewalk-stop, the correlation id", event)
			}
		}
	}
	for name, argv := range map[string][]string{
		"no run":              {"sync-runs", "cancel", "--org-stdin", "--dry-run"},
		"run not a uuid":      {"sync-runs", "cancel", "--org-stdin", "--run", "0c0c0c0c", "--dry-run"},
		"reason not a code":   {"sync-runs", "cancel", "--org-stdin", "--run", runID, "--reason", "Rewalk Stop", "--correlation-id", "card-00000-a1"},
		"write without words": {"sync-runs", "cancel", "--org-stdin", "--run", runID},
		"positional":          append(append([]string{}, dry...), "extra"),
	} {
		code, stderr, auditor := run(argv)
		if code != 2 || stderr != invalidRequestJSON || len(auditor.events) != 0 {
			t.Errorf("%s: code=%d stderr=%q audit rows %d, want invalid_request and no audit row", name, code, stderr, len(auditor.events))
		}
	}
}
