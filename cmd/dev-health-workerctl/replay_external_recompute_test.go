package main

// replay_external_recompute_test.go covers the command surface of the one-shot
// backlog drain (CHAOS-5296). The collapse and enqueue behaviour it drives is
// tested in internal/externalrecompute; what matters here is that the command
// cannot be run by accident, and cannot be run at all against a backend it has
// no business touching.

import (
	"bytes"
	"context"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/externalrecompute"
)

func TestDispatchExternalRecomputeRejectsUnknownVerb(t *testing.T) {
	for name, args := range map[string][]string{
		"no verb":      {},
		"unknown verb": {"drain"},
	} {
		var stdout, stderr bytes.Buffer
		code := dispatchExternalRecompute(context.Background(), &operatorRuntime{},
			args, &stdout, &stderr)
		if code != 1 || stderr.String() != invalidRequestJSON {
			t.Fatalf("%s: code=%d stdout=%q stderr=%q", name, code, stdout.String(), stderr.String())
		}
	}
}

// TestDispatchExternalRecomputeReplayRequiresReviewEvidence is the guard that
// makes this a deliberate act. The command retires rows and enqueues real
// recompute work across every org in the backlog; a bare `replay` with no
// stated reason must not be possible, exactly as for the manual workgraph and
// investment triggers.
func TestDispatchExternalRecomputeReplayRequiresReviewEvidence(t *testing.T) {
	for name, args := range map[string][]string{
		"absent": {"replay"},
		"blank":  {"replay", "--review-evidence", "   "},
	} {
		var stdout, stderr bytes.Buffer
		code := dispatchExternalRecomputeReplay(context.Background(), &operatorRuntime{},
			args[1:], &stdout, &stderr)
		if code != 1 || stderr.String() != invalidRequestJSON {
			t.Fatalf("%s review evidence: code=%d stdout=%q stderr=%q",
				name, code, stdout.String(), stderr.String())
		}
	}
}

func TestDispatchExternalRecomputeReplayRejectsBadFlags(t *testing.T) {
	for name, args := range map[string][]string{
		"non-positive limit": {"--review-evidence", "CHAOS-5296", "--limit", "0"},
		"unknown flag":       {"--review-evidence", "CHAOS-5296", "--force"},
		"stray argument":     {"--review-evidence", "CHAOS-5296", "extra"},
	} {
		var stdout, stderr bytes.Buffer
		code := dispatchExternalRecomputeReplay(context.Background(), &operatorRuntime{},
			args, &stdout, &stderr)
		if code != 1 || stderr.String() != invalidRequestJSON {
			t.Fatalf("%s: code=%d stdout=%q stderr=%q", name, code, stdout.String(), stderr.String())
		}
	}
}

// TestDispatchExternalRecomputeReplayNeedsABackend pins that even a --dry-run
// refuses without pools and a registry. Unlike the manual trigger commands,
// whose dry runs are pure flag arithmetic, this one's dry run READS the backlog
// to produce its report -- so there is nothing it can honestly print without a
// database, and printing an empty report would read as "no backlog".
func TestDispatchExternalRecomputeReplayNeedsABackend(t *testing.T) {
	for name, args := range map[string][]string{
		"real run": {"replay", "--review-evidence", "CHAOS-5296"},
		"dry run":  {"replay", "--review-evidence", "CHAOS-5296", "--dry-run"},
	} {
		var stdout, stderr bytes.Buffer
		code := dispatchExternalRecompute(context.Background(), &operatorRuntime{},
			args, &stdout, &stderr)
		if code != 1 {
			t.Fatalf("%s: code=%d stdout=%q", name, code, stdout.String())
		}
		if stderr.String() != "{\"error\":{\"code\":\"operator_backend_unavailable\"}}\n" {
			t.Fatalf("%s: stderr=%q", name, stderr.String())
		}
	}
}

// TestExternalRecomputeReplayExitCodeFollowsCompleteness is the r1 P2 fix at
// the command boundary: a replay that failed a group, or could not read a row,
// left work behind and must not exit 0. A runbook step or a script reading only
// the exit code would otherwise treat a partial drain as a finished one.
func TestExternalRecomputeReplayExitCodeFollowsCompleteness(t *testing.T) {
	for name, testCase := range map[string]struct {
		report externalrecompute.ReplayReport
		want   bool
	}{
		"clean":          {externalrecompute.ReplayReport{Rows: 2, Retired: 2}, false},
		"scopeless only": {externalrecompute.ReplayReport{Rows: 2, Retired: 2, ScopelessRows: 1}, false},
		"failed group":   {externalrecompute.ReplayReport{Rows: 2, Retired: 1, Failed: 1}, true},
		"unreadable row": {externalrecompute.ReplayReport{Rows: 2, Retired: 1, UnreadableRows: 1}, true},
		// A dry run does no work by design, so it is never "incomplete" in the
		// sense the exit code reports -- but it must still surface the counts.
		"dry run with failures": {
			externalrecompute.ReplayReport{DryRun: true, Rows: 2, UnreadableRows: 1}, true,
		},
	} {
		if got := testCase.report.Incomplete(); got != testCase.want {
			t.Fatalf("%s: Incomplete() = %v, want %v", name, got, testCase.want)
		}
	}
}
