package joboutbox

import (
	"errors"
	"fmt"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
)

func TestDeferredProducerPolicyIsNarrowAndNonExecutable(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		descriptor jobruntime.Descriptor
		deferred   bool
		want       bool
	}{
		{
			name: "reviewed celery handoff",
			descriptor: jobruntime.Descriptor{
				MigrationState: "go_implemented", Route: "celery", RollbackRoute: "celery",
			},
			deferred: true,
			want:     true,
		},
		{
			name: "unreviewed celery handoff",
			descriptor: jobruntime.Descriptor{
				MigrationState: "contract_frozen", Route: "celery", RollbackRoute: "celery",
			},
			deferred: true,
		},
		{
			name: "active route through deferred API",
			descriptor: jobruntime.Descriptor{
				MigrationState: "go_default", Route: "river", RollbackRoute: "celery",
			},
			deferred: true,
		},
		{
			name: "active route through normal API",
			descriptor: jobruntime.Descriptor{
				MigrationState: "go_default", Route: "river", RollbackRoute: "celery",
			},
			want: true,
		},
		{
			name: "celery route through normal API",
			descriptor: jobruntime.Descriptor{
				MigrationState: "go_implemented", Route: "celery", RollbackRoute: "celery",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := descriptorAllowsPublish(test.descriptor, test.deferred); got != test.want {
				t.Fatalf("descriptorAllowsPublish() = %t, want %t", got, test.want)
			}
		})
	}
}

// TestIsPublishedTreatsOnlyTheTerminalDeliverySentinelAsSuccess pins the exact
// membership of IsPublished, ungated, so it is proven in CI where no Postgres
// container runs.
//
// Both directions are load-bearing and each fails a different way. Collapsing
// too MUCH (a contract rejection, an unavailable database) turns a real fault
// into a silent success at every publisher in the tree -- which is the same
// class of defect ErrDeliveryAlreadyTerminal exists to end. Collapsing too
// LITTLE turns an ordinary idempotent republish into a hard failure for the
// fixed-schedule, work-graph, daily-metrics, remaining-metrics and post-sync
// publishers, none of which hold a logger or own a repair path.
func TestIsPublishedTreatsOnlyTheTerminalDeliverySentinelAsSuccess(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name string
		err  error
		want bool
	}{
		{name: "no error", err: nil, want: true},
		{name: "terminal delivery", err: ErrDeliveryAlreadyTerminal, want: true},
		{
			name: "terminal delivery, wrapped with its evidence",
			err:  fmt.Errorf("%w: status=delivered river_job_id=127714", ErrDeliveryAlreadyTerminal),
			want: true,
		},
		{name: "contract rejected", err: ErrContractRejected},
		{name: "policy rejected", err: ErrPolicyRejected},
		{name: "unavailable", err: ErrUnavailable},
		{name: "lease lost", err: ErrLeaseLost},
		{name: "invalid configuration", err: ErrInvalidConfiguration},
		{name: "an unrelated error", err: errors.New("something else entirely")},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			if got := IsPublished(testCase.err); got != testCase.want {
				t.Fatalf("IsPublished(%v) = %v, want %v", testCase.err, got, testCase.want)
			}
		})
	}
}

// TestTerminalOutboxStatusListsOnlyTheUnreachableStatuses pins the positive
// list. 'pending' and 'claimed' are both still reachable by Repository's claim
// SQL, so a publish landing on one of them genuinely has a delivery coming and
// must report a plain success; naming a third status here would start warning
// on healthy republishes at every publisher in the tree.
//
// A status this function does not know is NON-terminal by default. That is the
// direction that fails quiet rather than the direction that turns healthy work
// into noise, and it is deliberate -- see the function's own comment.
func TestTerminalOutboxStatusListsOnlyTheUnreachableStatuses(t *testing.T) {
	t.Parallel()
	for status, want := range map[string]bool{
		"delivered":       true,
		"dead":            true,
		"pending":         false,
		"claimed":         false,
		"":                false,
		"DELIVERED":       false,
		"a_future_status": false,
	} {
		if got := terminalOutboxStatus(status); got != want {
			t.Fatalf("terminalOutboxStatus(%q) = %v, want %v", status, got, want)
		}
	}
}
