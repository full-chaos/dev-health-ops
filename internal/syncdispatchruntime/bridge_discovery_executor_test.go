package syncdispatchruntime

import (
	"context"
	"errors"
	"testing"
)

type fakeReferenceDiscoveryPopulator struct {
	gotOrgID, gotRunID string
	summary            map[string]any
	err                error
}

func (populator *fakeReferenceDiscoveryPopulator) PopulateReferenceDiscovery(_ context.Context, orgID, runID string) (map[string]any, error) {
	populator.gotOrgID, populator.gotRunID = orgID, runID
	return populator.summary, populator.err
}

// TestBridgeDiscoveryExecutorDelegatesToThePopulateCall pins that
// BridgeDiscoveryExecutor.Discover is a thin, faithful pass-through: the
// exact (orgID, runID) it was given reach the populator, and its
// (summary, error) return is relayed unchanged in either direction.
func TestBridgeDiscoveryExecutorDelegatesToThePopulateCall(t *testing.T) {
	populator := &fakeReferenceDiscoveryPopulator{summary: map[string]any{"reference_team_keys": []string{"ENG"}}}
	executor := &BridgeDiscoveryExecutor{populator: populator}
	summary, err := executor.Discover(context.Background(), testOrg, testRun)
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if populator.gotOrgID != testOrg || populator.gotRunID != testRun {
		t.Fatalf("populator saw orgID=%q runID=%q want=%q,%q", populator.gotOrgID, populator.gotRunID, testOrg, testRun)
	}
	if len(summary) != 1 {
		t.Fatalf("summary=%#v", summary)
	}

	populator.err = errors.New("populate failed")
	if _, err := executor.Discover(context.Background(), testOrg, testRun); !errors.Is(err, populator.err) {
		t.Fatalf("Discover error=%v want=%v", err, populator.err)
	}
}

// TestNewBridgeDiscoveryExecutorRejectsANilBridge pins the constructor's
// guard -- a nil *HTTPBridge must never silently produce an executor that
// panics on first use.
func TestNewBridgeDiscoveryExecutorRejectsANilBridge(t *testing.T) {
	if _, err := NewBridgeDiscoveryExecutor(nil); !errors.Is(err, ErrInvalidBridge) {
		t.Fatalf("error=%v want=%v", err, ErrInvalidBridge)
	}
}

// TestBridgeDiscoveryExecutorFailsClosedWhenUnconstructed pins the same
// nil-safety convention every other worker/service type in this package
// uses (e.g. dispatchWorker.Work, NativeReferenceDiscoveryService.Discover):
// a zero-value or nil-populator executor must return an error, not panic.
func TestBridgeDiscoveryExecutorFailsClosedWhenUnconstructed(t *testing.T) {
	var nilExecutor *BridgeDiscoveryExecutor
	if _, err := nilExecutor.Discover(context.Background(), testOrg, testRun); !errors.Is(err, ErrReferenceDiscoveryUnavailable) {
		t.Fatalf("nil executor error=%v want=%v", err, ErrReferenceDiscoveryUnavailable)
	}
	zeroExecutor := &BridgeDiscoveryExecutor{}
	if _, err := zeroExecutor.Discover(context.Background(), testOrg, testRun); !errors.Is(err, ErrReferenceDiscoveryUnavailable) {
		t.Fatalf("zero-value executor error=%v want=%v", err, ErrReferenceDiscoveryUnavailable)
	}
}
