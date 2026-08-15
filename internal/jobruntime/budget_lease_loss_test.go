package jobruntime

import (
	"context"
	"testing"
	"time"
)

type testLossLease struct {
	lost chan struct{}
}

func (lease *testLossLease) Release() {}

func (lease *testLossLease) Lost() <-chan struct{} { return lease.lost }

func TestWithBudgetLeaseLossCancelsHandlerContext(t *testing.T) {
	lost := make(chan struct{})
	ctx, cancel := withBudgetLeaseLoss(context.Background(), &testLossLease{lost: lost})
	defer cancel()
	close(lost)
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("handler context was not canceled after lease loss")
	}
}

func TestWithBudgetLeaseLossKeepsOrdinaryLeaseContext(t *testing.T) {
	parent, cancelParent := context.WithCancel(context.Background())
	defer cancelParent()
	ctx, cancel := withBudgetLeaseLoss(parent, &testBudgetLease{})
	defer cancel()
	select {
	case <-ctx.Done():
		t.Fatal("ordinary lease context canceled without parent cancellation")
	default:
	}
	cancelParent()
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("ordinary lease context did not follow parent cancellation")
	}
}

type testBudgetLease struct{}

func (testBudgetLease) Release() {}
