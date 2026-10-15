package aftercommit

import (
	"context"
	"testing"
)

func TestDeferQueuesUntilRunAndRunsOnceInOrder(t *testing.T) {
	ctx, hooks := WithHooks(context.Background())
	var order []int
	for i := 1; i <= 3; i++ {
		i := i
		if !Defer(ctx, func() { order = append(order, i) }) {
			t.Fatal("Defer reported not queued on a context that carries hooks")
		}
	}
	if len(order) != 0 {
		t.Fatalf("work ran before Run: %v", order)
	}
	hooks.Run()
	hooks.Run()
	if len(order) != 3 || order[0] != 1 || order[1] != 2 || order[2] != 3 {
		t.Fatalf("order after Run = %v, want [1 2 3] once", order)
	}
}

func TestDeferWithoutHooksReportsNotQueued(t *testing.T) {
	ran := false
	if Defer(context.Background(), func() { ran = true }) {
		t.Fatal("Defer queued on a context with no hooks")
	}
	if ran {
		t.Fatal("Defer ran the work itself")
	}
}

func TestWorkQueuedForARolledBackTransactionNeverRuns(t *testing.T) {
	ctx, hooks := WithHooks(context.Background())
	ran := false
	Defer(ctx, func() { ran = true })
	_ = hooks // the owner rolled back: Run is never called
	if ran {
		t.Fatal("queued work ran without Run")
	}
}

func TestRunOnNilHooksIsSafe(t *testing.T) {
	var hooks *Hooks
	hooks.Run()
}
