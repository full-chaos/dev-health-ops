package shell

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/platform/lifecycle"
)

// derivedTimeoutComponent stands in for the worker's river-workers component: it
// states the shutdown timeout the composition derived and records the deadline
// of the shutdown context the runtime gives it.
type derivedTimeoutComponent struct {
	required time.Duration
	started  chan struct{}
	stopped  chan time.Duration // time left on the shutdown context when Shutdown began
}

func (derivedTimeoutComponent) Name() string { return "derived-timeout" }
func (component derivedTimeoutComponent) Start(context.Context) error {
	close(component.started)
	return nil
}
func (component derivedTimeoutComponent) RequiredShutdownTimeout() time.Duration {
	return component.required
}

// The budget reserved for this component is huge, so the attempt context is the
// REMAINING runtime shutdown deadline: what Shutdown sees is the runtime's own
// timeout.
func (derivedTimeoutComponent) ShutdownBudget() time.Duration { return time.Hour }
func (component derivedTimeoutComponent) Shutdown(ctx context.Context) error {
	deadline, _ := ctx.Deadline()
	component.stopped <- time.Until(deadline)
	return nil
}

// TestRuntimeRunsOnTheShutdownTimeoutTheCompositionDerived (CHAOS-8783 r1 P1):
// with the shutdown-timeout flag UNSET the worker composition derives its grace
// from the selected queues, and the lifecycle runtime shell.Execute builds must
// run on that same value, not on the 30 s package default. Through the real
// shell.Execute (the function internal/workerservice/service.go calls).
//
// Plant: build the runtime from cfg.ShutdownTimeout again (as before): the
// shutdown context then has ~30 s left, not ~90 s.
func TestRuntimeRunsOnTheShutdownTimeoutTheCompositionDerived(t *testing.T) {
	component := derivedTimeoutComponent{
		required: 90 * time.Second,
		started:  make(chan struct{}), stopped: make(chan time.Duration, 1),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var stdout, stderr bytes.Buffer
	done := make(chan int, 1)
	go func() {
		done <- Execute(ctx, Spec{
			Service: "dev-health-worker",
			ConfigureDependencies: func(context.Context, config.Config, *health.Registry) ([]lifecycle.Component, error) {
				return []lifecycle.Component{component}, nil
			},
		}, nil, testLookup(map[string]string{"DEV_HEALTH_HTTP_ADDR": "127.0.0.1:0"}), IO{Stdout: &stdout, Stderr: &stderr})
	}()
	select {
	case <-component.started:
	case <-time.After(20 * time.Second):
		t.Fatalf("the component never started: %s %s", stdout.String(), stderr.String())
	}
	cancel() // the stop signal
	var left time.Duration
	select {
	case left = <-component.stopped:
	case <-time.After(20 * time.Second):
		t.Fatal("the component was never shut down")
	}
	if left < 85*time.Second || left > 91*time.Second {
		t.Fatalf("shutdown context had %s left, want ~90s (the derived timeout), not the 30s default", left)
	}
	if code := <-done; code != 0 {
		t.Fatalf("Execute = %d: %s %s", code, stdout.String(), stderr.String())
	}
}

func TestRuntimeShutdownTimeoutNeverShrinksTheConfiguredOne(t *testing.T) {
	small := derivedTimeoutComponent{required: 10 * time.Second}
	large := derivedTimeoutComponent{required: 90 * time.Second}
	for _, testCase := range []struct {
		name       string
		configured time.Duration
		components []lifecycle.Component
		want       time.Duration
	}{
		{"no source", 30 * time.Second, nil, 30 * time.Second},
		{"derived is larger", 30 * time.Second, []lifecycle.Component{large}, 90 * time.Second},
		{"configured is larger", 120 * time.Second, []lifecycle.Component{large}, 120 * time.Second},
		{"smaller derived is ignored", 30 * time.Second, []lifecycle.Component{small}, 30 * time.Second},
	} {
		if got := RuntimeShutdownTimeout(testCase.configured, testCase.components); got != testCase.want {
			t.Fatalf("%s: = %s, want %s", testCase.name, got, testCase.want)
		}
	}
}
