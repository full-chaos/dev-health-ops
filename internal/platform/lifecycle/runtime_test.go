package lifecycle

import (
	"context"
	"errors"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

// capturingHandler records emitted slog.Record values so tests can assert on
// structured attributes directly, without going through a text/JSON encoder.
type capturingHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *capturingHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *capturingHandler) Handle(_ context.Context, record slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, record.Clone())
	return nil
}

func (h *capturingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *capturingHandler) WithGroup(string) slog.Handler      { return h }

func (h *capturingHandler) find(message string) (slog.Record, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, record := range h.records {
		if record.Message == message {
			return record, true
		}
	}
	return slog.Record{}, false
}

func attrMap(record slog.Record) map[string]any {
	attrs := make(map[string]any, record.NumAttrs())
	record.Attrs(func(attr slog.Attr) bool {
		attrs[attr.Key] = attr.Value.Any()
		return true
	})
	return attrs
}

type recordingComponent struct {
	name        string
	record      func(string)
	startErr    error
	shutdownErr error
	failures    chan error
}

func (c *recordingComponent) Name() string { return c.name }

func (c *recordingComponent) Start(context.Context) error {
	c.record("start:" + c.name)
	return c.startErr
}

func (c *recordingComponent) Shutdown(ctx context.Context) error {
	if ctx.Err() != nil {
		return errors.New("shutdown received canceled context")
	}
	c.record("stop:" + c.name)
	return c.shutdownErr
}

func (c *recordingComponent) Errors() <-chan error { return c.failures }

type nonCooperativeComponent struct {
	name    string
	record  func(string)
	entered chan struct{}
	release chan struct{}
	exited  chan struct{}
}

type budgetedComponent struct {
	*recordingComponent
	budget   time.Duration
	deadline time.Time
}

func (component *budgetedComponent) ShutdownBudget() time.Duration { return component.budget }
func (component *budgetedComponent) Shutdown(ctx context.Context) error {
	deadline, ok := ctx.Deadline()
	if !ok {
		return errors.New("shutdown deadline is missing")
	}
	component.deadline = deadline
	return component.recordingComponent.Shutdown(ctx)
}

func (c *nonCooperativeComponent) Name() string { return c.name }

func (c *nonCooperativeComponent) Start(context.Context) error { return nil }

func (c *nonCooperativeComponent) Shutdown(context.Context) error {
	c.record("stop:" + c.name)
	close(c.entered)
	defer close(c.exited)
	<-c.release
	return nil
}

func TestRuntimeStartsInOrderAndShutsDownInReverseWithFreshContext(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	var events []string
	record := func(event string) {
		mu.Lock()
		defer mu.Unlock()
		events = append(events, event)
	}
	a := &recordingComponent{name: "a", record: record}
	b := &recordingComponent{name: "b", record: record}
	c := &recordingComponent{name: "c", record: record}
	runtime, err := New(Options{ShutdownTimeout: time.Second, Components: []Component{a, b, c}})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx) }()
	deadline := time.Now().Add(time.Second)
	for {
		mu.Lock()
		started := len(events) >= 3
		mu.Unlock()
		if started {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("components did not start")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}

	want := []string{"start:a", "start:b", "start:c", "stop:c", "stop:b", "stop:a"}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("unexpected lifecycle order: got %v want %v", events, want)
	}
}

func TestRuntimeCleansUpStartedComponentsAfterStartFailure(t *testing.T) {
	t.Parallel()

	var events []string
	record := func(event string) { events = append(events, event) }
	a := &recordingComponent{name: "a", record: record}
	b := &recordingComponent{name: "b", record: record, startErr: errors.New("boom")}
	runtime, err := New(Options{ShutdownTimeout: time.Second, Components: []Component{a, b}})
	if err != nil {
		t.Fatal(err)
	}

	err = runtime.Run(context.Background())
	if err == nil || !reflect.DeepEqual(events, []string{"start:a", "start:b", "stop:a"}) {
		t.Fatalf("unexpected start failure cleanup: err=%v events=%v", err, events)
	}
}

func TestRuntimeStopsAfterAsynchronousComponentFailure(t *testing.T) {
	t.Parallel()

	var events []string
	failures := make(chan error, 1)
	component := &recordingComponent{
		name:     "async",
		record:   func(event string) { events = append(events, event) },
		failures: failures,
	}
	runtime, err := New(Options{ShutdownTimeout: time.Second, Components: []Component{component}})
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() { done <- runtime.Run(context.Background()) }()
	failures <- errors.New("serve failed")
	if err := <-done; err == nil {
		t.Fatal("expected asynchronous failure")
	}
	if !reflect.DeepEqual(events, []string{"start:async", "stop:async"}) {
		t.Fatalf("unexpected events: %v", events)
	}
}

// TestRuntimeComponentFailureLogsComponentNameAndCause pins CHAOS-3906: the
// "runtime component failed" log line must carry which component failed (a
// bounded, safe identifier) and the underlying cause, not just a category.
func TestRuntimeComponentFailureLogsComponentNameAndCause(t *testing.T) {
	t.Parallel()

	handler := &capturingHandler{}
	logger := slog.New(handler)
	failures := make(chan error, 1)
	component := &recordingComponent{
		name:     "async",
		record:   func(string) {},
		failures: failures,
	}
	runtime, err := New(Options{Logger: logger, ShutdownTimeout: time.Second, Components: []Component{component}})
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() { done <- runtime.Run(context.Background()) }()
	cause := errors.New("serve failed")
	failures <- cause
	if err := <-done; err == nil {
		t.Fatal("expected asynchronous failure")
	}

	record, ok := handler.find("runtime component failed")
	if !ok {
		t.Fatalf("did not log a runtime component failure: %+v", handler.records)
	}
	attrs := attrMap(record)
	if got := attrs["error_category"]; got != "component_failure" {
		t.Fatalf("error_category = %v, want component_failure", got)
	}
	if got := attrs["component"]; got != "async" {
		t.Fatalf("component = %v, want %q", got, "async")
	}
	loggedErr, ok := attrs["error"].(error)
	if !ok || !errors.Is(loggedErr, cause) {
		t.Fatalf("error attribute = %#v, want to wrap %v", attrs["error"], cause)
	}
}

func TestShutdownBoundsNonCooperativeComponentsAndAttemptsAllInReverse(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	var events []string
	record := func(event string) {
		mu.Lock()
		defer mu.Unlock()
		events = append(events, event)
	}
	newComponent := func(name string) *nonCooperativeComponent {
		return &nonCooperativeComponent{
			name:    name,
			record:  record,
			entered: make(chan struct{}),
			release: make(chan struct{}),
			exited:  make(chan struct{}),
		}
	}
	a := newComponent("a")
	b := newComponent("b")
	c := newComponent("c")
	components := []*nonCooperativeComponent{a, b, c}
	t.Cleanup(func() {
		for _, component := range components {
			select {
			case <-component.release:
			default:
				close(component.release)
			}
			select {
			case <-component.exited:
			case <-time.After(time.Second):
				t.Errorf("component %s did not exit after release", component.name)
			}
		}
	})
	const shutdownTimeout = 90 * time.Millisecond
	runtime, err := New(Options{ShutdownTimeout: shutdownTimeout})
	if err != nil {
		t.Fatal(err)
	}

	startedAt := time.Now()
	err = runtime.shutdown(context.Background(), []Component{a, b, c})
	elapsed := time.Since(startedAt)
	if err == nil {
		t.Fatal("expected non-cooperative shutdowns to report deadline errors")
	}
	for _, name := range []string{"a", "b", "c"} {
		if !strings.Contains(err.Error(), "shutdown component "+name+": context deadline exceeded") {
			t.Fatalf("missing %s deadline error: %v", name, err)
		}
	}
	if elapsed > shutdownTimeout+100*time.Millisecond {
		t.Fatalf("shutdown exceeded hard upper bound: elapsed=%s timeout=%s", elapsed, shutdownTimeout)
	}
	for _, component := range components {
		select {
		case <-component.entered:
		default:
			t.Fatalf("component %s was not attempted", component.name)
		}
	}

	mu.Lock()
	gotEvents := append([]string(nil), events...)
	mu.Unlock()
	if want := []string{"stop:c", "stop:b", "stop:a"}; !reflect.DeepEqual(gotEvents, want) {
		t.Fatalf("unexpected shutdown attempt order: got %v want %v", gotEvents, want)
	}

	for _, component := range components {
		close(component.release)
		select {
		case <-component.exited:
		case <-time.After(time.Second):
			t.Fatalf("component %s did not exit after release", component.name)
		}
	}
}

func TestShutdownHonorsReviewedComponentBudget(t *testing.T) {
	t.Parallel()
	record := func(string) {}
	reserved := &budgetedComponent{
		recordingComponent: &recordingComponent{name: "reserved", record: record},
		budget:             80 * time.Millisecond,
	}
	// The tail component declares no budget of its own: it is the last attempt, so it receives what is
	// left of the global shutdown deadline, which makes its deadline the global bound.
	tail := &budgetedComponent{recordingComponent: &recordingComponent{name: "tail", record: record}}
	runtime, err := New(Options{ShutdownTimeout: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	// The deadline the component was given is compared with clock readings taken before and after
	// the whole shutdown, never with the time the component happened to start running (a loaded
	// -race runner delays that start by tens of ms: CHAOS-9938). The attempt context is created
	// after `before` and the shutdown deadline before `after`, so both bounds hold on any machine.
	before := time.Now()
	if err := runtime.shutdown(context.Background(), []Component{tail, reserved}); err != nil {
		t.Fatal(err)
	}
	after := time.Now()
	if got := reserved.deadline.Sub(before); got < 80*time.Millisecond {
		t.Fatalf("reserved component was given a deadline %s after the shutdown began, want at least its 80ms budget (an equal share of 100ms over two components is 50ms)", got)
	}
	// The global bound, both ways, on the tail's deadline (the shutdown deadline is set after `before`
	// and before `after`): a global timeout longer or shorter than the 100ms the runtime was given fails.
	if got := tail.deadline.Sub(before); got < 100*time.Millisecond {
		t.Fatalf("the last component's deadline is %s after the shutdown began, want the full 100ms global budget", got)
	}
	if got := tail.deadline.Sub(after); got > 100*time.Millisecond {
		t.Fatalf("the last component's deadline is %s after the shutdown ended, want within the 100ms global budget", got)
	}
	if reserved.deadline.After(tail.deadline) {
		t.Fatalf("the reserved component's deadline %s is after the global deadline %s", reserved.deadline, tail.deadline)
	}
}
