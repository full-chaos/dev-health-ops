package lifecycle

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

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
