package lifecycle

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"
)

// releaseLog records, in order, which components started, shut down or were released.
type releaseLog struct {
	mu     sync.Mutex
	events []string
}

func (l *releaseLog) add(event string) {
	l.mu.Lock()
	l.events = append(l.events, event)
	l.mu.Unlock()
}

func (l *releaseLog) all() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.events...)
}

type releasingComponent struct {
	name     string
	log      *releaseLog
	startErr error
	started  chan struct{}
}

func (c *releasingComponent) Name() string { return c.name }
func (c *releasingComponent) Start(context.Context) error {
	c.log.add("start " + c.name)
	if c.started != nil {
		close(c.started)
	}
	return c.startErr
}
func (c *releasingComponent) Shutdown(context.Context) error {
	c.log.add("shutdown " + c.name)
	return nil
}
func (c *releasingComponent) Release() { c.log.add("release " + c.name) }

// CHAOS-8654: a component built before Run (a database whose readiness checks are already warmed)
// is never shut down when Run returns before starting it. Run releases each component it did not
// start, last first, after it shut down the ones it did start.
func TestRunReleasesTheComponentsItNeverStarted(t *testing.T) {
	failed := errors.New("bind refused")
	for _, tc := range []struct {
		name      string
		cancelled bool
		failAt    int
		want      []string
	}{
		{name: "cancelled_before_run", cancelled: true, failAt: -1, want: []string{"release c", "release b", "release a"}},
		{name: "first_start_fails", failAt: 0, want: []string{"start a", "release c", "release b", "release a"}},
		{name: "middle_start_fails", failAt: 1, want: []string{"start a", "start b", "shutdown a", "release c", "release b"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			log := &releaseLog{}
			var components []Component
			for index, name := range []string{"a", "b", "c"} {
				component := &releasingComponent{name: name, log: log}
				if index == tc.failAt {
					component.startErr = failed
				}
				components = append(components, component)
			}
			runtime, err := New(Options{ShutdownTimeout: time.Second, Components: components})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.cancelled {
				cancel()
			}
			if err := runtime.Run(ctx); err == nil {
				t.Fatal("Run() = nil, want the cancellation or the start error")
			}
			if got := log.all(); !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("events = %q, want %q", got, tc.want)
			}
		})
	}
}

// A component Run started and shut down is not released: its Shutdown owns its resources.
func TestRunDoesNotReleaseTheComponentsItShutDown(t *testing.T) {
	log := &releaseLog{}
	started := make(chan struct{})
	components := []Component{&releasingComponent{name: "a", log: log}, &releasingComponent{name: "b", log: log, started: started}}
	runtime, err := New(Options{ShutdownTimeout: time.Second, Components: components})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx) }()
	<-started
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if got, want := log.all(), []string{"start a", "start b", "shutdown b", "shutdown a"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("events = %q, want %q", got, want)
	}
}

func TestReleaseAllReleasesLastFirstAndSkipsOtherComponents(t *testing.T) {
	log := &releaseLog{}
	components := []Component{
		&releasingComponent{name: "a", log: log},
		&recordingComponent{name: "plain"},
		&releasingComponent{name: "b", log: log},
	}
	ReleaseAll(components)
	if got, want := log.all(), []string{"release b", "release a"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("events = %q, want %q", got, want)
	}
}
