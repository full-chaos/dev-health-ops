package httpapi

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/lifecycle"
)

// storeProbe stands in for the pool: it is listed before the API server, so the
// runtime stops it after the server, and it records whether a handler was still
// running when its shutdown began.
type storeProbe struct {
	handlerActive *atomic.Bool
	stopped       *atomic.Bool
	activeAtStop  *atomic.Bool
}

func (storeProbe) Name() string {
	return "store-probe"
}

func (storeProbe) Start(context.Context) error {
	return nil
}

func (p storeProbe) Shutdown(context.Context) error {
	p.activeAtStop.Store(p.handlerActive.Load())
	p.stopped.Store(true)
	return nil
}

// When the API drain budget runs out with a request in flight, the server closes
// the open connections, so request contexts end and the handler returns BEFORE the
// runtime goes on to the store. A handler that waits on its request context (here
// with a five-minute request timeout) must not be running when the runtime returns.
func TestRuntimeShutdownEndsHandlersBeforeTheStoreStops(t *testing.T) {
	var handlerActive, storeStopped, activeAtStoreStop atomic.Bool
	entered := make(chan struct{})
	options := testOptions(Route{
		Method:  http.MethodGet,
		Pattern: "/hold",
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			handlerActive.Store(true)
			defer handlerActive.Store(false)
			close(entered)
			select {
			case <-r.Context().Done():
			case <-time.After(30 * time.Second):
			}
		}),
	})
	options.RequestTimeout = 5 * time.Minute
	server, err := NewServer(options)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	runtime, err := lifecycle.New(lifecycle.Options{
		Logger:          slog.New(slog.NewJSONHandler(io.Discard, nil)),
		ShutdownTimeout: 400 * time.Millisecond,
		Components: []lifecycle.Component{
			storeProbe{handlerActive: &handlerActive, stopped: &storeStopped, activeAtStop: &activeAtStoreStop},
			server,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx) }()
	for deadline := time.Now().Add(5 * time.Second); server.Address() == "" || server.Address() == options.Address; {
		if time.Now().After(deadline) {
			t.Fatal("the server never bound")
		}
		time.Sleep(10 * time.Millisecond)
	}
	go func() {
		client := &http.Client{Timeout: time.Minute}
		if response, err := client.Get("http://" + server.Address() + "/hold"); err == nil {
			_ = response.Body.Close()
		}
	}()
	<-entered
	stop()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the runtime did not return")
	}
	if !storeStopped.Load() {
		t.Fatal("the store was never stopped")
	}
	if activeAtStoreStop.Load() {
		t.Error("a handler was still running when the store shutdown began")
	}
	if handlerActive.Load() {
		t.Error("a handler was still running after the runtime returned")
	}
}

// drainWatch is the API server as the runtime sees it: it reports when the runtime
// asks whether the handlers drained, and keeps the error its Shutdown returned.
type drainWatch struct {
	*Server
	asked       chan struct{}
	askOnce     sync.Once
	shutdownErr chan error
}

func (w *drainWatch) Shutdown(ctx context.Context) error {
	err := w.Server.Shutdown(ctx)
	w.shutdownErr <- err
	return err
}

func (w *drainWatch) Drained() <-chan struct{} {
	w.askOnce.Do(func() { close(w.asked) })
	return w.Server.Drained()
}

// storeStartProbe records, when its shutdown begins, whether a handler is still
// running and whether the global shutdown budget has already ended.
type storeStartProbe struct {
	handlerActive *atomic.Bool
	began         chan struct{}
	activeAtStop  *atomic.Bool
	expiredAtStop *atomic.Bool
}

func (storeStartProbe) Name() string                { return "store-probe" }
func (storeStartProbe) Start(context.Context) error { return nil }

func (p storeStartProbe) Shutdown(ctx context.Context) error {
	p.activeAtStop.Store(p.handlerActive.Load())
	p.expiredAtStop.Store(ctx.Err() != nil)
	close(p.began)
	return nil
}

// A handler that ignores its cancelled request context past the API server's own
// shutdown attempt keeps the store open: the runtime stops the store only after the
// handler ends (here: when the runtime waits for the drain), or after the global
// budget ends. The server's Shutdown names the breach. The handler ends on an
// event, never on a timer, so the order does not depend on the machine's load.
func TestRuntimeStopsTheStoreOnlyAfterASlowHandlerEnds(t *testing.T) {
	var handlerActive, activeAtStoreStop, expiredAtStoreStop atomic.Bool
	entered := make(chan struct{})
	storeBegan := make(chan struct{})
	watch := &drainWatch{asked: make(chan struct{}), shutdownErr: make(chan error, 1)}
	options := testOptions(Route{
		Method:  http.MethodGet,
		Pattern: "/hold",
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			handlerActive.Store(true)
			defer handlerActive.Store(false)
			close(entered)
			<-r.Context().Done()
			select {
			case <-watch.asked:
			case <-storeBegan:
			}
		}),
	})
	options.RequestTimeout = 5 * time.Minute
	server, err := NewServer(options)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	watch.Server = server
	runtime, err := lifecycle.New(lifecycle.Options{
		Logger:          slog.New(slog.NewJSONHandler(io.Discard, nil)),
		ShutdownTimeout: time.Second,
		Components: []lifecycle.Component{
			storeStartProbe{handlerActive: &handlerActive, began: storeBegan, activeAtStop: &activeAtStoreStop, expiredAtStop: &expiredAtStoreStop},
			watch,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx) }()
	for deadline := time.Now().Add(5 * time.Second); server.Address() == "" || server.Address() == options.Address; {
		if time.Now().After(deadline) {
			t.Fatal("the server never bound")
		}
		time.Sleep(10 * time.Millisecond)
	}
	go func() {
		client := &http.Client{Timeout: time.Minute}
		if response, err := client.Get("http://" + server.Address() + "/hold"); err == nil {
			_ = response.Body.Close()
		}
	}()
	<-entered
	stop()
	var runErr error
	select {
	case runErr = <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the runtime did not return")
	}
	select {
	case <-storeBegan:
	default:
		t.Fatal("the store was never stopped")
	}
	if activeAtStoreStop.Load() && !expiredAtStoreStop.Load() {
		t.Error("the store shutdown began while a handler was still running and the shutdown budget had not ended")
	}
	if shutdownErr := <-watch.shutdownErr; shutdownErr == nil || !strings.Contains(shutdownErr.Error(), "1 handlers still running") {
		t.Errorf("Shutdown returned %v at its deadline with a handler running, want an error that names the running handlers", shutdownErr)
	}
	if runErr == nil {
		t.Error("the runtime returned no error after a shutdown breach")
	}
}

// Drained stays open while a handler that outlived Shutdown still runs, and closes
// once it ends. The 50ms window can only miss a wrong close, never report one.
func TestDrainedStaysOpenWhileAHandlerRuns(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	options := testOptions(Route{
		Method:  http.MethodGet,
		Pattern: "/hold",
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			close(entered)
			<-release
		}),
	})
	options.RequestTimeout = 5 * time.Minute
	server, err := NewServer(options)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	if err := server.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	go func() {
		client := &http.Client{Timeout: time.Minute}
		if response, err := client.Get("http://" + server.Address() + "/hold"); err == nil {
			_ = response.Body.Close()
		}
	}()
	<-entered
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := server.Shutdown(ctx); err == nil || !strings.Contains(err.Error(), "1 handlers still running") {
		t.Fatalf("Shutdown returned %v with a handler running, want an error that names the running handlers", err)
	}
	select {
	case <-server.Drained():
		t.Fatal("Drained closed while a handler was still running")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	select {
	case <-server.Drained():
	case <-time.After(10 * time.Second):
		t.Fatal("Drained never closed after the handler ended")
	}
}
