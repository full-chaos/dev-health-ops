package httpapi

import (
	"context"
	"io"
	"log/slog"
	"net/http"
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
